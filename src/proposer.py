import logging
import json
import select  # We need the select module for non-blocking reads
from utils import mcast_receiver, mcast_sender

NUM_PROPOSERS = 2
NUM_ACCEPTORS = 3

class Proposer:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        # Socket for receiving values from clients
        self.client_r = mcast_receiver(config["proposers"])
        # Socket for synchronizing state by listening to decisions
        self.learner_r = mcast_receiver(config["learners"])
        self.s = mcast_sender()
        
        self.majority = (NUM_ACCEPTORS // 2) + 1
        # This counter tracks the next known available instance
        self.next_instance = 0
        self.c_rnd = self.id - 1
        # Buffer for client values that arrive while processing Paxos messages
        self.pending_client_values = []

    def sync_instance(self):
        """Non-blockingly read all pending decisions to update our instance counter."""
        while True:
            # Poll the learner socket with a zero timeout.
            ready, _, _ = select.select([self.learner_r], [], [], 0.0)
            if not ready:
                break  # No more pending decision messages
            
            msg, _ = self.learner_r.recvfrom(2**16)
            try:
                data = json.loads(msg.decode())
                if data.get('type') == 'DECISION':
                    # A decision was made. Ensure our next instance is ahead of it.
                    self.next_instance = max(self.next_instance, data['inst'] + 1)
            except (json.JSONDecodeError, KeyError):
                continue

    def run(self):
        logging.info(f"-> proposer {self.id}")
        while True:
            # 1. Get next client value (from buffer or socket)
            if self.pending_client_values:
                original_value = self.pending_client_values.pop(0)
            else:
                msg, _ = self.client_r.recvfrom(2**16)
                
                try:
                    # Ignore internal Paxos messages that other proposers are sending
                    data = json.loads(msg.decode())
                    if isinstance(data, dict) and 'type' in data:
                        continue
                except json.JSONDecodeError:
                    pass # This is a client value

                original_value = msg.decode().strip()

            # 2. Persistently try to get this value decided.
            my_value_decided = False
            while not my_value_decided:
                # CRITICAL: Before every attempt, sync with the global state.
                self.sync_instance()
                logging.info(f"Received '{original_value}'. Attempting to decide for instance {self.next_instance}.")

                self.c_rnd += NUM_PROPOSERS
                
                # --- Phase 1: Prepare ---
                promises = {}
                prepare_msg = {'type': 'PREPARE', 'inst': self.next_instance, 'rnd': self.c_rnd}
                self.s.sendto(json.dumps(prepare_msg).encode(), self.config["acceptors"])
                
                # Simplified wait loop for promises. A full implementation would use select here too.
                # For this project, a blocking wait is acceptable as timeouts handle livelocks.
                while len(promises) < self.majority:
                    response, _ = self.client_r.recvfrom(2**16)
                    try:
                        data = json.loads(response.decode())
                        if not isinstance(data, dict):
                            # This is a client value - buffer it for later
                            self.pending_client_values.append(response.decode().strip())
                            continue
                        if data.get('inst') == self.next_instance and data.get('type') == 'PROMISE' and data.get('rnd') == self.c_rnd:
                            promises[data['aid']] = data
                        elif data.get('rnd', data.get('v_rnd', 0)) > self.c_rnd:
                            break # Preempted
                    except (json.JSONDecodeError, KeyError):
                        # Raw client value (not JSON) - buffer it
                        self.pending_client_values.append(response.decode().strip())
                        continue

                if len(promises) < self.majority: continue # Restart round with higher number

                # --- Phase 2: Propose ---
                best_promise = max(promises.values(), key=lambda p: p['v_rnd'])
                value_to_propose = original_value
                if best_promise['v_rnd'] > 0:
                    value_to_propose = best_promise['v_val']

                acceptances = {}
                propose_msg = {'type': 'PROPOSE', 'inst': self.next_instance, 'rnd': self.c_rnd, 'val': value_to_propose}
                self.s.sendto(json.dumps(propose_msg).encode(), self.config["acceptors"])

                while len(acceptances) < self.majority:
                    response, _ = self.client_r.recvfrom(2**16)
                    try:
                        data = json.loads(response.decode())
                        if not isinstance(data, dict):
                            # This is a client value - buffer it for later
                            self.pending_client_values.append(response.decode().strip())
                            continue
                        if data.get('inst') == self.next_instance and data.get('type') == 'ACCEPTED' and data.get('v_rnd') == self.c_rnd:
                            acceptances[data['aid']] = data
                        elif data.get('rnd', data.get('v_rnd', 0)) > self.c_rnd:
                            break # Preempted
                    except (json.JSONDecodeError, KeyError):
                        # Raw client value (not JSON) - buffer it
                        self.pending_client_values.append(response.decode().strip())
                        continue
                
                if len(acceptances) < self.majority: continue # Restart round

                # --- Phase 3: Decide ---
                decision_msg = {'type': 'DECISION', 'inst': self.next_instance, 'val': value_to_propose}
                logging.info(f"Decided instance {self.next_instance}: value '{value_to_propose}'")
                self.s.sendto(json.dumps(decision_msg).encode(), self.config["learners"])
                
                # Increment next_instance since we just decided this one
                self.next_instance += 1
                
                # Check if our original value was the one decided.
                if value_to_propose == original_value:
                    my_value_decided = True # Our mission is complete.
                else:
                    # We helped another proposer. We must retry our value in the next instance.
                    logging.warning(f"Helped decide '{value_to_propose}', but my value was '{original_value}'. Retrying.")