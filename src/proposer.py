import json
import logging
import select
import time
from utils import mcast_receiver, mcast_sender


class Proposer:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.client_r = mcast_receiver(config["proposers"])
        self.s = mcast_sender()

        self.num_acceptors = 3  # as specified in the requirements
        self.majority = (self.num_acceptors // 2) + 1

        # Proposal number (avoids collisions between proposers)
        # Use counter * large_number + proposer_id to ensure uniqueness
        self.round_counter = 0
        self.c_rnd = self.id  # Start with proposer_id (P1:1, P2:2, etc.)

        # Cache of current instance (can be lost, it's just cache!)
        # Proposers do NOT store information about past accepted values.
        # All accepted values are stored ONLY in acceptors (self.instances).
        # Proposers learn about accepted values through 1B (PROMISE) messages from acceptors.
        self.next_instance = 0

        # Buffer for client values during Paxos message processing
        # We decided to have a buffer for client value to not lose value if 
        # proposer is still trying to decide the value but during the process new values are received.
        self.pending_client_values = []

    def run(self):
        logging.info(f"-> proposer {self.id}")
        
        while True:
            # 1. Get next value from client
            if self.pending_client_values:
                msg_data = self.pending_client_values.pop(0)
            else:
                msg, _ = self.client_r.recvfrom(2**16)
                data = json.loads(msg.decode())
                
                # Skip Paxos internal messages (1B/2B) - they're handled
                # in run_full_paxos when we're actively waiting
                if data.get('type') in ['1B', '2B']:
                    continue
                
                msg_data = data
            
            # Extract value
            value = msg_data['value']
            
            # Proposers do NOT maintain persistent state about past proposals.
            # All information about accepted values comes from acceptors via 1B messages.
            
            # Loop until this value is decided
            decided = False
            while not decided:
                logging.info(f"Attempting to decide '{value}' for instance {self.next_instance}")
                
                # Run full Paxos
                success, decided_value = self.run_full_paxos(value)
                
                if success:
                    if decided_value == value:
                        # OUR value was decided (either we decided it, or it was already decided)
                        decided = True
                        self.next_instance += 1
                        logging.info(f"✓ Value '{value}' is decided at instance {self.next_instance - 1}")
                    else:
                        # We helped decide someone else's value
                        logging.warning(f"Helped decide '{decided_value}', retrying our value")
                        self.next_instance += 1
                        # Continue loop to decide our value
                else:
                    # Failed - retry with new round
                    logging.warning(f"Failed to decide, retrying...")

    def run_full_paxos(self, original_value):
        """
        Execute full 2-phase Paxos for current instance.
        
        Returns: (success, decided_value)
        """
        # Generate new round number: counter * 1000 + proposer_id
        # This ensures uniqueness without knowing number of proposers
        self.round_counter += 1
        self.c_rnd = self.round_counter * 1000 + self.id
        logging.info(f"Running full Paxos for instance {self.next_instance} with round {self.c_rnd}")
        
        # --- PHASE 1: PREPARE (1A) ---
        prepare_msg = {
            'type': '1A',
            'inst': self.next_instance,
            'rnd': self.c_rnd
        }
        self.s.sendto(json.dumps(prepare_msg).encode(), self.config["acceptors"])
        
        promises = {}
        timeout = 0.5  # Timeout in seconds
        start_time = time.time()
        
        while len(promises) < self.majority:
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                logging.warning(f"Phase 1 timeout after {elapsed:.3f}s - only {len(promises)}/{self.majority} promises received")
                return (False, None)
            
            # Use smaller select timeout to check elapsed time more frequently
            remaining_time = timeout - elapsed
            select_timeout = min(0.1, remaining_time)
            ready, _, _ = select.select([self.client_r], [], [], select_timeout)
            if not ready:
                continue
            
            for sock in ready:
                response, _ = sock.recvfrom(2**16)
                data = json.loads(response.decode())
                
                if 'type' not in data:
                    # Client value, buffer it
                    self.pending_client_values.append(data)
                    continue
                
                if (data.get('inst') == self.next_instance and 
                    data.get('type') == '1B' and 
                    data.get('rnd') == self.c_rnd):
                    promises[data.get('aid')] = data
                elif data.get('rnd', 0) > self.c_rnd:
                    # Preempted
                    logging.warning(f"Preempted in Phase 1 by round {data.get('rnd')}")
                    return (False, None)
                
                elif data.get('type') == 'DECISION':
                    # Sync with decisions
                    inst = data.get('inst')
                    self.next_instance = max(self.next_instance, inst + 1)
        
        # SUCCESS Phase 1
        logging.info(f"✓ Prepared round {self.c_rnd} for instance {self.next_instance}")
        
        # Determine value to propose (inherit if necessary)
        best_promise = max(promises.values(), key=lambda p: p.get('v_rnd', 0))
        value_to_propose = original_value
        
        if best_promise.get('v_rnd', 0) > 0:
            # MUST inherit already accepted value
            value_to_propose = best_promise.get('v_val')
            logging.info(f"Inheriting value '{value_to_propose}' from promise")
            
            # Check in Phase 1: If we inherit our own value, it was already accepted
            # This means it's likely already decided (or will be decided soon)
            if value_to_propose == original_value:
                logging.info(f"Value '{original_value}' was already accepted by acceptors at instance {self.next_instance}")
                # Continue to Phase 2 to confirm it's decided (or decide it if not yet)
        
        # --- PHASE 2: PROPOSE (2A) ---
        propose_msg = {
            'type': '2A',
            'inst': self.next_instance,
            'rnd': self.c_rnd,
            'val': value_to_propose
        }
        self.s.sendto(json.dumps(propose_msg).encode(), self.config["acceptors"])
        
        acceptances = {}
        timeout = 0.5  # Timeout in seconds
        start_time = time.time()
        
        while len(acceptances) < self.majority:
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                logging.warning(f"Phase 2 timeout after {elapsed:.3f}s - only {len(acceptances)}/{self.majority} acceptances received")
                return (False, None)
            
            # Use smaller select timeout to check elapsed time more frequently
            remaining_time = timeout - elapsed
            select_timeout = min(0.1, remaining_time)
            ready, _, _ = select.select([self.client_r], [], [], select_timeout)
            if not ready:
                continue
            
            for sock in ready:
                response, _ = sock.recvfrom(2**16)
                data = json.loads(response.decode())
                
                if 'type' not in data:
                    # Client value, buffer it
                    self.pending_client_values.append(data)
                    continue
                
                if (data.get('inst') == self.next_instance and 
                    data.get('type') == '2B' and 
                    data.get('v_rnd') == self.c_rnd):
                    acceptances[data.get('aid')] = data
                elif data.get('v_rnd', 0) > self.c_rnd:
                    # Preempted
                    logging.warning(f"Preempted in Phase 2 by round {data.get('v_rnd')}")
                    return (False, None)
                
                elif data.get('type') == 'DECISION':
                    # Sync with decisions
                    inst = data.get('inst')
                    self.next_instance = max(self.next_instance, inst + 1)
        
        # --- PHASE 3: DECIDE ---
        decision_msg = {
            'type': 'DECISION',
            'inst': self.next_instance,
            'val': value_to_propose
        }
        self.s.sendto(json.dumps(decision_msg).encode(), self.config["learners"])
        
        logging.info(f"DECIDED instance {self.next_instance} = '{value_to_propose}'")
        
        return (True, value_to_propose)

