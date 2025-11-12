import json
import logging
import select
import time
from utils import mcast_receiver, mcast_sender, RndGeq


class Proposer:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.client_r = mcast_receiver(config["proposers"])
        self.s = mcast_sender()

        self.num_acceptors = 3  # as specified in the requirements
        self.majority = (self.num_acceptors // 2) + 1

        # Proposal number (avoids collisions between proposers)
        self.round_counter = 0

        # keep track of the instance number that needs to be proposed.
        self.next_instance = 0

        # Buffer for client values during Paxos message processing
        # We decided to have a buffer for client value to not lose value if 
        # proposer is still trying to decide the value but during the process new values are received.
        self.pending_client_values = []
        
        # Track values we've learned were already decided to avoid duplicates
        self.known_decided_values = set()

    def run(self):
        logging.info(f"-> proposer {self.id}")
        
        while True:
            # get next value from client
            if self.pending_client_values:
                msg_data = self.pending_client_values.pop(0)
            else:
                msg, _ = self.client_r.recvfrom(2**16)
                data = json.loads(msg.decode())
                 
                # To update the proposer state correctly we rely on run_full_paxos function
                if data.get('type') in ['1B', '2B', 'DECISION']:
                    continue
                
                msg_data = data
            
            # Extract value
            value = msg_data.get('value')
            
            # Skip if this value was already decided
            if value in self.known_decided_values:
                pass
                continue
            
            # Proposers do NOT maintain persistent state about past proposals.
            # All information about accepted values comes from acceptors via 1B messages.
            
            # Loop until this value is decided
            decided = False
            while not decided:
                # Run Paxos on the current instance
                success, decided_value = self.run_full_paxos(value)
                
                if success:
                    # Mark decided value as known
                    self.known_decided_values.add(decided_value)
                    
                    if decided_value == value:
                        # Our value was decided
                        decided = True
                        logging.info(f"Value '{value}' decided at instance {self.next_instance}")
                    else:
                        # Different value decided, will retry at next instance
                        pass
                    
                    self.next_instance += 1
                else:
                    pass

    def _extract_accepted_history(self, data):
        if 'accepted_values' in data:
            for inst_num, val in data['accepted_values'].items():
                inst_num = int(inst_num)
                if val not in self.known_decided_values:
                    logging.info(f"Learned value '{val}' was accepted at instance {inst_num}")
                    self.known_decided_values.add(val)

    def run_full_paxos(self, original_value):
        self.round_counter += 1
        self.c_rnd = {'bal': self.round_counter, 'pid': self.id} 
        logging.info(f"Running Paxos for instance {self.next_instance} with round {self.c_rnd}")
        
        # HASE 1: (1A)
        prepare_msg = {
            'type': '1A',
            'inst': self.next_instance,
            'rnd': self.c_rnd
        }
        self.s.sendto(json.dumps(prepare_msg).encode(), self.config["acceptors"])
        
        promises = {}
        timeout = 0.25
        start_time = time.time()
        
        while len(promises) < self.majority:

            #=================TIMEOUT HANDLING WITH SELECT()=================
            # This need to be analyzed better, at the moment we use this approach:
            # source: https://stackoverflow.com/questions/19410651/how-to-use-select-when-network-socket-is-always-ready-to-read
            # https://stackoverflow.com/questions/12625224/how-is-select-alerted-to-an-fd-becoming-ready
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                logging.warning(f"Phase 1 timeout - {len(promises)}/{self.majority} promises")
                return (False, None)
            
            remaining_time = timeout - elapsed
            select_timeout = min(0.1, remaining_time)
            ready, _, _ = select.select([self.client_r], [], [], select_timeout)
            if not ready:
                continue
            #=================TIMEOUT HANDLING WITH SELECT()=================

            # So, if the socket is ready we can read from it
            # (ready contains sockets with data to read)
            for sock in ready:
                response, _ = sock.recvfrom(2**16)
                data = json.loads(response.decode())
                
                # If doesn't have type, it's a client value to buffer
                if 'type' not in data:
                    self.pending_client_values.append(data)
                    continue
                
                if (data.get('inst') == self.next_instance and 
                    data.get('type') == '1B' and 
                    data.get('rnd') == self.c_rnd):
                    
                    # Extract accepted values history from the promise 
                    self._extract_accepted_history(data)
                    aid = data.get('aid', len(promises))
                    promises[aid] = data
                    
                elif data.get('type') == '1B' and data.get('inst') == self.next_instance:
                    received_rnd = data.get('rnd')
                    if received_rnd and RndGeq(received_rnd, self.c_rnd) and received_rnd != self.c_rnd:
                        logging.warning(f"Preempted in Phase 1 by round {received_rnd}")
                        return (False, None)
        
        logging.info(f"Phase 1 complete for instance {self.next_instance}")
        
        # Determine value to propose using RndGeq
        max_v_rnd = {'bal': 0, 'pid': 0}
        value_to_propose = original_value
        
        # Find the highest v_rnd among promises
        for promise in promises.values():
            v_rnd = promise.get('v_rnd', {'bal': 0, 'pid': 0})
            if RndGeq(v_rnd, max_v_rnd):
                max_v_rnd = v_rnd
                if v_rnd.get('bal', 0) > 0:
                    value_to_propose = promise.get('v_val')
        
        # PHASE 2: (2A)
        propose_msg = {
            'type': '2A',
            'inst': self.next_instance,
            'rnd': self.c_rnd,
            'val': value_to_propose
        }
        self.s.sendto(json.dumps(propose_msg).encode(), self.config["acceptors"])
        
        acceptances = {}
        timeout = 0.25
        start_time = time.time()
        
        while len(acceptances) < self.majority:
            #=================TIMEOUT HANDLING WITH SELECT()=================
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                logging.warning(f"Phase 2 timeout - {len(acceptances)}/{self.majority} acceptances")
                return (False, None)
            
            remaining_time = timeout - elapsed
            select_timeout = min(0.1, remaining_time)
            ready, _, _ = select.select([self.client_r], [], [], select_timeout)
            if not ready:
                continue
            #=================TIMEOUT HANDLING WITH SELECT()=================

            for sock in ready:
                response, _ = sock.recvfrom(2**16)
                data = json.loads(response.decode())
                
                if 'type' not in data:
                    self.pending_client_values.append(data)
                    continue
                
                if (data.get('inst') == self.next_instance and 
                    data.get('type') == '2B' and 
                    data.get('v_rnd') == self.c_rnd):
                    
                    self._extract_accepted_history(data)
                    aid = data.get('aid', len(acceptances))
                    acceptances[aid] = data
                    
                elif data.get('type') == '2B' and data.get('inst') == self.next_instance:
                    received_v_rnd = data.get('v_rnd')
                    if received_v_rnd and RndGeq(received_v_rnd, self.c_rnd) and received_v_rnd != self.c_rnd:
                        logging.warning(f"Preempted in Phase 2 by round {received_v_rnd}")
                        return (False, None)
        
        # DECIDE
        decision_msg = {
            'type': 'DECISION',
            'inst': self.next_instance,
            'val': value_to_propose
        }
        self.s.sendto(json.dumps(decision_msg).encode(), self.config["learners"])
        
        logging.info(f"DECIDED instance {self.next_instance} = '{value_to_propose}'")
        
        return (True, value_to_propose)

