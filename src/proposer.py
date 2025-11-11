import json
import logging
import select
import time
from utils import mcast_receiver, mcast_sender, decode_message


class Proposer:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.client_r = mcast_receiver(config["proposers"])
        self.s = mcast_sender()

        self.num_acceptors = 3  # as specified in the requirements
        self.majority = (self.num_acceptors // 2) + 1
        self.num_proposers = 2  # Typically 2 proposers

        # Proposal number (avoids collisions between proposers)
        # Start with id-1, then increment by NUM_PROPOSERS
        self.c_rnd = self.id - 1  # P1:1, P2:2, then increment by NUM_PROPOSERS

        # Cache of current instance (can be lost, it's just cache!)
        self.next_instance = 0

        # Buffer for client values during Paxos message processing
        self.pending_client_values = []

        # MULTIPAXOS OPTIMIZATION (fast path)
        self.prepared_rnd = None  # Round with which we did Phase 1
        self.prepared_since_inst = None  # From which instance it's valid

        # ANTI-DUPLICATION (CRITICAL!)
        self.proposed_values = set()  # Values already proposed (for integrity)

    def run(self):
        logging.info(f"-> proposer {self.id}")
        
        while True:
            # 1. Get next value from client
            if self.pending_client_values:
                msg_data = self.pending_client_values.pop(0)
            else:
                msg, _ = self.client_r.recvfrom(2**16)
                
                try:
                    data = json.loads(msg.decode())
                    
                    # Skip Paxos internal messages (PROMISE/ACCEPTED) - they're handled
                    # in try_fast_path/run_full_paxos when we're actively waiting
                    if isinstance(data, dict) and data.get('type') in ['PROMISE', 'ACCEPTED', 'REJECT']:
                        # Store for later processing if needed, or just skip
                        continue
                    
                    msg_data = data
                except json.JSONDecodeError:
                    # Raw message (backward compatibility with M1)
                    msg_data = {'value': msg.decode().strip()}
            
            # Extract value and ID
            value = msg_data.get('value')
            if value is None:
                continue
            
            client_id = msg_data.get('client_id')
            seq_num = msg_data.get('seq_num')
            
            # 2. INTEGRITY CHECK: Verify duplicates
            value_id = (client_id, seq_num) if client_id is not None else value
            
            if value_id in self.proposed_values:
                logging.warning(f"Value {value_id} already proposed, skipping")
                continue
            
            self.proposed_values.add(value_id)
            
            # 3. Loop until this value is decided
            decided = False
            while not decided:
                # Sync with decisions
                self.sync_instance()
                
                logging.info(f"Attempting to decide '{value}' for instance {self.next_instance}")
                
                # Try to propose the value
                if self.can_use_fast_path():
                    success, decided_value = self.try_fast_path(value)
                else:
                    success, decided_value = self.run_full_paxos(value)
                
                if success:
                    if decided_value == value:
                        # OUR value was decided
                        decided = True
                        self.next_instance += 1
                        logging.info(f"✓ Decided '{value}' at instance {self.next_instance - 1}")
                    else:
                        # We helped decide someone else's value
                        logging.warning(f"Helped decide '{decided_value}', retrying our value")
                        self.next_instance += 1
                        # Continue loop to decide our value
                else:
                    # Failed - lost prepared status
                    self.prepared_rnd = None
                    self.prepared_since_inst = None
                    logging.warning(f"Failed to decide, retrying...")

    def sync_instance(self):
        """
        Sync with decisions already made.
        For now, we track our own decisions. In a more sophisticated implementation,
        we could infer decisions from PROMISE messages (if v_rnd > 0, instance was decided).
        """
        # We track next_instance ourselves when we decide
        # This is sufficient for basic MultiPaxos
        pass

    def can_use_fast_path(self):
        """
        Check if we can use fast path (skip Phase 1).
        """
        return (self.prepared_rnd is not None and 
                self.next_instance >= self.prepared_since_inst)

    def try_fast_path(self, value):
        """
        MULTIPAXOS OPTIMIZATION: Skip Phase 1, go directly to Phase 2.
        Use the prepared_rnd we already obtained.
        
        Returns: (success, decided_value)
        """
        logging.debug(f"Using fast path for instance {self.next_instance}")
        
        # Phase 2: PROPOSE
        propose_msg = {
            'type': 'PROPOSE',
            'inst': self.next_instance,
            'rnd': self.prepared_rnd,
            'val': value
        }
        self.s.sendto(json.dumps(propose_msg).encode(), self.config["acceptors"])
        
        # Wait for ACCEPTED from majority
        acceptances = {}
        timeout = 0.5
        start_time = time.time()
        
        while len(acceptances) < self.majority and time.time() - start_time < timeout:
            ready, _, _ = select.select([self.client_r], [], [], 0.1)
            if not ready:
                continue
            
            for sock in ready:
                response, _ = sock.recvfrom(2**16)
                try:
                    data = json.loads(response.decode())
                    
                    if not isinstance(data, dict) or 'type' not in data:
                        # Client value, buffer it
                        try:
                            client_data = json.loads(response.decode())
                            self.pending_client_values.append(client_data)
                        except:
                            self.pending_client_values.append({'value': response.decode().strip()})
                        continue
                    
                    if data.get('type') == 'ACCEPTED' and data.get('inst') == self.next_instance:
                        if data.get('v_rnd') == self.prepared_rnd:
                            acceptances[data.get('aid')] = data
                        elif data.get('v_rnd', 0) > self.prepared_rnd:
                            # Preempted!
                            logging.warning(f"Preempted by higher round {data.get('v_rnd')}")
                            return (False, None)
                    
                    elif data.get('type') == 'REJECT' and data.get('inst') == self.next_instance:
                        logging.warning(f"Rejected by acceptor {data.get('aid')}")
                        return (False, None)
                
                except (json.JSONDecodeError, KeyError) as e:
                    # Client value raw
                    try:
                        self.pending_client_values.append({'value': response.decode().strip()})
                    except:
                        pass
                    continue
        
        if len(acceptances) < self.majority:
            logging.warning(f"Fast path timeout - only {len(acceptances)} acceptances")
            return (False, None)
        
        # SUCCESS! Send DECISION
        decision_msg = {
            'type': 'DECISION',
            'inst': self.next_instance,
            'val': value
        }
        self.s.sendto(json.dumps(decision_msg).encode(), self.config["learners"])
        
        logging.info(f"DECIDED (fast path) instance {self.next_instance} = '{value}'")
        
        return (True, value)

    def run_full_paxos(self, original_value):
        """
        Execute full 2-phase Paxos for current instance.
        If successful, set prepared_rnd for future optimizations.
        
        Returns: (success, decided_value)
        """
        self.c_rnd += self.num_proposers
        logging.info(f"Running full Paxos for instance {self.next_instance} with round {self.c_rnd}")
        
        # --- PHASE 1: PREPARE ---
        prepare_msg = {
            'type': 'PREPARE',
            'inst': self.next_instance,
            'rnd': self.c_rnd
        }
        self.s.sendto(json.dumps(prepare_msg).encode(), self.config["acceptors"])
        
        promises = {}
        timeout = 1.0
        start_time = time.time()
        
        while len(promises) < self.majority and time.time() - start_time < timeout:
            ready, _, _ = select.select([self.client_r], [], [], 0.1)
            if not ready:
                continue
            
            for sock in ready:
                response, _ = sock.recvfrom(2**16)
                try:
                    data = json.loads(response.decode())
                    
                    if not isinstance(data, dict) or 'type' not in data:
                        # Client value, buffer it
                        try:
                            client_data = json.loads(response.decode())
                            self.pending_client_values.append(client_data)
                        except:
                            self.pending_client_values.append({'value': response.decode().strip()})
                        continue
                    
                    if (data.get('inst') == self.next_instance and 
                        data.get('type') == 'PROMISE' and 
                        data.get('rnd') == self.c_rnd):
                        promises[data.get('aid')] = data
                    elif data.get('rnd', 0) > self.c_rnd:
                        # Preempted
                        logging.warning(f"Preempted in Phase 1 by round {data.get('rnd')}")
                        return (False, None)
                    
                    elif data.get('type') == 'DECISION':
                        # Sync with decisions
                        inst = data.get('inst')
                        if inst is not None:
                            self.next_instance = max(self.next_instance, inst + 1)
                
                except (json.JSONDecodeError, KeyError):
                    try:
                        self.pending_client_values.append({'value': response.decode().strip()})
                    except:
                        pass
                    continue
        
        if len(promises) < self.majority:
            logging.warning(f"Phase 1 failed - only {len(promises)} promises")
            return (False, None)
        
        # SUCCESS Phase 1: Mark as "prepared"
        self.prepared_rnd = self.c_rnd
        self.prepared_since_inst = self.next_instance
        logging.info(f"✓ Prepared round {self.c_rnd} for instances >= {self.next_instance}")
        
        # Determine value to propose (inherit if necessary)
        best_promise = max(promises.values(), key=lambda p: p.get('v_rnd', 0))
        value_to_propose = original_value
        
        if best_promise.get('v_rnd', 0) > 0:
            # MUST inherit already accepted value
            value_to_propose = best_promise.get('v_val')
            logging.info(f"Inheriting value '{value_to_propose}' from promise")
        
        # --- PHASE 2: PROPOSE ---
        propose_msg = {
            'type': 'PROPOSE',
            'inst': self.next_instance,
            'rnd': self.c_rnd,
            'val': value_to_propose
        }
        self.s.sendto(json.dumps(propose_msg).encode(), self.config["acceptors"])
        
        acceptances = {}
        start_time = time.time()
        
        while len(acceptances) < self.majority and time.time() - start_time < timeout:
            ready, _, _ = select.select([self.client_r], [], [], 0.1)
            if not ready:
                continue
            
            for sock in ready:
                response, _ = sock.recvfrom(2**16)
                try:
                    data = json.loads(response.decode())
                    
                    if not isinstance(data, dict) or 'type' not in data:
                        # Client value, buffer it
                        try:
                            client_data = json.loads(response.decode())
                            self.pending_client_values.append(client_data)
                        except:
                            self.pending_client_values.append({'value': response.decode().strip()})
                        continue
                    
                    if (data.get('inst') == self.next_instance and 
                        data.get('type') == 'ACCEPTED' and 
                        data.get('v_rnd') == self.c_rnd):
                        acceptances[data.get('aid')] = data
                    elif data.get('v_rnd', 0) > self.c_rnd:
                        # Preempted
                        logging.warning(f"Preempted in Phase 2 by round {data.get('v_rnd')}")
                        self.prepared_rnd = None
                        return (False, None)
                    
                    elif data.get('type') == 'DECISION':
                        # Sync with decisions
                        inst = data.get('inst')
                        if inst is not None:
                            self.next_instance = max(self.next_instance, inst + 1)
                
                except (json.JSONDecodeError, KeyError):
                    try:
                        self.pending_client_values.append({'value': response.decode().strip()})
                    except:
                        pass
                    continue
        
        if len(acceptances) < self.majority:
            logging.warning(f"Phase 2 failed - only {len(acceptances)} acceptances")
            self.prepared_rnd = None
            return (False, None)
        
        # --- PHASE 3: DECIDE ---
        decision_msg = {
            'type': 'DECISION',
            'inst': self.next_instance,
            'val': value_to_propose
        }
        self.s.sendto(json.dumps(decision_msg).encode(), self.config["learners"])
        
        logging.info(f"DECIDED instance {self.next_instance} = '{value_to_propose}'")
        
        return (True, value_to_propose)

