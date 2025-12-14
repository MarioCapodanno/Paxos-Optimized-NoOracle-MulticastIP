import json
import logging
import select
import time
from utils import mcast_receiver, mcast_sender, RndGeq


class Proposer:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["proposers"])
        # Set non-blocking to allow timeouts, otherwise recvfrom() would block indefinitely
        # Example:
        # - no messages to receive: 
        #      ready, _, _ = select.select([self.r], [], [], 0.1) -> ready = []
        # - message received:
        #      ready, _, _ = select.select([self.r], [], [], 0.1) -> ready = [self.r]
        self.r.setblocking(False)
        self.s = mcast_sender()

        # Paxos quorum configuration
        self.num_acceptors = 3
        self.majority = (self.num_acceptors // 2) + 1
        
        # Ballot number tracking (will increase monotonically)
        self.round_counter = 0
        
        # Next Paxos instance number to propose
        self.next_instance = 0
        
        # track seen client requests (client_id, sequence_number)
        self.seen_requests = set() 
        
        # Milestone 3 Optimization 1 (referenced as Mil3_Opt1)
        self.prepared_instance = None  # instance with completed Phase 1
        self.prepared_round = None     # ballot number used
        self.prepared_promises = None  # 1B from acceptors majority
        
        # Milestone 3 Optimization 3 (Configuration)
        self.BATCH_SIZE = 50           
        self.BATCH_TIMEOUT = 0.01      
        self.pending_values = []       

    def run(self):

        # Logging always printed in the proposer log file
        logging.info(f"-> proposer {self.id}")
        
        while True:

            #================PHASE 1 OPTIMIZATION ================#
            # Mil3_Opt1: Try Phase 1 if not client value received
            if not self.pending_values:
                self.phase1_opt()
            
            #================BATCH COLLECTION ================#
            # Mil3_Opt3: Collect batch of values
            batch = self.collect_batch()
            
            if not batch:
                continue
            
            # Log batch details
            batch_vals = [item.get('val', item) if isinstance(item, dict) else item for item in batch]
            logging.debug(f"Collected batch: {batch_vals[:5]}{'...' if len(batch_vals) > 5 else ''} ({len(batch)} values)")
            logging.info(f"Proposing batch of {len(batch)} values for instance {self.next_instance}")
            
            #================PAXOS RUNNING ================#
            # Flag to track if the current instance where Proposer is working is decided
            decided = False

            # If for the current instance, the batch is not decided, run Paxos
            # otherwise, move to the next instance
            while not decided:
                success, decided_value = self.run_paxos(batch)
                
                # Check if Paxos was successful
                if success: 
                    # Check if the decided value is our batch
                    if decided_value == batch:
                        decided = True
                        logging.info(f"Batch decided at instance {self.next_instance}")
                    else:
                        # Another proposer won this instance with a different value
                        # Retry our batch at the next instance to ensure all values get decided
                        logging.debug(f"Different value decided, retrying our batch in the next instance")
                    
                    self.next_instance += 1
                else:
                    # Paxos is considered failed if timeout occurs during phase1 or phase2
                    logging.debug(f"Paxos failed, retrying immediately")
    
    def phase1_opt(self):
        # Check if we need to run a fresh Phase 1 for the current instance
        # or if we can reuse existing prepared state
        # If conditions met, run Phase 1 and store promises for future use
        # otherwise, skip Phase 1 because we can reuse existing state (no client values yet and already prepared)
        if (self.prepared_instance != self.next_instance or 
            self.prepared_promises is None):

            self.round_counter += 1
            next_round = {'bal': self.round_counter, 'pid': self.id}
            
            # run phase 1 but with timeout (otherwise we could wait to long and lose the purpose of this optimization)
            ok, promises = self.phase1(self.next_instance, next_round, timeout=0.2)
            if ok:
                self.prepared_instance = self.next_instance
                self.prepared_round = next_round
                self.prepared_promises = promises
                logging.debug(f"Pre-Phase1 complete for instance {self.next_instance}")
    
    def collect_batch(self):
        batch = []
        batch_start = None # track starting time for timeout
        
        # Fill from pending buffer first
        while self.pending_values and len(batch) < self.BATCH_SIZE:
            batch.append(self.pending_values.pop(0))
        
        if batch:
            batch_start = time.time()
        
        # Try to collect batch values until reached timeout or max size
        while len(batch) < self.BATCH_SIZE:
            # Calculate remaining time
            if batch:
                elapsed = time.time() - batch_start
                # If timeout reached, break and return collected batch
                if elapsed >= self.BATCH_TIMEOUT:
                    break
                wait_time = self.BATCH_TIMEOUT - elapsed
            else:
                wait_time = None
            
            # If batch is empty, then wait_time is None -> block until a message arrives
            # Otherwise, wait until batch timeout expires
            # N.B. : Remember that the outer while loop checks the batch size condition
            ready, _, _ = select.select([self.r], [], [], wait_time)

            if not ready:
                break
            
            # If the socket is ready, try to receive messages until no more are available or batch is full
            while len(batch) < self.BATCH_SIZE:
                try:
                    msg, _ = self.r.recvfrom(2**16)
                    data = json.loads(msg.decode())
                    
                    # Ignore paxos messages (paxos messages have 'type' field for example '1A', '1B', '2A', '2B')
                    if 'type' in data:
                        continue
                    
                    # Extract and validate client request
                    client_id = data.get('client_id')
                    seq_num = data.get('seq_num')
                    value = data.get('value')
                    
                    # Skip invalid messages (usually not happens but just in case)
                    if client_id is None or seq_num is None or value is None:
                        continue
                    
                    # Check for duplicate requests from client
                    # N.B.: It could happen that a client request arrives here while we are in phase 1 or 2
                    #       In that case, we still need to track it to avoid proposing it multiple times 
                    req_id = (client_id, seq_num)
                    if req_id in self.seen_requests:
                        logging.debug(f"Duplicate request {req_id}, skipping")
                        continue
                    
                    self.seen_requests.add(req_id)
                    batch.append({'cid': client_id, 'sn': seq_num, 'val': value})
                    
                    # If this is the first message in the batch (so, we did not have any in pending_values before) 
                    # start the batch timer
                    if not batch_start:
                        batch_start = time.time()
                except:
                    break
        
        return batch

    def run_paxos(self, value):
        # Instance index we are trying to decide in this run
        inst = self.next_instance
        
        # Mil3_Opt1: Reuse prepared Phase1 if available
        # If we already ran Phase 1 for this instance and stored a majority
        # of promises, we can skip doing another prepare round.
        if (self.prepared_instance == inst and 
            self.prepared_round is not None and
            self.prepared_promises is not None):
            c_rnd = self.prepared_round
            promises = self.prepared_promises
            logging.debug(f"Reusing prepared Phase1 for instance {inst}")
            # Clear prepared state so it is not reused for a different instance
            self.prepared_instance = None
            self.prepared_round = None
            self.prepared_promises = None
        else:
            # Run Phase 1 for this instance with a new ballot
            self.round_counter += 1
            c_rnd = {'bal': self.round_counter, 'pid': self.id}
            ok, promises = self.phase1(inst, c_rnd)
            if not ok:
                # Could not get a majority of promises (e.g., timeout)
                return (False, None)
        
        # ================MAJORITY PROMISES COLLECTED================#
        
        # Value selection according to Paxos rules:
        #   - iff any acceptor already accepted a value, we must propose the one
        #     with the highest v_rnd; otherwise we keep our original "value".
        max_v_rnd = {'bal': 0, 'pid': 0}
        value_to_propose = value
        
        # Iterate over all promises and save the highest accepted round
        for promise in promises.values():
            v_rnd = promise.get('v_rnd', {'bal': 0, 'pid': 0})
            if RndGeq(v_rnd, max_v_rnd):
                max_v_rnd = v_rnd
                if v_rnd.get('bal', 0) > 0:
                    value_to_propose = promise.get('v_val')
                    logging.debug(f"Instance {inst}: adopting previously accepted value from round {v_rnd}")
        
        # Run phase 2
        logging.debug(f"Instance {inst}: starting Phase2 with ballot {c_rnd}")
        ok, decided_val = self.phase2(inst, c_rnd, value_to_propose)
        if ok:
            logging.debug(f"Instance {inst}: Phase2 complete, value decided")
        return (ok, decided_val)

    def phase1(self, inst, c_rnd, timeout=1.0):
        """
        Sends 1A prepare message to all acceptors with ballot number c_rnd
        and waits for 1B promise messages from a majority. This phase
        establishes the proposer's leadership for the instance.
        """
        prepare_msg = {
            'type': '1A',
            'inst': inst,
            'rnd': c_rnd
        }
        # Broadcast prepare (1A) message to all acceptors
        logging.debug(f"Instance {inst}: sending 1A (prepare) with ballot {c_rnd}")
        self.s.sendto(json.dumps(prepare_msg).encode(), self.config["acceptors"])
        
        promises = {}
        start_time = time.time()
        
        # Wait until we collect promises from a majority or the timeout expires
        while len(promises) < self.majority:
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                return (False, None)
            
            ready, _, _ = select.select([self.r], [], [], 0.1)
            if not ready:
                continue
            
            response, _ = self.r.recvfrom(2**16)
            try:
                data = json.loads(response.decode())
            except:
                continue
            
            # Skip non-dict messages 
            if not isinstance(data, dict):
                continue
            
            # If we receive a msg with no type, it is a client request
            if 'type' not in data:
                # Since we are in phase 1, we buffer it in pending_values and keep waiting for 1B messages.
                client_id = data.get('client_id')
                seq_num = data.get('seq_num')
                value = data.get('value')
                # Check if valid client request
                if client_id is not None and seq_num is not None and value is not None:
                    req_id = (client_id, seq_num)
                    # Buffer it only if it's a new value request
                    if req_id not in self.seen_requests:
                        self.seen_requests.add(req_id)
                        self.pending_values.append({'cid': client_id, 'sn': seq_num, 'val': value})
                continue
            
            # Store 1B messages that match our instance and round
            if (data.get('type') == '1B' and 
                data.get('inst') == inst and 
                data.get('rnd') == c_rnd):
                # aid = acceptor id
                aid = data.get('aid')
                promises[aid] = data
        
        return (True, promises)

    def phase2(self, inst, c_rnd, value):
        """
        Sends 2A propose message to all acceptors with the value to be
        decided and waits for 2B acceptance messages from a majority.
        This phase actually decides the value for the instance.
        """

        propose_msg = {
            'type': '2A',
            'inst': inst,
            'rnd': c_rnd,
            'val': value
        }

        self.s.sendto(json.dumps(propose_msg).encode(), self.config["acceptors"])
        
        acceptances = {} # store the 2B responses
        timeout = 1.0
        start_time = time.time()
        
        # Wait until we receive 2B from a majority or the timeout expires
        while len(acceptances) < self.majority:
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                return (False, None)
            
            ready, _, _ = select.select([self.r], [], [], 0.1)
            if not ready:
                continue
            
            response, _ = self.r.recvfrom(2**16)
            try:
                data = json.loads(response.decode())
            except:
                continue

            # Same logic as phase 1
            if not isinstance(data, dict):
                continue

            # Same logic as phase 1
            if 'type' not in data:
                client_id = data.get('client_id')
                seq_num = data.get('seq_num')
                value = data.get('value')
                if client_id is not None and seq_num is not None and value is not None:
                    req_id = (client_id, seq_num)
                    if req_id not in self.seen_requests:
                        self.seen_requests.add(req_id)
                        self.pending_values.append({'cid': client_id, 'sn': seq_num, 'val': value})
                continue
            
            # Count only 2B messages for this instance and current round
            if (data.get('type') == '2B' and 
                data.get('inst') == inst and 
                data.get('v_rnd') == c_rnd):
                aid = data.get('aid')
                acceptances[aid] = data
        
        return (True, value)

