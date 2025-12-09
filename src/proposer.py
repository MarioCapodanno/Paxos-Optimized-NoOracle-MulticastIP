import json
import logging
import random
import select
import time
from utils import mcast_receiver, mcast_sender, RndGeq


class Proposer:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["proposers"])
        self.r.setblocking(False)  # Non-blocking for batching
        self.s = mcast_sender()

        self.num_acceptors = 3
        self.majority = (self.num_acceptors // 2) + 1
        self.round_counter = 0
        self.next_instance = 0
        
        # Track seen requests to prevent duplicates
        self.seen_requests = set()  # (client_id, seq_num)
        
        # Optimization 2: Pre-Phase1 state
        self.prepared_instance = None
        self.prepared_round = None
        self.prepared_promises = None
        
        # Optimization 3: Batching
        self.BATCH_SIZE = 50
        self.BATCH_TIMEOUT = 0.05  # 50ms to collect batch (increased for better batching)
        self.pending_values = []  # Buffer for batching
        
        # Backoff parameters for contention handling
        self.BASE_BACKOFF = 0.01  # 10ms base
        self.MAX_BACKOFF = 0.5    # 500ms max
        self.current_backoff = self.BASE_BACKOFF

    def run(self):
        logging.info(f"-> proposer {self.id}")
        
        while True:
            # Optimization 2: Pre-Phase1 while idle
            if not self.pending_values:
                self.try_prepare_next_instance()
            
            # Optimization 3: Collect batch of values
            batch = self.collect_batch()
            
            if not batch:
                continue
            
            logging.info(f"Proposing batch of {len(batch)} values for instance {self.next_instance}")
            
            # Run Paxos for this batch
            decided = False
            while not decided:
                success, decided_value = self.run_paxos(batch)
                
                if success:
                    if decided_value == batch:
                        decided = True
                        logging.info(f"Batch decided at instance {self.next_instance}")
                        # Reset backoff on success
                        self.current_backoff = self.BASE_BACKOFF
                    else:
                        logging.info(f"Different value decided, retrying our batch")
                        # Small random delay to desync proposers without heavy backoff
                        time.sleep(random.uniform(0.001, 0.01))
                    
                    self.next_instance += 1
                else:
                    logging.debug(f"Paxos failed, retrying")
                    # Apply backoff on failure
                    self._apply_backoff()
    
    def _apply_backoff(self):
        """Apply exponential backoff with jitter to reduce contention."""
        # Add random jitter (0.5x to 1.5x of current backoff)
        jitter = self.current_backoff * (0.5 + random.random())
        time.sleep(jitter)
        # Exponential increase, capped at MAX_BACKOFF
        self.current_backoff = min(self.current_backoff * 2, self.MAX_BACKOFF)
    
    def try_prepare_next_instance(self):
        """Optimization 2: Run Phase1 before receiving client values"""
        if (self.prepared_instance != self.next_instance or 
            self.prepared_promises is None):
            
            self.round_counter += 1
            next_round = {'bal': self.round_counter, 'pid': self.id}
            
            ok, promises = self.phase1(self.next_instance, next_round, timeout=0.2)
            if ok:
                self.prepared_instance = self.next_instance
                self.prepared_round = next_round
                self.prepared_promises = promises
                logging.debug(f"Pre-Phase1 complete for instance {self.next_instance}")
    
    def collect_batch(self):
        """Optimization 3: Collect multiple values into a batch"""
        batch = []
        batch_start = None
        
        # Fill from pending buffer first
        while self.pending_values and len(batch) < self.BATCH_SIZE:
            batch.append(self.pending_values.pop(0))
        
        if batch:
            batch_start = time.time()
        
        # Try to collect more values from network
        while len(batch) < self.BATCH_SIZE:
            # Calculate remaining time
            if batch:
                elapsed = time.time() - batch_start
                if elapsed >= self.BATCH_TIMEOUT:
                    break
                wait_time = self.BATCH_TIMEOUT - elapsed
            else:
                wait_time = None  # Block until first message
            
            ready, _, _ = select.select([self.r], [], [], wait_time)
            if not ready:
                break
            
            # Drain socket
            while len(batch) < self.BATCH_SIZE:
                try:
                    msg, _ = self.r.recvfrom(2**16)
                    data = json.loads(msg.decode())
                    
                    # Ignore Paxos messages
                    if 'type' in data:
                        continue
                    
                    # Extract and validate client request
                    client_id = data.get('client_id')
                    seq_num = data.get('seq_num')
                    value = data.get('value')
                    
                    if client_id is None or seq_num is None or value is None:
                        continue
                    
                    # Check for duplicate
                    req_id = (client_id, seq_num)
                    if req_id in self.seen_requests:
                        logging.debug(f"Duplicate request {req_id}, skipping")
                        continue
                    
                    self.seen_requests.add(req_id)
                    batch.append({'cid': client_id, 'sn': seq_num, 'val': value})
                    
                    if not batch_start:
                        batch_start = time.time()
                    
                except BlockingIOError:
                    break
                except:
                    continue
        
        return batch

    def run_paxos(self, value):
        inst = self.next_instance
        
        # Optimization 2: Reuse prepared Phase1 if available
        if (self.prepared_instance == inst and 
            self.prepared_round is not None and
            self.prepared_promises is not None):
            c_rnd = self.prepared_round
            promises = self.prepared_promises
            logging.debug(f"Reusing prepared Phase1 for instance {inst}")
            # Clear prepared state
            self.prepared_instance = None
            self.prepared_round = None
            self.prepared_promises = None
        else:
            # Run fresh Phase1
            self.round_counter += 1
            c_rnd = {'bal': self.round_counter, 'pid': self.id}
            ok, promises = self.phase1(inst, c_rnd)
            if not ok:
                return (False, None)
        
        # Value selection
        max_v_rnd = {'bal': 0, 'pid': 0}
        value_to_propose = value
        
        for promise in promises.values():
            v_rnd = promise.get('v_rnd', {'bal': 0, 'pid': 0})
            if RndGeq(v_rnd, max_v_rnd):
                max_v_rnd = v_rnd
                if v_rnd.get('bal', 0) > 0:
                    value_to_propose = promise.get('v_val')
        
        # Phase 2
        ok, decided_val = self.phase2(inst, c_rnd, value_to_propose)
        return (ok, decided_val)

    def phase1(self, inst, c_rnd, timeout=1.0):
        prepare_msg = {
            'type': '1A',
            'inst': inst,
            'rnd': c_rnd
        }
        self.s.sendto(json.dumps(prepare_msg).encode(), self.config["acceptors"])
        
        promises = {}
        start_time = time.time()
        
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
            
            if (data.get('type') == '1B' and 
                data.get('inst') == inst and 
                data.get('rnd') == c_rnd):
                aid = data.get('aid')
                promises[aid] = data
        
        return (True, promises)

    def phase2(self, inst, c_rnd, value):
        propose_msg = {
            'type': '2A',
            'inst': inst,
            'rnd': c_rnd,
            'val': value
        }
        self.s.sendto(json.dumps(propose_msg).encode(), self.config["acceptors"])
        
        acceptances = {}
        timeout = 1.0
        start_time = time.time()
        
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
            
            if (data.get('type') == '2B' and 
                data.get('inst') == inst and 
                data.get('v_rnd') == c_rnd):
                aid = data.get('aid')
                acceptances[aid] = data
        
        return (True, value)

