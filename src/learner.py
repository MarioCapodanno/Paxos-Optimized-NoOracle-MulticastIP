import sys
import json
import logging
from utils import mcast_receiver, mcast_sender


class Learner:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["learners"])
        self.s = mcast_sender()
        
        # Quorum configuration (kept in sync with acceptors)
        self.num_acceptors = 3
        self.majority = (self.num_acceptors // 2) + 1
        
        # Next instance index expected to be deliver
        self.next_instance = 0
        # Buffers decided values until they can be delivered in order
        self.buffer = {}  
        # Votes from 2B messages
        self.votes = {}  
        # Aggregated votes for catchup responses
        self.catchup_votes = {}  
        # Track (client_id, seq_num) already delivered to avoid duplicates
        self.delivered_reqs = set()
        
        # Gap detection for non startup catchup
        self.GAP_THRESHOLD = 5  # request catchup if we are this many instances behind
        self.catchup_pending = False 

    def run(self):
        logging.debug(f"-> learner {self.id}")
        
        # Send initial catchup request
        catchup_msg = {'type': 'CATCHUP_REQUEST', 'lid': self.id}
        self.s.sendto(json.dumps(catchup_msg).encode(), self.config["acceptors"])
        logging.debug(f"Learner {self.id}: sent initial CATCHUP_REQUEST")
        
        # Main loop: receive 2B and catchup messages and update local state
        while True:
            msg, _ = self.r.recvfrom(2**16)
            try:
                data = json.loads(msg.decode())
            except:
                continue
            
            msg_type = data.get('type')
            if msg_type == '2B':
                self.handle_2B(data)
            elif msg_type == 'CATCHUP_RESPONSE':
                self.handle_catchup(data)

    def handle_2B(self, msg):
        inst = msg['inst']
        raw_val = msg['v_val']
        aid = msg['aid']
        
        # Gap detection
        if inst >= self.next_instance + self.GAP_THRESHOLD and not self.catchup_pending:
            logging.debug(f"Learner {self.id}: gap detected (inst={inst}, next={self.next_instance}), requesting catchup")
            catchup_msg = {'type': 'CATCHUP_REQUEST', 'lid': self.id}
            self.s.sendto(json.dumps(catchup_msg).encode(), self.config["acceptors"])
            self.catchup_pending = True
        
        # Skip if already decided for this instance
        if inst in self.buffer:
            return
        
        # Create hashable key for vote tracking
        # raw_val is a list of dicts or a list of strings
        val_key = json.dumps(raw_val, sort_keys=True) if isinstance(raw_val, list) else raw_val
        
        # Initialize vote tracking structure if needed
        if inst not in self.votes:
            self.votes[inst] = {}
        if val_key not in self.votes[inst]:
            self.votes[inst][val_key] = set()
        
        # Record this acceptor's vote
        self.votes[inst][val_key].add(aid)
        
        # If we see a majority of 2B for the same value, consider it decided
        if len(self.votes[inst][val_key]) >= self.majority:
            # Store original list in buffer for ordered delivery
            self.buffer[inst] = raw_val if isinstance(raw_val, list) else [raw_val]
            self.deliver()
            del self.votes[inst]

    def deliver(self):
        delivered_any = False
        while self.next_instance in self.buffer:
            val = self.buffer.pop(self.next_instance)
            
            # Handle batched values
            if isinstance(val, list):
                for item in val:
                    self._deliver_item(item)
            else:
                self._deliver_item(val)
            
            self.next_instance += 1
            delivered_any = True
        
        # Reset catchup flag once we've made progress
        if delivered_any:
            self.catchup_pending = False
    
    def _deliver_item(self, item):
        # Extract metadata if present (new format: {'cid', 'sn', 'val'})
        if isinstance(item, dict) and 'cid' in item and 'sn' in item:
            cid = item['cid']
            sn = item['sn']
            value = item['val']
            
            # Check for duplicate
            req_id = (cid, sn)
            if req_id in self.delivered_reqs:
                return
            self.delivered_reqs.add(req_id)
            
            # Notify clients with metadata for precise matching
            notification = {'cid': cid, 'sn': sn, 'value': value}
        
        # Print value to stdout (for checker) and notify clients
        print(value)
        sys.stdout.flush()
        self.s.sendto(json.dumps(notification).encode(), self.config["learners"])
    
    def handle_catchup(self, msg):
        aid = msg.get('aid')
        accepted_values = msg.get('accepted_values', {})
        
        logging.debug(f"Learner {self.id}: received CATCHUP_RESPONSE from acceptor {aid}")
        
        for inst_str, raw_val in accepted_values.items():
            inst = int(inst_str)
            
            # Skip if already decided for this instance
            if inst in self.buffer:
                continue
            
            # Create hashable key for vote tracking (same approach as handle_2B)
            val_key = json.dumps(raw_val, sort_keys=True) if isinstance(raw_val, list) else raw_val
            
            # Count votes for this (inst, val) across acceptors
            if inst not in self.catchup_votes:
                self.catchup_votes[inst] = {}
            if val_key not in self.catchup_votes[inst]:
                self.catchup_votes[inst][val_key] = {'count': 0, 'raw_val': raw_val}
            
            self.catchup_votes[inst][val_key]['count'] += 1
            
            # If majority reached, buffer it as decided and try to deliver
            if self.catchup_votes[inst][val_key]['count'] >= self.majority:
                logging.debug(f"Learner {self.id}: catchup decided instance {inst}")
                # Store original value in buffer
                decided_val = self.catchup_votes[inst][val_key]['raw_val']
                self.buffer[inst] = decided_val if isinstance(decided_val, list) else [decided_val]
                self.deliver()
                del self.catchup_votes[inst]
