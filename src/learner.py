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
        
        self.num_acceptors = 3
        self.majority = (self.num_acceptors // 2) + 1
        
        self.next_instance = 0
        self.buffer = {}  # inst -> value
        self.votes = {}  # inst -> {val: set(aid)}
        self.catchup_votes = {}  # inst -> {val: count} for catchup
        self.delivered_reqs = set()  # (client_id, seq_num) for deduplication

    def run(self):
        logging.debug(f"-> learner {self.id}")
        
        # Send initial catchup request
        catchup_msg = {'type': 'CATCHUP_REQUEST', 'lid': self.id}
        self.s.sendto(json.dumps(catchup_msg).encode(), self.config["acceptors"])
        logging.debug(f"Learner {self.id}: sent initial CATCHUP_REQUEST")
        
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
        
        # Skip if already decided
        if inst in self.buffer:
            return
        
        # Create hashable key for vote tracking
        # raw_val is a list of dicts or a list of strings
        val_key = json.dumps(raw_val, sort_keys=True) if isinstance(raw_val, list) else raw_val
        
        # Initialize vote tracking
        if inst not in self.votes:
            self.votes[inst] = {}
        if val_key not in self.votes[inst]:
            self.votes[inst][val_key] = set()
        
        # Add vote
        self.votes[inst][val_key].add(aid)
        
        # Check for majority
        if len(self.votes[inst][val_key]) >= self.majority:
            # Store original list in buffer for delivery
            self.buffer[inst] = raw_val if isinstance(raw_val, list) else [raw_val]
            self.deliver()

    def deliver(self):
        while self.next_instance in self.buffer:
            val = self.buffer.pop(self.next_instance)
            
            # Handle batched values
            if isinstance(val, list):
                for item in val:
                    self._deliver_item(item)
            else:
                self._deliver_item(val)
            
            self.next_instance += 1
    
    def _deliver_item(self, item):
        """Deliver a single item, handling deduplication."""
        # Extract metadata if present (new format: {'cid', 'sn', 'val'})
        if isinstance(item, dict) and 'cid' in item and 'sn' in item:
            cid = item['cid']
            sn = item['sn']
            value = item['val']
            
            # Check for duplicate
            req_id = (cid, sn)
            if req_id in self.delivered_reqs:
                logging.debug(f"Learner {self.id}: skipping duplicate {req_id}")
                return
            self.delivered_reqs.add(req_id)
        else:
            # Legacy format (just a value)
            value = item
        
        print(value)
        sys.stdout.flush()
        # Notify clients
        notification = {'value': value}
        self.s.sendto(json.dumps(notification).encode(), self.config["learners"])
    
    def handle_catchup(self, msg):
        aid = msg.get('aid')
        accepted_values = msg.get('accepted_values', {})
        
        logging.debug(f"Learner {self.id}: received CATCHUP_RESPONSE from acceptor {aid}")
        
        for inst_str, raw_val in accepted_values.items():
            inst = int(inst_str)
            
            # Skip if already decided
            if inst in self.buffer:
                continue
            
            # Convert list to tuple for dict key
            val = tuple(raw_val) if isinstance(raw_val, list) else raw_val
            
            # Count votes for this (inst, val)
            if inst not in self.catchup_votes:
                self.catchup_votes[inst] = {}
            if val not in self.catchup_votes[inst]:
                self.catchup_votes[inst][val] = 0
            
            self.catchup_votes[inst][val] += 1
            
            # If majority reached, buffer it
            if self.catchup_votes[inst][val] >= self.majority:
                logging.debug(f"Learner {self.id}: catchup decided instance {inst}")
                # Store as list if it was a batch
                self.buffer[inst] = list(val) if isinstance(val, tuple) else val
                self.deliver()
