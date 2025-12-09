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
        
        # Convert list to tuple for dict key
        val = tuple(raw_val) if isinstance(raw_val, list) else raw_val
        
        # Initialize vote tracking
        if inst not in self.votes:
            self.votes[inst] = {}
        if val not in self.votes[inst]:
            self.votes[inst][val] = set()
        
        # Add vote
        self.votes[inst][val].add(aid)
        
        # Check for majority
        if len(self.votes[inst][val]) >= self.majority:
            # Store as list if it was a batch
            self.buffer[inst] = list(val) if isinstance(val, tuple) else val
            self.deliver()

    def deliver(self):
        while self.next_instance in self.buffer:
            val = self.buffer.pop(self.next_instance)
            
            # Handle batched values
            if isinstance(val, list):
                for item in val:
                    print(item)
                    sys.stdout.flush()
                    # Notify clients
                    notification = {'value': item}
                    self.s.sendto(json.dumps(notification).encode(), self.config["learners"])
            else:
                print(val)
                sys.stdout.flush()
                # Notify clients
                notification = {'value': val}
                self.s.sendto(json.dumps(notification).encode(), self.config["learners"])
            
            self.next_instance += 1
    
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
