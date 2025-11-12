import sys
import json
import logging
from utils import mcast_receiver, decode_message


class Learner:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["learners"])

        # For in-order delivery
        self.next = 0
        self.buffer = {}  # instance -> value (for out-of-order messages)
        
        self.delivered_instances = set()

    def run(self):
        logging.debug(f"-> learner {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")

            decoded = decode_message(msg)
            
            if decoded.get('type') == 'DECISION':
                self.handle_decision(decoded)

    def deliver_values(self):
        while self.next in self.buffer:
            curr_inst = self.next
            
            # Skip if already delivered (shouldn't happen, but safety check)
            if curr_inst in self.delivered_instances:
                self.buffer.pop(curr_inst)
                self.next += 1
                continue
            
            value_to_print = self.buffer.pop(curr_inst)
            
            # output the current instance value
            logging.debug(f"Learner {self.id}: delivering instance {curr_inst} = {value_to_print}")
            self.delivered_instances.add(curr_inst)
            print(value_to_print)
            sys.stdout.flush()
            
            self.next += 1
    
    def handle_decision(self, msg):
        inst = msg['inst']
        val = msg['val']
        
        if inst in self.delivered_instances or inst in self.buffer:
            # ignore duplicates (should not happen but still checking)
            return
        
        if inst == self.next:
            # Deliver immediately if in order
            self.delivered_instances.add(inst)
        else:
            # If not in order, buffer it
            self.buffer[inst] = val
        
        if inst == self.next:
            logging.debug(f"Learner {self.id}: delivering instance {inst} = {val}")
            print(val)
            sys.stdout.flush() # becuase we want real-time output
            self.next += 1
            
            # After delivering check if we can deliver more from buffer
            self.deliver_values()
        else:
            # Out of order: already buffered above
            logging.debug(f"Learner {self.id}: learned instance {inst} = '{val}' from DECISION (out of order, buffered)")
            # Try to deliver if this completes a sequence
            self.deliver_values()
