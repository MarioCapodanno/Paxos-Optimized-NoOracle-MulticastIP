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
        self.next_instance_to_print = 0
        self.buffer = {}  # instance -> value (for out-of-order messages)
        
        # Track learned instances to prevent duplicates
        self.learned_instances = set()  # Set of instances we've already learned

    def run(self):
        logging.debug(f"-> learner {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")

            decoded = decode_message(msg)
            
            # Handle JSON format (MultiPaxos)
            if decoded.get('type') == 'DECISION':
                self.handle_decision_json(decoded)

    def deliver_values(self):
        """Deliver all consecutive values in order from buffer"""
        while self.next_instance_to_print in self.buffer:
            value_to_print = self.buffer.pop(self.next_instance_to_print)
            
            # DELIVER: Output final value (only once per instance)
            logging.debug(f"Learner {self.id}: delivering instance {self.next_instance_to_print} = {value_to_print}")
            print(value_to_print)
            sys.stdout.flush()
            
            self.next_instance_to_print += 1
    
    def handle_decision_json(self, msg):
        """Handle DECISION message in JSON format for MultiPaxos"""
        inst = msg['inst']
        val = msg['val']
        
        # Skip if already learned
        if inst in self.learned_instances:
            return
        
        # If this is the next expected instance, deliver immediately
        if inst == self.next_instance_to_print:
            logging.debug(f"Learner {self.id}: learned instance {inst} = '{val}' from DECISION (in order, delivering immediately)")
            self.learned_instances.add(inst)
            print(val)
            sys.stdout.flush()
            self.next_instance_to_print += 1
            
            # After delivering, check if we can deliver more from buffer
            self.deliver_values()
        else:
            # Out of order: buffer it for later delivery
            logging.debug(f"Learner {self.id}: learned instance {inst} = '{val}' from DECISION (out of order, buffering)")
            self.buffer[inst] = val
            self.learned_instances.add(inst)
            # Try to deliver if this completes a sequence
            self.deliver_values()
