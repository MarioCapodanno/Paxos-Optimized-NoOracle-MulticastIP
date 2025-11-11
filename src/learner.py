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

    def run(self):
        logging.debug(f"-> learner {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")

            try:
                decoded = decode_message(msg)
                
                # Handle JSON format (MultiPaxos)
                if isinstance(decoded, dict):
                    if decoded.get('type') == 'DECISION':
                        self.handle_decision_json(decoded)
                    continue
                
                # Handle pipe-separated format (backward compatibility)
                if isinstance(decoded, tuple):
                    msg_type = decoded[0]
                    if msg_type == "DECISION":
                        self.handle_decision(decoded)
            except Exception as e:
                logging.debug(f"Error processing message: {e}")

    def handle_decision_json(self, msg):
        """Handle DECISION message in JSON format for MultiPaxos"""
        inst = msg.get('inst')
        val = msg.get('val')
        
        if inst is None or val is None:
            return
        
        # Buffer for out-of-order delivery
        self.buffer[inst] = val
        
        # Deliver all consecutive values
        while self.next_instance_to_print in self.buffer:
            value_to_print = self.buffer.pop(self.next_instance_to_print)
            
            # DELIVER: Output final value
            logging.debug(f"Learner {self.id}: delivering instance {self.next_instance_to_print} = {value_to_print}")
            print(value_to_print)
            sys.stdout.flush()
            
            self.next_instance_to_print += 1

    def handle_decision(self, msg):
        """Handle DECISION message from proposer with fields:
        msg_type(in this case "DECISION", can be ignored),
        value
        """
        _, value = msg

        # For backward compatibility (single instance), print immediately
        logging.debug(f"Learner {self.id}: decided value {value}")
        print(value)
        sys.stdout.flush()
