import sys
import logging
import json
from utils import mcast_receiver

class Learner:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["learners"])
        self.next_instance_to_print = 0
        self.buffer = {} # Buffer for out-of-order instances

    def run(self):
        logging.debug(f"-> learner {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            try:
                data = json.loads(msg.decode())
                if data.get('type') != 'DECISION':
                    continue
                
                inst = data['inst']
                val = data['val']
                
                self.buffer[inst] = val
                
                # Print all consecutive learned values from the buffer
                while self.next_instance_to_print in self.buffer:
                    value_to_print = self.buffer.pop(self.next_instance_to_print)
                    print(value_to_print)
                    sys.stdout.flush()
                    self.next_instance_to_print += 1

            except (json.JSONDecodeError, KeyError):
                logging.debug(f"Received non-decision message: {msg.decode()}")