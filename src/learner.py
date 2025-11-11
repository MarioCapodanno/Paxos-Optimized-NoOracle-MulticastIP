import sys
import logging
from utils import mcast_receiver, decode_message


class Learner:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["learners"])
        self.next_instance_to_print = 0
        self.buffer = {} # Buffer for out-of-order instances

        # Track learned values with a set to avoid duplicates
        self.learned = set()

    def run(self):
        logging.debug(f"-> learner {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")

            try:
                decoded = decode_message(msg)
                msg_type = decoded[0]

                if msg_type == "DECISION":
                    self.handle_decision(decoded)
            except Exception as e:
                logging.debug(f"Error processing message: {e}")

    def handle_decision(self, msg):
        """Handle DECISION message from proposer with fields:
        msg_type(in this case "DECISION", can be ignored),
        value
        """
        _, value = msg

        # Print the decided value if we haven't already
        if value not in self.learned:
            self.learned.add(value)
            logging.debug(f"Learner {self.id}: decided value {value}")
            print(value)
            sys.stdout.flush()
