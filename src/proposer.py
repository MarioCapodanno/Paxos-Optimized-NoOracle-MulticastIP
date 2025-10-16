import logging
from utils import mcast_receiver, mcast_sender


class Proposer:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["proposers"])
        self.s = mcast_sender()

    def run(self):
        logging.info(f"-> proposer {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")
            if self.id == 1:
                logging.debug(f"Sending {msg.decode()} to acceptors")
                self.s.sendto(msg, self.config["acceptors"])
