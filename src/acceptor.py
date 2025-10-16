import logging
from utils import mcast_receiver, mcast_sender


class Acceptor:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["acceptors"])
        self.s = mcast_sender()

    def run(self):
        logging.info(f"-> acceptor {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")
            if self.id == 1:
                logging.debug(f"Sending {msg.decode()} to learners")
                self.s.sendto(msg, self.config["learners"])
