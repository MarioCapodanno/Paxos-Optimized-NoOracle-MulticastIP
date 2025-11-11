import sys
import logging
import time
from utils import mcast_sender, mcast_receiver


class Client:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.s = mcast_sender()
        self.r = mcast_receiver(config["learners"])
        
        self.output_file = f"logs/latency_client{self.id}"
        self.measuring = True

    def run(self):
        logging.debug(f"-> client {self.id}")
        for value in sys.stdin:
            value = value.strip()
            
            if self.measuring:
                start_time = time.perf_counter()

            logging.debug(f"client: sending {value} to proposers")
            self.s.sendto(value.encode(), self.config["proposers"])
            
            if self.measuring:
                msg, addr = self.r.recvfrom(2**16)
                
                end_time = time.perf_counter()
                latency = (end_time - start_time) * 1_000_000 # us
                with open(self.output_file, "a") as f:
                    f.write(f"{latency:.6f}\n")
                
                print(msg.decode())
                sys.stdout.flush()
                
        logging.debug("client done.")
