import sys
import json
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
        
        # Sequence number for unique request IDs
        self.seq_num = 0

    def run(self):
        logging.debug(f"-> client {self.id}")
        for value in sys.stdin:
            value = value.strip()
            
            # Generate unique ID
            request_id = (self.id, self.seq_num)
            self.seq_num += 1
            
            # Prepare JSON message with ID
            request_msg = {
                'client_id': self.id,
                'seq_num': self.seq_num - 1,
                'value': value
            }
            
            if self.measuring:
                start_time = time.perf_counter()

            logging.debug(f"client: sending {value} (id={request_id}) to proposers")
            self.s.sendto(json.dumps(request_msg).encode(), self.config["proposers"])
            
            # CLOSED LOOP: Wait for response
            if self.measuring:
                msg, addr = self.r.recvfrom(2**16)
                
                end_time = time.perf_counter()
                latency = (end_time - start_time) * 1_000_000 # us
                with open(self.output_file, "a") as f:
                    f.write(f"{latency:.6f}\n")
                
                # Print learned value (for verification)
                print(msg.decode())
                sys.stdout.flush()
                
        logging.debug("client done.")