import sys
import json
import logging
import time
import select
from utils import mcast_sender, mcast_receiver


class Client:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.s = mcast_sender()
        self.r = mcast_receiver(config["learners"])
        
        self.output_file = f"logs/latency_client{self.id}"
        self.measuring = True
        self.seq_num = 0

    def run(self):
        logging.debug(f"-> client {self.id}")
        
        for line in sys.stdin:
            val = line.strip()
            if not val:
                continue
            
            req_msg = {
                'client_id': self.id,
                'seq_num': self.seq_num,
                'value': val
            }
            self.seq_num += 1
            
            msg_bytes = json.dumps(req_msg).encode()
            start_time = time.perf_counter() if self.measuring else None
            
            self.s.sendto(msg_bytes, self.config["proposers"])
            logging.debug(f"Client {self.id} sent: {val}")
            
            # Wait for confirmation with timeout and retry
            RETRY_TIMEOUT = 1.0
            last_send = time.time()
            confirmed = False
            
            while not confirmed:
                if (time.time() - last_send) > RETRY_TIMEOUT:
                    self.s.sendto(msg_bytes, self.config["proposers"])
                    last_send = time.time()
                    logging.debug(f"Client {self.id} retrying: {val}")
                
                ready, _, _ = select.select([self.r], [], [], 0.1)
                if ready:
                    try:
                        response, _ = self.r.recvfrom(65535)
                        data = json.loads(response.decode())
                        decided_val = data.get('value')
                        
                        if decided_val == val:
                            confirmed = True
                            
                            if self.measuring:
                                lat = (time.perf_counter() - start_time) * 1_000_000
                                with open(self.output_file, "a") as f:
                                    f.write(f"{lat:.6f}\n")
                            
                            print(val)
                            sys.stdout.flush()
                    except Exception:
                        continue
        
        logging.debug("client done.")