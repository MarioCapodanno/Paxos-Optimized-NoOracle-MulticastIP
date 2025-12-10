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
        # Mainted same configuration as specified on the initial skeleton code
        self.r = mcast_receiver(config["learners"])
        
        self.output_file = f"logs/latency_client{self.id}"
        self.measuring = True # activate latency measurement
        self.seq_num = 0 # order of value sent by the client
        
        # Burst configuration
        self.WINDOW_SIZE_BURST = 20
        self.TIMEOUT_BURST = 1.0

    def run(self):
        logging.debug(f"-> client {self.id}")
        
        # Track sent requests with dict:  (client_id, seq_num)
        pending = {}
        
        # Iterator over input values (needed to have burst)
        input_iter = (line.strip() for line in sys.stdin)
        finished_input = False
        
        while not finished_input or pending:
            # first pick value from the iteartion until we have space in the window
            while not finished_input and len(pending) < self.WINDOW_SIZE_BURST:
                try:
                    val = next(input_iter)
                except StopIteration:
                    finished_input = True
                    break
                
                seq = self.seq_num
                self.seq_num += 1
                req_id = (self.id, seq)

                # Encapsule the value inside the message
                req_msg = {
                    'client_id': self.id,
                    'seq_num': seq,
                    'value': val
                }
                # Serialize the dict to a json and encode it to be sent
                msg_bytes = json.dumps(req_msg).encode()
                
                start_time = time.perf_counter() if self.measuring else None
                self.s.sendto(msg_bytes, self.config["proposers"])
                logging.debug(f"Client {self.id} sent: {val}")
                
                pending[req_id] = {
                    'value': val,
                    'msg_bytes': msg_bytes,
                    'start_time': start_time, # to calculate the total latency
                    'last_send': time.time(), # for retry timeout 
                }
            
            # Retransmit pending requests if they timeout
            now = time.time()
            for req_id, state in pending.items():
                if now - state['last_send'] > self.TIMEOUT_BURST:
                    self.s.sendto(state['msg_bytes'], self.config["proposers"])
                    state['last_send'] = now
                    logging.debug(f"Client {self.id} retrying: {state['value']}")
            
            # check if socket is ready to receive
            ready, _, _ = select.select([self.r], [], [], 0.1)
            if ready:
                try:
                    response, _ = self.r.recvfrom(65535)
                    data = json.loads(response.decode())

                    # ignore non client protocol messages (ex. 2B or catchup msgs)
                    if 'type' in data:
                        continue
                    
                    # Messages are in the format {cid, sn, value} sent by the learner to notify the delivery
                    cid = data.get('cid') # client_id       
                    sn = data.get('sn') # seq_num

                    req_id = None
                    if cid is not None and sn is not None:
                        req_id = (cid, sn)
                    
                    if req_id and req_id in pending:
                        state = pending.pop(req_id)
                        val = state['value']
                        
                        if self.measuring and state['start_time'] is not None:
                            lat = (time.perf_counter() - state['start_time']) * 1_000_000
                            with open(self.output_file, "a") as f:
                                f.write(f"{lat:.6f}\n")
                        
                        print(val)
                        sys.stdout.flush()
                except Exception:
                    continue
        
        logging.debug("client done.")
