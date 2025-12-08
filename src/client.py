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
        
        # Sequence number for unique request IDs
        self.seq_num = 0

    def run(self):
        logging.debug(f"-> client {self.id}")
        
        # Pipeline configuration
        BURST_SIZE = 25  # Number of requests to send before waiting
        
        # Read all lines into a list to handle them in batches
        all_values = [line.strip() for line in sys.stdin if line.strip()]
        
        # Process in batches (Bursts)
        for i in range(0, len(all_values), BURST_SIZE):
            # 1. Prepare the group of requests (Burst)
            batch_values = all_values[i : i + BURST_SIZE]
            pending_requests = {} # Map: value -> {req_msg, start_time}
            
            # 2. Send the entire group quickly (Fire all)
            for val in batch_values:
                self.seq_num += 1
                req_msg = {
                    'client_id': self.id,
                    'seq_num': self.seq_num - 1,
                    'value': val
                }
                
                # Store for retry and latency measurement
                pending_requests[val] = {
                    'msg': json.dumps(req_msg).encode(),
                    'time': time.perf_counter() if self.measuring else None
                }
                
                logging.debug(f"Client sending (burst): {val}")
                self.s.sendto(pending_requests[val]['msg'], self.config["proposers"])

            # 3. Wait for confirmations (with intelligent retry for the group)
            # Exit only when ALL values in the group are confirmed
            RETRY_TIMEOUT = 1.0
            last_send_time = time.time()
            
            while pending_requests:
                # If too much time has passed without completing the group, resend missing items
                if (time.time() - last_send_time) > RETRY_TIMEOUT:
                    logging.debug(f"Burst timeout. Resending {len(pending_requests)} pending items...")
                    for val, info in pending_requests.items():
                        self.s.sendto(info['msg'], self.config["proposers"])
                    last_send_time = time.time()
                
                # Listen for responses
                ready, _, _ = select.select([self.r], [], [], 0.1) # Check frequently
                
                if ready:
                    try:
                        msg, _ = self.r.recvfrom(65535)
                        data = json.loads(msg.decode())
                        decided_val = data.get('val') or data.get('v_val')
                        
                        # Check which values in our group have been decided
                        # (Handles both incoming batches and single values)
                        confirmed_now = []
                        
                        # Normalize decided_val into a list for unique checking
                        candidates = decided_val if isinstance(decided_val, list) else [decided_val]
                        
                        for candidate in candidates:
                            if candidate in pending_requests:
                                confirmed_now.append(candidate)
                        
                        # Process confirmations
                        for val in confirmed_now:
                            info = pending_requests.pop(val) # Remove from pending
                            
                            # Log Latency
                            if self.measuring:
                                lat = (time.perf_counter() - info['time']) * 1_000_000
                                with open(self.output_file, "a") as f:
                                    f.write(f"{lat:.6f}\n")
                            
                            # Output per check.sh
                            print(val)
                            sys.stdout.flush()
                            
                    except Exception:
                        continue

        logging.debug("client done.")