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
        for value in sys.stdin:
            value = value.strip()
            
            # Generate unique ID
            request_id = (self.id, self.seq_num)
            self.seq_num += 1
            
            # Pack the request information inside a JSON object to be sent to the proposers
            request_msg = {
                'client_id': self.id,
                'seq_num': self.seq_num - 1,
                'value': value
            }
            
            if self.measuring:
                start_time = time.perf_counter()

            logging.debug(f"client: sending {value} (id={request_id}) to proposers")
            # Value sent to all proposers
            self.s.sendto(json.dumps(request_msg).encode(), self.config["proposers"])
            
            # ==================================================================
            # CLOSED LOOP: WAIT FOR CONFIRMATION
            # ==================================================================
            # We must wait until we see OUR value being decided.
            # Since we use Multicast, we will receive decisions for other clients too.
            # Since we use Batching, the decision might be a list of values.
            # ==================================================================
            if self.measuring:
                while True:
                    # 1. Receive any message from the Learners multicast group
                    # blocking call until a packet arrives
                    msg, addr = self.r.recvfrom(2**16)
                    
                    try:
                        # 2. Decode the JSON message
                        data = json.loads(msg.decode())
                        
                        # The field containing the decided value might differ based on the sender:
                        # - 'val' if sent by a Proposer (DECISION message)(retro-compatibility for MultiPaxos whithout optimizations)
                        # - 'v_val' if sent by an Acceptor (2B message)
                        decided_val = data.get('val') or data.get('v_val')
                        
                        # 3. Check if OUR value is contained in the decision.
                        # This is crucial for two reasons:
                        # a) Multicast: We receive decisions for other clients too.
                        # b) Batching: The decided value might be a list of values.
                        found = False

                        if isinstance(decided_val, list):
                            # Batching case: check if our value is inside the batch list
                            if value in decided_val:
                                found = True
                        elif decided_val == value:
                            # Standard case (no batching): exact match
                            found = True
                            
                        # 4. If we found our value, stop the timer and log
                        if found:
                            end_time = time.perf_counter()
                            latency = (end_time - start_time) * 1_000_000 # Convert to microseconds
                            
                            with open(self.output_file, "a") as f:
                                f.write(f"{latency:.6f}\n")
                            
                            # Print only the learned value (required for the verification script)
                            print(value)
                            sys.stdout.flush()
                            
                            # Exit the waiting loop and process the next input from stdin
                            break 
                            
                    except json.JSONDecodeError:
                        # Ignore malformed messages or noise on the network
                        continue

        logging.debug("client done.")