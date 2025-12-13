import sys
import json
import logging
import time
import select
from utils import mcast_sender, mcast_receiver


'''
Client module:

Each client reads a set of value from a log file (in this case trought stdin) and sends them to the proposers and wait
to receive the confirmation of delivery from the learners.

In our implementation, at the end of Milestone3, the client send a set of values with maximum size of WINDOW_SIZE_BURST and then wait
for their delivery confirmation for a certain TIMEOUT_BURST.

Example:
  - A, B, C are values read from stdin and pending ={}
  - pending.size < WINDOW_SIZE_BURST, so:
    - send A, pending = {A}
    - send B, pending = {A, B}
    - send C, pending = {A, B, C}
  - Now, finished_input = True (no more values to read from stdin)
  - Wait for delivery confirmation from learners for values in pending with state:
    - A : (1, 0, t0, t0)  # (value, msg_bytes, start_time, last_send)
    - B : (2, 0, t0, t0)
    - C : (3, 0, t0, t0)
  - select.select(...,0.1) waits for 0.1s for a response from learners:
    - if no response after 0.1s, ready = [] so ready is false
    - if response received, ready = [self.r] so ready is true and we can read from the socket
  - If before reaching the timeout TIMEOUT_BURST we receive a response from learners:
    - we read the response from the socket, if it is for B delivery, we remove B from pending, compute latency and print B to stdout
  - If after TIMEOUT_BURST we have not received a response for B and C:
    - we resend B and C to proposers and update last_send to current time (possibly t1 = t0 + TIMEOUT_BURST or t0)
  
  - When ACK is received for all pending values and finished_input = True, the client terminates.

  Why using select.select(...) with timeout 0.1s?
  - We want to be able to retry timeouted requests and at the same time be able to receive responses from learners.
  - Otherwise, if we use a blocking recvfrom(), we would not be able to retry sending timeouted requests until a response is received,
    which could lead to deadlocks if the response is lost!

'''


class Client:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        # Multicast sockets from which send messages to proposers
        self.s = mcast_sender()
        # Mainted same configuration as specified on the initial skeleton code
        self.r = mcast_receiver(config["learners"])
        
        # File to store latency measurements
        self.output_file = f"logs/latency_client{self.id}"
        self.measuring = True # activate latency measurement
        self.seq_num = 0 # order of value sent by the client
        
        # Burst configuration
        self.WINDOW_SIZE_BURST = 20 # number of maximum requests sent before waiting for responses
        self.TIMEOUT_BURST = 1.0 # time to wait for responses before retrying (in seconds)

    def run(self):
        # If debug is active (-d flag during ./run.sh)
        logging.debug(f"-> client {self.id}")
        
        # Track sent requests with dict: client_id, seq_num)
        pending = {}
        
        # Iterator over input values (needed to have burst)
        input_iter = (line.strip() for line in sys.stdin)
        finished_input = False
        
        # Until all the value are sent from stdin and all the pending requests are answered:
        while not finished_input or pending:

            #================MESSAGE SENDING PHASE ================#

            # Pick value from the iterator until we have space left in the window
            while not finished_input and len(pending) < self.WINDOW_SIZE_BURST:
                try:
                    # Take the next value from stdin
                    val = next(input_iter)
                except StopIteration: # no more input
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
                
                # Take note of the send time for latency measurement (if enabled)
                start_time = time.perf_counter() if self.measuring else None
                self.s.sendto(msg_bytes, self.config["proposers"])
                logging.debug(f"Client {self.id} sent: {val}")
                
                pending[req_id] = {
                    'value': val,
                    'msg_bytes': msg_bytes,
                    'start_time': start_time, # to calculate the total latency
                    'last_send': time.time(), # for retry timeout 
                }
            
            #================MESSAGE TIMEOUT CHECK================#

            # Retransmit pending requests if they timeout
            now = time.time()
            for req_id, state in pending.items():
                if now - state['last_send'] > self.TIMEOUT_BURST:
                    # Retransmit the message with msg_bytes (:= serialized json encoded)
                    self.s.sendto(state['msg_bytes'], self.config["proposers"])
                    state['last_send'] = now
                    logging.debug(f"Client {self.id} retrying: {state['value']}")
            
            #================MESSAGE RECEIVING PHASE ================#
            # check if socket is ready to receive
            ready, _, _ = select.select([self.r], [], [], 0.1)
            if ready: # socket is ready, try to receive
                try:
                    # Take response from the socket (learner response)
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