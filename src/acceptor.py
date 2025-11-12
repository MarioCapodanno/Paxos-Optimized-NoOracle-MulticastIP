import logging
import json
from utils import mcast_receiver, mcast_sender, decode_message


class Acceptor:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["acceptors"])
        self.s = mcast_sender()

        # ACCEPTORS ARE THE ONLY PERSISTENT STORE OF ACCEPTED VALUES
        # Per-instance state: instances[inst] = (rnd, v_rnd, v_val)
        # rnd: highest round promised
        # v_rnd: round of accepted value
        # v_val: accepted value
        # This dictionary stores ALL accepted values for ALL instances.
        # Proposers learn about accepted values ONLY through 1B (PROMISE) messages
        # that include v_rnd and v_val from this store.
        self.instances = {}

    def run(self):
        logging.info(f"-> acceptor {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")

            decoded = decode_message(msg)
            
            # Handle JSON format (MultiPaxos)
            msg_type = decoded.get('type')
            inst = decoded.get('inst')
            
            if msg_type == "1A":
                self.handle_prepare_json(decoded)
            elif msg_type == "2A":
                self.handle_propose_json(decoded)

    def handle_prepare_json(self, msg):
        """Handle 1A (PREPARE) message in JSON format for MultiPaxos"""
        inst = msg['inst']
        rnd_val = msg['rnd']
        
        # Get state for this instance
        rnd, v_rnd, v_val = self.instances.get(inst, (0, 0, None))
        
        logging.debug(f"Acceptor {self.id}: received 1A (PREPARE) for inst={inst}, rnd={rnd_val}")
        
        # If message has a higher round number, update the state and send a 1B (PROMISE) message to the proposer
        if rnd_val > rnd:
            rnd = rnd_val
            # update acceptor state for current instance
            self.instances[inst] = (rnd, v_rnd, v_val)
            
            promise_msg = {
                'type': '1B',
                'inst': inst,
                'rnd': rnd,
                'v_rnd': v_rnd,
                'v_val': v_val,
                'aid': self.id
            }
            logging.info(f"Acceptor {self.id}: sending 1B (PROMISE) for inst={inst}, rnd={rnd}, v_rnd={v_rnd}")
            self.s.sendto(json.dumps(promise_msg).encode(), self.config["proposers"])
        else:
            # Just ignore the message
            logging.debug(f"Acceptor {self.id}: ignoring 1A (PREPARE) for inst={inst}: rnd={rnd_val} <= {rnd}")
    
    def handle_propose_json(self, msg):
        """Handle 2A (PROPOSE) message in JSON format for MultiPaxos"""
        inst = msg['inst']
        rnd_val = msg['rnd']
        c_val = msg['val']
        
        # Get state for this instance
        rnd, v_rnd, v_val = self.instances.get(inst, (0, 0, None))
        
        logging.debug(f"Acceptor {self.id}: received 2A (PROPOSE) for inst={inst}, rnd={rnd_val}, val={c_val}")
        
        if rnd_val >= rnd:
            rnd = rnd_val
            v_rnd = rnd_val
            v_val = c_val
            self.instances[inst] = (rnd, v_rnd, v_val)
            
            accepted_msg = {
                'type': '2B',
                'inst': inst,
                'v_rnd': v_rnd,
                'v_val': v_val,
                'aid': self.id
            }
            logging.info(f"Acceptor {self.id}: sending 2B (ACCEPTED) for inst={inst}, v_rnd={v_rnd}")
            self.s.sendto(json.dumps(accepted_msg).encode(), self.config["proposers"])
        else:
            # Just ignore the message (no REJECT message)
            logging.debug(f"Acceptor {self.id}: ignoring 2A (PROPOSE) for inst={inst}: rnd={rnd_val} < {rnd}")
