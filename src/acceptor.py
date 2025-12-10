import logging
import json
from utils import mcast_receiver, mcast_sender, RndGeq


class Acceptor:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["acceptors"])
        self.s = mcast_sender()
        # instance state: promise round and last accepted value ((rnd, v_rnd, v_val))
        self.instances = {}  # inst ->      
        # Store accepted values per instance for learner catchup {(instance :value)}
        self.accepted_values = {} 

    def run(self):
        logging.info(f"-> acceptor {self.id}")
        while True:
            msg, _ = self.r.recvfrom(2**16)
            try:
                decoded = json.loads(msg.decode())
            except:
                continue
            
            msg_type = decoded.get('type')
            
            if msg_type == "1A":
                self.handle_prepare(decoded)
            elif msg_type == "2A":
                self.handle_propose(decoded)
            elif msg_type == "CATCHUP_REQUEST":
                self.handle_catchup(decoded)

    def handle_prepare(self, msg):
        # Extract from 1A the target instance and proposed round number
        inst = msg['inst']
        rnd_val = msg['rnd']
        
        logging.debug(f"Acceptor {self.id}: received 1A for instance {inst}, ballot {rnd_val}")
        
        # Load current state for this instance, defaults mean "no promises/accepts yet"
        rnd, v_rnd, v_val = self.instances.get(
            inst, ({'bal': 0, 'pid': 0}, {'bal': 0, 'pid': 0}, None)
        )
        
        # Only update our promised round and respond if the new round is >= current
        if RndGeq(rnd_val, rnd):
            rnd = rnd_val
            self.instances[inst] = (rnd, v_rnd, v_val)
            
            # Send promise back, including any previously accepted value for this instance
            promise_msg = {
                'type': '1B',
                'inst': inst,
                'rnd': rnd,
                'v_rnd': v_rnd,
                'v_val': v_val,
                'aid': self.id
            }
            logging.debug(f"Acceptor {self.id}: sending 1B (promise) for instance {inst}, ballot {rnd}")
            self.s.sendto(json.dumps(promise_msg).encode(), self.config["proposers"])
        else:
            logging.debug(f"Acceptor {self.id}: rejecting 1A for instance {inst}, ballot {rnd_val} < {rnd}")
    
    def handle_propose(self, msg):
        inst = msg['inst']
        rnd_val = msg['rnd']
        c_val = msg['val']
        
        # Count values in batch for logging
        batch_size = len(c_val) if isinstance(c_val, list) else 1
        logging.debug(f"Acceptor {self.id}: received 2A for instance {inst}, ballot {rnd_val}, batch_size={batch_size}")
        
        # Current promised/accepted state for this instance
        rnd, v_rnd, v_val = self.instances.get(
            inst, ({'bal': 0, 'pid': 0}, {'bal': 0, 'pid': 0}, None)
        )
        
        # Accept only if the 2A round is >= promised round
        if RndGeq(rnd_val, rnd):
            rnd = rnd_val
            v_rnd = rnd_val
            v_val = c_val
            self.instances[inst] = (rnd, v_rnd, v_val)
            # Track accepted value so learners can later reconstruct the log
            self.accepted_values[inst] = c_val  # Track for catchup
            
            accepted_msg = {
                'type': '2B',
                'inst': inst,
                'v_rnd': v_rnd,
                'v_val': v_val,
                'aid': self.id
            }
            
            logging.debug(f"Acceptor {self.id}: sending 2B (accept) for instance {inst}, batch_size={batch_size}")
            # Notify proposers and learners about the acceptance
            self.s.sendto(json.dumps(accepted_msg).encode(), self.config["proposers"])
            self.s.sendto(json.dumps(accepted_msg).encode(), self.config["learners"])
        else:
            logging.debug(f"Acceptor {self.id}: rejecting 2A for instance {inst}, ballot {rnd_val} < {rnd}")
    
    def handle_catchup(self, msg):
        lid = msg.get('lid') # learner_id
        logging.info(f"Acceptor {self.id}: received CATCHUP_REQUEST from learner {lid}")
        
        # Send back all values we have accepted so far, indexed by instance
        response = {
            'type': 'CATCHUP_RESPONSE',
            'aid': self.id,
            'accepted_values': self.accepted_values
        }
        self.s.sendto(json.dumps(response).encode(), self.config["learners"])