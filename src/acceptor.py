import logging
import json
from utils import mcast_receiver, mcast_sender, decode_message, RndGeq


class Acceptor:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["acceptors"])
        self.s = mcast_sender()
        self.instances = {}  # inst -> (rnd, v_rnd, v_val)
        self.accepted_values = {}  # inst -> value (for catchup)

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
        inst = msg['inst']
        rnd_val = msg['rnd']
        
        rnd, v_rnd, v_val = self.instances.get(inst, ({'bal': 0, 'pid': 0}, {'bal': 0, 'pid': 0}, None))
        
        if RndGeq(rnd_val, rnd):
            rnd = rnd_val
            self.instances[inst] = (rnd, v_rnd, v_val)
            
            promise_msg = {
                'type': '1B',
                'inst': inst,
                'rnd': rnd,
                'v_rnd': v_rnd,
                'v_val': v_val,
                'aid': self.id
            }
            self.s.sendto(json.dumps(promise_msg).encode(), self.config["proposers"])
    
    def handle_propose(self, msg):
        inst = msg['inst']
        rnd_val = msg['rnd']
        c_val = msg['val']
        
        rnd, v_rnd, v_val = self.instances.get(inst, ({'bal': 0, 'pid': 0}, {'bal': 0, 'pid': 0}, None))
        
        if RndGeq(rnd_val, rnd):
            rnd = rnd_val
            v_rnd = rnd_val
            v_val = c_val
            self.instances[inst] = (rnd, v_rnd, v_val)
            self.accepted_values[inst] = c_val  # Track for catchup
            
            accepted_msg = {
                'type': '2B',
                'inst': inst,
                'v_rnd': v_rnd,
                'v_val': v_val,
                'aid': self.id
            }
            
            self.s.sendto(json.dumps(accepted_msg).encode(), self.config["proposers"])
            self.s.sendto(json.dumps(accepted_msg).encode(), self.config["learners"])
    
    def handle_catchup(self, msg):
        lid = msg.get('lid', 'unknown')
        logging.info(f"Acceptor {self.id}: received CATCHUP_REQUEST from learner {lid}")
        
        response = {
            'type': 'CATCHUP_RESPONSE',
            'aid': self.id,
            'accepted_values': self.accepted_values
        }
        self.s.sendto(json.dumps(response).encode(), self.config["learners"])