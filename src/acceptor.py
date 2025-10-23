import logging
import json
from utils import mcast_receiver, mcast_sender

class Acceptor:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["acceptors"])
        self.s = mcast_sender()
        
        # State is now per-instance
        # self.instances[instance_id] = (rnd, v_rnd, v_val)
        self.instances = {}

    def run(self):
        logging.info(f"-> acceptor {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            try:
                data = json.loads(msg.decode())
                inst = data.get('inst')
                if inst is None:
                    continue

                # Get the state for this specific instance, or create it if new
                rnd, v_rnd, v_val = self.instances.get(inst, (0, 0, None))

                if data.get('type') == 'PREPARE':
                    c_rnd = data['rnd']
                    logging.debug(f"Received PREPARE for inst={inst}, rnd={c_rnd} (current rnd={rnd})")
                    if c_rnd > rnd:
                        rnd = c_rnd
                        self.instances[inst] = (rnd, v_rnd, v_val)
                        promise_msg = {
                            'type': 'PROMISE',
                            'inst': inst,
                            'rnd': rnd,
                            'v_rnd': v_rnd,
                            'v_val': v_val,
                            'aid': self.id
                        }
                        logging.info(f"Sending PROMISE for inst={inst}, rnd={rnd}, v_rnd={v_rnd}, v_val={v_val}")
                        self.s.sendto(json.dumps(promise_msg).encode(), self.config["proposers"])
                    else:
                        logging.debug(f"Rejected PREPARE for inst={inst}: c_rnd={c_rnd} <= rnd={rnd}")
                
                elif data.get('type') == 'PROPOSE':
                    c_rnd = data['rnd']
                    c_val = data['val']
                    logging.debug(f"Received PROPOSE for inst={inst}, rnd={c_rnd}, val={c_val} (current rnd={rnd})")
                    if c_rnd >= rnd:
                        rnd = c_rnd
                        v_rnd = c_rnd
                        v_val = c_val
                        self.instances[inst] = (rnd, v_rnd, v_val)
                        accepted_msg = {
                            'type': 'ACCEPTED',
                            'inst': inst,
                            'v_rnd': v_rnd,
                            'v_val': v_val,
                            'aid': self.id
                        }
                        logging.info(f"Sending ACCEPTED for inst={inst}, v_rnd={v_rnd}, v_val={v_val}")
                        self.s.sendto(json.dumps(accepted_msg).encode(), self.config["proposers"])
                    else:
                        logging.debug(f"Rejected PROPOSE for inst={inst}: c_rnd={c_rnd} < rnd={rnd}")

            except (json.JSONDecodeError, KeyError) as e:
                logging.debug(f"Received malformed message: {e}")