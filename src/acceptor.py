import logging
import json
from utils import mcast_receiver, mcast_sender, decode_message, encode_message, RndGeq


class Acceptor:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["acceptors"])
        self.s = mcast_sender()

        # Per-instance state: instances[inst] = (rnd, v_rnd, v_val)
        # rnd: highest round promised
        # v_rnd: round of accepted value
        # v_val: accepted value
        self.instances = {}

    def run(self):
        logging.info(f"-> acceptor {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")

            try:
                decoded = decode_message(msg)
                
                # Handle JSON format (MultiPaxos)
                if isinstance(decoded, dict):
                    msg_type = decoded.get('type')
                    inst = decoded.get('inst')
                    
                    if inst is None:
                        continue  # Ignore messages without instance
                    
                    if msg_type == "PREPARE":
                        self.handle_prepare_json(decoded)
                    elif msg_type == "PROPOSE":
                        self.handle_propose_json(decoded)
                    continue
                
                # Handle pipe-separated format (backward compatibility)
                msg_type = decoded[0] if isinstance(decoded, tuple) else None

                if msg_type == "PREPARE":
                    self.handle_prepare(decoded)
                elif msg_type == "ACCEPT":
                    self.handle_accept(decoded)
            except Exception as e:
                logging.debug(f"Error processing message: {e}")

    def handle_prepare_json(self, msg):
        """Handle PREPARE message in JSON format for MultiPaxos"""
        inst = msg.get('inst')
        rnd_val = msg.get('rnd')
        
        if inst is None or rnd_val is None:
            return
        
        # Get state for this instance
        rnd, v_rnd, v_val = self.instances.get(inst, (0, 0, None))
        
        logging.debug(f"Acceptor {self.id}: received PREPARE for inst={inst}, rnd={rnd_val}")
        
        if rnd_val > rnd:
            rnd = rnd_val
            self.instances[inst] = (rnd, v_rnd, v_val)
            
            promise_msg = {
                'type': 'PROMISE',
                'inst': inst,
                'rnd': rnd,
                'v_rnd': v_rnd,
                'v_val': v_val,
                'aid': self.id
            }
            logging.info(f"Acceptor {self.id}: sending PROMISE for inst={inst}, rnd={rnd}, v_rnd={v_rnd}")
            self.s.sendto(json.dumps(promise_msg).encode(), self.config["proposers"])
        else:
            logging.debug(f"Acceptor {self.id}: ignoring PREPARE for inst={inst}: rnd={rnd_val} <= {rnd}")
    
    def handle_propose_json(self, msg):
        """Handle PROPOSE message in JSON format for MultiPaxos"""
        inst = msg.get('inst')
        rnd_val = msg.get('rnd')
        c_val = msg.get('val')
        
        if inst is None or rnd_val is None:
            return
        
        # Get state for this instance
        rnd, v_rnd, v_val = self.instances.get(inst, (0, 0, None))
        
        logging.debug(f"Acceptor {self.id}: received PROPOSE for inst={inst}, rnd={rnd_val}, val={c_val}")
        
        if rnd_val >= rnd:
            rnd = rnd_val
            v_rnd = rnd_val
            v_val = c_val
            self.instances[inst] = (rnd, v_rnd, v_val)
            
            accepted_msg = {
                'type': 'ACCEPTED',
                'inst': inst,
                'v_rnd': v_rnd,
                'v_val': v_val,
                'aid': self.id
            }
            logging.info(f"Acceptor {self.id}: sending ACCEPTED for inst={inst}, v_rnd={v_rnd}")
            self.s.sendto(json.dumps(accepted_msg).encode(), self.config["proposers"])
        else:
            # Optional: send REJECT for fast preemption detection
            reject_msg = {
                'type': 'REJECT',
                'inst': inst,
                'rnd': rnd,
                'aid': self.id
            }
            logging.debug(f"Acceptor {self.id}: rejecting PROPOSE for inst={inst}: rnd={rnd_val} < {rnd}")
            self.s.sendto(json.dumps(reject_msg).encode(), self.config["proposers"])

    def handle_prepare(self, msg):
        """Handle PREPARE message from proposer with fields
        msg_type(in this case "PREPARE", can be ignored),
        ballot,
        proposer_id,
        """
        _, ballot, proposer_id = msg
        rnd = (ballot, proposer_id)

        logging.debug(f"Acceptor {self.id}: received PREPARE {rnd}")

        # If this round is greater than what we've promised, update and respond
        if RndGeq(rnd, self.promised_rnd):
            self.promised_rnd = rnd

            # Send PROMISE with our accepted value (if any)
            response = encode_message(
                "PROMISE",
                ballot,
                proposer_id,
                self.accepted_rnd[0],
                self.accepted_rnd[1],
                self.accepted_value,
                self.id,
            )

            logging.debug(f"Acceptor {self.id}: sending PROMISE to proposers")
            self.s.sendto(response, self.config["proposers"])
        else:
            logging.debug(
                f"Acceptor {self.id}: ignoring PREPARE {rnd} (promised {self.promised_rnd})"
            )

    def handle_accept(self, msg):
        """Handle ACCEPT message from proposer with fields:
        msg_type(in this case "ACCEPT", can be ignored),
        ballot,
        proposer_id,
        value,
        """
        _, ballot, proposer_id, value = msg
        rnd = (ballot, proposer_id)

        logging.debug(f"Acceptor {self.id}: received ACCEPT {rnd} with value {value}")

        # If this round is >= what we've promised, accept it
        if RndGeq(rnd, self.promised_rnd):
            self.promised_rnd = rnd
            self.accepted_rnd = rnd
            self.accepted_value = value

            # Send ACCEPTED to proposers (Phase 2B)
            response = encode_message("ACCEPTED", ballot, proposer_id, value, self.id)

            logging.debug(f"Acceptor {self.id}: sending ACCEPTED to proposers")
            self.s.sendto(response, self.config["proposers"])
        else:
            logging.debug(
                f"Acceptor {self.id}: ignoring ACCEPT {rnd} (promised {self.promised_rnd})"
            )
