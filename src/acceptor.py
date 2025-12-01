import logging
import json
from utils import mcast_receiver, mcast_sender, decode_message, RndGeq


class Acceptor:
    """
    Paxos Acceptor - The FAULT-TOLERANT component of the consensus protocol.
    
    Acceptors are the ONLY components that hold authoritative consensus state.
    The safety of Paxos depends on the intersection property of majorities:
    any two majorities share at least one acceptor, ensuring consistency.
    
    Each acceptor maintains per-instance state:
    - rnd: highest round promised (will reject lower rounds)
    - v_rnd: round of the last accepted value
    - v_val: the last accepted value
    
    If up to floor((N-1)/2) acceptors crash, the remaining majority still
    holds enough state to preserve safety and allow progress.
    
    Reference: Lamport, "Paxos Made Simple", 2001.
    """
    
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["acceptors"])
        self.s = mcast_sender()

        # =======================================================================
        # CANONICAL PAXOS STATE (per-instance)
        # =======================================================================
        # This is the FAULT-TOLERANT state of the system.
        # Acceptors are the only components that hold authoritative consensus state.
        # Safety depends on majority intersection of acceptor states.
        # Reference: Lamport, "Paxos Made Simple", 2001.
        #
        # Format: instances[inst] = (rnd, v_rnd, v_val)
        #   - rnd:   highest round number promised (will not accept lower rounds)
        #   - v_rnd: round number of the last accepted value (0 if none)
        #   - v_val: the last accepted value (None if none)
        #
        # Proposers learn about accepted values ONLY through 1B or 2B messages
        # that include v_rnd and v_val from this store.
        # =======================================================================
        self.instances = {}
        
        # =======================================================================
        # LOCAL ACCEPTED HISTORY (for catch-up and debugging)
        # =======================================================================
        # IMPORTANT: This is the history of values THIS acceptor has accepted.
        # It is NOT guaranteed to be the globally decided values.
        # A value is globally decided only when a MAJORITY of acceptors have
        # accepted it for the same instance.
        #
        # This history is sent to proposers in 1B/2B messages as a hint.
        # Proposers should NOT treat this as authoritative "decided" state.
        # =======================================================================
        self.accepted_values = {}  # {instance: value}

    def run(self):
        logging.info(f"-> acceptor {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")

            decoded = decode_message(msg)
            
            msg_type = decoded.get('type')
            inst = decoded.get('inst')
            
            if msg_type == "1A":
                self.handle_prepare(decoded)
            elif msg_type == "2A":
                self.handle_propose(decoded)
            elif msg_type == "CATCHUP_REQUEST":
                self.handle_catchup_request(decoded)

    def handle_prepare(self, msg):
        inst = msg['inst']
        rnd_val = msg['rnd']
        
        # Get state for this instance
        rnd, v_rnd, v_val = self.instances.get(inst, ({'bal': 0, 'pid': 0}, {'bal': 0, 'pid': 0}, None))
        
        logging.debug(f"Acceptor {self.id}: received 1A (PREPARE) for inst={inst}, rnd={rnd_val}")
        
        # If message has a higher round number, update the state and send a 1B (PROMISE) message to the proposer
        if RndGeq(rnd_val, rnd):
            rnd = rnd_val
            # update acceptor state for current instance
            self.instances[inst] = (rnd, v_rnd, v_val)
            
            promise_msg = {
                'type': '1B',
                'inst': inst,
                'rnd': rnd,
                'v_rnd': v_rnd,
                'v_val': v_val,
                'aid': self.id,
                'accepted_values': self.accepted_values  # send history of accepted values
            }
            logging.info(f"Acceptor {self.id}: sending 1B (PROMISE) for inst={inst}, rnd={rnd}, v_rnd={v_rnd}")
            self.s.sendto(json.dumps(promise_msg).encode(), self.config["proposers"])
        else:
            # Just ignore the message
            pass
    
    def handle_propose(self, msg):
        inst = msg['inst']
        rnd_val = msg['rnd']
        c_val = msg['val']
        
        # Get state for this instance
        rnd, v_rnd, v_val = self.instances.get(inst, ({'bal': 0, 'pid': 0}, {'bal': 0, 'pid': 0}, None))
        
        logging.debug(f"Acceptor {self.id}: received 2A (PROPOSE) for inst={inst}, rnd={rnd_val}, val={c_val}")
        
        if RndGeq(rnd_val, rnd):
            rnd = rnd_val
            v_rnd = rnd_val
            v_val = c_val
            self.instances[inst] = (rnd, v_rnd, v_val)
            
            # Track this accepted value in history
            self.accepted_values[inst] = c_val
            
            accepted_msg = {
                'type': '2B',
                'inst': inst,
                'v_rnd': v_rnd,
                'v_val': v_val,
                'aid': self.id,
                'accepted_values': self.accepted_values
            }
            logging.info(f"Acceptor {self.id}: sending 2B (ACCEPTED) for inst={inst}, v_rnd={v_rnd}")
            
            # Send 2B to proposers (for Phase 2 completion)
            self.s.sendto(json.dumps(accepted_msg).encode(), self.config["proposers"])
            
            # Send 2B to learners (Optimization 1: learners decide from majority of 2B)
            self.s.sendto(json.dumps(accepted_msg).encode(), self.config["learners"])
        else:
            # Just ignore the message (no REJECT message)
            pass

    def handle_catchup_request(self, msg):
        """
        Handle catch-up request from a late-joining learner.
        
        Learners that start late need to reconstruct the decided sequence.
        Since acceptors are the FAULT-TOLERANT component, learners query
        acceptors for their accepted_values history.
        
        The learner will aggregate responses from multiple acceptors and
        treat a value as decided only if a MAJORITY of acceptors report it.
        """
        lid = msg.get('lid', 'unknown')
        logging.info(f"Acceptor {self.id}: received CATCHUP_REQUEST from learner {lid}")
        
        response = {
            'type': 'CATCHUP_RESPONSE',
            'aid': self.id,
            'accepted_values': self.accepted_values
        }
        logging.info(f"Acceptor {self.id}: sending CATCHUP_RESPONSE with {len(self.accepted_values)} instances")
        self.s.sendto(json.dumps(response).encode(), self.config["learners"])