import sys
import json
import logging
import select
from utils import mcast_receiver, mcast_sender, decode_message


class Learner:
    """
    Paxos Learner - Learns and delivers decided values in total order.
    
    The learner receives DECISION messages from proposers and outputs
    the decided values in instance order (0, 1, 2, ...).
    
    CATCH-UP MECHANISM:
    Late-joining learners query ACCEPTORS (the fault-tolerant component)
    to reconstruct the decided sequence. A value is considered decided
    only if a MAJORITY of acceptors report having accepted it.
    
    Reference: Lamport, "Paxos Made Simple", 2001.
    """
    
    def __init__(self, config, id):
        self.config = config
        self.id = id
        
        # Socket to receive DECISION messages from proposers
        self.r = mcast_receiver(config["learners"])
        
        # Socket to send CATCHUP_REQUEST to acceptors
        self.s = mcast_sender()

        # =======================================================================
        # IN-ORDER DELIVERY STATE
        # =======================================================================
        # next: the next instance number we expect to deliver
        # buffer: out-of-order decisions waiting to be delivered
        # delivered_instances: set of already delivered instances (dedup)
        #
        # This ensures total order: we only print instance N after N-1.
        # =======================================================================
        self.next = 0
        self.buffer = {}  # instance -> value (for out-of-order messages)
        self.delivered_instances = set()
        
        # =======================================================================
        # CATCH-UP STATE
        # =======================================================================
        # For reconstructing decisions from acceptor responses.
        # catchup_votes[inst][val] = count of acceptors that accepted val for inst
        # We need a MAJORITY to consider a value decided.
        # =======================================================================
        self.num_acceptors = 3  # as specified in the requirements
        self.majority = (self.num_acceptors // 2) + 1
        self.catchup_votes = {}  # inst -> {val: count}

    def run(self):
        """
        Main loop: receive and process DECISION and CATCHUP_RESPONSE messages.
        
        On startup, sends a CATCHUP_REQUEST to acceptors to learn about
        any decisions that may have been made before this learner started.
        """
        logging.debug(f"-> learner {self.id}")
        
        # ===================================================================
        # CATCH-UP: Request history from acceptors on startup
        # ===================================================================
        # This allows late-joining learners to reconstruct the decided
        # sequence from the fault-tolerant acceptor state.
        # ===================================================================
        catchup_msg = {
            'type': 'CATCHUP_REQUEST',
            'lid': self.id
        }
        self.s.sendto(json.dumps(catchup_msg).encode(), self.config["acceptors"])
        logging.debug(f"Learner {self.id}: sent CATCHUP_REQUEST to acceptors")
        
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")

            decoded = decode_message(msg)
            msg_type = decoded.get('type')
            
            if msg_type == 'DECISION':
                self.handle_decision(decoded)
            elif msg_type == 'CATCHUP_RESPONSE':
                self.handle_catchup_response(decoded)

    def deliver_values(self):
        while self.next in self.buffer:
            curr_inst = self.next
            
            # Skip if already delivered (shouldn't happen, but safety check)
            if curr_inst in self.delivered_instances:
                self.buffer.pop(curr_inst)
                self.next += 1
                continue
            
            value_to_print = self.buffer.pop(curr_inst)
            
            # output the current instance value
            logging.debug(f"Learner {self.id}: delivering instance {curr_inst} = {value_to_print}")
            self.delivered_instances.add(curr_inst)
            print(value_to_print)
            sys.stdout.flush()
            
            self.next += 1
    
    def handle_decision(self, msg):
        inst = msg['inst']
        val = msg['val']
        
        if inst in self.delivered_instances or inst in self.buffer:
            # ignore duplicates (should not happen but still checking)
            return
        
        if inst == self.next:
            # Deliver immediately if in order
            self.delivered_instances.add(inst)
        else:
            # If not in order, buffer it
            self.buffer[inst] = val
        
        if inst == self.next:
            logging.debug(f"Learner {self.id}: delivering instance {inst} = {val}")
            print(val)
            sys.stdout.flush() # becuase we want real-time output
            self.next += 1
            
            # After delivering check if we can deliver more from buffer
            self.deliver_values()
        else:
            # Out of order: already buffered above
            logging.debug(f"Learner {self.id}: learned instance {inst} = '{val}' from DECISION (out of order, buffered)")
            # Try to deliver if this completes a sequence
            self.deliver_values()

    def handle_catchup_response(self, msg):
        """
        Handle catch-up response from an acceptor.
        
        Aggregates accepted_values from multiple acceptors. For each instance,
        if a MAJORITY of acceptors report the same value, we treat it as decided
        and inject it into the normal delivery pipeline.
        
        This is safe because:
        - Once a majority has accepted a value for an instance, no different
          value can ever be chosen for that instance (Paxos safety).
        - We only mark (inst, val) as decided when we have majority confirmation.
        """
        aid = msg.get('aid', 'unknown')
        accepted_values = msg.get('accepted_values', {})
        
        logging.debug(f"Learner {self.id}: received CATCHUP_RESPONSE from acceptor {aid} with {len(accepted_values)} instances")
        
        for inst_str, val in accepted_values.items():
            inst = int(inst_str)
            
            # Skip if already delivered or buffered
            if inst in self.delivered_instances or inst in self.buffer:
                continue
            
            # Update vote counts for (inst, val)
            if inst not in self.catchup_votes:
                self.catchup_votes[inst] = {}
            
            inst_votes = self.catchup_votes[inst]
            inst_votes[val] = inst_votes.get(val, 0) + 1
            
            # If we reached a majority for this (inst, val), treat as decided
            if inst_votes[val] >= self.majority:
                logging.debug(
                    f"Learner {self.id}: catch-up decided instance {inst} = '{val}' "
                    f"(confirmed by {inst_votes[val]} acceptors)"
                )
                self._deliver_value(inst, val, source="catch-up")

    def _deliver_value(self, inst, val, source="DECISION"):
        """
        Common delivery logic for both DECISION messages and catch-up.
        
        Handles in-order delivery: if inst == self.next, deliver immediately;
        otherwise buffer for later delivery.
        """
        # Skip if already delivered or buffered
        if inst in self.delivered_instances or inst in self.buffer:
            return
        
        if inst == self.next:
            # Deliver immediately if in order
            self.delivered_instances.add(inst)
            logging.debug(f"Learner {self.id}: delivering instance {inst} = '{val}' ({source})")
            print(val)
            sys.stdout.flush()
            self.next += 1
            
            # Check if we can deliver more from buffer
            self.deliver_values()
        else:
            # Out of order: buffer it
            self.buffer[inst] = val
            logging.debug(f"Learner {self.id}: buffered instance {inst} = '{val}' ({source}, expected next={self.next})")
            # Try to deliver if this completes a sequence
            self.deliver_values()
