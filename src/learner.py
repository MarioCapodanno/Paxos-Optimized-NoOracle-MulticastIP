import sys
import json
import logging
import time
from utils import mcast_receiver, mcast_sender, decode_message


class Learner:
    """
    Paxos Learner - Learns and delivers decided values in total order.
    
    LEARNING FROM 2B (Optimization 1):
    Learners receive 2B messages directly from acceptors.
    A value is decided when a MAJORITY of acceptors send 2B for (inst, val).
    This is the standard Paxos "chosen" definition.
    
    CATCH-UP MECHANISM:
    Late-joining learners query ACCEPTORS (the fault-tolerant component)
    to reconstruct the decided sequence using the same majority rule.
    
    Reference: Lamport, "Paxos Made Simple", 2001.
    """
    
    def __init__(self, config, id):
        self.config = config
        
        # Learner Id, not strictly necessary but useful for debugging
        self.id = id
        
        # Socket to receive 2B from acceptors and CATCHUP_RESPONSE
        self.r = mcast_receiver(config["learners"])
        
        # Socket to send CATCHUP_REQUEST to acceptors 
        self.s = mcast_sender()

        # =======================================================================
        # HOW TO GUARANTEE IN-ORDER DELIVERY
        # =======================================================================
        # next: the next instance number we expect to deliver
        # buffer: out-of-order decisions waiting to be delivered, can trigger catch-up requests
        # delivered_instances: set of already delivered instances (to have deduplication)
        #
        # Having that we deliver value of instance N only after N-1.
        # =======================================================================
        self.next = 0
        self.buffer = {}  # {instance -> value}
        self.delivered_instances = set()
        
        # =======================================================================
        # 2B VOTING (primary learning path)
        # =======================================================================
        # When the learner starts "late" or it receives out-of-order instance,
        # it requests catch-up from acceptors to fill the "gap" and the out-of-order
        # value is stored in the buffer.
        #
        # Example:
        # - Learner 1: next = 0, buffer = {}, delivered_instances = {}
        # - Learner 2: next = 1, buffer = {}, delivered_instances = {0}
        # - Proposer 1: send DECISION for instance 1 to Learner 1 and Learner 2
        # - Learner 2 receives DECISION for instance 1, take it and deliver it since it is in order.
        #   (Learner_2_state: next = 2, buffer = {}, delivered_instances = {0, 1})
        # - Learner 1 receives DECISION for instance 1, but it is waiting for instance 0,
        #   so it put the value in the buffer and requests catch-up from acceptors.
        #   (Learner_1_state: next = 0, buffer = {1: val}, delivered_instances = {})
        #
        # When the learner receives the catch-up response, for each instance it checks if 
        # the value is accepted by a majority of acceptors.
        # If so, it delivers the value and updates the delivered_instances set with:
        #   
        #    catchup_votes[inst][val] = count of acceptors that accepted val for inst
        #
        # Example:
        # - Learner 1: catchup_votes = {}
        # - Acceptor 1: sends accepted_values = { (0, val0), (1, val1) } to Learner 1
        # - Learner 1 receives CATCHUP_RESPONSE from Acceptor 1, updates catchup_votes with:
        #   catchup_votes[0] = 1 // since we received val0 from Acceptor 1
        #   catchup_votes[1] = 1 // since we received val1 from Acceptor 1
        # 
        # - Acceptor 2: sends accepted_values = { (0, val0), (1, val1) } to Learner 1
        # - Learner 1 receives CATCHUP_RESPONSE from Acceptor 2, updates catchup_votes with:
        #   catchup_votes[0] = 2 // since we received val0 from Acceptor 1 and Acceptor 2
        #   catchup_votes[1] = 2 // since we received val1 from Acceptor 1 and Acceptor 2
        # 
        # - Learner 1: since catchup_votes[0] = 2 and catchup_votes[1] = 2 are >= majority (2), 
        #   it delivers val0 for instance 0 and val1 for instance 1.
        # =======================================================================
        self.num_acceptors = 3
        self.majority = (self.num_acceptors // 2) + 1
        self.twoB_votes = {}  # inst -> {val: set(aid)}
        
        # =======================================================================
        # CATCH-UP STATE (for late-joining learners)
        # =======================================================================
        # catchup_votes uses the same majority rule but aggregates from
        # CATCHUP_RESPONSE messages instead of live 2B.
        # =======================================================================
        self.catchup_votes = {}  # inst -> {val: count}
        self.last_catchup_time = 0

    def run(self):
        """
        Main loop: receive and process 2B and CATCHUP_RESPONSE messages.
        
        On startup, sends a CATCHUP_REQUEST to acceptors to learn about
        any decisions that may have been made before this learner started.
        """
        logging.debug(f"-> learner {self.id}")

        # ===================INITIAL_CATCHUP===============================
        # Compose the catch-up request message, 
        catchup_msg = {
            'type': 'CATCHUP_REQUEST',
            'lid': self.id
        }
        self.s.sendto(json.dumps(catchup_msg).encode(), self.config["acceptors"])
        logging.debug(f"Learner {self.id}: sent CATCHUP_REQUEST to acceptors")
        # ===================================================================
        
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")

            decoded = decode_message(msg)
            msg_type = decoded.get('type')
            
            if msg_type == '2B':
                self.handle_2B(decoded)
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
            self.delivered_instances.add(curr_inst)
            
            # output the current instance value
            logging.debug(f"Learner {self.id}: delivering instance {curr_inst} = {value_to_print}")

            # === FIX FOR BATCHING ===
            # Check if it is a container (List or Tuple) or a single value
            if isinstance(value_to_print, (list, tuple)):
                # It is a Batch! Iterate and print one by one
                for item in value_to_print:
                    print(item)
            else:
                # It is a single value (old version)
                print(value_to_print)

            sys.stdout.flush()
            
            self.next += 1
    
    def handle_2B(self, msg):
        """
        Handle 2B (ACCEPTED) message from an acceptor.
        
        Count votes per (inst, val). When a majority of acceptors
        send 2B for the same (inst, val), the value is decided.
        """
        inst = msg['inst']
        raw_val = msg['v_val']
        aid = msg['aid']
        
        # FIX FOR BATCHING: Convert the list to a tuple to use as a dict key
        # If raw_val is a list, convert it to a tuple. If it's a string, leave it as is.
        if isinstance(raw_val, list):
            val = tuple(raw_val)
        else:
            val = raw_val
        
        # Skip if already decided
        if inst in self.delivered_instances or inst in self.buffer:
            return
        
        # Initialize vote tracking for this instance
        if inst not in self.twoB_votes:
            self.twoB_votes[inst] = {}
        
        # Initialize vote set for this value
        if val not in self.twoB_votes[inst]:
            self.twoB_votes[inst][val] = set()
        
        # Add this acceptor's vote (set avoids double-counting)
        self.twoB_votes[inst][val].add(aid)
        
        # If majority reached, deliver the value
        if len(self.twoB_votes[inst][val]) >= self.majority:
            logging.debug(f"Learner {self.id}: 2B majority for instance {inst} = '{val}'")
            self._deliver_value(inst, val, source="2B")

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
        
        for inst_str, raw_val in accepted_values.items():
            inst = int(inst_str)

            # === FIX FOR BATCHING ===
            # If raw_val is a list (batch), convert it to a tuple to use as a dict key
            if isinstance(raw_val, list):
                val = tuple(raw_val)
            else:
                val = raw_val
            
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
            Common delivery logic.
            Simplification: We let deliver_values() handle the unpacking and printing
            to avoid code duplication.
            """
            # Skip if already delivered or buffered
            if inst in self.delivered_instances or inst in self.buffer:
                return
            
            # Put always in the buffer, even if it is in order.
            # deliver_values() is smart enough to take it immediately
            # and print it correctly (handling batching).
            self.buffer[inst] = val
            
            logging.debug(f"Learner {self.id}: received inst {inst}, handing over to delivery loop")
            
            # Call the delivery engine
            self.deliver_values()

    def _request_catchup_if_needed(self):
        """
        Send a catch-up request to acceptors if we haven't done so recently.
        Used to fill gaps when we receive out-of-order instances.
        """
        now = time.time()
        
        # Rate limit: don't spam catch-up requests (wait at least 0.5s between requests)
        if now - self.last_catchup_time < 0.5:
            return
        
        catchup_msg = {
            'type': 'CATCHUP_REQUEST',
            'lid': self.id
        }
        self.s.sendto(json.dumps(catchup_msg).encode(), self.config["acceptors"])
        self.last_catchup_time = now
        
        logging.debug(f"Learner {self.id}: gap catch-up request sent (next={self.next})")
