import json
import logging
import select
import time
from utils import mcast_receiver, mcast_sender, RndGeq


class Proposer:
    """
    Paxos Proposer - Coordinator role in the consensus protocol.
    
    IMPORTANT: Proposers are NOT the fault-tolerant state holders.
    They are ephemeral coordinators that drive the protocol.
    The authoritative consensus state lives in the ACCEPTORS.
    
    If a proposer crashes, another proposer can continue from acceptor state.
    Reference: Lamport, "Paxos Made Simple", 2001.
    """
    
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.client_r = mcast_receiver(config["proposers"])
        self.s = mcast_sender()

        # Number of acceptors and majority threshold for quorum
        self.num_acceptors = 3  # as specified in the requirements
        self.majority = (self.num_acceptors // 2) + 1

        # =======================================================================
        # ROUND COUNTER
        # =======================================================================
        # Used to generate unique, monotonically increasing proposal numbers.
        # Combined with proposer id to avoid collisions: c_rnd = {bal, pid}
        # =======================================================================
        self.round_counter = 0

        # =======================================================================
        # INSTANCE TRACKING
        # =======================================================================
        # The next instance number to propose for.
        # Instances are decided in order: 0, 1, 2, ...
        # A proposer only advances to instance N+1 after instance N is decided.
        # =======================================================================
        self.next_instance = 0

        # =======================================================================
        # CLIENT VALUE BUFFER
        # =======================================================================
        # Buffer for client values received during Paxos message processing.
        # This prevents losing values if new client requests arrive while
        # the proposer is still running a Paxos round.
        # =======================================================================
        self.pending_client_values = []
        
        # =======================================================================
        # LOCAL DECISION VIEW (soft state, non-authoritative)
        # =======================================================================
        # Tracks which instances this proposer has seen decided.
        # This is a LOCAL VIEW, not the authoritative state.
        # The authoritative state is in the acceptors.
        # Used for logging and local bookkeeping only.
        # =======================================================================
        self.decided_instances = {}  # inst -> decided_value
        
        # =======================================================================
        # ACCEPTED HISTORY (hints from acceptors, NOT decisions)
        # =======================================================================
        # Values observed as accepted by some acceptor.
        # IMPORTANT: These are NOT guaranteed to be globally decided.
        # A single acceptor's accept is just a vote, not a decision.
        # Used for debugging/logging only, NOT for control flow.
        # =======================================================================
        self.accepted_history = {}  # inst -> value (hints only)
        
        # =======================================================================
        # PRE-PHASE1 STATE (Optimization 2)
        # =======================================================================
        # Run Phase 1 early (before client value arrives) and reuse the result.
        # This reduces latency when we already have a prepared instance.
        # =======================================================================
        self.prepared_instance = None   # instance number
        self.prepared_round = None      # c_rnd for that instance
        self.prepared_promises = None   # dict[aid] -> 1B message

    def run(self):
        """
        Main loop: receive client values and run Paxos to decide them.
        
        For each client value:
        1. Run Paxos on the current instance until some value is decided.
        2. If our value was decided, move to next client request.
        3. If a different value was decided (due to prior acceptor state),
           advance to the next instance and retry our value there.
        
        Optimization 2: Pre-Phase1
        Before waiting for a client value, we try to run Phase 1 for the
        next instance. This way, when a value arrives, we can skip Phase 1
        and go directly to value selection + Phase 2.
        
        IMPORTANT: We do NOT skip client values based on "already seen" heuristics.
        Every client request is processed through Paxos to guarantee integrity.
        Reference: Cachin et al., "Introduction to Reliable and Secure
                   Distributed Programming", Ch. 3 (atomic broadcast).
        """
        logging.info(f"-> proposer {self.id}")
        
        while True:
            # ===================================================================
            # PRE-PHASE1: Prepare next instance while idle (Optimization 2)
            # ===================================================================
            if not self.pending_client_values:
                if (self.prepared_instance != self.next_instance or
                    self.prepared_promises is None):
                    # Try to prepare the next instance
                    next_round = {'bal': self.round_counter + 1, 'pid': self.id}
                    ok, promises = self._run_phase1(self.next_instance, next_round)
                    if ok:
                        self.round_counter += 1
                        self.prepared_instance = self.next_instance
                        self.prepared_round = next_round
                        self.prepared_promises = promises
                        logging.debug(f"Pre-Phase1 complete for instance {self.next_instance}")
            
            # ===================================================================
            # GET NEXT CLIENT VALUE
            # ===================================================================
            if self.pending_client_values:
                # Process buffered values first (received during Paxos rounds)
                msg_data = self.pending_client_values.pop(0)
            else:
                # Wait for new client request
                msg, _ = self.client_r.recvfrom(2**16)
                data = json.loads(msg.decode())
                 
                # Ignore Paxos protocol messages at the top level.
                # These are handled inside run_full_paxos.
                if data.get('type') in ['1B', '2B', 'DECISION']:
                    continue
                
                msg_data = data
            
            # Extract the value to propose
            value = msg_data.get('value')
            
            # ===================================================================
            # RUN PAXOS UNTIL THIS VALUE IS DECIDED
            # ===================================================================
            # We keep trying until our value is chosen at some instance.
            # If a different value is chosen (due to prior acceptor state),
            # we advance to the next instance and retry.
            # ===================================================================
            decided = False
            while not decided:
                success, decided_value = self.run_full_paxos(value)
                
                if success:
                    # Record decision in our local view (soft state)
                    self.decided_instances[self.next_instance] = decided_value
                    
                    if decided_value == value:
                        # Our value was decided at this instance
                        decided = True
                        logging.info(f"Value '{value}' decided at instance {self.next_instance}")
                    else:
                        # A different value was decided (from prior acceptor state).
                        # This preserves safety: we adopt what was already chosen.
                        # Our value will be retried at the next instance.
                        logging.info(f"Instance {self.next_instance} decided with different value '{decided_value}', retrying our value at next instance")
                    
                    # Advance to next instance (this one is now closed)
                    self.next_instance += 1
                else:
                    # Paxos round failed (timeout or preemption).
                    # Stay on the same instance and retry with a higher round.
                    logging.debug(f"Paxos round failed for instance {self.next_instance}, retrying")

    def _extract_accepted_history(self, data):
        """
        Extract accepted values history from acceptor messages (1B/2B).
        
        IMPORTANT: This is for DEBUGGING and LOGGING only.
        The values in 'accepted_values' are what a single acceptor has accepted.
        They are NOT guaranteed to be globally decided values.
        
        A value is globally decided only when a MAJORITY of acceptors have
        accepted it. We cannot infer that from a single acceptor's history.
        
        Therefore, we store this in accepted_history (hints) and do NOT use
        it to skip client requests or make control flow decisions.
        """
        if 'accepted_values' in data:
            for inst_num, val in data['accepted_values'].items():
                inst_num = int(inst_num)
                # Only log if we haven't seen this instance before
                if inst_num not in self.accepted_history:
                    logging.debug(f"Hint from acceptor: instance {inst_num} has accepted value '{val}'")
                # Store as hint (may be overwritten by later messages)
                self.accepted_history[inst_num] = val

    def _run_phase1(self, inst, c_rnd):
        """
        Run Phase 1 (PREPARE/PROMISE) for given instance and round.
        
        Returns:
            (True, promises_dict): Majority of 1B with matching rnd
            (False, None): Timeout or preemption
        """
        prepare_msg = {
            'type': '1A',
            'inst': inst,
            'rnd': c_rnd
        }
        self.s.sendto(json.dumps(prepare_msg).encode(), self.config["acceptors"])
        
        promises = {}
        timeout = 0.25
        start_time = time.time()
        
        while len(promises) < self.majority:
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                logging.debug(f"Phase 1 timeout for instance {inst}")
                return (False, None)
            
            remaining_time = timeout - elapsed
            select_timeout = min(0.1, remaining_time)
            ready, _, _ = select.select([self.client_r], [], [], select_timeout)
            if not ready:
                continue
            
            for sock in ready:
                response, _ = sock.recvfrom(2**16)
                data = json.loads(response.decode())
                
                # Buffer client values
                if 'type' not in data:
                    self.pending_client_values.append(data)
                    continue
                
                if (data.get('inst') == inst and 
                    data.get('type') == '1B' and 
                    data.get('rnd') == c_rnd):
                    self._extract_accepted_history(data)
                    aid = data.get('aid', len(promises))
                    promises[aid] = data
                    
                elif data.get('type') == '1B' and data.get('inst') == inst:
                    received_rnd = data.get('rnd')
                    if received_rnd and RndGeq(received_rnd, c_rnd) and received_rnd != c_rnd:
                        logging.debug(f"Preempted in Phase 1 by round {received_rnd}")
                        return (False, None)
        
        return (True, promises)

    def _clear_prepared(self):
        """Clear prepared Phase 1 state."""
        self.prepared_instance = None
        self.prepared_round = None
        self.prepared_promises = None

    def run_full_paxos(self, original_value):
        """
        Run a complete Paxos round (Phase 1 + Phase 2) for the current instance.
        
        Args:
            original_value: The value we want to propose.
        
        Returns:
            (True, decided_value): If a value was decided for this instance.
                                   decided_value may differ from original_value
                                   if acceptors had prior accepted state.
            (False, None): If the round failed (timeout or preemption).
        
        The Paxos safety rule:
        - In Phase 1, we learn what values acceptors have already accepted.
        - If any acceptor has accepted a value, we MUST propose that value
          (the one with the highest v_rnd) to preserve safety.
        - Only if no acceptor has accepted anything can we propose our own value.
        
        Reference: Lamport, "Paxos Made Simple", 2001.
        """
        inst = self.next_instance
        
        # ===================================================================
        # PHASE 1: Reuse prepared or run fresh (Optimization 2)
        # ===================================================================
        if (self.prepared_instance == inst and
            self.prepared_round is not None and
            self.prepared_promises is not None):
            # Reuse prepared Phase 1
            self.c_rnd = self.prepared_round
            promises = self.prepared_promises
            logging.info(f"Reusing prepared Phase 1 for instance {inst} with round {self.c_rnd}")
        else:
            # Run fresh Phase 1
            self.round_counter += 1
            self.c_rnd = {'bal': self.round_counter, 'pid': self.id}
            logging.info(f"Running fresh Phase 1 for instance {inst} with round {self.c_rnd}")
            
            ok, promises = self._run_phase1(inst, self.c_rnd)
            if not ok:
                self._clear_prepared()
                return (False, None)
        
        # Clear prepared state (will be used or was just used)
        self._clear_prepared()
        
        # ===================================================================
        # VALUE SELECTION (Paxos safety rule)
        # ===================================================================
        # We MUST propose the value with the highest v_rnd among all promises.
        # This ensures we don't overwrite a value that may have been chosen.
        # Only if no acceptor has accepted anything (all v_rnd = 0) can we
        # propose our original_value.
        # ===================================================================
        max_v_rnd = {'bal': 0, 'pid': 0}
        value_to_propose = original_value
        
        for promise in promises.values():
            v_rnd = promise.get('v_rnd', {'bal': 0, 'pid': 0})
            if RndGeq(v_rnd, max_v_rnd):
                max_v_rnd = v_rnd
                if v_rnd.get('bal', 0) > 0:
                    # Adopt the value from the highest round
                    value_to_propose = promise.get('v_val')
        
        if value_to_propose != original_value:
            logging.info(f"Adopting previously accepted value '{value_to_propose}' (from round {max_v_rnd})")
        
        # ===================================================================
        # PHASE 2: PROPOSE (2A) and collect ACCEPTED (2B)
        # ===================================================================
        propose_msg = {
            'type': '2A',
            'inst': self.next_instance,
            'rnd': self.c_rnd,
            'val': value_to_propose
        }
        self.s.sendto(json.dumps(propose_msg).encode(), self.config["acceptors"])
        
        acceptances = {}
        timeout = 0.25
        start_time = time.time()
        
        while len(acceptances) < self.majority:
            #=================TIMEOUT HANDLING WITH SELECT()=================
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                logging.warning(f"Phase 2 timeout - {len(acceptances)}/{self.majority} acceptances")
                return (False, None)
            
            remaining_time = timeout - elapsed
            select_timeout = min(0.1, remaining_time)
            ready, _, _ = select.select([self.client_r], [], [], select_timeout)
            if not ready:
                continue
            #=================TIMEOUT HANDLING WITH SELECT()=================

            for sock in ready:
                response, _ = sock.recvfrom(2**16)
                data = json.loads(response.decode())
                
                if 'type' not in data:
                    self.pending_client_values.append(data)
                    continue
                
                if (data.get('inst') == self.next_instance and 
                    data.get('type') == '2B' and 
                    data.get('v_rnd') == self.c_rnd):
                    
                    self._extract_accepted_history(data)
                    aid = data.get('aid', len(acceptances))
                    acceptances[aid] = data
                    
                elif data.get('type') == '2B' and data.get('inst') == self.next_instance:
                    received_v_rnd = data.get('v_rnd')
                    if received_v_rnd and RndGeq(received_v_rnd, self.c_rnd) and received_v_rnd != self.c_rnd:
                        logging.warning(f"Preempted in Phase 2 by round {received_v_rnd}")
                        return (False, None)
        
        # ===================================================================
        # DECISION REACHED
        # ===================================================================
        # A majority of acceptors have accepted (inst, c_rnd, value_to_propose).
        # By Paxos safety, this value is now the unique decided value for
        # this instance. No other value can ever be decided for this instance.
        #
        # Note: We do NOT send a separate DECISION message to learners.
        # Learners receive 2B directly from acceptors and decide when they
        # see a majority (Optimization 1: direct learning from acceptors).
        # ===================================================================
        logging.info(f"DECIDED instance {self.next_instance} = '{value_to_propose}'")
        
        return (True, value_to_propose)

