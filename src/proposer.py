import logging
from utils import mcast_receiver, mcast_sender, decode_message, encode_message, RndGeq


class Proposer:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["proposers"])
        self.s = mcast_sender()

        self.ballot_counter = 0
        self.num_acceptors = 3  # as specified in the requirements
        self.quorum = (self.num_acceptors // 2) + 1

        # Current proposal state
        self.current_ballot = None
        self.current_value = None
        self.phase = None

        # for phase 1
        self.promises = {}

        # for phase 2
        self.accepted_count = 0

        # The idea is to check if the number ofpromises with a ballot number,
        # higher than the current one, are reaching the quorum,
        # if so, we retry with a higher ballot number.

        # By doing this we can avoid getting stuck in a situation where a proposer is waiting for promises.
        # There could be the possibility to have a 'livelock' (both proposer keep interrupting each other)
        # but it is very unlikely to happen (the value is decided pretty fast due to network latency)
        # [information taken from "Paxos Made Live" [Chandra et al., 2007, Section 2.3]]
        # N.B. : to implement liveness, we could instead implement a timeout.
        self.ignored_promises = 0

    def run(self):
        logging.info(f"-> proposer {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")

            try:
                # msg received is in tuple format (msg_type, *args)
                decoded = decode_message(msg)
                msg_type = decoded[0]

                # Based on msg_type call the corresponding handler
                if msg_type == "PROMISE":
                    self.handle_promise(decoded)
                elif msg_type == "ACCEPTED":
                    self.handle_accepted(decoded)
                else:
                    # start new proposal
                    self.handle_client_value(msg.decode())
            except Exception as e:
                logging.debug(f"Error processing message: {e}")

    def handle_client_value(self, value):
        """Set the value to propose and start Phase 1."""
        logging.debug(f"Proposer {self.id}: received client value {value}")

        # Start a new proposal
        self.current_value = value
        self.start_phase1()

    def start_phase1(self):
        """Phase1: send PREPARE to acceptors."""
        # Generate the ballot number (it has to be unique)
        self.current_ballot = self.ballot_counter * 100 + self.id
        self.ballot_counter += 1

        # Go to state PREPARE
        self.phase = "PREPARE"
        self.promises = {}
        self.accepted_count = 0
        self.ignored_promises = 0

        prepare_msg = encode_message("PREPARE", self.current_ballot, self.id)

        logging.debug(
            f"Proposer {self.id}: starting Phase 1 with ballot {self.current_ballot}"
        )
        self.s.sendto(prepare_msg, self.config["acceptors"])

    def handle_promise(self, msg):
        """Handle the received PROMISE message from acceptor.
        The 'msg' object has fields: (
            msg_type(in this case "PROMISE", can be ignored),
            ballot,
            proposer_id,
            accepted_ballot,
            accepted_pid,
            accepted_value,
            acceptor_id)
        """
        (
            _,
            ballot,
            proposer_id,
            accepted_ballot,
            accepted_pid,
            accepted_value,
            acceptor_id,
        ) = msg

        # If we get a promise with a higher ballot number, we retry with a higher value
        if self.phase == "PREPARE" and ballot > self.current_ballot:
            self.ignored_promises += 1
            if self.ignored_promises >= self.quorum and self.current_value:
                logging.debug(f"Proposer {self.id}: retrying with higher ballot.")
                self.start_phase1()
            return

        # Ignore if not for our current proposal
        if (
            self.phase != "PREPARE"
            or ballot != self.current_ballot
            or proposer_id != self.id
        ):
            logging.debug(
                f"Proposer {self.id}: ignoring PROMISE (not current proposal)"
            )
            return

        logging.debug(
            f"Proposer {self.id}: received PROMISE from acceptor {acceptor_id}"
        )

        # Store the promise in the proposer promises dictionary (need to take track of the highest accepted round)
        self.promises[acceptor_id] = (accepted_ballot, accepted_pid, accepted_value)

        # Check if we reached the quorum
        if len(self.promises) >= self.quorum:
            logging.debug(
                f"Proposer {self.id}: received quorum of promises, starting Phase 2"
            )
            self.start_phase2()

    def start_phase2(self):
        """Phase 2: send ACCEPT to acceptors."""
        self.phase = "ACCEPT"

        # Select value: use the value from the highest accepted round, or our value
        highest_rnd = (-1, -1)
        selected_value = self.current_value

        # Iterate over promises messages recived to find the highest accepted round
        for acceptor_id, (acc_ballot, acc_pid, acc_value) in self.promises.items():
            if acc_value is not None:
                acc_rnd = (acc_ballot, acc_pid)
                if RndGeq(acc_rnd, highest_rnd):
                    highest_rnd = acc_rnd
                    selected_value = acc_value

        self.current_value = selected_value

        accept_msg = encode_message(
            "ACCEPT", self.current_ballot, self.id, self.current_value
        )

        logging.debug(
            f"Proposer {self.id}: starting Phase 2 with value {self.current_value}"
        )
        self.s.sendto(accept_msg, self.config["acceptors"])

    def handle_accepted(self, msg):
        """Handle ACCEPTED message from acceptor with fields:
        msg_type(in this case "ACCEPTED", can be ignored),
        ballot,
        proposer_id,
        value,
        acceptor_id
        """
        _, ballot, proposer_id, value, acceptor_id = msg

        # Ignore if not for our current proposal
        if (
            self.phase != "ACCEPT"
            or ballot != self.current_ballot
            or proposer_id != self.id
        ):
            logging.debug(
                f"Proposer {self.id}: ignoring ACCEPTED (not current proposal)"
            )
            return

        logging.debug(
            f"Proposer {self.id}: received ACCEPTED from acceptor {acceptor_id}"
        )

        self.accepted_count += 1

        # Check if we have a quorum
        if self.accepted_count >= self.quorum:
            logging.debug(
                f"Proposer {self.id}: quorum reached, sending DECISION to learners"
            )

            decision_msg = encode_message("DECISION", value)
            self.s.sendto(decision_msg, self.config["learners"])

            # Reset for future proposals...
            self.phase = None
            self.current_value = None
