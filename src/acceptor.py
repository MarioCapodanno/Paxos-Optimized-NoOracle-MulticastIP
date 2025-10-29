import logging
from utils import mcast_receiver, mcast_sender, decode_message, encode_message, RndGeq


class Acceptor:
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.r = mcast_receiver(config["acceptors"])
        self.s = mcast_sender()

        self.promised_rnd = (-1, -1)  # just initialized to -1,-1
        self.accepted_rnd = (-1, -1)
        self.accepted_value = None

    def run(self):
        logging.info(f"-> acceptor {self.id}")
        while True:
            msg, addr = self.r.recvfrom(2**16)
            logging.debug(f"Received {msg.decode()} from {addr}")

            try:
                decoded = decode_message(msg)
                msg_type = decoded[0]

                if msg_type == "PREPARE":
                    self.handle_prepare(decoded)
                elif msg_type == "ACCEPT":
                    self.handle_accept(decoded)
            except Exception as e:
                logging.debug(f"Error processing message: {e}")

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
