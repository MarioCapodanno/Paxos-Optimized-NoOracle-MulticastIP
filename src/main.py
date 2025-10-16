import sys
import argparse
import logging
from client import Client
from proposer import Proposer
from acceptor import Acceptor
from learner import Learner
from utils import load_config


def main():
    parser = argparse.ArgumentParser(description="Paxos DA project")
    parser.add_argument("-r", "--role", help="Role of the process")
    parser.add_argument("-p", "--pid", help="Id of the process")
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Enable debug output", default=False
    )
    args = parser.parse_args()

    role = args.role
    node_id = int(args.pid)

    config = load_config()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format=f"[{str.capitalize(role[0])}{node_id}:%(levelname)s] %(message)s",
    )

    if role == "client":
        node = Client(config, node_id)
        node.run()
    elif role == "proposer":
        node = Proposer(config, node_id)
        node.run()
    elif role == "acceptor":
        node = Acceptor(config, node_id)
        node.run()
    elif role == "learner":
        node = Learner(config, node_id)
        node.run()
    else:
        print("Unknown role. Use client | proposer | acceptor | learner")


if __name__ == "__main__":
    main()
