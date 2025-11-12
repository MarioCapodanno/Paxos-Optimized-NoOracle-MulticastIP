import json
import socket
import struct
import os


def load_config(path=""):
    if path == "":
        script_dir = os.path.dirname(os.path.abspath(__file__))
        path = script_dir + "/../logs/config.json"

    with open(path, "r") as f:
        config = {}
        for role, value in dict(json.load(f)).items():
            config[role] = (value["ip"], int(value["port"]))
        return config


def mcast_receiver(hostport):
    """create a multicast socket listening to the address"""
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recv_sock.bind(hostport)

    mcast_group = struct.pack("4sl", socket.inet_aton(hostport[0]), socket.INADDR_ANY)
    recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mcast_group)
    return recv_sock


def mcast_sender(ttl=1):
    """create a udp socket"""
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    send_sock.setsockopt(
        socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("b", ttl)
    )
    return send_sock


def decode_message(msg_bytes):
    """Decode a Paxos message from bytes to tuple"""
    msg_str = msg_bytes.decode()
    
    # Try JSON first (for MultiPaxos)
    try:
        data = json.loads(msg_str)
        if isinstance(data, dict) and 'type' in data:
            return data
    except (json.JSONDecodeError, ValueError):
        pass
    
    # Fall back to pipe-separated format (for backward compatibility)
    parts = msg_str.split("|")  # msg is in the format "msg_type|arg1|arg2|..."

    # Convert numeric fields
    converted_parts = [parts[0]]  # msg_type stays as string
    for part in parts[1:]:
        if part == "None":
            converted_parts.append(None)
        else:
            # Try to convert to int
            try:
                converted_parts.append(int(part))
            except ValueError:
                converted_parts.append(part)

    return tuple(converted_parts)
