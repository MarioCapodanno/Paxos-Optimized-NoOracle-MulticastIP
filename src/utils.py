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


def RndGeq(rnd1, rnd2):
    """
    Compare two ballot/round numbers according to TLA+ specification.
    Returns True if rnd1 >= rnd2 in the total order.
    
    Total order: rnd1.bal > rnd2.bal OR (rnd1.bal == rnd2.bal AND rnd1.pid >= rnd2.pid)
    """
    if rnd1 is None:
        rnd1 = {'bal': 0, 'pid': 0}
    if rnd2 is None:
        rnd2 = {'bal': 0, 'pid': 0}
    
    if rnd1['bal'] > rnd2['bal']:
        return True
    elif rnd1['bal'] == rnd2['bal'] and rnd1['pid'] >= rnd2['pid']:
        return True
    else:
        return False