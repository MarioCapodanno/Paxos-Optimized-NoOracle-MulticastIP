#!/bin/bash

MCAST_IP=239.1.2.3

pkill -f "python3 src/main.py"

# Reset packet loss
sudo iptables -S | grep $MCAST_IP | sed 's/^-A //' | while read rule; do
  sudo iptables -D $rule
done
