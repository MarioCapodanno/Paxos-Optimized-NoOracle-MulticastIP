# Distributed Algorithms: Paxos implementation

The implementation is accomplished through three milestones:
1) Synod algorithm
2) Multi Paxos
3) Optimizations

## Dependencies and preliminaries

The bash scripts can be runned from a clean Ubuntu 24.04 image. You may need `sudo` access to run the scripts.

Some of the scripts use `iptables` to simulate message loss through firewall rules. Adding these rules on your local machine is risky, as they greatly impact the performance of the network communication. The suggestion is to run everything inside a virtual machine (e.g., with `multipass`).

Depending on the network interface used, ip multicast might not be enabled. You can check using `ifconfig` and checking that the "MULTICAST" flag is set. You *might* have to enable it using:

```
ifconfig IFACE multicast
```

where `IFACE` is the name of the interface. Using a connected cable/wifi interface probably will not have this problem (e.g. "eth0", "wlan0").

## How to run the tests

1. `cd` to this directory. All the scripts should be run from here. Make sure the scripts in the `scripts` folder have executable permissions.

2. Permorm a run and check the results as follows:
```
scripts/run.sh -n 1000
scripts/check.sh
```

3. Once `gnuplot` is installed with `sudo apt install gnuplot`, you can collect latency data and plot it with the following commands:
```
scripts/run.sh -n 1000 -l 0
gnuplot scripts/plotting/cdf.gp
gnuplot scripts/plotting/cartesian.gp
```
These will generate the two plots in the logs folder. Not that clients need to learn the values to compute the latencies. Reset the corresponding flag in the client to disable this feature: then, only the learners will learn.

## Caveats/Tips

1. The scripts will try to `pkill` your processes (SIGTERM). You might need to "flush" the output of your learners to make sure values are printed when learned.

2. The output of your "learners" should be **ONLY** the values learned, one per line. Anything else will fail the checks.

3. The scripts have many parameters to test for different cases:
    - `-n X` allows each client to generate `X` values
    - `-d` enables debug
    - `--loss X` drops `X`% (e.g., `0.1`) of sent messages
    - `--catchup` starts a late learner to test catchup
    - `-s` sets the sleep time used in the scripts: it may be increased if a lot of values are sent
    - `--ip` changes the default multicast ip address (`239.1.2.3`)
    - `-c` sets the number of clients
    - `-p` sets the number of proposers
    - `-a` sets the number of acceptors
    - `-l` sets the number of learners

4. In case you specify a loss probability and kill the script, you may need to manually remove the firewall rule. Run `scripts/cleanup.sh` to cleanup. You can check the active rules with `sudo iptables -L INPUT -v --line-numbers`.

5. If you started a run, but then you stopped it with `Ctrl+C`, it is a good idea to close the current terminal and reopen another one, since some processes may still be running and they may still send/receive messages for a new run.
