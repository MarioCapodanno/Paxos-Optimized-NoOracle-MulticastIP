# Distributed Algorithms: Paxos implementation

The implementation is accomplished through three milestones:
1) Synod algorithm
2) Multi Paxos
3) Optimizations

TODO: in MILESTONE 2, implement learner catchup (later learner learns values from previous learners)

TODO: optimize learner catchup by removing quorum checks.

BAtches: proposer wait for a while, collect values from 1 or more clients, then do paxos on the whole batch. At some point we will have a decision on the whole batch. The order inside the batch is defined by the proposer.
There will be proposer duty to not proposed again a value that was already been decided in previous batches.

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

## REQUIREMENTS: 

Paxos is a protocol used to solve consensus in asynchronous systems. Simply put, consensus can be used by a set
of processes that need to agree on a single value. More commonly though, processes need to agree on a sequence
of totally ordered values - a problem known as atomic broadcast. In this project, you’ll use the Paxos protocol to
implement atomic broadcast.
In your implementation, four roles need to be provided:
• clients: submit values to proposers
• proposers: coordinate Paxos rounds to propose values to be decided
• acceptors: Paxos acceptors
• learners: learn about the sequence of values as they are decided
Your protocol should always guarantee safety. To guarantee liveness, in a complete Paxos implementation, one of
the proposers should be elected as the leader. For simplicity, in this project, you will not be asked to implement a
leader election oracle. However, you should not make any assumptions about which proposer is the leader, and you
should support more than one proposer. Therefore, if two proposers are proposing in parallel and preventing each
other from executing Phase 2 of the protocol, your implementation should ensure that they will keep trying until one
of them hopefully succeeds.
Assumptions
You may assume crash failures. That is, processes fail by halting and do not recover. This allows the following
simplifications:
• no need to implement a recovery procedure for acceptors or learners
• all state can be kept in memory - no need to use stable storage
Implementation
You should implement your solution respecting the following constraints:
• Your Paxos implementation must be based on IP multicast only. You will support four different multicast
groups, one for each role of the protocol.
• Your solution should be implemented inside the src directory (containing the source code). The other scripts
are used to run the implementation, perform checks on the output and cleanup, generate the plots from the
collected data. In case you want to modify them for testing purposes, you should make sure that your final
implementation works with the original scripts.
• You are not allowed to introduce synchrony in the system, i.e., you are not allowed to use time.sleep() or
similar procedures.
There will be 3 milestones for the implementation:
1. Synod algorithm: the basic version of Paxos seen in class. It supports the decision of a single value.
2. MultiPaxos: an extension of the Synod algorithm to support atomic broadcast. It supports the decision of
multiple values in the same total order.
3. Optimizations: we will focus on 3 of them:
• One communication step is saved by allowing acceptors to send the PH2B messages directly to the learners.
• Two more communication steps are saved on the critical path by the proposers performing PHASE1 before
receiving the value from the clients.
• With batching, more values can be decided in a single instance of the Synod algorithm.
Your implementation should guarantee the following:
• message loss or processes crashing should never violate safety (total order, agreement or integrity)
• if a majority of acceptors are killed, no progress should be made (asynchronous consensus assumption)
• learning values must be possible if there are a majority of acceptors and 1 of each other role (no crashes or
message loss)
Here are some tests you may perform on your solution:
1. Proposing 100 values per client (2 clients, 2 proposers, 3 acceptors, 2 learners). Check that learners learn values
in total order. Check that values that were proposed were learned. Repeat with 1000 and 10000 values per client.
2. Repeat test 1 with only 2 acceptors.
3. Repeat test 1 with only 1 acceptor.
4. Repeat test 1 with some % of message loss.
5. Learners catch up. Start 3 acceptors, 1 proposer, and 1 learner. Clients start proposing values. After some time,
start an additional learner. Check that learners learn values in total order. Specifically, the newer learner needs
to learn previous values.
Please check the README of the project for further information.
Deliverables
Your submissions should include the content of the src folder only, as we assume all other scripts are left unchanged.
In case you needed to modify the scripts, please add a report to the submission that explains why and how you modified
them. In this case, include the modified scripts in the submission too.


At the end of the project we expect a working system, that can be easily tested on a cluster of machines.
Presentations
Each group will have 5 minutes to present the implementation. The implementation should include:
• A general overview of the system design
• The main difficulties you encountered, and how you addressed them
• The final takeaways from the project
Each presentation will be followed by a few questions. We expect all people in the group to be able to answer them.
The grade of your project will also consider the source code correctness, completeness, readability and performance.
2