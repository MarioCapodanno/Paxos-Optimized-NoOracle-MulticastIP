# Paxos Test (ours
)
This folder contains test cases for the Paxos implementation. Each test has its own folder:
- `run.sh` - Runs the test
- `check.sh` - Verifies the results
- `README.md` - Explains what the test does

## Overview

| Test | Description | Key Verification |
|------|-------------|------------------|
| **test01_basic** | Basic functionality with standard config | Consensus works, agreement |
| **test02_two_acceptors** | Only 2 acceptors (no fault tolerance) | Edge case majority |
| **test03_one_acceptor** | Only 1 acceptor (no majority possible) | Safety under stall |
| **test04_message_loss_5** | 5% message loss | Retry mechanism |
| **test05_message_loss_10** | 10% message loss | Higher loss tolerance |
| **test06_message_loss_20** | 20% message loss (stress) | Safety under severe loss |
| **test07_catchup** | Late-joining learner | Catchup mechanism |
| **test08_three_clients** | 3 concurrent clients | Multi-client handling |
| **test09_single_proposer** | Only 1 proposer (no contention) | Optimizations work |
| **test10_four_proposers** | 4 proposers (high contention) | Contention handling |
| **test11_single_learner** | Only 1 learner | Minimal config |
| **test12_catchup_with_loss** | Catchup + 10% loss | Combined stress test |

## Running Tests

### Run a single test
```bash
cd scripts/tests/test01_basic
./run.sh
./check.sh
```

### Run all tests
```bash
./scripts/tests/run_all.sh
```

## Safety Properties Verified

All tests verify these properties:

| Property | How Verified |
|----------|--------------|
| **Agreement** | `diff learner1.log learner2.log` |
| **Total Order** | Learners have identical sequence |
| **Integrity** | All learned values exist in inputs |
| **Validity** | Only proposed values are learned |

