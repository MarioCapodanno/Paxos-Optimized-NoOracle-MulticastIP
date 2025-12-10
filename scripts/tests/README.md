# Paxos Test (ours
)
This folder contains test cases for the Paxos implementation. Each test has its own folder:
- `run.sh` - Runs the test
- `check.sh` - Verifies the results
- `README.md` - Explains briefly what the test does


## Running Tests

### Run a single test
```bash
cd scripts/tests/test01_basic
./run.sh
./check.sh
```

## Safety Properties Verified

All tests verify these properties:

| Property | How Verified |
|----------|--------------|
| **Agreement** | `diff learner1.log learner2.log` |
| **Total Order** | Learners have identical sequence |
| **Integrity** | All learned values exist in inputs |
| **Validity** | Only proposed values are learned |

