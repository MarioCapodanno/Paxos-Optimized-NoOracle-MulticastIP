# Learner Catchup Test

| Parameter | Value |
|-----------|-------|
| NUM_VALUES | 100 |
| NUM_CLIENTS | 2 |
| NUM_PROPOSERS | 1 |
| NUM_ACCEPTORS | 3 |
| NUM_LEARNERS | 2 |
| LOSS | 0.0 |
| CATCHUP | true |

## What it verifies
- Late learner catches up via CATCHUP_REQUEST
- Both learners end up with identical logs

## Usage
```bash
./run.sh   # Run the test
./check.sh # Verify results
```
