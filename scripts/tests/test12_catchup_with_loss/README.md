# Catchup with Message Loss


| Parameter | Value |
|-----------|-------|
| NUM_VALUES | 100 |
| NUM_CLIENTS | 2 |
| NUM_PROPOSERS | 1 |
| NUM_ACCEPTORS | 3 |
| NUM_LEARNERS | 2 |
| LOSS | 0.1 |
| CATCHUP | true |

## What it verifies
- Catchup mechanism robust to message loss
- Retries and timeouts work together

## Usage
```bash
./run.sh   # Run the test
./check.sh # Verify results
```
