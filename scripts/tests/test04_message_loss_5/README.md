# 5% Message Loss Test

| Parameter | Value |
|-----------|-------|
| NUM_VALUES | 100 |
| NUM_CLIENTS | 2 |
| NUM_PROPOSERS | 2 |
| NUM_ACCEPTORS | 3 |
| NUM_LEARNERS | 2 |
| LOSS | 0.05 |
| CATCHUP | false |

## What it verifies
- Timeout-based retries handle message loss
- Safety maintained under network issues

## Usage
```bash
./run.sh   # Run the test
./check.sh # Verify results
```
