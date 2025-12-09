# Single Proposer Test

| Parameter | Value |
|-----------|-------|
| NUM_VALUES | 100 |
| NUM_CLIENTS | 2 |
| NUM_PROPOSERS | 1 |
| NUM_ACCEPTORS | 3 |
| NUM_LEARNERS | 2 |
| LOSS | 0.0 |
| CATCHUP | false |

## What it verifies
- System works without proposer competition
- Pre-Phase1 optimization is effective

## Usage
```bash
./run.sh   # Run the test
./check.sh # Verify results
```
