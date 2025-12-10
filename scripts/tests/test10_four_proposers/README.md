# Four Proposers Test

| Parameter | Value |
|-----------|-------|
| NUM_VALUES | 100 |
| NUM_CLIENTS | 2 |
| NUM_PROPOSERS | 4 |
| NUM_ACCEPTORS | 3 |
| NUM_LEARNERS | 2 |
| LOSS | 0.0 |
| CATCHUP | false |

## What it verifies
- Paxos handles high contention correctly
- Proposers retry with higher ballot numbers

## Usage
```bash
./run.sh   # Run the test
./check.sh # Verify results
```
