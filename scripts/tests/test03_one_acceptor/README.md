# One Acceptor Test

| Parameter | Value |
|-----------|-------|
| NUM_VALUES | 10 |
| NUM_CLIENTS | 2 |
| NUM_PROPOSERS | 2 |
| NUM_ACCEPTORS | 1 |
| NUM_LEARNERS | 2 |
| LOSS | 0.0 |
| CATCHUP | false |

## What it verifies
- Safety: No incorrect values are learned when majority impossible
- System correctly stalls

## Usage
```bash
./run.sh   # Run the test
./check.sh # Verify results
```
