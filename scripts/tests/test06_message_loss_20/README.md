# 20% Message Loss Test

| Parameter | Value |
|-----------|-------|
| NUM_VALUES | 50 |
| NUM_CLIENTS | 2 |
| NUM_PROPOSERS | 2 |
| NUM_ACCEPTORS | 3 |
| NUM_LEARNERS | 2 |
| LOSS | 0.2 |
| CATCHUP | false |

## What it verifies
- System remains safe under severe network conditions
- Agreement property never violated

## Usage
```bash
./run.sh   # Run the test
./check.sh # Verify results
```
