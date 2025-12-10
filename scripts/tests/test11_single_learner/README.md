# Single Learner Test

| Parameter | Value |
|-----------|-------|
| NUM_VALUES | 100 |
| NUM_CLIENTS | 2 |
| NUM_PROPOSERS | 2 |
| NUM_ACCEPTORS | 3 |
| NUM_LEARNERS | 1 |
| LOSS | 0.0 |
| CATCHUP | false |

## What it verifies
- System works with minimal learner configuration
- Learner correctly receives 2B messages

## Usage
```bash
./run.sh   # Run the test
./check.sh # Verify results
```
