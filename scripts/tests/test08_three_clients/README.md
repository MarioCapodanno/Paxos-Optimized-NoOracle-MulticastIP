# Three Clients Test

| Parameter | Value |
|-----------|-------|
| NUM_VALUES | 100 |
| NUM_CLIENTS | 3 |
| NUM_PROPOSERS | 2 |
| NUM_ACCEPTORS | 3 |
| NUM_LEARNERS | 2 |
| LOSS | 0.0 |
| CATCHUP | false |

## What it verifies
- Multiple clients can submit values concurrently
- Duplicate detection by (client_id, seq_num) works

## Usage
```bash
./run.sh   # Run the test
./check.sh # Verify results
```
