#!/usr/bin/env bash
# Special check for one acceptor test - expects no progress (stall)

NUM_CLIENTS=2
NUM_LEARNERS=2

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir/../../../"

echo "Test: One Acceptor (No Majority) - Expects stall"

# Count learned values - should be 0
LEARNED=0
if [[ -f "logs/learner1.log" ]]; then
    LEARNED=$(wc -l < logs/learner1.log 2>/dev/null || echo 0)
fi

echo "  Learned values: $LEARNED"

if [[ $LEARNED -eq 0 ]]; then
    echo "  > OK (no progress as expected - safety maintained)"
else
    echo "  > Warning: Some values learned ($LEARNED) - unexpected with 1 acceptor"
fi

echo
echo "All checks complete."
