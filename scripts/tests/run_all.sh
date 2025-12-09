#!/bin/bash
# Run all Paxos tests sequentially

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "           PAXOS IMPLEMENTATION TEST SUITE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

PASSED=0
FAILED=0
TESTS=()

# Find all test directories
for dir in "$SCRIPT_DIR"/test*/; do
    if [ -d "$dir" ] && [ -f "$dir/run.sh" ]; then
        TESTS+=("$dir")
    fi
done

# Sort tests
IFS=$'\n' TESTS=($(sort <<<"${TESTS[*]}")); unset IFS

for test_dir in "${TESTS[@]}"; do
    test_name=$(basename "$test_dir")
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "TEST: $test_name"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Run the test
    cd "$test_dir"
    chmod +x run.sh check.sh 2>/dev/null
    
    echo "Running test..."
    ./run.sh
    
    echo ""
    echo "Checking results..."
    if ./check.sh; then
        echo ""
        echo "✓ $test_name PASSED"
        ((PASSED++))
    else
        echo ""
        echo "✗ $test_name FAILED"
        ((FAILED++))
    fi
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "                    SUMMARY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Passed: $PASSED"
echo "  Failed: $FAILED"
echo "  Total:  $((PASSED + FAILED))"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$FAILED" -eq 0 ]; then
    echo "All tests passed!"
    exit 0
else
    echo "Some tests failed."
    exit 1
fi
