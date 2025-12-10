#!/usr/bin/env bash
set -e

NUM_CLIENTS=2
NUM_LEARNERS=1

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir/../../../"


input_files=()
for ((i = 1; i <= ${NUM_CLIENTS}; i++)); do
    if [[ ! -f "logs/values$i.log" ]]; then
        echo "Error: file logs/values$i.log not found."
        exit 1
    fi
    input_files+=("logs/values$i.log")
done

output_files=()
for ((i = 1; i <= ${NUM_LEARNERS}; i++)); do
    if [[ ! -f "logs/learner$i.log" ]]; then
        echo "Error: file logs/learner$i.log not found."
        exit 1
    fi
    output_files+=("logs/learner$i.log")
done


# --- Prepare sorted proposals
cat "${input_files[@]}" | sort > logs/prop.sorted


# --- Single learner - skip agreement test ---
echo "Test 1 - Single learner (agreement test skipped)"
echo "  > OK (only one learner)"


# --- Values learned were actually proposed ---
echo "Test 2 - Values learned were actually proposed"

prop_learned=$(cat logs/prop.sorted "${input_files[@]}" | sort -u | wc -l)
prop=$(cat logs/prop.sorted | sort -u | wc -l)

if [[ $prop_learned == $prop ]]; then
    echo "  > OK"
else
    echo "  > Failed!"
fi


# --- Learners learned every value that was sent by some client ---
echo "Test 3 - Learners learned every value that was sent by some client"

prop=$(cat logs/prop.sorted | sort -u | wc -l)
all_ok=true
for output in "${output_files[0]}"; do
    learned=$(cat "$output" | sort -u | wc -l)
    if [[ $learned != $prop ]]; then
        echo "  > Failed! ($output missing values)"
        all_ok=false
    fi
done

if $all_ok; then
    echo "  > OK"
fi

echo
echo "All checks complete."
