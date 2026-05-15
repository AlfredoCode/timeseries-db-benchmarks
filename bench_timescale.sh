#!/bin/bash

# --- Configuration ---
NUM_RUNS=100
PYTHON_SCRIPT="timescale.py"
PYTHON_ARGS="--batch-size 1000 --dataset-file dataset.txt"
OUTPUT_FILE="latency_results_timescale.txt"

# --- Main Script ---

# Clear old results file and add header
echo "latency;time_full" > "$OUTPUT_FILE"

echo "Starting TimescaleDB Write Latency Benchmark..."
echo "Running $PYTHON_SCRIPT $PYTHON_ARGS"
echo "Target Runs: $NUM_RUNS"
echo "--------------------------------------------------"

for i in $(seq 1 $NUM_RUNS); do
    echo "Running benchmark $i of $NUM_RUNS..."

    OUTPUT=$(python3 "$PYTHON_SCRIPT" $PYTHON_ARGS 2>&1 | sed -r 's/\x1b\[[0-9;]*m//g')  # remove ANSI colors
    EXIT_CODE=$?

    if [ $EXIT_CODE -ne 0 ]; then
        echo "Error: Python script failed on run $i. Skipping."
        continue
    fi

    # For debugging: Save last run output
    echo "$OUTPUT" > last_run_output.log

    # Extract P50 latency and total time from output, ignoring emojis/spaces issues
    P50_MS=$(echo "$OUTPUT" | grep -Eo 'P50: [0-9.]+' | awk '{print $2}')
    TOTAL_TIME_S=$(echo "$OUTPUT" | grep -Eo 'Time: [0-9.]+s' | awk '{print $2}' | tr -d 's')

    if [ -z "$P50_MS" ] || [ -z "$TOTAL_TIME_S" ]; then
        echo "Error: Could not find P50 latency or total time in run $i output. Skipping."
        echo "---- last_run_output.log content start ----"
        head -40 last_run_output.log
        echo "---- last_run_output.log content end ----"
        continue
    fi

    echo "${P50_MS};${TOTAL_TIME_S}" >> "$OUTPUT_FILE"
    echo "  -> P50 Latency: ${P50_MS} ms, Total Time: ${TOTAL_TIME_S} s recorded."
done

echo "--------------------------------------------------"

# Calculate Final Averages (skip header line)
if [ -f "$OUTPUT_FILE" ] && [ -s "$OUTPUT_FILE" ]; then
    # Exclude header line for wc and awk
    TOTAL_SUCCESS_RUNS=$(($(wc -l < "$OUTPUT_FILE") - 1))

    if [ "$TOTAL_SUCCESS_RUNS" -gt 0 ]; then
        AVERAGE_P50_MS=$(tail -n +2 "$OUTPUT_FILE" | awk -F';' '{sum+=$1} END {printf "%.2f", sum/NR}')
        AVERAGE_TOTAL_TIME_S=$(tail -n +2 "$OUTPUT_FILE" | awk -F';' '{sum+=$2} END {printf "%.2f", sum/NR}')

        echo "✅ Benchmark Complete. Successfully recorded $TOTAL_SUCCESS_RUNS runs."
        echo "📈 Final Average P50 Write Latency (from $TOTAL_SUCCESS_RUNS runs): ${AVERAGE_P50_MS} ms"
        echo "📈 Final Average Total Transfer Time (from $TOTAL_SUCCESS_RUNS runs): ${AVERAGE_TOTAL_TIME_S} seconds"
    else
        echo "❌ ERROR: No successful runs recorded."
    fi
else
    echo "❌ ERROR: $OUTPUT_FILE is missing or empty."
fi
