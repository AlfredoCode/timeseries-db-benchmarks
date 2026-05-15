#!/bin/bash

# --- Configuration ---
NUM_RUNS=100
PYTHON_SCRIPT="mongo.py"
PYTHON_ARGS="--batch-size 1000 --dataset-file dataset.txt"
OUTPUT_FILE="latency_results_mongo_1k.txt"

# --- Main Script ---

# Clear old results file
> "$OUTPUT_FILE"

echo "Starting MongoDB Write Latency Benchmark..."
echo "Running $PYTHON_SCRIPT $PYTHON_ARGS"
echo "Target Runs: $NUM_RUNS"
echo "--------------------------------------------------"

# Add header to output file
echo "latency;time_full" > "$OUTPUT_FILE"
for i in $(seq 1 $NUM_RUNS); do
    echo "Running benchmark $i of $NUM_RUNS..."

    OUTPUT=$(python3 "$PYTHON_SCRIPT" $PYTHON_ARGS 2>&1)
    EXIT_CODE=$?

    if [ $EXIT_CODE -ne 0 ]; then
        echo "Error: Python script failed on run $i. Output:"
        echo "----------------------------------------------"
        echo "$OUTPUT"
        echo "----------------------------------------------"
        echo "Skipping."
        continue
    fi

    P50_MS=$(echo "$OUTPUT" | grep "P50:" | awk '{print $2}')
    TOTAL_TIME_S=$(echo "$OUTPUT" | grep "Elapsed time:" | awk '{print $3}')

    if [ -z "$P50_MS" ] || [ -z "$TOTAL_TIME_S" ]; then
        echo "Error: Could not find P50 latency or total time in run $i output. Skipping."
        continue
    fi

    echo "${P50_MS};${TOTAL_TIME_S}" >> "$OUTPUT_FILE"
    echo "  -> P50 Latency: ${P50_MS} ms, Total Time: ${TOTAL_TIME_S} s recorded."
done


echo "--------------------------------------------------"

# Calculate Final Averages if results file exists and is not empty
if [ -f "$OUTPUT_FILE" ] && [ -s "$OUTPUT_FILE" ]; then
    # Skip header line for counting runs
    TOTAL_SUCCESS_RUNS=$(($(wc -l < "$OUTPUT_FILE") - 1))

    AVERAGE_P50_MS=$(tail -n +2 "$OUTPUT_FILE" | awk -F';' '{sum+=$1; count++} END {if (count > 0) printf "%.2f", sum/count}')
    AVERAGE_TOTAL_TIME_S=$(tail -n +2 "$OUTPUT_FILE" | awk -F';' '{sum+=$2; count++} END {if (count > 0) printf "%.2f", sum/count}')

    echo "✅ Benchmark Complete. Successfully recorded $TOTAL_SUCCESS_RUNS runs."
    echo "📈 Final Average P50 Write Latency (from $TOTAL_SUCCESS_RUNS runs): ${AVERAGE_P50_MS} ms"
    echo "📈 Final Average Total Transfer Time (from $TOTAL_SUCCESS_RUNS runs): ${AVERAGE_TOTAL_TIME_S} seconds"
else
    echo "❌ ERROR: No successful runs recorded or $OUTPUT_FILE is empty."
fi

# Optional: clean up results file
# rm "$OUTPUT_FILE"
