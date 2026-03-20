#!/bin/bash
################################################################################
# Benchmark: Naive Rebuild vs Delta-Buffered HINT^m
#
# Compares two approaches for handling inserts/deletes:
#   1. NAIVE   — rebuild the full HINT^m index from scratch after every batch
#   2. DELTA   — use HINT_M_Dynamic with delta buffers + periodic merge
#
# Tests across different operation batch sizes.
################################################################################

set -e

DATA="samples/AARHUS-BOOKS_2013.dat"
QUERIES="samples/AARHUS-BOOKS_2013_20k.qry"
BITS=10
QUERY_LIMIT=1000   # Use first N queries for faster benchmarking

# Check prerequisites
if [ ! -f "$DATA" ]; then
    echo "Decompressing sample data..."
    gunzip -k samples/AARHUS-BOOKS_2013.dat.gz
fi

if [ ! -f "query_hint_m_delta.exec" ]; then
    echo "Error: query_hint_m_delta.exec not found. Run 'make hint_m_delta' first."
    exit 1
fi

# Create a limited query file for faster benchmarks
QUERIES_SHORT="/tmp/hint_bench_queries.qry"
head -n $QUERY_LIMIT "$QUERIES" > "$QUERIES_SHORT"

# Get domain bounds from data (approximate — use first and last lines)
DOMAIN_START=$(head -1 "$DATA" | awk '{print $1}')
DOMAIN_END=$(tail -1 "$DATA" | awk '{print $2}')
DOMAIN_SIZE=$((DOMAIN_END - DOMAIN_START))
NUM_RECORDS=$(wc -l < "$DATA" | tr -d ' ')

echo "================================================================="
echo " HINT^m Delta Index Benchmark"
echo "================================================================="
echo " Data file     : $DATA"
echo " Records       : $NUM_RECORDS"
echo " Domain size   : $DOMAIN_SIZE"
echo " Bits          : $BITS"
echo " Query file    : $QUERIES (limited to $QUERY_LIMIT queries)"
echo "================================================================="
echo ""


################################################################################
# Helper: Generate random operations file
################################################################################
generate_ops() {
    local num_inserts=$1
    local num_deletes=$2
    local ops_file=$3

    python3 -c "
import random
random.seed(42)
domain_start = $DOMAIN_START
domain_end = $DOMAIN_END
num_records = $NUM_RECORDS

with open('$ops_file', 'w') as f:
    # Inserts: random intervals within domain
    for i in range($num_inserts):
        s = random.randint(domain_start, domain_end - 100)
        e = s + random.randint(1, min(1000, domain_end - s))
        f.write(f'I {s} {e}\n')
    # Deletes: random existing record IDs
    ids = random.sample(range(num_records), min($num_deletes, num_records))
    for rid in ids:
        f.write(f'D {rid}\n')
"
}


################################################################################
# Helper: Run naive approach (full rebuild for each batch of operations)
################################################################################
run_naive() {
    local ops_file=$1
    local num_inserts=$2
    local num_deletes=$3

    # The naive approach: for each operation, we'd rebuild.
    # We simulate this by setting threshold=1 so every operation triggers a merge.
    ./query_hint_m_delta.exec -m $BITS -q gOVERLAPS \
        -u "$ops_file" -i 1 -d 1 \
        "$DATA" "$QUERIES_SHORT" 2>/dev/null
}


################################################################################
# Helper: Run delta approach (buffered with larger threshold)
################################################################################
run_delta() {
    local ops_file=$1
    local insert_threshold=$2
    local delete_threshold=$3

    ./query_hint_m_delta.exec -m $BITS -q gOVERLAPS \
        -u "$ops_file" -i "$insert_threshold" -d "$delete_threshold" \
        "$DATA" "$QUERIES_SHORT" 2>/dev/null
}


################################################################################
# Helper: Extract key metrics from output
################################################################################
extract_metric() {
    local output="$1"
    local label="$2"
    echo "$output" | grep "$label" | awk -F: '{print $2}' | tr -d ' '
}


################################################################################
# Benchmark 0: Baseline (no operations)
################################################################################
echo "-----------------------------------------------------------------"
echo " TEST 0: Baseline (no operations)"
echo "-----------------------------------------------------------------"

BASELINE_OUTPUT=$(./query_hint_m_delta.exec -m $BITS -q gOVERLAPS \
    "$DATA" "$QUERIES_SHORT" 2>/dev/null)

BASELINE_INDEX_TIME=$(extract_metric "$BASELINE_OUTPUT" "Indexing time")
BASELINE_QUERY_TIME=$(extract_metric "$BASELINE_OUTPUT" "Total querying time")
BASELINE_RESULT=$(extract_metric "$BASELINE_OUTPUT" "Total result")
BASELINE_THROUGHPUT=$(extract_metric "$BASELINE_OUTPUT" "Throughput")

echo "  Indexing time   : ${BASELINE_INDEX_TIME}s"
echo "  Query time      : ${BASELINE_QUERY_TIME}s"
echo "  XOR result      : $BASELINE_RESULT"
echo "  Throughput      : $BASELINE_THROUGHPUT queries/sec"
echo ""


################################################################################
# Benchmark across batch sizes
################################################################################

# Test different operation counts
BATCH_SIZES=(10 50 100 500 1000)

echo "================================================================="
echo " COMPARATIVE BENCHMARK: Naive (threshold=1) vs Delta (buffered)"
echo "================================================================="
echo ""
printf "%-10s | %-8s | %-14s | %-14s | %-14s | %-14s | %-8s | %-8s\n" \
       "Ops" "Approach" "Ops Time (s)" "Query Time (s)" "Total Time (s)" "Throughput" "Merges" "Result"
printf "%s\n" "---------- | -------- | -------------- | -------------- | -------------- | -------------- | -------- | --------"

for BATCH in "${BATCH_SIZES[@]}"; do
    NUM_INS=$BATCH
    NUM_DEL=$((BATCH / 5))  # 20% deletes
    TOTAL_OPS=$((NUM_INS + NUM_DEL))

    OPS_FILE="/tmp/hint_bench_ops_${BATCH}.txt"
    generate_ops $NUM_INS $NUM_DEL "$OPS_FILE"

    # --- NAIVE: threshold=1 (rebuild after every operation) ---
    NAIVE_OUTPUT=$(run_naive "$OPS_FILE" $NUM_INS $NUM_DEL)

    NAIVE_OPS_TIME=$(extract_metric "$NAIVE_OUTPUT" "Operations time")
    NAIVE_QUERY_TIME=$(extract_metric "$NAIVE_OUTPUT" "Total querying time")
    NAIVE_RESULT=$(extract_metric "$NAIVE_OUTPUT" "Total result")
    NAIVE_THROUGHPUT=$(extract_metric "$NAIVE_OUTPUT" "Throughput")
    NAIVE_MERGES=$(extract_metric "$NAIVE_OUTPUT" "Total merges performed")
    NAIVE_TOTAL=$(python3 -c "print(f'{${NAIVE_OPS_TIME:-0} + ${NAIVE_QUERY_TIME:-0}:.6f}')")

    printf "%-10s | %-8s | %-14s | %-14s | %-14s | %-14s | %-8s | %-8s\n" \
           "${NUM_INS}I+${NUM_DEL}D" "Naive" "$NAIVE_OPS_TIME" "$NAIVE_QUERY_TIME" "$NAIVE_TOTAL" "$NAIVE_THROUGHPUT" "$NAIVE_MERGES" "$NAIVE_RESULT"

    # --- DELTA: threshold = batch size (single merge at end if needed) ---
    DELTA_THRESH=$((TOTAL_OPS + 1))  # Set high enough to avoid auto-merge during ops
    DELTA_OUTPUT=$(run_delta "$OPS_FILE" $DELTA_THRESH $DELTA_THRESH)

    DELTA_OPS_TIME=$(extract_metric "$DELTA_OUTPUT" "Operations time")
    DELTA_QUERY_TIME=$(extract_metric "$DELTA_OUTPUT" "Total querying time")
    DELTA_RESULT=$(extract_metric "$DELTA_OUTPUT" "Total result")
    DELTA_THROUGHPUT=$(extract_metric "$DELTA_OUTPUT" "Throughput")
    DELTA_MERGES=$(extract_metric "$DELTA_OUTPUT" "Total merges performed")
    DELTA_TOTAL=$(python3 -c "print(f'{${DELTA_OPS_TIME:-0} + ${DELTA_QUERY_TIME:-0}:.6f}')")

    printf "%-10s | %-8s | %-14s | %-14s | %-14s | %-14s | %-8s | %-8s\n" \
           "" "Delta" "$DELTA_OPS_TIME" "$DELTA_QUERY_TIME" "$DELTA_TOTAL" "$DELTA_THROUGHPUT" "$DELTA_MERGES" "$DELTA_RESULT"

    # --- DELTA with moderate threshold (merge periodically) ---
    MOD_THRESH=$((BATCH / 2))
    if [ $MOD_THRESH -lt 2 ]; then MOD_THRESH=2; fi
    MODERATE_OUTPUT=$(run_delta "$OPS_FILE" $MOD_THRESH $MOD_THRESH)

    MOD_OPS_TIME=$(extract_metric "$MODERATE_OUTPUT" "Operations time")
    MOD_QUERY_TIME=$(extract_metric "$MODERATE_OUTPUT" "Total querying time")
    MOD_RESULT=$(extract_metric "$MODERATE_OUTPUT" "Total result")
    MOD_THROUGHPUT=$(extract_metric "$MODERATE_OUTPUT" "Throughput")
    MOD_MERGES=$(extract_metric "$MODERATE_OUTPUT" "Total merges performed")
    MOD_TOTAL=$(python3 -c "print(f'{${MOD_OPS_TIME:-0} + ${MOD_QUERY_TIME:-0}:.6f}')")

    printf "%-10s | %-8s | %-14s | %-14s | %-14s | %-14s | %-8s | %-8s\n" \
           "" "Moderate" "$MOD_OPS_TIME" "$MOD_QUERY_TIME" "$MOD_TOTAL" "$MOD_THROUGHPUT" "$MOD_MERGES" "$MOD_RESULT"

    printf "%s\n" "---------- | -------- | -------------- | -------------- | -------------- | -------------- | -------- | --------"
done

echo ""
echo "================================================================="
echo " Legend"
echo "================================================================="
echo "  Naive    : threshold=1 (rebuild after every single operation)"
echo "  Delta    : threshold=total_ops+1 (no merge during ops, all buffered)"
echo "  Moderate : threshold=batch/2 (periodic merges)"
echo ""
echo " Ops Time    = time for insert/delete operations"
echo " Query Time  = time for gOVERLAPS queries"
echo " Total Time  = Ops Time + Query Time"
echo " Merges      = number of index rebuilds triggered"
echo "================================================================="

# Cleanup
rm -f /tmp/hint_bench_ops_*.txt /tmp/hint_bench_queries.qry
