#!/bin/bash
################################################################################
# Comprehensive Benchmark: Naive Rebuild vs Delta-Buffered HINT^m
#
# Tests:
#   1. VARYING INSERT/DELETE RATIOS  — insert-only, delete-only, mixed
#   2. THRESHOLD SENSITIVITY         — how threshold choice affects performance
#   3. SCALING                       — small to large operation counts
#   4. MEMORY USAGE                  — track RSS/VM across approaches
#
# Approaches compared:
#   NAIVE    — threshold=1 (rebuild after every single operation)
#   DELTA    — threshold > total_ops (fully buffered, no merge during ops)
#   MODERATE — threshold = batch/2 (periodic merges)
################################################################################

set -e

DATA="samples/AARHUS-BOOKS_2013.dat"
QUERIES="samples/AARHUS-BOOKS_2013_20k.qry"
BITS=10
QUERY_LIMIT=1000
# Skip naive for batches above this size (too slow — each op rebuilds index)
NAIVE_MAX_OPS=100
RESULTS_DIR="benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Check prerequisites
if [ ! -f "$DATA" ]; then
    if [ -f "${DATA}.gz" ]; then
        echo "Decompressing sample data..."
        gunzip -k "${DATA}.gz"
    else
        echo "Error: $DATA not found. Place the data file in samples/"
        exit 1
    fi
fi

if [ ! -f "query_hint_m_delta.exec" ]; then
    echo "Building query_hint_m_delta.exec..."
    make hint_m_delta
fi

mkdir -p "$RESULTS_DIR"

QUERIES_SHORT="/tmp/hint_bench_queries.qry"
head -n $QUERY_LIMIT "$QUERIES" > "$QUERIES_SHORT"

DOMAIN_START=$(head -1 "$DATA" | awk '{print $1}')
DOMAIN_END=$(tail -1 "$DATA" | awk '{print $2}')
DOMAIN_SIZE=$((DOMAIN_END - DOMAIN_START))
NUM_RECORDS=$(wc -l < "$DATA" | tr -d ' ')


################################################################################
# Helpers
################################################################################

generate_ops() {
    local num_inserts=$1
    local num_deletes=$2
    local ops_file=$3

    python3 -c "
import random
random.seed(42)
with open('$ops_file', 'w') as f:
    for i in range($num_inserts):
        s = random.randint($DOMAIN_START, $DOMAIN_END - 100)
        e = s + random.randint(1, min(1000, $DOMAIN_END - s))
        f.write(f'I {s} {e}\n')
    ids = random.sample(range($NUM_RECORDS), min($num_deletes, $NUM_RECORDS))
    for rid in ids:
        f.write(f'D {rid}\n')
"
}

run_test() {
    local ops_file=$1
    local insert_thresh=$2
    local delete_thresh=$3
    local bits=$4

    ./query_hint_m_delta.exec -m "$bits" -q gOVERLAPS \
        -u "$ops_file" -i "$insert_thresh" -d "$delete_thresh" \
        "$DATA" "$QUERIES_SHORT" 2>/dev/null
}

run_no_ops() {
    local bits=$1
    ./query_hint_m_delta.exec -m "$bits" -q gOVERLAPS \
        "$DATA" "$QUERIES_SHORT" 2>/dev/null
}

extract() {
    local output="$1"
    local label="$2"
    echo "$output" | grep "$label" | head -1 | awk -F: '{print $2}' | tr -d ' '
}

safe_total() {
    python3 -c "print(f'{${1:-0} + ${2:-0}:.6f}')"
}

print_header() {
    echo ""
    printf "%-14s | %-10s | %-14s | %-14s | %-14s | %-14s | %-8s\n" \
           "Config" "Approach" "Ops Time (s)" "Query Time (s)" "Total Time (s)" "Throughput" "Merges"
    printf "%s\n" "-------------- | ---------- | -------------- | -------------- | -------------- | -------------- | --------"
}

print_row() {
    printf "%-14s | %-10s | %-14s | %-14s | %-14s | %-14s | %-8s\n" "$1" "$2" "$3" "$4" "$5" "$6" "$7"
}

print_separator() {
    printf "%s\n" "-------------- | ---------- | -------------- | -------------- | -------------- | -------------- | --------"
}


################################################################################
# Header
################################################################################

REPORT="$RESULTS_DIR/benchmark_${TIMESTAMP}.txt"

{
echo "================================================================="
echo " HINT^m Delta Index — Comprehensive Benchmark"
echo " $(date)"
echo "================================================================="
echo " Data           : $DATA ($NUM_RECORDS records)"
echo " Domain         : $DOMAIN_SIZE"
echo " Bits           : $BITS"
echo " Queries        : $QUERY_LIMIT"
echo " Naive cutoff   : skip naive for > $NAIVE_MAX_OPS total ops"
echo "================================================================="


################################################################################
# TEST 1: Baseline (no operations)
################################################################################

echo ""
echo "================================================================="
echo " TEST 1: Baseline (no operations)"
echo "================================================================="

BASELINE=$(run_no_ops $BITS)
B_IDX=$(extract "$BASELINE" "Indexing time")
B_QRY=$(extract "$BASELINE" "Total querying time")
B_RES=$(extract "$BASELINE" "Total result")
B_THR=$(extract "$BASELINE" "Throughput")
B_VM=$(extract "$BASELINE" "Read VM")
B_RSS=$(extract "$BASELINE" "Read RSS")

echo "  Indexing time   : ${B_IDX}s"
echo "  Query time      : ${B_QRY}s"
echo "  Result          : $B_RES"
echo "  Throughput      : $B_THR q/s"
echo "  VM              : $B_VM bytes"


################################################################################
# TEST 2: Varying Insert/Delete Ratios
#   Fixed total = 100 ops, vary the mix
################################################################################

echo ""
echo "================================================================="
echo " TEST 2: Insert/Delete Ratio Sensitivity (100 total operations)"
echo "================================================================="
echo " Shows how performance changes with different workload mixes."

RATIOS=(
    "100:0"    # Insert-only
    "80:20"    # Mostly inserts
    "50:50"    # Balanced
    "20:80"    # Mostly deletes
    "0:100"    # Delete-only
)

print_header

for RATIO in "${RATIOS[@]}"; do
    INS_PCT=${RATIO%%:*}
    DEL_PCT=${RATIO##*:}
    NUM_INS=$INS_PCT
    NUM_DEL=$DEL_PCT
    TOTAL=$((NUM_INS + NUM_DEL))
    OPS_FILE="/tmp/hint_bench_ratio_${INS_PCT}_${DEL_PCT}.txt"
    generate_ops $NUM_INS $NUM_DEL "$OPS_FILE"

    LABEL="${INS_PCT}I/${DEL_PCT}D"

    # Naive (threshold=1)
    NOUT=$(run_test "$OPS_FILE" 1 1 $BITS)
    N_OPS=$(extract "$NOUT" "Operations time")
    N_QRY=$(extract "$NOUT" "Total querying time")
    N_THR=$(extract "$NOUT" "Throughput")
    N_MRG=$(extract "$NOUT" "Total merges performed")
    N_TOT=$(safe_total "$N_OPS" "$N_QRY")
    print_row "$LABEL" "Naive" "$N_OPS" "$N_QRY" "$N_TOT" "$N_THR" "$N_MRG"

    # Delta (no merge)
    D_THRESH=$((TOTAL + 1))
    DOUT=$(run_test "$OPS_FILE" $D_THRESH $D_THRESH $BITS)
    D_OPS=$(extract "$DOUT" "Operations time")
    D_QRY=$(extract "$DOUT" "Total querying time")
    D_THR=$(extract "$DOUT" "Throughput")
    D_MRG=$(extract "$DOUT" "Total merges performed")
    D_TOT=$(safe_total "$D_OPS" "$D_QRY")
    print_row "" "Delta" "$D_OPS" "$D_QRY" "$D_TOT" "$D_THR" "$D_MRG"

    print_separator
done


################################################################################
# TEST 3: Scaling — increasing operation counts
#   Fixed ratio 80/20, scale batch size
#   Skip naive for large batches
################################################################################

echo ""
echo "================================================================="
echo " TEST 3: Scaling (80% insert / 20% delete, increasing batch size)"
echo "================================================================="
echo " Naive skipped for batches > $NAIVE_MAX_OPS ops (too slow)."

BATCH_SIZES=(10 50 100 200 500 1000 5000)

print_header

for BATCH in "${BATCH_SIZES[@]}"; do
    NUM_INS=$((BATCH * 80 / 100))
    NUM_DEL=$((BATCH * 20 / 100))
    if [ $NUM_DEL -lt 1 ] && [ $BATCH -gt 0 ]; then NUM_DEL=1; fi
    TOTAL=$((NUM_INS + NUM_DEL))
    OPS_FILE="/tmp/hint_bench_scale_${BATCH}.txt"
    generate_ops $NUM_INS $NUM_DEL "$OPS_FILE"

    LABEL="${NUM_INS}I+${NUM_DEL}D"

    # Naive (only for small batches)
    if [ $TOTAL -le $NAIVE_MAX_OPS ]; then
        NOUT=$(run_test "$OPS_FILE" 1 1 $BITS)
        N_OPS=$(extract "$NOUT" "Operations time")
        N_QRY=$(extract "$NOUT" "Total querying time")
        N_THR=$(extract "$NOUT" "Throughput")
        N_MRG=$(extract "$NOUT" "Total merges performed")
        N_TOT=$(safe_total "$N_OPS" "$N_QRY")
        print_row "$LABEL" "Naive" "$N_OPS" "$N_QRY" "$N_TOT" "$N_THR" "$N_MRG"
    else
        print_row "$LABEL" "Naive" "SKIPPED" "—" "—" "—" "—"
    fi

    # Delta (no merge)
    D_THRESH=$((TOTAL + 1))
    DOUT=$(run_test "$OPS_FILE" $D_THRESH $D_THRESH $BITS)
    D_OPS=$(extract "$DOUT" "Operations time")
    D_QRY=$(extract "$DOUT" "Total querying time")
    D_THR=$(extract "$DOUT" "Throughput")
    D_MRG=$(extract "$DOUT" "Total merges performed")
    D_TOT=$(safe_total "$D_OPS" "$D_QRY")
    print_row "" "Delta" "$D_OPS" "$D_QRY" "$D_TOT" "$D_THR" "$D_MRG"

    # Moderate (threshold = batch/2)
    MOD_THRESH=$((BATCH / 2))
    if [ $MOD_THRESH -lt 2 ]; then MOD_THRESH=2; fi
    MOUT=$(run_test "$OPS_FILE" $MOD_THRESH $MOD_THRESH $BITS)
    M_OPS=$(extract "$MOUT" "Operations time")
    M_QRY=$(extract "$MOUT" "Total querying time")
    M_THR=$(extract "$MOUT" "Throughput")
    M_MRG=$(extract "$MOUT" "Total merges performed")
    M_TOT=$(safe_total "$M_OPS" "$M_QRY")
    print_row "" "Moderate" "$M_OPS" "$M_QRY" "$M_TOT" "$M_THR" "$M_MRG"

    print_separator
done


################################################################################
# TEST 4: Threshold Sensitivity
#   Fixed 500 ops (400I + 100D), vary threshold
################################################################################

echo ""
echo "================================================================="
echo " TEST 4: Threshold Sensitivity (400 inserts + 100 deletes)"
echo "================================================================="
echo " Shows how different merge threshold values affect performance."

THRESH_OPS_FILE="/tmp/hint_bench_thresh.txt"
generate_ops 400 100 "$THRESH_OPS_FILE"

THRESHOLDS=(1 5 10 25 50 100 250 500 1000)

printf "\n%-12s | %-14s | %-14s | %-14s | %-14s | %-8s\n" \
       "Threshold" "Ops Time (s)" "Query Time (s)" "Total Time (s)" "Throughput" "Merges"
printf "%s\n" "------------ | -------------- | -------------- | -------------- | -------------- | --------"

for THRESH in "${THRESHOLDS[@]}"; do
    TOUT=$(run_test "$THRESH_OPS_FILE" $THRESH $THRESH $BITS)
    T_OPS=$(extract "$TOUT" "Operations time")
    T_QRY=$(extract "$TOUT" "Total querying time")
    T_THR=$(extract "$TOUT" "Throughput")
    T_MRG=$(extract "$TOUT" "Total merges performed")
    T_TOT=$(safe_total "$T_OPS" "$T_QRY")

    printf "%-12s | %-14s | %-14s | %-14s | %-14s | %-8s\n" \
           "$THRESH" "$T_OPS" "$T_QRY" "$T_TOT" "$T_THR" "$T_MRG"
done


################################################################################
# TEST 5: numBits Sensitivity
#   Fixed 200 ops (160I + 40D), threshold=100, vary bits
################################################################################

echo ""
echo "================================================================="
echo " TEST 5: numBits Sensitivity (160 inserts + 40 deletes, threshold=100)"
echo "================================================================="
echo " Shows how index granularity (numBits) affects delta performance."

BITS_OPS_FILE="/tmp/hint_bench_bits.txt"
generate_ops 160 40 "$BITS_OPS_FILE"

BIT_VALUES=(5 8 10 12 15 18 20)

printf "\n%-6s | %-14s | %-14s | %-14s | %-14s | %-8s\n" \
       "Bits" "Ops Time (s)" "Query Time (s)" "Total Time (s)" "Throughput" "Merges"
printf "%s\n" "------ | -------------- | -------------- | -------------- | -------------- | --------"

for B in "${BIT_VALUES[@]}"; do
    BOUT=$(run_test "$BITS_OPS_FILE" 100 100 $B)
    B_OPS=$(extract "$BOUT" "Operations time")
    B_QRY=$(extract "$BOUT" "Total querying time")
    B_THR=$(extract "$BOUT" "Throughput")
    B_MRG=$(extract "$BOUT" "Total merges performed")
    B_TOT=$(safe_total "$B_OPS" "$B_QRY")

    printf "%-6s | %-14s | %-14s | %-14s | %-14s | %-8s\n" \
           "$B" "$B_OPS" "$B_QRY" "$B_TOT" "$B_THR" "$B_MRG"
done


################################################################################
# TEST 6: Insert-Only vs Delete-Only Scaling
################################################################################

echo ""
echo "================================================================="
echo " TEST 6: Pure Insert vs Pure Delete (Delta approach, no merge)"
echo "================================================================="
echo " Compares operation time and query impact for insert-only vs delete-only."

INS_ONLY_SIZES=(50 100 500 1000 5000)

printf "\n%-14s | %-14s | %-14s | %-14s | %-14s\n" \
       "Ops" "Ops Time (s)" "Query Time (s)" "Total Time (s)" "Throughput"
printf "%s\n" "-------------- | -------------- | -------------- | -------------- | --------------"

for SZ in "${INS_ONLY_SIZES[@]}"; do
    # Insert-only
    OPS_FILE="/tmp/hint_bench_insonly_${SZ}.txt"
    generate_ops $SZ 0 "$OPS_FILE"
    IOUT=$(run_test "$OPS_FILE" $((SZ + 1)) $((SZ + 1)) $BITS)
    I_OPS=$(extract "$IOUT" "Operations time")
    I_QRY=$(extract "$IOUT" "Total querying time")
    I_THR=$(extract "$IOUT" "Throughput")
    I_TOT=$(safe_total "$I_OPS" "$I_QRY")
    printf "%-14s | %-14s | %-14s | %-14s | %-14s\n" \
           "${SZ} inserts" "$I_OPS" "$I_QRY" "$I_TOT" "$I_THR"

    # Delete-only
    OPS_FILE="/tmp/hint_bench_delonly_${SZ}.txt"
    generate_ops 0 $SZ "$OPS_FILE"
    DOUT=$(run_test "$OPS_FILE" $((SZ + 1)) $((SZ + 1)) $BITS)
    D_OPS=$(extract "$DOUT" "Operations time")
    D_QRY=$(extract "$DOUT" "Total querying time")
    D_THR=$(extract "$DOUT" "Throughput")
    D_TOT=$(safe_total "$D_OPS" "$D_QRY")
    printf "%-14s | %-14s | %-14s | %-14s | %-14s\n" \
           "${SZ} deletes" "$D_OPS" "$D_QRY" "$D_TOT" "$D_THR"

    printf "%s\n" "-------------- | -------------- | -------------- | -------------- | --------------"
done


################################################################################
# Summary
################################################################################

echo ""
echo "================================================================="
echo " Summary"
echo "================================================================="
echo ""
echo " TEST 1: Baseline         — reference point without any operations"
echo " TEST 2: I/D Ratios       — how insert vs delete mix affects cost"
echo " TEST 3: Scaling          — how performance changes with more operations"
echo " TEST 4: Threshold Tuning — finding the optimal merge threshold"
echo " TEST 5: numBits          — index granularity impact on delta approach"
echo " TEST 6: Insert vs Delete — isolating the cost of each operation type"
echo ""
echo " Key trade-off:"
echo "   - Low threshold  → more merges, fast queries (like naive)"
echo "   - High threshold → fewer merges, slower queries (delta scan)"
echo "   - Delete-heavy   → triggers slow-path linear scan for queries"
echo "   - Insert-only    → queries stay fast (HINT + small linear scan)"
echo ""
echo "================================================================="
echo " Results saved to: $REPORT"
echo "================================================================="

} 2>&1 | tee "$REPORT"

# Cleanup temp files
rm -f /tmp/hint_bench_*.txt /tmp/hint_bench_*.qry
