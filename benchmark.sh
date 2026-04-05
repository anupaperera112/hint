#!/bin/bash
# Benchmark: HINT^m Original vs SIMD (SoA + AVX2)
# Uses base HINT_M (no -o flag) which is the class modified by hint_m_simd.cpp.
# Previous benchmark was WRONG: -o all uses HINT_M_ALL, bypassing our SIMD code entirely.

set -e

DATASET="samples/AARHUS-BOOKS_2013.dat"
QUERIES="samples/AARHUS-BOOKS_2013_20k.qry"
BITS=10
RUNS=10
NUM_ITERATIONS=5

# Ensure dataset is available
if [ ! -f "$DATASET" ]; then
    if [ -f "${DATASET}.gz" ]; then
        echo "Extracting dataset..."
        gunzip -k "${DATASET}.gz"
    else
        echo "ERROR: Dataset not found: $DATASET"
        exit 1
    fi
fi

echo "============================================================"
echo "  HINT^m AVX2 SIMD Benchmark (SoA Layout)"
echo "============================================================"
echo ""
echo "Dataset  : $DATASET"
echo "Queries  : $QUERIES"
echo "Bits     : $BITS"
echo "Runs/qry : $RUNS"
echo "Iterations: $NUM_ITERATIONS"
echo ""
echo "IMPORTANT: Using base HINT^m (no -o flag), NOT -o all."
echo "           -o all uses HINT_M_ALL which is NOT SIMD-modified."
echo ""

# ---- Original ----
echo "============================================================"
echo "  Original HINT^m (scalar)"
echo "============================================================"
orig_times=()
for iter in $(seq 1 $NUM_ITERATIONS); do
    output=$(./query_hint_m.exec -m $BITS -q gOVERLAPS -r $RUNS $DATASET $QUERIES 2>/dev/null)
    t=$(echo "$output" | grep "Total querying time" | awk '{print $NF}')
    tp=$(echo "$output" | grep "Throughput" | awk '{print $NF}')
    printf "  Run %d: Time = %s s, Throughput = %s q/s\n" "$iter" "$t" "$tp"
    orig_times+=("$t")
done

# ---- SIMD ----
echo ""
echo "============================================================"
echo "  SIMD HINT^m (SoA + AVX2)"
echo "============================================================"
simd_times=()
for iter in $(seq 1 $NUM_ITERATIONS); do
    output=$(./query_hint_m_simd.exec -m $BITS -q gOVERLAPS -r $RUNS $DATASET $QUERIES 2>/dev/null)
    t=$(echo "$output" | grep "Total querying time" | awk '{print $NF}')
    tp=$(echo "$output" | grep "Throughput" | awk '{print $NF}')
    printf "  Run %d: Time = %s s, Throughput = %s q/s\n" "$iter" "$t" "$tp"
    simd_times+=("$t")
done

# ---- Summary ----
echo ""
echo "============================================================"
echo "  Summary"
echo "============================================================"
# Calculate averages using awk
orig_avg=$(printf '%s\n' "${orig_times[@]}" | awk '{s+=$1} END {printf "%.6f", s/NR}')
simd_avg=$(printf '%s\n' "${simd_times[@]}" | awk '{s+=$1} END {printf "%.6f", s/NR}')
speedup=$(echo "$orig_avg $simd_avg" | awk '{printf "%.2f", ($1/$2)}')
pct=$(echo "$orig_avg $simd_avg" | awk '{printf "%.1f", (($1-$2)/$1)*100}')

echo "  Original avg time : ${orig_avg}s"
echo "  SIMD avg time     : ${simd_avg}s"
echo "  Speedup           : ${speedup}x"
echo "  Time reduction    : ${pct}%"
echo ""

# Also run with top-down strategy
echo "============================================================"
echo "  Top-Down Strategy Comparison"
echo "============================================================"
echo ""
echo "  --- Original (Top-Down) ---"
orig_td_times=()
for iter in $(seq 1 $NUM_ITERATIONS); do
    output=$(./query_hint_m.exec -m $BITS -t -q gOVERLAPS -r $RUNS $DATASET $QUERIES 2>/dev/null)
    t=$(echo "$output" | grep "Total querying time" | awk '{print $NF}')
    tp=$(echo "$output" | grep "Throughput" | awk '{print $NF}')
    printf "  Run %d: Time = %s s, Throughput = %s q/s\n" "$iter" "$t" "$tp"
    orig_td_times+=("$t")
done

echo ""
echo "  --- SIMD (Top-Down) ---"
simd_td_times=()
for iter in $(seq 1 $NUM_ITERATIONS); do
    output=$(./query_hint_m_simd.exec -m $BITS -t -q gOVERLAPS -r $RUNS $DATASET $QUERIES 2>/dev/null)
    t=$(echo "$output" | grep "Total querying time" | awk '{print $NF}')
    tp=$(echo "$output" | grep "Throughput" | awk '{print $NF}')
    printf "  Run %d: Time = %s s, Throughput = %s q/s\n" "$iter" "$t" "$tp"
    simd_td_times+=("$t")
done

orig_td_avg=$(printf '%s\n' "${orig_td_times[@]}" | awk '{s+=$1} END {printf "%.6f", s/NR}')
simd_td_avg=$(printf '%s\n' "${simd_td_times[@]}" | awk '{s+=$1} END {printf "%.6f", s/NR}')
speedup_td=$(echo "$orig_td_avg $simd_td_avg" | awk '{printf "%.2f", ($1/$2)}')
pct_td=$(echo "$orig_td_avg $simd_td_avg" | awk '{printf "%.1f", (($1-$2)/$1)*100}')

echo ""
echo "  Top-Down avg original : ${orig_td_avg}s"
echo "  Top-Down avg SIMD     : ${simd_td_avg}s"
echo "  Speedup               : ${speedup_td}x"
echo "  Time reduction        : ${pct_td}%"
echo ""
echo "============================================================"
echo "  Benchmark complete."
echo "============================================================"
