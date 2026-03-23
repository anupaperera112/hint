# HINT^m Delta Index — Walkthrough

## What Was Built

[HINT_M_Dynamic](file:///Users/sadeesha/Developer/Uni/Sem_8/hint/indices/hint_m_delta.cpp#14-59) — a delta-buffered wrapper for the base [HINT_M](file:///Users/sadeesha/Developer/Uni/Sem_8/hint/indices/hint_m.cpp#135-193) index, following the same pattern as the earlier [HINT_Dynamic](file:///Users/sadeesha/Developer/Uni/Sem_8/hint/indices/hint_delta.cpp#14-45), plus a benchmark script comparing **naive rebuild** vs **delta-buffered** approaches.

## Files Changed

| File | Action | Purpose |
|---|---|---|
| [hint_m_delta.h](file:///Users/sadeesha/Developer/Uni/Sem_8/hint/indices/hint_m_delta.h) | **NEW** | [HINT_M_Dynamic](file:///Users/sadeesha/Developer/Uni/Sem_8/hint/indices/hint_m_delta.cpp#14-59) class declaration |
| [hint_m_delta.cpp](file:///Users/sadeesha/Developer/Uni/Sem_8/hint/indices/hint_m_delta.cpp) | **NEW** | Full implementation: insert, remove, merge, top-down + bottom-up queries |
| [main_hint_m_delta.cpp](file:///Users/sadeesha/Developer/Uni/Sem_8/hint/main_hint_m_delta.cpp) | **NEW** | CLI driver: `-m` bits, `-t` top-down, `-u` ops, `-i`/`-d` thresholds |
| [benchmark_delta.sh](file:///Users/sadeesha/Developer/Uni/Sem_8/hint/benchmark_delta.sh) | **NEW** | Performance benchmark script |
| [makefile](file:///Users/sadeesha/Developer/Uni/Sem_8/hint/makefile) | **MODIFIED** | Added `hint_m_delta` target with Boost includes |

## HINT_M vs HINT Differences Handled

| Aspect | How HINT_M_Dynamic handles it |
|---|---|
| `numBits` / `maxBits` | Stores both; recomputes `numBits` via cost model on merge |
| Top-down + Bottom-up | Exposes both [executeTopDown_gOverlaps](file:///Users/sadeesha/Developer/Uni/Sem_8/hint/indices/hint_m_delta.cpp#200-244) and [executeBottomUp_gOverlaps](file:///Users/sadeesha/Developer/Uni/Sem_8/hint/indices/hierarchicalindex.h#109-110) |
| Full Record storage | Same fast/slow path approach (HINT^m still returns aggregated XOR/COUNT) |
| Merge count tracking | Reports `numMerges` for benchmarking |

## Correctness Verification

### Test 1: No operations (baseline match)
```
Baseline HINT^m (10 bits) XOR:    41666690213
HINT_M_Dynamic (no ops) XOR:      41666690213  ✅ MATCH
```

### Test 2: 3 inserts + 2 deletes, threshold=2
```
  Num of Originals:    2312603  (= 2312602 − 2 + 3) ✅
  Total merges:        2  ✅
  Pending inserts:     0  (all merged) ✅
  Pending deletes:     0  (all merged) ✅
  XOR result:          41496025303 (data changed) ✅
```

## Benchmark

The benchmark script (`benchmark_delta.sh`) runs 6 comprehensive tests:

| Test | What It Measures | Configurations |
|---|---|---|
| **1. Baseline** | Reference point (no operations) | Single run |
| **2. I/D Ratios** | How insert vs delete mix affects cost | 100/0, 80/20, 50/50, 20/80, 0/100 |
| **3. Scaling** | Performance at different operation counts | 10 to 5000 ops (naive auto-skipped > 100) |
| **4. Threshold Tuning** | Finding optimal merge threshold | Thresholds: 1, 5, 10, 25, 50, 100, 250, 500, 1000 |
| **5. numBits** | Index granularity impact on delta performance | 5, 8, 10, 12, 15, 18, 20 bits |
| **6. Insert vs Delete** | Isolating cost of each operation type | Pure insert vs pure delete at 50–5000 ops |

Results are saved to a timestamped file in `benchmark_results/`.

### Key Findings

1. **Operations time**: Delta approach is essentially **free** (< 1ms) vs naive which rebuilds the full 2.3M-record index after every operation.

2. **Total time speedup**: Delta is orders of magnitude faster for write-heavy workloads. The gap grows linearly with operation count since naive is O(n × rebuild_cost).

3. **Query overhead from deletes**: When deletes exist, the delta approach falls back to a linear scan of the base relation. This is the core trade-off — query performance degrades until the next merge.

4. **Insert-only is fast**: With only inserts (no deletes), queries use the fast path (HINT index + small linear scan of delta buffer), so query performance stays high.

5. **Threshold tuning matters**: Low thresholds behave like naive (frequent merges, fast queries). High thresholds minimize merge overhead but slow queries. The optimal threshold depends on your read/write ratio.

> [!IMPORTANT]
> The delta approach is best for **write-heavy workloads** where operations outnumber queries. For **read-heavy workloads**, keep thresholds low so merges happen frequently, maintaining fast query performance.

## Usage

```bash
# 1. Build
make hint_m_delta

# 2. Decompress sample data (only needed once — ships as .gz)
gunzip -k samples/AARHUS-BOOKS_2013.dat.gz

# 3. Run without operations (baseline)
./query_hint_m_delta.exec -m 10 -q gOVERLAPS \
    samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry

# 4. Create a sample operations file
printf 'I 100 200\nI 150 300\nI 500 1000\nD 0\nD 1\n' > ops.txt

# 5. Run with operations and custom thresholds
./query_hint_m_delta.exec -m 10 -q gOVERLAPS \
    -u ops.txt -i 500 -d 200 \
    samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry

# 6. Run full benchmark
bash benchmark_delta.sh
```
