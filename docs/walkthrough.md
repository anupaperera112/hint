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

## Benchmark Results

**Dataset:** AARHUS-BOOKS 2013 (2.3M intervals) · **1000 queries** · **HINT^m with 10 bits**

Three approaches compared:
- **Naive**: threshold=1 (rebuilds after every single operation)
- **Delta**: threshold > total_ops (fully buffered, no merge during operations)
- **Moderate**: threshold = batch/2 (periodic merges)

| Operations | Approach | Ops Time (s) | Query Time (s) | **Total Time (s)** | Merges |
|---|---|---:|---:|---:|---:|
| 10I + 2D | Naive | 4.17 | 0.06 | **4.23** | 12 |
| | Delta | 0.00 | 7.80 | **7.80** | 0 |
| | Moderate | 0.70 | 7.79 | **8.48** | 2 |
| 50I + 10D | Naive | 20.79 | 0.06 | **20.85** | 60 |
| | Delta | 0.00 | 8.30 | **8.30** | 0 |
| | Moderate | 0.69 | 8.29 | **8.98** | 2 |
| 100I + 20D | Naive | 41.26 | 0.06 | **41.32** | 120 |
| | Delta | 0.00 | 8.58 | **8.58** | 0 |
| | Moderate | 0.69 | 8.85 | **9.55** | 2 |
| 500I + 100D | Naive | 206.91 | 0.06 | **206.97** | 600 |
| | **Delta** | **0.00** | **6.68** | **6.68** | **0** |
| | Moderate | 0.69 | 6.82 | **7.51** | 2 |

### Key Findings

1. **Operations time**: Delta approach is essentially **free** (< 1ms for 600 ops) vs naive (207s for 600 ops—each rebuilds the full 2.3M-record index)

2. **Total time speedup**: Delta is **31× faster** at 500 ops (6.7s vs 207s). This gap grows linearly with operation count since naive is O(n × rebuild_cost) per operation.

3. **Query overhead from deletes**: When deletes exist, the delta approach falls back to linear scan (~8s vs 0.06s indexed). This is the trade-off — query performance degrades until the next merge.

4. **Moderate threshold**: The balanced approach (threshold = batch/2) adds periodic merge cost (~0.7s per merge) but keeps query performance similar to delta.

5. **XOR results match exactly** across all three approaches for each batch size, confirming correctness.

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
