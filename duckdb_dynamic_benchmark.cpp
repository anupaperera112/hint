/******************************************************************************
 * Project:  hint
 * Purpose:  Benchmark dynamic HINT^m index (delta-buffered, threshold-based
 *           auto-rebuild) against DuckDB full table scan.
 *
 * Scenarios:
 *   Phase 1 — Baseline:       query HINT vs DuckDB scan on clean data
 *   Phase 2 — Post-insert:    bulk-insert N records, auto-rebuild, re-query
 *   Phase 3 — Post-mixed:     insert + delete ops, auto-rebuild, re-query
 *   Phase 4 — Threshold sweep: vary rebuild threshold (10–1000), measure cost
 *
 * Output:
 *   Structured tables with latency, throughput, rebuild count, speedup.
 *   Correctness verified by comparing HINT results against DuckDB full scan.
 ******************************************************************************/

#define WORKLOAD_COUNT // Use COUNT mode for verifiable results

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "containers/relation.h"
#include "duckdb_amalg/duckdb.hpp"
#include "dynamic_hint_manager.cpp" // Includes HINT_M_Dynamic via DynamicHINTManager

using namespace std;

// ============================================================================
//  Global state for the DuckDB table function
// ============================================================================
HINT_M_Dynamic *global_index_ptr = nullptr;

// ============================================================================
//  DuckDB Table Function: hint_search(start, end)
// ============================================================================

struct HintTableFunctionData : public duckdb::TableFunctionData {
  Timestamp start_ts;
  Timestamp end_ts;
};

static duckdb::unique_ptr<duckdb::FunctionData>
HintSearchBind(duckdb::ClientContext &context,
               duckdb::TableFunctionBindInput &input,
               duckdb::vector<duckdb::LogicalType> &return_types,
               duckdb::vector<string> &names) {
  auto result = duckdb::make_uniq<HintTableFunctionData>();
  result->start_ts = input.inputs[0].GetValue<int64_t>();
  result->end_ts = input.inputs[1].GetValue<int64_t>();

  names.emplace_back("id");
  return_types.emplace_back(duckdb::LogicalType::BIGINT);

  return std::move(result);
}

struct HintGlobalState : public duckdb::GlobalTableFunctionState {
  std::vector<RecordId> result_ids;
  duckdb::idx_t offset = 0;
};

static duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
HintSearchInit(duckdb::ClientContext &context,
               duckdb::TableFunctionInitInput &input) {
  auto result = duckdb::make_uniq<HintGlobalState>();
  auto &bind_data = input.bind_data->Cast<HintTableFunctionData>();

  RangeQuery Q;
  Q.start = bind_data.start_ts;
  Q.end = bind_data.end_ts;

  if (global_index_ptr != nullptr) {
    global_index_ptr->collectBottomUp_gOverlaps(Q, result->result_ids);
  }

  return std::move(result);
}

static void HintSearchExecute(duckdb::ClientContext &context,
                              duckdb::TableFunctionInput &data_input,
                              duckdb::DataChunk &output) {
  auto &state = data_input.global_state->Cast<HintGlobalState>();

  duckdb::idx_t output_count = 0;
  while (state.offset < state.result_ids.size() &&
         output_count < STANDARD_VECTOR_SIZE) {
    output.data[0].SetValue(
        output_count, duckdb::Value::BIGINT(state.result_ids[state.offset]));
    state.offset++;
    output_count++;
  }

  output.SetCardinality(output_count);
}

// ============================================================================
//  Helper: Load relation from file
// ============================================================================
void loadRelation(const char *filepath, Relation &R) {
  ifstream in(filepath);
  if (!in.is_open()) {
    cerr << "Error: Cannot open file " << filepath << endl;
    exit(1);
  }

  RecordId id = 0;
  Timestamp start, end;
  while (in >> start >> end) {
    R.push_back(Record(id++, start, end));
    R.gstart = min(R.gstart, start);
    R.gend = max(R.gend, end);
    R.longestRecord = max(R.longestRecord, (Timestamp)(end - start + 1));
  }
  in.close();

  if (!R.empty()) {
    float sum = 0;
    for (const auto &r : R)
      sum += r.end - r.start;
    R.avgRecordExtent = sum / R.size();
  }
}

// ============================================================================
//  Helper: Load queries from file
// ============================================================================
vector<RangeQuery> loadQueries(const char *filepath, size_t limit = 0) {
  ifstream qin(filepath);
  if (!qin.is_open()) {
    cerr << "Error: Cannot open queries file " << filepath << endl;
    exit(1);
  }

  vector<RangeQuery> queries;
  Timestamp qstart, qend;
  size_t idx = 0;
  while (qin >> qstart >> qend) {
    RangeQuery Q;
    Q.id = idx++;
    Q.start = qstart;
    Q.end = qend;
    queries.push_back(Q);
    if (limit > 0 && queries.size() >= limit)
      break;
  }
  qin.close();
  return queries;
}

// ============================================================================
//  Helper: Generate random insert records
// ============================================================================
vector<pair<Timestamp, Timestamp>> generateInserts(size_t count,
                                                   Timestamp domainStart,
                                                   Timestamp domainEnd,
                                                   unsigned seed = 42) {
  mt19937 rng(seed);
  uniform_int_distribution<Timestamp> startDist(domainStart, domainEnd - 100);
  vector<pair<Timestamp, Timestamp>> inserts;

  for (size_t i = 0; i < count; i++) {
    Timestamp s = startDist(rng);
    Timestamp len = 1 + (rng() % min((Timestamp)1000, domainEnd - s));
    inserts.push_back({s, s + len});
  }
  return inserts;
}

// ============================================================================
//  Helper: Generate random delete IDs
// ============================================================================
vector<RecordId> generateDeletes(size_t count, size_t maxId,
                                 unsigned seed = 123) {
  mt19937 rng(seed);
  uniform_int_distribution<RecordId> idDist(0, (RecordId)(maxId - 1));
  vector<RecordId> deletes;

  // Use a set to avoid duplicate delete IDs
  unordered_set<RecordId> seen;
  while (deletes.size() < count && deletes.size() < maxId) {
    RecordId id = idDist(rng);
    if (seen.find(id) == seen.end()) {
      seen.insert(id);
      deletes.push_back(id);
    }
  }
  return deletes;
}

// ============================================================================
//  Helper: Sync DuckDB table with current operations
//  (Applies inserts/deletes to the DuckDB "intervals" table)
// ============================================================================
void syncDuckDBInserts(duckdb::Connection &con,
                       const vector<pair<Timestamp, Timestamp>> &inserts,
                       RecordId startId) {
  for (size_t i = 0; i < inserts.size(); i++) {
    string sql = "INSERT INTO intervals VALUES (" +
                 to_string(startId + (RecordId)i) + ", " +
                 to_string(inserts[i].first) + ", " +
                 to_string(inserts[i].second) + ")";
    con.Query(sql);
  }
}

void syncDuckDBDeletes(duckdb::Connection &con,
                       const vector<RecordId> &deletes) {
  for (RecordId id : deletes) {
    string sql = "DELETE FROM intervals WHERE id = " + to_string(id);
    con.Query(sql);
  }
}

// ============================================================================
//  Benchmark runner: executes queries on both HINT and DuckDB scan
// ============================================================================
struct BenchmarkResult {
  double hint_time_sec;
  double scan_time_sec;
  size_t num_queries;
  int mismatches;
  double hint_latency_ms; // ms per query
  double scan_latency_ms;
  double hint_throughput; // queries/sec
  double scan_throughput;
  double speedup;
};

BenchmarkResult runQueryBenchmark(duckdb::Connection &con,
                                  const vector<RangeQuery> &queries) {
  BenchmarkResult res;
  res.num_queries = queries.size();
  res.mismatches = 0;

  vector<size_t> hint_results(queries.size(), 0);
  vector<size_t> scan_results(queries.size(), 0);

  // --- HINT Index queries ---
  auto t1 = chrono::high_resolution_clock::now();
  for (size_t i = 0; i < queries.size(); i++) {
    string sql = "SELECT count(*) FROM hint_search(" +
                 to_string(queries[i].start) + ", " +
                 to_string(queries[i].end) + ")";
    auto result = con.Query(sql);
    if (!result->HasError()) {
      hint_results[i] = result->GetValue(0, 0).GetValue<int64_t>();
    }
  }
  auto t2 = chrono::high_resolution_clock::now();
  res.hint_time_sec = chrono::duration<double>(t2 - t1).count();

  // --- DuckDB full scan queries ---
  auto t3 = chrono::high_resolution_clock::now();
  for (size_t i = 0; i < queries.size(); i++) {
    string sql = "SELECT count(*) FROM intervals WHERE start_ts <= " +
                 to_string(queries[i].end) +
                 " AND end_ts >= " + to_string(queries[i].start);
    auto result = con.Query(sql);
    if (!result->HasError()) {
      scan_results[i] = result->GetValue(0, 0).GetValue<int64_t>();
    }
  }
  auto t4 = chrono::high_resolution_clock::now();
  res.scan_time_sec = chrono::duration<double>(t4 - t3).count();

  // Verification
  for (size_t i = 0; i < queries.size(); i++) {
    if (hint_results[i] != scan_results[i])
      res.mismatches++;
  }

  // Compute derived metrics
  res.hint_latency_ms = (res.hint_time_sec / res.num_queries) * 1000.0;
  res.scan_latency_ms = (res.scan_time_sec / res.num_queries) * 1000.0;
  res.hint_throughput = res.num_queries / res.hint_time_sec;
  res.scan_throughput = res.num_queries / res.scan_time_sec;
  res.speedup = res.scan_time_sec / res.hint_time_sec;

  return res;
}

// ============================================================================
//  Helper: Print section separator
// ============================================================================
void printSeparator(const string &title) {
  cout << "\n" << string(72, '=') << endl;
  cout << "  " << title << endl;
  cout << string(72, '=') << endl;
}

void printResultRow(const string &label, const BenchmarkResult &r) {
  printf("  %-22s | %8.3f ms | %10.0f ops/s | %6.2fx | %s\n", label.c_str(),
         r.hint_latency_ms, r.hint_throughput, r.speedup,
         r.mismatches == 0 ? "PASS" : "FAIL");
}

// ============================================================================
//  MAIN
// ============================================================================
int main() {
  const char *DATA_FILE = "samples/AARHUS-BOOKS_2013.dat";
  const char *QUERIES_FILE = "samples/AARHUS-BOOKS_2013_20k.qry";
  const size_t QUERY_LIMIT = 2000; // Use first 2000 queries for speed

  const size_t NUM_INSERTS = 500; // Inserts per scenario
  const size_t NUM_DELETES = 100; // Deletes per scenario

  cout << "╔═══════════════════════════════════════════════════════════════════"
          "═══╗"
       << endl;
  cout << "║     Dynamic HINT Index + DuckDB Benchmark                         "
          " ║"
       << endl;
  cout << "║     Threshold-Based Auto-Rebuild Performance Analysis             "
          " ║"
       << endl;
  cout << "╚═══════════════════════════════════════════════════════════════════"
          "═══╝"
       << endl;
  cout << endl;

  // ========================================================================
  //  1. Load Data
  // ========================================================================
  cout << "[1/6] Loading dataset " << DATA_FILE << " ..." << flush;
  Relation R;
  R.gstart = std::numeric_limits<Timestamp>::max();
  R.gend = std::numeric_limits<Timestamp>::min();
  R.longestRecord = 0;
  loadRelation(DATA_FILE, R);
  cout << " " << R.size() << " records loaded." << endl;
  cout << "  Domain: [" << R.gstart << ", " << R.gend << "]"
       << "  Range: " << (R.gend - R.gstart) << endl;

  // ========================================================================
  //  2. Load Queries
  // ========================================================================
  cout << "[2/6] Loading queries from " << QUERIES_FILE << " ..." << flush;
  vector<RangeQuery> queries = loadQueries(QUERIES_FILE, QUERY_LIMIT);
  cout << " " << queries.size() << " queries loaded." << endl;

  // ========================================================================
  //  3. Initialize DuckDB
  // ========================================================================
  cout << "[3/6] Initializing DuckDB (in-memory) ..." << flush;
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  con.Query("PRAGMA threads=1");

  // Register HINT table function
  duckdb::TableFunction hint_search_func(
      "hint_search", {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT},
      HintSearchExecute, HintSearchBind, HintSearchInit);

  duckdb::CreateTableFunctionInfo hint_search_info(hint_search_func);
  auto &context = *con.context;
  context.RunFunctionInTransaction([&]() {
    auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
    catalog.CreateTableFunction(context, &hint_search_info);
  });

  // Create DuckDB native table
  con.Query(
      "CREATE TABLE intervals (id BIGINT, start_ts BIGINT, end_ts BIGINT)");
  {
    auto appender = duckdb::make_uniq<duckdb::Appender>(con, "intervals");
    for (const auto &r : R) {
      appender->BeginRow();
      appender->Append<int64_t>(r.id);
      appender->Append<int64_t>(r.start);
      appender->Append<int64_t>(r.end);
      appender->EndRow();
    }
    appender->Close();
  }
  cout << " Done." << endl;

  // ========================================================================
  //  4. Generate workloads
  // ========================================================================
  cout << "[4/6] Generating workloads (" << NUM_INSERTS << " inserts, "
       << NUM_DELETES << " deletes) ..." << flush;

  auto insertRecords = generateInserts(NUM_INSERTS, R.gstart, R.gend);
  auto deleteIds = generateDeletes(NUM_DELETES, R.size());
  cout << " Done." << endl;

  // ========================================================================
  //  PHASE 1: Baseline — static HINT index, no modifications
  // ========================================================================
  printSeparator("PHASE 1: BASELINE (Static HINT Index)");

  cout << "  Building HINT_M_Dynamic (threshold=1000) ..." << flush;
  auto build_t1 = chrono::high_resolution_clock::now();
  HINT_M_Dynamic hint_baseline(R, 0, 0, 1000, 1000);
  auto build_t2 = chrono::high_resolution_clock::now();
  double build_time = chrono::duration<double>(build_t2 - build_t1).count();
  cout << " Done (" << fixed << setprecision(4) << build_time << "s)" << endl;

  global_index_ptr = &hint_baseline;

  cout << "  Running " << queries.size() << " queries ..." << flush;
  BenchmarkResult baseline = runQueryBenchmark(con, queries);
  cout << " Done." << endl;

  cout << "\n  Verification        : "
       << (baseline.mismatches == 0 ? "PASS" : "FAIL") << " ("
       << baseline.mismatches << " mismatches)" << endl;
  cout << "  ┌──────────────────────┬─────────────────┬─────────────────────┐"
       << endl;
  cout << "  │ Method               │ Latency (ms/op) │ Throughput (ops/s)  │"
       << endl;
  cout << "  ├──────────────────────┼─────────────────┼─────────────────────┤"
       << endl;
  printf("  │ HINT Index           │ %13.4f   │ %15.0f     │\n",
         baseline.hint_latency_ms, baseline.hint_throughput);
  printf("  │ DuckDB Full Scan     │ %13.4f   │ %15.0f     │\n",
         baseline.scan_latency_ms, baseline.scan_throughput);
  cout << "  └──────────────────────┴─────────────────┴─────────────────────┘"
       << endl;
  printf("  Speedup: HINT is %.2fx faster than DuckDB scan\n",
         baseline.speedup);

  // Clean up baseline index
  global_index_ptr = nullptr;

  // ========================================================================
  //  PHASE 2: Post-Insert — bulk insert, auto-rebuild, re-query
  // ========================================================================
  printSeparator("PHASE 2: POST-INSERT WORKLOAD");
  cout << "  Inserting " << NUM_INSERTS << " records with threshold=100 ..."
       << flush;

  HINT_M_Dynamic hint_insert(R, 0, 0, 100, 100);
  global_index_ptr = &hint_insert;

  auto ins_t1 = chrono::high_resolution_clock::now();
  for (size_t i = 0; i < insertRecords.size(); i++) {
    hint_insert.insert(insertRecords[i].first, insertRecords[i].second);
  }
  auto ins_t2 = chrono::high_resolution_clock::now();
  double ins_time = chrono::duration<double>(ins_t2 - ins_t1).count();

  cout << " Done (" << fixed << setprecision(4) << ins_time << "s)" << endl;
  cout << "  Auto-rebuilds triggered: " << hint_insert.numMerges << endl;
  cout << "  Pending inserts: " << hint_insert.getDeltaInsertsSize()
       << "  Pending deletes: " << hint_insert.getDeltaDeletesSize() << endl;

  // Sync DuckDB table
  RecordId startInsId = (RecordId)R.size();
  syncDuckDBInserts(con, insertRecords, startInsId);

  cout << "  Running " << queries.size() << " queries post-insert ..." << flush;
  BenchmarkResult postInsert = runQueryBenchmark(con, queries);
  cout << " Done." << endl;

  cout << "\n  Verification        : "
       << (postInsert.mismatches == 0 ? "PASS" : "FAIL") << " ("
       << postInsert.mismatches << " mismatches)" << endl;
  cout << "  ┌──────────────────────┬─────────────────┬─────────────────────┐"
       << endl;
  cout << "  │ Method               │ Latency (ms/op) │ Throughput (ops/s)  │"
       << endl;
  cout << "  ├──────────────────────┼─────────────────┼─────────────────────┤"
       << endl;
  printf("  │ HINT (post-insert)   │ %13.4f   │ %15.0f     │\n",
         postInsert.hint_latency_ms, postInsert.hint_throughput);
  printf("  │ DuckDB Full Scan     │ %13.4f   │ %15.0f     │\n",
         postInsert.scan_latency_ms, postInsert.scan_throughput);
  cout << "  └──────────────────────┴─────────────────┴─────────────────────┘"
       << endl;
  printf("  Speedup: HINT is %.2fx faster than DuckDB scan\n",
         postInsert.speedup);
  printf("  Insert ops time: %.4f s  |  Rebuilds: %zu\n", ins_time,
         hint_insert.numMerges);

  global_index_ptr = nullptr;

  // ========================================================================
  //  PHASE 3: Mixed Workload — inserts + deletes, auto-rebuild, re-query
  // ========================================================================
  printSeparator("PHASE 3: MIXED WORKLOAD (Insert + Delete)");

  // Reset DuckDB table to original data
  con.Query("DROP TABLE intervals");
  con.Query(
      "CREATE TABLE intervals (id BIGINT, start_ts BIGINT, end_ts BIGINT)");
  {
    auto appender = duckdb::make_uniq<duckdb::Appender>(con, "intervals");
    for (const auto &r : R) {
      appender->BeginRow();
      appender->Append<int64_t>(r.id);
      appender->Append<int64_t>(r.start);
      appender->Append<int64_t>(r.end);
      appender->EndRow();
    }
    appender->Close();
  }

  cout << "  Inserting " << NUM_INSERTS << " + deleting " << NUM_DELETES
       << " records (threshold=100) ..." << flush;

  HINT_M_Dynamic hint_mixed(R, 0, 0, 100, 100);
  global_index_ptr = &hint_mixed;

  auto mix_t1 = chrono::high_resolution_clock::now();

  // Interleave inserts and deletes for realism
  size_t insIdx = 0, delIdx = 0;
  while (insIdx < insertRecords.size() || delIdx < deleteIds.size()) {
    // Insert a batch of 5
    for (int b = 0; b < 5 && insIdx < insertRecords.size(); b++, insIdx++) {
      hint_mixed.insert(insertRecords[insIdx].first,
                        insertRecords[insIdx].second);
    }
    // Delete 1
    if (delIdx < deleteIds.size()) {
      hint_mixed.remove(deleteIds[delIdx]);
      delIdx++;
    }
  }

  auto mix_t2 = chrono::high_resolution_clock::now();
  double mix_time = chrono::duration<double>(mix_t2 - mix_t1).count();

  cout << " Done (" << fixed << setprecision(4) << mix_time << "s)" << endl;
  cout << "  Auto-rebuilds triggered: " << hint_mixed.numMerges << endl;
  cout << "  Pending inserts: " << hint_mixed.getDeltaInsertsSize()
       << "  Pending deletes: " << hint_mixed.getDeltaDeletesSize() << endl;

  // Sync DuckDB
  syncDuckDBInserts(con, insertRecords, startInsId);
  syncDuckDBDeletes(con, deleteIds);

  cout << "  Running " << queries.size() << " queries post-mixed ..." << flush;
  BenchmarkResult postMixed = runQueryBenchmark(con, queries);
  cout << " Done." << endl;

  cout << "\n  Verification        : "
       << (postMixed.mismatches == 0 ? "PASS" : "FAIL") << " ("
       << postMixed.mismatches << " mismatches)" << endl;
  cout << "  ┌──────────────────────┬─────────────────┬─────────────────────┐"
       << endl;
  cout << "  │ Method               │ Latency (ms/op) │ Throughput (ops/s)  │"
       << endl;
  cout << "  ├──────────────────────┼─────────────────┼─────────────────────┤"
       << endl;
  printf("  │ HINT (mixed ops)     │ %13.4f   │ %15.0f     │\n",
         postMixed.hint_latency_ms, postMixed.hint_throughput);
  printf("  │ DuckDB Full Scan     │ %13.4f   │ %15.0f     │\n",
         postMixed.scan_latency_ms, postMixed.scan_throughput);
  cout << "  └──────────────────────┴─────────────────┴─────────────────────┘"
       << endl;
  printf("  Speedup: HINT is %.2fx faster than DuckDB scan\n",
         postMixed.speedup);
  printf("  Mixed ops time: %.4f s  |  Rebuilds: %zu\n", mix_time,
         hint_mixed.numMerges);

  global_index_ptr = nullptr;

  // ========================================================================
  //  PHASE 4: Threshold Sweep
  // ========================================================================
  printSeparator("PHASE 4: THRESHOLD SWEEP (Optimal Rebuild Frequency)");

  cout << "  Testing thresholds: 10, 50, 100, 250, 500, 1000" << endl;
  cout << "  Each: " << NUM_INSERTS << " inserts + " << NUM_DELETES
       << " deletes, then " << queries.size() << " queries" << endl;

  vector<unsigned int> thresholds = {10, 50, 100, 250, 500, 1000};

  cout << "\n  "
          "┌───────────┬──────────┬───────────┬─────────────┬──────────────────"
          "┬─────────┬────────┐"
       << endl;
  cout << "  │ Threshold │ Rebuilds │ Ops (s)   │ Query (ms)  │ Throughput "
          "(q/s) │ Speedup │ Verify │"
       << endl;
  cout << "  "
          "├───────────┼──────────┼───────────┼─────────────┼──────────────────"
          "┼─────────┼────────┤"
       << endl;

  for (unsigned int thresh : thresholds) {
    // Reset DuckDB table
    con.Query("DROP TABLE intervals");
    con.Query(
        "CREATE TABLE intervals (id BIGINT, start_ts BIGINT, end_ts BIGINT)");
    {
      auto appender = duckdb::make_uniq<duckdb::Appender>(con, "intervals");
      for (const auto &r : R) {
        appender->BeginRow();
        appender->Append<int64_t>(r.id);
        appender->Append<int64_t>(r.start);
        appender->Append<int64_t>(r.end);
        appender->EndRow();
      }
      appender->Close();
    }

    // Build index with this threshold
    HINT_M_Dynamic hint_sweep(R, 0, 0, thresh, thresh);
    global_index_ptr = &hint_sweep;

    // Apply operations
    auto sw_t1 = chrono::high_resolution_clock::now();

    size_t si = 0, sd = 0;
    while (si < insertRecords.size() || sd < deleteIds.size()) {
      for (int b = 0; b < 5 && si < insertRecords.size(); b++, si++) {
        hint_sweep.insert(insertRecords[si].first, insertRecords[si].second);
      }
      if (sd < deleteIds.size()) {
        hint_sweep.remove(deleteIds[sd]);
        sd++;
      }
    }

    auto sw_t2 = chrono::high_resolution_clock::now();
    double sw_ops_time = chrono::duration<double>(sw_t2 - sw_t1).count();

    // Sync DuckDB
    syncDuckDBInserts(con, insertRecords, startInsId);
    syncDuckDBDeletes(con, deleteIds);

    // Run queries
    BenchmarkResult sw_result = runQueryBenchmark(con, queries);

    printf("  │ %9u │ %8zu │ %9.4f │ %11.4f │ %16.0f │ %7.2f │ %s  │\n", thresh,
           hint_sweep.numMerges, sw_ops_time, sw_result.hint_latency_ms,
           sw_result.hint_throughput, sw_result.speedup,
           sw_result.mismatches == 0 ? "PASS" : "FAIL");

    global_index_ptr = nullptr;
  }

  cout << "  "
          "└───────────┴──────────┴───────────┴─────────────┴──────────────────"
          "┴─────────┴────────┘"
       << endl;

  // ========================================================================
  //  PHASE 5: Direct HINT-only benchmark (no DuckDB overhead)
  // ========================================================================
  printSeparator("PHASE 5: DIRECT HINT QUERY (No DuckDB Overhead)");

  cout << "  Measuring raw HINT^m query performance (bypassing DuckDB) ..."
       << endl;

  // Build fresh index
  HINT_M_Dynamic hint_direct(R, 0, 0, 1000, 1000);

  auto dir_t1 = chrono::high_resolution_clock::now();
  size_t totalCountDirect = 0;
  for (size_t i = 0; i < queries.size(); i++) {
    totalCountDirect += hint_direct.executeBottomUp_gOverlaps(
        RangeQuery(queries[i].id, queries[i].start, queries[i].end));
  }
  auto dir_t2 = chrono::high_resolution_clock::now();
  double direct_time = chrono::duration<double>(dir_t2 - dir_t1).count();

  double direct_latency = (direct_time / queries.size()) * 1000.0;
  double direct_throughput = queries.size() / direct_time;

  printf("  Total count      : %zu\n", totalCountDirect);
  printf("  Latency          : %.4f ms/query\n", direct_latency);
  printf("  Throughput       : %.0f queries/sec\n", direct_throughput);
  printf("  Total time       : %.4f s\n", direct_time);

  // ========================================================================
  //  SUMMARY
  // ========================================================================
  printSeparator("FINAL SUMMARY");

  cout << "  "
          "┌──────────────────────────┬──────────────┬──────────────────┬──────"
          "───┬────────┐"
       << endl;
  cout << "  │ Scenario                 │ Latency (ms) │ Throughput (q/s) │ "
          "Speedup │ Verify │"
       << endl;
  cout << "  "
          "├──────────────────────────┼──────────────┼──────────────────┼──────"
          "───┼────────┤"
       << endl;

  auto printSummaryRow = [](const char *label, const BenchmarkResult &r) {
    printf("  │ %-24s │ %12.4f │ %16.0f │ %7.2f │ %s  │\n", label,
           r.hint_latency_ms, r.hint_throughput, r.speedup,
           r.mismatches == 0 ? "PASS" : "FAIL");
  };

  printSummaryRow("Baseline (static)", baseline);
  printSummaryRow("Post-Insert (500 ins)", postInsert);
  printSummaryRow("Mixed (500I + 100D)", postMixed);
  printf("  │ %-24s │ %12.4f │ %16.0f │     N/A │  N/A  │\n",
         "Direct HINT (no DuckDB)", direct_latency, direct_throughput);

  cout << "  "
          "├──────────────────────────┼──────────────┼──────────────────┼──────"
          "───┼────────┤"
       << endl;
  printf("  │ %-24s │ %12.4f │ %16.0f │     ref │  N/A  │\n",
         "DuckDB Full Scan (ref)", baseline.scan_latency_ms,
         baseline.scan_throughput);
  cout << "  "
          "└──────────────────────────┴──────────────┴──────────────────┴──────"
          "───┴────────┘"
       << endl;

  cout << "\n  Dataset   : " << R.size() << " records" << endl;
  cout << "  Queries   : " << queries.size() << endl;
  cout << "  Inserts   : " << NUM_INSERTS << endl;
  cout << "  Deletes   : " << NUM_DELETES << endl;

  cout << "\n" << string(72, '=') << endl;
  cout << "  Benchmark complete." << endl;
  cout << string(72, '=') << endl;

  return 0;
}
