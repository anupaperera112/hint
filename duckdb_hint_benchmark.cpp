#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <chrono>

#include "duckdb_amalg/duckdb.hpp"
#include "containers/relation.h"
#include "dynamic_hint_manager.cpp" // Includes HINT_M_Dynamic

using namespace std;

// Global index pointer for the DuckDB table function to access
HINT_M_Dynamic* global_index_ptr = nullptr;

// ----------------------------------------------------------------------------
// DuckDB Table Function: hint_search(start, end)
// ----------------------------------------------------------------------------

// Bind phase: Define output schema and read parameters
struct HintTableFunctionData : public duckdb::TableFunctionData {
    Timestamp start_ts; // HINT Timestamp
    Timestamp end_ts;
};

static duckdb::unique_ptr<duckdb::FunctionData> HintSearchBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                               duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<string> &names) {
    auto result = duckdb::make_uniq<HintTableFunctionData>();
    // Read the passed start and end interval timestamps
    result->start_ts = input.inputs[0].GetValue<int64_t>();
    result->end_ts = input.inputs[1].GetValue<int64_t>();

    // Define output: a single 'id' column of BIGINT
    names.emplace_back("id");
    return_types.emplace_back(duckdb::LogicalType::BIGINT);

    return std::move(result);
}

// Init phase: Run the query once and store results in state
struct HintGlobalState : public duckdb::GlobalTableFunctionState {
    std::vector<RecordId> result_ids;
    duckdb::idx_t offset = 0;
};

static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> HintSearchInit(duckdb::ClientContext &context, duckdb::TableFunctionInitInput &input) {
    auto result = duckdb::make_uniq<HintGlobalState>();
    auto &bind_data = input.bind_data->Cast<HintTableFunctionData>();

    // Create the range query object
    RangeQuery Q;
    Q.start = bind_data.start_ts;
    Q.end = bind_data.end_ts;

    // Call the HINT index collecting query
    if (global_index_ptr != nullptr) {
        global_index_ptr->collectBottomUp_gOverlaps(Q, result->result_ids);
    }
    
    return std::move(result);
}

// Execute phase: Yield chunks to DuckDB's vectorized engine
static void HintSearchExecute(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_input, duckdb::DataChunk &output) {
    auto &state = data_input.global_state->Cast<HintGlobalState>();
    
    duckdb::idx_t output_count = 0;
    while (state.offset < state.result_ids.size() && output_count < STANDARD_VECTOR_SIZE) {
        output.data[0].SetValue(output_count, duckdb::Value::BIGINT(state.result_ids[state.offset]));
        state.offset++;
        output_count++;
    }
    
    // Crucial: Tell DuckDB how many rows we wrote
    output.SetCardinality(output_count);
}


// ----------------------------------------------------------------------------
// Helper to Load Relation Data
// ----------------------------------------------------------------------------
void loadRelation(const char* filepath, Relation& R) {
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
    
    // Setup basics
    if (!R.empty()) {
        float sum = 0;
        for (const auto& r : R) sum += r.end - r.start;
        R.avgRecordExtent = sum / R.size();
    }
}


// ----------------------------------------------------------------------------
// Main Benchmark
// ----------------------------------------------------------------------------
int main() {
    cout << "=== HINT Index + DuckDB Vectorized Extension Benchmark ===\n" << endl;

    // 1. Load Data
    cout << "Loading dataset samples/AARHUS-BOOKS_2013.dat..." << endl;
    Relation R;
    R.gstart = std::numeric_limits<Timestamp>::max();
    R.gend = std::numeric_limits<Timestamp>::min();
    R.longestRecord = 0;
    loadRelation("samples/AARHUS-BOOKS_2013.dat", R);
    cout << "Loaded " << R.size() << " records." << endl;

    // 2. Initialize HINT Index
    cout << "Building HINT_M_Dynamic index..." << endl;
    HINT_M_Dynamic hint_index(R, 0, 0, 1000, 1000); // Auto numBits
    global_index_ptr = &hint_index;

    // 3. Initialize DuckDB
    cout << "Initializing in-memory DuckDB database..." << endl;
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    
    // Disable multi-threading in DuckDB for more consistent query benchmarking
    con.Query("PRAGMA threads=1"); 
    
    // Register the table function natively via DuckDB's internal C++ API
    duckdb::TableFunction hint_search_func("hint_search", {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT}, HintSearchExecute, HintSearchBind, HintSearchInit);
    
    duckdb::CreateTableFunctionInfo hint_search_info(hint_search_func);
    auto &context = *con.context;
    context.RunFunctionInTransaction([&]() {
        auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
        catalog.CreateTableFunction(context, &hint_search_info);
    });
    cout << "Table function hint_search(start, end) registered." << endl;

    // 4. Create DuckDB standard table and load data for full-scan benchmark
    cout << "Creating DuckDB native 'intervals' table..." << endl;
    con.Query("CREATE TABLE intervals (id BIGINT, start_ts BIGINT, end_ts BIGINT)");
    
    auto appender = duckdb::make_uniq<duckdb::Appender>(con, "intervals");
    for (const auto& r : R) {
        appender->BeginRow();
        appender->Append<int64_t>(r.id);
        appender->Append<int64_t>(r.start);
        appender->Append<int64_t>(r.end);
        appender->EndRow();
    }
    appender->Close();
    cout << "Data loaded into DuckDB native table." << endl;

    // 5. Benchmark Configuration
    const char* queries_file = "samples/AARHUS-BOOKS_2013_20k.qry";
    ifstream qin(queries_file);
    if (!qin.is_open()) {
        cerr << "Error: Cannot open queries file " << queries_file << endl;
        exit(1);
    }
    
    vector<RangeQuery> queries;
    Timestamp qstart, qend;
    while (qin >> qstart >> qend) {
        RangeQuery Q; Q.start = qstart; Q.end = qend;
        queries.push_back(Q);
    }
    qin.close();
    cout << "Loaded " << queries.size() << " queries for benchmarking.\n" << endl;


    // 6. Execute Benchmarks
    vector<size_t> hint_results(queries.size(), 0);
    vector<size_t> scan_results(queries.size(), 0);

    // ---- HINT INDEX BENCHMARK ----
    cout << "Running HINT Index queries via hint_search()..." << flush;
    auto t1 = chrono::high_resolution_clock::now();
    for (size_t i = 0; i < queries.size(); i++) {
        string sql = "SELECT count(*) FROM hint_search(" + to_string(queries[i].start) + ", " + to_string(queries[i].end) + ")";
        auto result = con.Query(sql);
        if (!result->HasError()) {
            hint_results[i] = result->GetValue(0, 0).GetValue<int64_t>();
        } else {
            cerr << "DuckDB Query Error: " << result->GetError() << endl;
        }
    }
    auto t2 = chrono::high_resolution_clock::now();
    double hint_time = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count() / 1000.0;
    cout << " Done (" << hint_time << "s)" << endl;


    // ---- DUCKDB NATIVE SCAN BENCHMARK ----
    cout << "Running DuckDB Native Full-Scan queries..." << flush;
    auto t3 = chrono::high_resolution_clock::now();
    for (size_t i = 0; i < queries.size(); i++) {
        string sql = "SELECT count(*) FROM intervals WHERE start_ts <= " + to_string(queries[i].end) + " AND end_ts >= " + to_string(queries[i].start);
        auto result = con.Query(sql);
        if (!result->HasError()) {
            scan_results[i] = result->GetValue(0, 0).GetValue<int64_t>();
        } else {
            cerr << "DuckDB Query Error: " << result->GetError() << endl;
        }
    }
    auto t4 = chrono::high_resolution_clock::now();
    double scan_time = chrono::duration_cast<chrono::milliseconds>(t4 - t3).count() / 1000.0;
    cout << " Done (" << scan_time << "s)\n" << endl;


    // 7. Verification & Summary
    int mismatches = 0;
    for (size_t i = 0; i < queries.size(); i++) {
        if (hint_results[i] != scan_results[i]) mismatches++;
    }

    cout << "=== BENCHMARK SUMMARY ===" << endl;
    cout << "Total Queries       : " << queries.size() << endl;
    cout << "Verification        : " << (mismatches == 0 ? "SUCCESS (All " + to_string(queries.size()) + " results match perfectly)" : "FAILED (" + to_string(mismatches) + " mismatches)") << endl;
    cout << "---------------------------------------" << endl;
    cout << "Method              | Latency (ms/op) | Throughput (ops/sec)" << endl;
    cout << "---------------------------------------" << endl;
    cout << "HINT Table Function | " << (hint_time/queries.size()*1000.0) << " ms        | " << (queries.size()/hint_time) << " ops/s" << endl;
    cout << "DuckDB Full Scan    | " << (scan_time/queries.size()*1000.0) << " ms        | " << (queries.size()/scan_time) << " ops/s" << endl;
    cout << "---------------------------------------" << endl;

    if (hint_time < scan_time) {
        cout << "Speedup: HINT is " << (scan_time / hint_time) << "x faster than DuckDB native scan." << endl;
    }

    return 0;
}
