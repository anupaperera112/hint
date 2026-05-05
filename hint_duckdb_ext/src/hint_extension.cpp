#include <cmath>
#include <iostream>
#define DUCKDB_EXTENSION_MAIN

#include "hint_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

// Include your HINT^m files
#include "relation.h"
#include "hint_m.h"

namespace duckdb {

// ---------------------------------------------------------
// 1. STATE OBJECT: Keeps HINT^m loaded in memory 
// ---------------------------------------------------------
struct HintIndexState : public GlobalTableFunctionState {
    ::Relation R;
    ::HINT_M* index; // Pointer to your HINT^m index

    HintIndexState() {
        std::cout << "Loading dataset..." << std::endl;
        
        // Use an absolute path or correct relative path to where duckdb is executed from
        R.load("../samples/AARHUS-BOOKS_2013.dat");
        
        if (R.size() == 0) {
            std::cerr << "CRITICAL ERROR: Dataset is empty! The file path is totally wrong." << std::endl;
        } else {
             std::cout << "Data successfully loaded: " << R.size() << " records." << std::endl;
             size_t maxBits = int(log2(R.gend-R.gstart)+1); size_t numBits = 10;
             index = new ::HINT_M(R, numBits, maxBits);
             std::cout << "HINT_M index built successfully." << std::endl;
        }
    }
    
    // Clean up memory when DB closes
    ~HintIndexState() {
        if (index) delete index;
    }
};

unique_ptr<GlobalTableFunctionState> InitHintState(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<HintIndexState>();
}

// ---------------------------------------------------------
// 2. EXECUTION LOGIC: What to do when a query hits
// ---------------------------------------------------------

struct HintLocalState : public LocalTableFunctionState {
    bool done = false;
};

unique_ptr<LocalTableFunctionState> InitHintLocalState(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
    return make_uniq<HintLocalState>();
}

void HintSearchOp(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<HintIndexState>();
    auto &lstate = data_p.local_state->Cast<HintLocalState>();

    if (lstate.done) {
        output.SetCardinality(0);
        return;
    }

    // 1. Grab the start & end timestamps the user typed in their SQL Query
    auto &start_input = data_p.bind_data->Cast<TableFunctionData>();
    ::Timestamp start_time = output.data[0].GetValue(0).GetValue<int64_t>(); // (Placeholder implementation tweak for full bind data)

    // For a single query test, we'll extract directly from the execution state via a fixed query
    ::RangeQuery Q;
    Q.start = 1356994800; // Let's hardcode a known interval for a quick test
    Q.end   = 1357081200;

    // 2. EXECUTE YOUR HINT^M WIZARDRY!!
    size_t overlapping_results = state.index->execute_gOverlaps(Q);

    // 3. Return the result to the SQL console
    output.SetCardinality(1);
    output.SetValue(0, 0, Value::BIGINT(overlapping_results)); 
    lstate.done = true;
}

// ---------------------------------------------------------
// 3. DATABASE BINDING: Tells SQL what to expect
// ---------------------------------------------------------
unique_ptr<FunctionData> HintBind(ClientContext &context, TableFunctionBindInput &input,
                                  vector<LogicalType> &return_types, vector<string> &names) {
    // We are returning a column of results named "overlapping_ids"
    names.push_back("overlapping_ids");
    return_types.push_back(LogicalType::BIGINT);
    return make_uniq<TableFunctionData>();
}
// ---------------------------------------------------------
// 4. REGISTRATION: Exposing it to DuckDB at boot
// ---------------------------------------------------------
static void LoadInternal(ExtensionLoader &loader) {
    // We add two input parameters: BIGINT (start) and BIGINT (end)
    TableFunction hint_search_func("hint_search", {LogicalType::BIGINT, LogicalType::BIGINT}, 
                                   HintSearchOp, HintBind, InitHintState, InitHintLocalState);
    loader.RegisterFunction(hint_search_func);
}

void HintExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string HintExtension::Name() {
    return "hint";
}

std::string HintExtension::Version() const {
#ifdef EXT_VERSION_HINT
    return EXT_VERSION_HINT;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(hint, loader) {
    duckdb::LoadInternal(loader);
}
}