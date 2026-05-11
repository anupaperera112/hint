#include <cmath>
#include <iostream>
#include <mutex>
#include <vector>
#define DUCKDB_EXTENSION_MAIN

#include "hint_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"

// Include your HINT^m files
#include "relation.h"
#include "hint_m.h"

namespace duckdb
{

    // =========================================================================
    // HINT+ GLOBAL STATE (Enhancement 1: HUD Architecture)
    // =========================================================================
    static std::mutex g_HintMutex;
    static std::unique_ptr<::Relation> g_MainRelation;
    static std::unique_ptr<::HINT_M> g_MainIndex;
    static std::vector<::Record> g_DeltaLog;
    static ::RecordId g_NextRecordId = 1;

    // Live Statistics tracking
    static ::Timestamp g_LiveGlobalStart = std::numeric_limits<::Timestamp>::max();
    static ::Timestamp g_LiveGlobalEnd = std::numeric_limits<::Timestamp>::min();
    static ::Timestamp g_LiveLongestRecord = 0;
    static size_t g_LiveTotalLengthSum = 0;
    static size_t g_LiveTotalRecords = 0;

    // =========================================================================
    // 1. hint_insert(start, end) -> BIGINT
    // O(1) insertion directly into the Delta Log.
    // =========================================================================
    static void HintInsertFunction(DataChunk &args, ExpressionState &state, Vector &result)
    {
        auto &start_vector = args.data[0];
        auto &end_vector = args.data[1];

        std::lock_guard<std::mutex> lock(g_HintMutex);

        BinaryExecutor::Execute<int64_t, int64_t, int64_t>(
            start_vector, end_vector, result, args.size(),
            [&](int64_t start_time, int64_t end_time)
            {
                ::RecordId id = g_NextRecordId++;
                g_DeltaLog.emplace_back(id, start_time, end_time);

                // Live statistics tracking calculation
                g_LiveGlobalStart = std::min(g_LiveGlobalStart, (::Timestamp)start_time);
                g_LiveGlobalEnd = std::max(g_LiveGlobalEnd, (::Timestamp)end_time);
                g_LiveLongestRecord = std::max(g_LiveLongestRecord, (::Timestamp)(end_time - start_time + 1));
                g_LiveTotalLengthSum += (size_t)(end_time - start_time);
                g_LiveTotalRecords++;

                return id;
            });
    }

    // =========================================================================
    // 2. hint_merge() -> VARCHAR
    // Lazy Merge mechanism for HINT+ Component Architecture
    // =========================================================================
    static void HintMergeFunction(DataChunk &args, ExpressionState &state, Vector &result)
    {
        std::lock_guard<std::mutex> lock(g_HintMutex);

        if (!g_MainRelation)
        {
            g_MainRelation = make_uniq<::Relation>();
        }

        size_t delta_size = g_DeltaLog.size();
        if (delta_size == 0)
        {
            result.SetValue(0, Value("No delta log records to merge."));
            return;
        }

        // Push everything from delta log to MainRelation (without inline calculations)
        for (const auto &rec : g_DeltaLog)
        {
            g_MainRelation->push_back(rec);
        }

        // Apply Live statistics directly
        g_MainRelation->gstart = g_LiveGlobalStart;
        g_MainRelation->gend = g_LiveGlobalEnd;
        g_MainRelation->longestRecord = g_LiveLongestRecord;

        if (g_LiveTotalRecords > 0)
        {
            g_MainRelation->avgRecordExtent = (float)g_LiveTotalLengthSum / g_LiveTotalRecords;
        }

        g_MainRelation->sortByStart();
        g_DeltaLog.clear();

        // Rebuild the index completely (Adaptive M tuning goes here later)
        size_t maxBits = (g_MainRelation->gend > 0) ? int(log2(g_MainRelation->gend) + 1) : 1;
        size_t numBits = 10;

        // Clean up old index
        try
        {
            g_MainIndex = make_uniq<::HINT_M>(*g_MainRelation, numBits, maxBits);
        }
        catch (...)
        {
            result.SetValue(0, Value("Error building index"));
            return;
        }

        char buffer[256];
        snprintf(buffer, sizeof(buffer), "Merged %zu delta items. Main index now holds %zu records.", delta_size, g_MainRelation->size());

        for (idx_t i = 0; i < args.size(); i++)
        {
            result.SetValue(i, Value(buffer));
        }
    }

    // =========================================================================
    // 3. hint_search(start, end) -> TABLE(count BIGINT)
    // Scans both HINT Main Index and Delta Log
    // =========================================================================
    struct HintSearchBindData : public TableFunctionData
    {
        int64_t start_time;
        int64_t end_time;

        HintSearchBindData(int64_t start, int64_t end) : start_time(start), end_time(end) {}
    };

    unique_ptr<FunctionData> HintSearchBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names)
    {
        names.push_back("overlapping_ids");
        return_types.push_back(LogicalType::BIGINT);
        return make_uniq<HintSearchBindData>(input.inputs[0].GetValue<int64_t>(), input.inputs[1].GetValue<int64_t>());
    }

    struct HintLocalState : public LocalTableFunctionState
    {
        bool done = false;
    };

    unique_ptr<GlobalTableFunctionState> InitHintState(ClientContext &context, TableFunctionInitInput &input)
    {
        return make_uniq<GlobalTableFunctionState>();
    }

    unique_ptr<LocalTableFunctionState> InitHintLocalState(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state)
    {
        return make_uniq<HintLocalState>();
    }

    void HintSearchOp(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
    {
        auto &lstate = data_p.local_state->Cast<HintLocalState>();

        if (lstate.done)
        {
            output.SetCardinality(0);
            return;
        }

        auto &bind_data = data_p.bind_data->Cast<HintSearchBindData>();
        ::RangeQuery Q;
        Q.start = bind_data.start_time;
        Q.end = bind_data.end_time;

        size_t overlapping_results = 0;

        std::lock_guard<std::mutex> lock(g_HintMutex);

        // 1. Query the main index
        if (g_MainIndex)
        {
            overlapping_results += g_MainIndex->execute_gOverlaps(Q);
        }

        // 2. Scan the Delta Log (simulating enhancement 1 pipeline)
        for (const auto &rec : g_DeltaLog)
        {
            // Condition for interval overlap: (A.start <= B.end) AND (A.end >= B.start)
            if (rec.start <= Q.end && rec.end >= Q.start)
            {
                overlapping_results++;
            }
        }

        output.SetCardinality(1);
        output.SetValue(0, 0, Value::BIGINT(overlapping_results));
        lstate.done = true;
    }

    // =========================================================================
    // REGISTRATION
    // =========================================================================
    static void LoadInternal(ExtensionLoader &loader)
    {
        // Scalar function for insert
        ScalarFunction hint_insert("hint_insert", {LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::BIGINT, HintInsertFunction);
        loader.RegisterFunction(hint_insert);

        // Scalar function to trigger merge
        ScalarFunction hint_merge("hint_merge", {}, LogicalType::VARCHAR, HintMergeFunction);
        loader.RegisterFunction(hint_merge);

        // Table function to search
        TableFunction hint_search_func("hint_search", {LogicalType::BIGINT, LogicalType::BIGINT},
                                       HintSearchOp, HintSearchBind, InitHintState, InitHintLocalState);
        loader.RegisterFunction(hint_search_func);
    }

    void HintExtension::Load(ExtensionLoader &loader)
    {
        LoadInternal(loader);
    }

    std::string HintExtension::Name()
    {
        return "hint";
    }

    std::string HintExtension::Version() const
    {
#ifdef EXT_VERSION_HINT
        return EXT_VERSION_HINT;
#else
        return "";
#endif
    }

} // namespace duckdb

extern "C"
{
    DUCKDB_CPP_EXTENSION_ENTRY(hint, loader)
    {
        duckdb::LoadInternal(loader);
    }
}
