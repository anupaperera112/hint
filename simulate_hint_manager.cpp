/******************************************************************************
 * Simulation driver for DynamicHINTManager
 *
 * Demonstrates and verifies:
 *   1. Initial index build
 *   2. Insertions (explicit ID and auto-ID)
 *   3. Deletions
 *   4. Updates (delete + insert with same ID)
 *   5. Manual rebuild (forceRebuild)
 *   6. Querying across all mutation types
 ******************************************************************************/

#define WORKLOAD_COUNT   // use COUNT mode for verifiable output

#include <iostream>
#include <cassert>
#include "dynamic_hint_manager.cpp"

using namespace std;

int main()
{
    // -----------------------------------------------------------------------
    // 1. Build initial dataset
    // -----------------------------------------------------------------------
    Relation initial_data;
    initial_data.push_back(Record(0, 10,  20));
    initial_data.push_back(Record(1, 50,  100));
    initial_data.push_back(Record(2, 80,  120));
    initial_data.push_back(Record(3, 150, 200));

    // Set global domain bounds
    initial_data.gstart          = 10;
    initial_data.gend            = 200;
    initial_data.longestRecord   = 51;  // record 1: 100-50+1
    initial_data.avgRecordExtent = 37.5;

    cout << "=== Initializing DynamicHINTManager ===" << endl;
    // Using low thresholds so we can test auto-merge behavior
    DynamicHINTManager manager(initial_data,
                               /*numBits=*/0,
                               /*insThreshold=*/100,
                               /*delThreshold=*/100);

    cout << "  Initial dataset size: " << manager.datasetSize() << endl;
    cout << "  Pending inserts: " << manager.pendingInserts() << endl;
    cout << "  Pending deletes: " << manager.pendingDeletes() << endl;

    // -----------------------------------------------------------------------
    // 2. Test query on initial data
    //    Query: overlaps with [90, 150]
    //    Expected matches: records 1 [50,100], 2 [80,120], 3 [150,200]
    // -----------------------------------------------------------------------
    cout << "\n=== Query: overlaps [90, 150] on initial data ===" << endl;
    RangeQuery rq1(1, 90, 150);
    size_t result = manager.executeQuery(rq1);
    cout << "  Result: " << result << " (expected: 3 -> records 1, 2, 3)" << endl;

    // -----------------------------------------------------------------------
    // 3. Test insertions
    //    Insert 3 new records
    // -----------------------------------------------------------------------
    cout << "\n=== Inserting 3 new records ===" << endl;
    manager.insertRecord(Record(4, 100, 110));  // Overlaps [90,150]
    manager.insertRecord(Record(5, 120, 130));  // Overlaps [90,150]
    manager.insertRecord(Record(6, 300, 350));  // Does NOT overlap

    cout << "  Pending inserts: " << manager.pendingInserts() << endl;
    result = manager.executeQuery(rq1);
    cout << "  Query [90, 150] after inserts: " << result
         << " (expected: 5 -> records 1, 2, 3, 4, 5)" << endl;

    // -----------------------------------------------------------------------
    // 4. Test deletions
    //    Delete records 4 and 2
    // -----------------------------------------------------------------------
    cout << "\n=== Deleting records 4 (from delta) and 2 (from base) ===" << endl;
    manager.deleteRecord(4);
    manager.deleteRecord(2);

    cout << "  Pending deletes: " << manager.pendingDeletes() << endl;
    result = manager.executeQuery(rq1);
    cout << "  Query [90, 150] after deletes: " << result
         << " (expected: 3 -> records 1, 3, 5)" << endl;

    // -----------------------------------------------------------------------
    // 5. Test update (delete + insert with same ID)
    //    Update record 3: [150, 200] -> [500, 600]  (should no longer overlap)
    // -----------------------------------------------------------------------
    cout << "\n=== Updating record 3: [150,200] -> [500,600] ===" << endl;
    manager.updateRecord(3, 500, 600);

    result = manager.executeQuery(rq1);
    cout << "  Query [90, 150] after update: " << result
         << " (expected: 2 -> records 1, 5)" << endl;

    // Verify record 3 is now queryable at its new position
    RangeQuery rq2(2, 490, 610);
    result = manager.executeQuery(rq2);
    cout << "  Query [490, 610]: " << result
         << " (expected: 1 -> record 3 at [500,600])" << endl;

    // -----------------------------------------------------------------------
    // 6. Test auto-ID insert
    // -----------------------------------------------------------------------
    cout << "\n=== Inserting with auto-generated ID ===" << endl;
    manager.insertRecord(95, 105);  // overlaps [90,150]
    result = manager.executeQuery(rq1);
    cout << "  Query [90, 150] after auto-ID insert: " << result
         << " (expected: 3 -> records 1, 5, auto)" << endl;

    // -----------------------------------------------------------------------
    // 7. Test force rebuild
    // -----------------------------------------------------------------------
    cout << "\n=== Force rebuild ===" << endl;
    cout << "  Merges before: " << manager.totalMerges() << endl;
    cout << "  Pending inserts before: " << manager.pendingInserts() << endl;
    cout << "  Pending deletes before: " << manager.pendingDeletes() << endl;

    manager.forceRebuild();

    cout << "  Merges after:  " << manager.totalMerges() << endl;
    cout << "  Pending inserts after:  " << manager.pendingInserts() << endl;
    cout << "  Pending deletes after:  " << manager.pendingDeletes() << endl;
    cout << "  Dataset size after:     " << manager.datasetSize() << endl;

    // Verify queries still work after rebuild
    result = manager.executeQuery(rq1);
    cout << "  Query [90, 150] after rebuild: " << result
         << " (expected: 3 -> same as before)" << endl;

    result = manager.executeQuery(rq2);
    cout << "  Query [490, 610] after rebuild: " << result
         << " (expected: 1 -> record 3)" << endl;

    // -----------------------------------------------------------------------
    // 8. Print final statistics
    // -----------------------------------------------------------------------
    cout << endl;
    manager.printStats();

    cout << "\n=== All operations completed successfully ===" << endl;
    return 0;
}
