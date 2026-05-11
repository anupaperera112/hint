/******************************************************************************
 * Project:  hint
 * Purpose:  Indexing interval data - Dynamic HINT^m with delta indexes
 * Author:   Extended from original HINT by Bouros, Christodoulou, Mamoulis
 ******************************************************************************
 * Delta-index extension: adds insert/delete/update buffering with
 * threshold-based merge to the base HINT^m index.
 ******************************************************************************/

#ifndef _HINT_M_DELTA_H_
#define _HINT_M_DELTA_H_

#include "../def_global.h"
#include "../containers/relation.h"
#include "hint_m.h"
#include <unordered_set>



// Dynamic HINT^m wrapper with delta insert/delete indexes
class HINT_M_Dynamic
{
private:
    // The main static HINT^m index
    HINT_M *mainIndex;

    // The base relation (ground truth for rebuilds)
    Relation baseRelation;
    unsigned int numBits;
    unsigned int maxBits;

    // Delta insert buffer: new records not yet in the main index
    Relation deltaInserts;

    // Delta delete set: record IDs logically removed from the main index
    std::unordered_set<RecordId> deltaDeletes;

    // Separate merge thresholds; if EITHER is met, BOTH deltas merge
    unsigned int insertThreshold;
    unsigned int deleteThreshold;

    // Auto-incrementing ID for new inserts
    RecordId nextId;

    // Whether to use the cost model to determine numBits on rebuild
    bool autoNumBits;

public:
    // Statistics (mirror HierarchicalIndex public stats)
    size_t numPartitions;
    size_t numEmptyPartitions;
    float  avgPartitionSize;
    size_t numOriginals, numReplicas;
    size_t numDeltaInserts, numDeltaDeletes;
    size_t numMerges;

    // Construction
    HINT_M_Dynamic(const Relation &R, const unsigned int numBits,
                   const unsigned int maxBits,
                   const unsigned int insertThreshold,
                   const unsigned int deleteThreshold);
    ~HINT_M_Dynamic();

    // Mutation operations
    void insert(Timestamp start, Timestamp end);
    void insert(const Record &r);
    void remove(RecordId id);

    // Update = delete old record + insert new record with same ID
    void update(RecordId id, Timestamp newStart, Timestamp newEnd);

    // Check whether merge thresholds have been reached
    bool needsMerge() const;

    // Merge both deltas into the main index (rebuild)
    void merge();

    // Force an immediate rebuild regardless of thresholds
    void forceRebuild();

    // Querying — combines main index + delta insert, minus delta delete
    size_t executeTopDown_gOverlaps(RangeQuery Q);
    size_t executeBottomUp_gOverlaps(RangeQuery Q);

    // ID-collecting query for DuckDB integration
    void collectBottomUp_gOverlaps(RangeQuery Q, std::vector<RecordId> &result);

    // Statistics
    void getStats();
    void printStats() const;

    // Accessors
    unsigned int getNumBits() const { return this->numBits; }
    unsigned int getMaxBits() const { return this->maxBits; }
    size_t getBaseRelationSize() const { return this->baseRelation.size(); }
    size_t getDeltaInsertsSize() const { return this->deltaInserts.size(); }
    size_t getDeltaDeletesSize() const { return this->deltaDeletes.size(); }
    RecordId getNextId() const { return this->nextId; }
};

#endif // _HINT_M_DELTA_H_
