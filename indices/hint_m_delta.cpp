/******************************************************************************
 * Project:  hint
 * Purpose:  Indexing interval data - Dynamic HINT^m with delta indexes
 * Author:   Extended from original HINT by Bouros, Christodoulou, Mamoulis
 ******************************************************************************
 * Delta-index extension: adds insert/delete buffering with threshold-based
 * merge to the base HINT^m index.
 ******************************************************************************/

#include "hint_m_delta.h"



HINT_M_Dynamic::HINT_M_Dynamic(const Relation &R, const unsigned int numBits,
                               const unsigned int maxBits,
                               const unsigned int insertThreshold,
                               const unsigned int deleteThreshold)
{
    this->numBits          = numBits;
    this->maxBits          = maxBits;
    this->insertThreshold  = insertThreshold;
    this->deleteThreshold  = deleteThreshold;
    this->numMerges        = 0;

    // If numBits was 0 (auto), determine via cost model
    if (numBits == 0)
    {
        this->numBits = determineOptimalNumBitsForHINT_M(R, 0.1);
        this->autoNumBits = true;
    }
    else
    {
        this->autoNumBits = false;
    }

    // Store a copy of the base relation for future rebuilds
    this->baseRelation.gstart          = R.gstart;
    this->baseRelation.gend            = R.gend;
    this->baseRelation.longestRecord   = R.longestRecord;
    this->baseRelation.avgRecordExtent = R.avgRecordExtent;
    for (const Record &r : R)
        this->baseRelation.push_back(r);

    // Track the next available record ID
    this->nextId = (RecordId)R.size();

    // Build the initial main HINT^m index
    this->mainIndex = new HINT_M(R, this->numBits, this->maxBits);

    // Initialise statistics
    this->numPartitions      = 0;
    this->numEmptyPartitions = 0;
    this->avgPartitionSize   = 0;
    this->numOriginals       = 0;
    this->numReplicas        = 0;
    this->numDeltaInserts    = 0;
    this->numDeltaDeletes    = 0;
}


HINT_M_Dynamic::~HINT_M_Dynamic()
{
    delete this->mainIndex;
}


// ---------------------------------------------------------------------------
//  Mutation operations
// ---------------------------------------------------------------------------

void HINT_M_Dynamic::insert(Timestamp start, Timestamp end)
{
    Record r(this->nextId++, start, end);
    this->insert(r);
}


void HINT_M_Dynamic::insert(const Record &r)
{
    // If this record was previously deleted, undo the delete
    auto it = this->deltaDeletes.find(r.id);
    if (it != this->deltaDeletes.end())
        this->deltaDeletes.erase(it);

    this->deltaInserts.push_back(r);

    // Auto-merge if insert threshold is reached
    if (this->needsMerge())
        this->merge();
}


void HINT_M_Dynamic::remove(RecordId id)
{
    this->deltaDeletes.insert(id);

    // Auto-merge if delete threshold is reached
    if (this->needsMerge())
        this->merge();
}


bool HINT_M_Dynamic::needsMerge() const
{
    return (this->deltaInserts.size() >= this->insertThreshold) ||
           (this->deltaDeletes.size() >= this->deleteThreshold);
}


// ---------------------------------------------------------------------------
//  Merge: rebuild the main index incorporating both deltas
// ---------------------------------------------------------------------------

void HINT_M_Dynamic::merge()
{
    // Step 1: Build a new relation = baseRelation - deletes + inserts
    Relation newRelation;
    newRelation.gstart          = std::numeric_limits<Timestamp>::max();
    newRelation.gend            = std::numeric_limits<Timestamp>::min();
    newRelation.longestRecord   = std::numeric_limits<Timestamp>::min();
    newRelation.avgRecordExtent = 0;

    size_t sum = 0;

    // Copy base records that are not deleted
    for (const Record &r : this->baseRelation)
    {
        if (this->deltaDeletes.find(r.id) == this->deltaDeletes.end())
        {
            newRelation.push_back(r);
            newRelation.gstart = std::min(newRelation.gstart, r.start);
            newRelation.gend   = std::max(newRelation.gend, r.end);
            newRelation.longestRecord = std::max(newRelation.longestRecord, r.end - r.start + 1);
            sum += r.end - r.start;
        }
    }

    // Add inserted records that are not (subsequently) deleted
    for (const Record &r : this->deltaInserts)
    {
        if (this->deltaDeletes.find(r.id) == this->deltaDeletes.end())
        {
            newRelation.push_back(r);
            newRelation.gstart = std::min(newRelation.gstart, r.start);
            newRelation.gend   = std::max(newRelation.gend, r.end);
            newRelation.longestRecord = std::max(newRelation.longestRecord, r.end - r.start + 1);
            sum += r.end - r.start;
        }
    }

    if (!newRelation.empty())
        newRelation.avgRecordExtent = (float)sum / newRelation.size();

    // Step 2: Recalculate maxBits based on new domain
    unsigned int newMaxBits = (newRelation.gend > newRelation.gstart)
                              ? (unsigned int)(log2(newRelation.gend - newRelation.gstart) + 1)
                              : 1;

    // Step 3: Recalculate numBits via cost model if auto, else keep user-specified
    unsigned int newNumBits;
    if (this->autoNumBits)
        newNumBits = determineOptimalNumBitsForHINT_M(newRelation, 0.1);
    else
        newNumBits = std::min(this->numBits, newMaxBits);

    // Step 4: Rebuild the HINT^m index
    delete this->mainIndex;
    this->mainIndex = new HINT_M(newRelation, newNumBits, newMaxBits);
    this->numBits = newNumBits;
    this->maxBits = newMaxBits;

    // Step 5: Replace base relation with the merged version
    this->baseRelation.clear();
    this->baseRelation.gstart          = newRelation.gstart;
    this->baseRelation.gend            = newRelation.gend;
    this->baseRelation.longestRecord   = newRelation.longestRecord;
    this->baseRelation.avgRecordExtent = newRelation.avgRecordExtent;
    for (const Record &r : newRelation)
        this->baseRelation.push_back(r);

    // Step 6: Clear both delta buffers
    this->deltaInserts.clear();
    this->deltaDeletes.clear();

    // Update nextId to be safe
    for (const Record &r : this->baseRelation)
    {
        if (r.id >= this->nextId)
            this->nextId = r.id + 1;
    }

    this->numMerges++;
}


// ---------------------------------------------------------------------------
//  Querying — union of main index and delta inserts, minus delta deletes
// ---------------------------------------------------------------------------

size_t HINT_M_Dynamic::executeTopDown_gOverlaps(RangeQuery Q)
{
    size_t result = 0;

    if (this->deltaDeletes.empty())
    {
        // Fast path: no deletes pending, use HINT^m directly
        result = this->mainIndex->executeTopDown_gOverlaps(Q);
    }
    else
    {
        // Slow path: scan base relation, skip deleted IDs
        for (const Record &r : this->baseRelation)
        {
            if (this->deltaDeletes.find(r.id) != this->deltaDeletes.end())
                continue;
            if ((r.start <= Q.end) && (Q.start <= r.end))
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= r.id;
#endif
            }
        }
    }

    // Scan delta inserts
    for (const Record &r : this->deltaInserts)
    {
        if (this->deltaDeletes.find(r.id) != this->deltaDeletes.end())
            continue;
        if ((r.start <= Q.end) && (Q.start <= r.end))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= r.id;
#endif
        }
    }

    return result;
}


size_t HINT_M_Dynamic::executeBottomUp_gOverlaps(RangeQuery Q)
{
    size_t result = 0;

    if (this->deltaDeletes.empty())
    {
        // Fast path: no deletes pending, use HINT^m directly
        result = this->mainIndex->executeBottomUp_gOverlaps(Q);
    }
    else
    {
        // Slow path: scan base relation, skip deleted IDs
        for (const Record &r : this->baseRelation)
        {
            if (this->deltaDeletes.find(r.id) != this->deltaDeletes.end())
                continue;
            if ((r.start <= Q.end) && (Q.start <= r.end))
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= r.id;
#endif
            }
        }
    }

    // Scan delta inserts
    for (const Record &r : this->deltaInserts)
    {
        if (this->deltaDeletes.find(r.id) != this->deltaDeletes.end())
            continue;
        if ((r.start <= Q.end) && (Q.start <= r.end))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= r.id;
#endif
        }
    }

    return result;
}


// ---------------------------------------------------------------------------
//  Statistics
// ---------------------------------------------------------------------------

void HINT_M_Dynamic::getStats()
{
    this->mainIndex->getStats();
    this->numPartitions      = this->mainIndex->numPartitions;
    this->numEmptyPartitions = this->mainIndex->numEmptyPartitions;
    this->avgPartitionSize   = this->mainIndex->avgPartitionSize;
    this->numOriginals       = this->mainIndex->numOriginals;
    this->numReplicas        = this->mainIndex->numReplicas;
    this->numDeltaInserts    = this->deltaInserts.size();
    this->numDeltaDeletes    = this->deltaDeletes.size();
}


void HINT_M_Dynamic::printStats() const
{
    printf("  Delta inserts (pending)   : %zu\n", this->deltaInserts.size());
    printf("  Delta deletes (pending)   : %zu\n", this->deltaDeletes.size());
    printf("  Insert threshold          : %u\n",  this->insertThreshold);
    printf("  Delete threshold          : %u\n",  this->deleteThreshold);
    printf("  Total merges performed    : %zu\n", this->numMerges);
    printf("  Base relation size        : %zu\n", this->baseRelation.size());
}
