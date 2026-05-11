/******************************************************************************
 * Project:  hint
 * Purpose:  DynamicHINTManager — high-level wrapper around HINT_M_Dynamic
 * Author:   Extended from original HINT by Bouros, Christodoulou, Mamoulis
 ******************************************************************************
 * Provides a clean API for insert / delete / update / query / rebuild
 * operations on a dynamic interval index backed by HINT^m with delta
 * buffering.
 *
 * Architecture:
 *   This manager delegates all index work to HINT_M_Dynamic (hint_m_delta),
 *   which maintains:
 *     - mainIndex     : a static HINT^m over the base relation
 *     - deltaInserts  : buffer of records not yet merged into mainIndex
 *     - deltaDeletes  : set of IDs logically removed from mainIndex
 *
 * Merge/Rebuild policy:
 *   Automatic: when |deltaInserts| >= insertThreshold  OR
 *              |deltaDeletes|  >= deleteThreshold
 *   Manual:    via forceRebuild()
 ******************************************************************************/

#include "def_global.h"
#include "containers/relation.h"
#include "indices/hint_m_delta.h"
#include <cmath>
#include <limits>


class DynamicHINTManager
{
private:
    HINT_M_Dynamic *index;

    // Keep a reference copy of config for diagnostics
    unsigned int insertThreshold;
    unsigned int deleteThreshold;

public:
    // -----------------------------------------------------------------------
    //  Construction
    // -----------------------------------------------------------------------

    /// Build from an initial Relation.
    /// @param initial_data  pre-loaded Relation (with gstart/gend set)
    /// @param numBits       number of partitioning bits (0 = auto via cost model)
    /// @param insThreshold  merge when pending inserts reach this count
    /// @param delThreshold  merge when pending deletes reach this count
    DynamicHINTManager(const Relation &initial_data,
                       unsigned int numBits      = 0,
                       unsigned int insThreshold = 1000,
                       unsigned int delThreshold = 1000)
    {
        this->insertThreshold = insThreshold;
        this->deleteThreshold = delThreshold;

        // Compute maxBits from domain
        unsigned int maxBits = 1;
        if (initial_data.gend > initial_data.gstart)
            maxBits = (unsigned int)(log2(initial_data.gend - initial_data.gstart) + 1);

        this->index = new HINT_M_Dynamic(initial_data, numBits, maxBits,
                                         insThreshold, delThreshold);
    }


    ~DynamicHINTManager()
    {
        if (this->index != nullptr)
            delete this->index;
    }


    // -----------------------------------------------------------------------
    //  Mutation operations
    // -----------------------------------------------------------------------

    /// Insert a record with an explicit Record object (id, start, end).
    void insertRecord(const Record &rec)
    {
        this->index->insert(rec);
    }

    /// Insert with just timestamps; the index auto-assigns a unique ID.
    void insertRecord(Timestamp start, Timestamp end)
    {
        this->index->insert(start, end);
    }

    /// Soft-delete a record by its ID.
    /// The record is logically removed immediately (excluded from queries)
    /// and physically removed at the next merge/rebuild.
    void deleteRecord(RecordId id)
    {
        this->index->remove(id);
    }

    /// Update = atomic delete + insert with the SAME record ID.
    /// The old interval is replaced by [newStart, newEnd].
    void updateRecord(RecordId id, Timestamp newStart, Timestamp newEnd)
    {
        this->index->update(id, newStart, newEnd);
    }


    // -----------------------------------------------------------------------
    //  Rebuild / Merge
    // -----------------------------------------------------------------------

    /// Force an immediate full rebuild of the index, regardless of thresholds.
    /// Useful after a batch of mutations or before a latency-critical query.
    void forceRebuild()
    {
        this->index->forceRebuild();
    }

    /// Check if automatic rebuild thresholds have been reached.
    bool isRebuildNeeded() const
    {
        return this->index->needsMerge();
    }


    // -----------------------------------------------------------------------
    //  Querying
    // -----------------------------------------------------------------------

    /// Execute an overlap query using the bottom-up HINT^m strategy.
    size_t executeQuery(const RangeQuery &q)
    {
        // Use non-const copy for the HINT^m API
        RangeQuery qCopy = q;
        return this->index->executeBottomUp_gOverlaps(qCopy);
    }

    /// Execute an overlap query using the top-down HINT^m strategy.
    size_t executeQueryTopDown(const RangeQuery &q)
    {
        RangeQuery qCopy = q;
        return this->index->executeTopDown_gOverlaps(qCopy);
    }


    // -----------------------------------------------------------------------
    //  Diagnostics
    // -----------------------------------------------------------------------

    size_t pendingInserts() const { return this->index->getDeltaInsertsSize(); }
    size_t pendingDeletes() const { return this->index->getDeltaDeletesSize(); }
    size_t totalMerges()    const { return this->index->numMerges; }
    size_t datasetSize()    const { return this->index->getBaseRelationSize(); }

    void getStats()
    {
        this->index->getStats();
    }

    void printStats() const
    {
        printf("=== DynamicHINTManager Statistics ===\n");
        this->index->printStats();
    }

    // Direct access to the underlying dynamic index (advanced use)
    HINT_M_Dynamic* getIndex() { return this->index; }
};
