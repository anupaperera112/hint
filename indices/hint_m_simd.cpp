/******************************************************************************
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 * Author:   George Christodoulou
 * Author:   Nikos Mamoulis
 ******************************************************************************
 * Copyright (c) 2020 - 2022
 *
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

#include "hint_m.h"
#include <immintrin.h>

// ============================================================================
// SoA-based SIMD helper functions.
//
// Each SoA block is laid out as: [ids (N) | starts (N) | ends (N)].
// Given a pointer `soa` and size `n`:
//   ids    = soa
//   starts = soa + n
//   ends   = soa + 2*n
//
// Using contiguous _mm256_loadu_si256 loads (~1 cycle) instead of
// _mm256_i32gather_epi32 (~12 cycles).
// ============================================================================

// Evaluate: (start <= Q.end) AND (Q.start <= end)
inline void simd_eval_both(const int *soa, size_t n, const RangeQuery &Q, size_t &result) {
    if (n == 0) return;
    const int *ids    = soa;
    const int *starts = soa + n;
    const int *ends   = soa + 2 * n;

    const __m256i v_qend   = _mm256_set1_epi32(Q.end);
    const __m256i v_qstart = _mm256_set1_epi32(Q.start);
    const __m256i v_ones   = _mm256_set1_epi32(-1);

    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        __m256i v_s = _mm256_loadu_si256((const __m256i*)(starts + i));
        __m256i v_e = _mm256_loadu_si256((const __m256i*)(ends   + i));

        // start <= Q.end  <=>  NOT(start > Q.end)
        __m256i cmp1 = _mm256_andnot_si256(_mm256_cmpgt_epi32(v_s, v_qend), v_ones);
        // Q.start <= end  <=>  NOT(Q.start > end)
        __m256i cmp2 = _mm256_andnot_si256(_mm256_cmpgt_epi32(v_qstart, v_e), v_ones);

        int mask = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_and_si256(cmp1, cmp2)));
        if (mask) {
#ifdef WORKLOAD_COUNT
            result += __builtin_popcount(mask);
#else
            __m256i v_id = _mm256_loadu_si256((const __m256i*)(ids + i));
            int id_buf[8];
            _mm256_storeu_si256((__m256i*)id_buf, v_id);
            while (mask) {
                result ^= id_buf[__builtin_ctz(mask)];
                mask &= mask - 1;
            }
#endif
        }
    }
    // Scalar tail
    for (; i < n; ++i) {
        if ((starts[i] <= Q.end) && (Q.start <= ends[i])) {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= ids[i];
#endif
        }
    }
}

// Evaluate: start <= Q.end  (end is already guaranteed)
inline void simd_eval_start(const int *soa, size_t n, const RangeQuery &Q, size_t &result) {
    if (n == 0) return;
    const int *ids    = soa;
    const int *starts = soa + n;

    const __m256i v_qend = _mm256_set1_epi32(Q.end);
    const __m256i v_ones = _mm256_set1_epi32(-1);

    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        __m256i v_s = _mm256_loadu_si256((const __m256i*)(starts + i));
        int mask = _mm256_movemask_ps(_mm256_castsi256_ps(
            _mm256_andnot_si256(_mm256_cmpgt_epi32(v_s, v_qend), v_ones)));
        if (mask) {
#ifdef WORKLOAD_COUNT
            result += __builtin_popcount(mask);
#else
            __m256i v_id = _mm256_loadu_si256((const __m256i*)(ids + i));
            int id_buf[8];
            _mm256_storeu_si256((__m256i*)id_buf, v_id);
            while (mask) {
                result ^= id_buf[__builtin_ctz(mask)];
                mask &= mask - 1;
            }
#endif
        }
    }
    for (; i < n; ++i) {
        if (starts[i] <= Q.end) {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= ids[i];
#endif
        }
    }
}

// Evaluate: Q.start <= end  (start is already guaranteed)
inline void simd_eval_end(const int *soa, size_t n, const RangeQuery &Q, size_t &result) {
    if (n == 0) return;
    const int *ids  = soa;
    const int *ends = soa + 2 * n;

    const __m256i v_qstart = _mm256_set1_epi32(Q.start);
    const __m256i v_ones   = _mm256_set1_epi32(-1);

    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        __m256i v_e = _mm256_loadu_si256((const __m256i*)(ends + i));
        int mask = _mm256_movemask_ps(_mm256_castsi256_ps(
            _mm256_andnot_si256(_mm256_cmpgt_epi32(v_qstart, v_e), v_ones)));
        if (mask) {
#ifdef WORKLOAD_COUNT
            result += __builtin_popcount(mask);
#else
            __m256i v_id = _mm256_loadu_si256((const __m256i*)(ids + i));
            int id_buf[8];
            _mm256_storeu_si256((__m256i*)id_buf, v_id);
            while (mask) {
                result ^= id_buf[__builtin_ctz(mask)];
                mask &= mask - 1;
            }
#endif
        }
    }
    for (; i < n; ++i) {
        if (Q.start <= ends[i]) {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= ids[i];
#endif
        }
    }
}

// No conditions needed – all records in partition are results
inline void simd_eval_none(const int *soa, size_t n, size_t &result) {
    if (n == 0) return;
#ifdef WORKLOAD_COUNT
    result += n;
#else
    const int *ids = soa;
    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        __m256i v_id = _mm256_loadu_si256((const __m256i*)(ids + i));
        int id_buf[8];
        _mm256_storeu_si256((__m256i*)id_buf, v_id);
        result ^= id_buf[0] ^ id_buf[1] ^ id_buf[2] ^ id_buf[3]
                 ^ id_buf[4] ^ id_buf[5] ^ id_buf[6] ^ id_buf[7];
    }
    for (; i < n; ++i)
        result ^= ids[i];
#endif
}


// ============================================================================
// Helper: build a SoA block from a Relation (vector<Record>).
// Layout: [ids (N) | starts (N) | ends (N)]
// ============================================================================
static int* buildSoABlock(const Relation &rel) {
    size_t n = rel.size();
    if (n == 0) return nullptr;
    int *block = new int[3 * n];
    int *ids    = block;
    int *starts = block + n;
    int *ends   = block + 2 * n;
    for (size_t k = 0; k < n; k++) {
        ids[k]    = rel[k].id;
        starts[k] = rel[k].start;
        ends[k]   = rel[k].end;
    }
    return block;
}


// ============================================================================
// Construction helpers (identical to base hint_m.cpp)
// ============================================================================

inline void HINT_M::updateCounters(const Record &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits - this->numBits);
    Timestamp b = r.end >> (this->maxBits - this->numBits);
    Timestamp prevb;
    int firstfound = 0;

    while (level < this->height && a <= b)
    {
        if (a % 2)
        { // last bit of a is 1
            if (firstfound)
            {
                // printf("added to level %d, bucket %d, class B\n",level,a);
                this->pReps_sizes[level][a]++;
            }
            else
            {
                // printf("added to level %d, bucket %d, class A\n",level,a);
                this->pOrgs_sizes[level][a]++;
                firstfound = 1;
            }
            // a+=(int)(pow(2,level));
            a++;
        }
        if (!(b % 2))
        { // last bit of b is 0
            prevb = b;
            // b-=(int)(pow(2,level));
            b--;
            if ((!firstfound) && b < a)
            {
                // printf("added to level %d, bucket %d, class A\n",level,prevb);
                this->pOrgs_sizes[level][prevb]++;
            }
            else
            {
                // printf("added to level %d, bucket %d, class B\n",level,prevb);
                this->pReps_sizes[level][prevb]++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}

inline void HINT_M::updatePartitions(const Record &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits - this->numBits);
    Timestamp b = r.end >> (this->maxBits - this->numBits);
    Timestamp prevb;
    int firstfound = 0;

    while (level < this->height && a <= b)
    {
        if (a % 2)
        { // last bit of a is 1
            if (firstfound)
            {
                this->pReps[level][a][this->pReps_sizes[level][a]] = r;
                this->pReps_sizes[level][a]++;
            }
            else
            {
                this->pOrgs[level][a][this->pOrgs_sizes[level][a]] = r;
                this->pOrgs_sizes[level][a]++;
                firstfound = 1;
            }
            // a+=(int)(pow(2,level));
            a++;
        }
        if (!(b % 2))
        { // last bit of b is 0
            prevb = b;
            b--;
            // b-=(int)(pow(2,level));
            if ((!firstfound) && b < a)
            {
                this->pOrgs[level][prevb][this->pOrgs_sizes[level][prevb]] = r;
                this->pOrgs_sizes[level][prevb]++;
            }
            else
            {
                this->pReps[level][prevb][this->pReps_sizes[level][prevb]] = r;
                this->pReps_sizes[level][prevb]++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}

HINT_M::HINT_M(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    // Step 1: one pass to count the contents inside each partition.
    //to store the only sizes of each partition from the relation data 
    //mallocs for each level
    this->pOrgs_sizes = (RecordId **)malloc(this->height * sizeof(RecordId *));
    this->pReps_sizes = (size_t **)malloc(this->height * sizeof(size_t *));


    // allocate space for each partionon inside the level 
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits - l));

        // calloc allocates memory and sets each counter to 0
        this->pOrgs_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pReps_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
    }

    // filling the correct sizes 
    for (const Record &r : R)
        this->updateCounters(r);

    // Step 2: allocate necessary memory using the porg sizes and preplsizes 
    this->pOrgs = new Relation *[this->height];
    this->pReps = new Relation *[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits - l));

        this->pOrgs[l] = new Relation[cnt];
        this->pReps[l] = new Relation[cnt];

        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgs[l][j].resize(this->pOrgs_sizes[l][j]);
            this->pReps[l][j].resize(this->pReps_sizes[l][j]);
        }
    }
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits - l));

        memset(this->pOrgs_sizes[l], 0, cnt * sizeof(RecordId));
        memset(this->pReps_sizes[l], 0, cnt * sizeof(size_t));
    }

    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);

    // Step 4: Build SoA (Structure of Arrays) layout for SIMD.
    // Convert each partition from AoS (Record[]) to SoA (int[3*N]).
    this->pOrgsSoA = new int**[this->height];
    this->pRepsSoA = new int**[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits - l));
        this->pOrgsSoA[l] = new int*[cnt];
        this->pRepsSoA[l] = new int*[cnt];
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsSoA[l][j] = buildSoABlock(this->pOrgs[l][j]);
            this->pRepsSoA[l][j] = buildSoABlock(this->pReps[l][j]);
        }
    }

    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(pOrgs_sizes[l]);
        free(pReps_sizes[l]);
    }
    free(pOrgs_sizes);
    free(pReps_sizes);
}

void HINT_M::print(char c)
{
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits - l);

        printf("Level %d: %d partitions\n", l, cnt);
        for (auto j = 0; j < cnt; j++)
        {
            printf("Orgs %d (%d): ", j, this->pOrgs[l][j].size());
            //            for (auto k = 0; k < this->bucketcountersA[i][j]; k++)
            //                printf("%d ", this->pOrgs[i][j][k].id);
            printf("\n");
            printf("Reps %d (%d): ", j, this->pReps[l][j].size());
            //            for (auto k = 0; k < this->bucketcountersB[i][j]; k++)
            //                printf("%d ", this->pReps[i][j][k].id);
            printf("\n\n");
        }
    }
}

void HINT_M::getStats()
{
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits - l);

        this->numPartitions += cnt;
        for (int p = 0; p < cnt; p++)
        {
            this->numOriginals += this->pOrgs[l][p].size();
            this->numReplicas += this->pReps[l][p].size();
            if ((this->pOrgs[l][p].empty()) && (this->pReps[l][p].empty()))
                this->numEmptyPartitions++;
        }
    }

    this->avgPartitionSize = (float)(this->numIndexedRecords + this->numReplicas) / (this->numPartitions - numEmptyPartitions);
}

HINT_M::~HINT_M()
{
    // Free SoA arrays
    if (this->pOrgsSoA)
    {
        for (auto l = 0; l < this->height; l++)
        {
            auto cnt = (int)(pow(2, this->numBits - l));
            for (auto j = 0; j < cnt; j++)
            {
                delete[] this->pOrgsSoA[l][j];
                delete[] this->pRepsSoA[l][j];
            }
            delete[] this->pOrgsSoA[l];
            delete[] this->pRepsSoA[l];
        }
        delete[] this->pOrgsSoA;
        delete[] this->pRepsSoA;
    }

    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgs[l];
        delete[] this->pReps[l];
    }
    delete[] this->pOrgs;
    delete[] this->pReps;
}

// Generalized predicates, ACM SIGMOD'22 gOverlaps
size_t HINT_M::executeTopDown_gOverlaps(RangeQuery Q)
{
    size_t result = 0;
    Timestamp a = Q.start >> (this->maxBits - this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits - this->numBits);   // prefix

    for (auto l = 0; l < this->numBits; l++)
    {
        // Handle the partition that contains a: consider both originals and replicas, comparisons needed
        size_t nOrg = this->pOrgs[l][a].size();
        simd_eval_both(this->pOrgsSoA[l][a], nOrg, Q, result);

        size_t nRep = this->pReps[l][a].size();
        simd_eval_both(this->pRepsSoA[l][a], nRep, Q, result);

        if (a < b)
        {
            // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
            for (auto j = a + 1; j < b; j++)
            {
                size_t n = this->pOrgs[l][j].size();
                simd_eval_none(this->pOrgsSoA[l][j], n, result);
            }

            // Handle the partition that contains b: consider only originals, comparisons needed
            size_t nB = this->pOrgs[l][b].size();
            simd_eval_start(this->pOrgsSoA[l][b], nB, Q, result);
        }

        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }

    // Handle root: consider only originals, comparisons needed
    size_t nRoot = this->pOrgs[this->numBits][0].size();
    simd_eval_both(this->pOrgsSoA[this->numBits][0], nRoot, Q, result);

    return result;
}

size_t HINT_M::executeBottomUp_gOverlaps(RangeQuery Q)
{
    size_t result = 0;
    Timestamp a = Q.start >> (this->maxBits - this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits - this->numBits);   // prefix
    bool foundzero = false;
    bool foundone = false;

    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results

            // Handle the partition that contains a: consider both originals and replicas
            size_t nRep = this->pReps[l][a].size();
            simd_eval_none(this->pRepsSoA[l][a], nRep, result);

            // Handle rest: consider only originals
            for (auto j = a; j <= b; j++)
            {
                size_t n = this->pOrgs[l][j].size();
                simd_eval_none(this->pOrgsSoA[l][j], n, result);
            }
        }
        else
        {
            // Comparisons needed

            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                size_t nOrg = this->pOrgs[l][a].size();
                // Special case when query overlaps only one partition, Lemma 3
                if (!foundzero && !foundone)
                {
                    simd_eval_both(this->pOrgsSoA[l][a], nOrg, Q, result);
                }
                else if (foundzero)
                {
                    simd_eval_start(this->pOrgsSoA[l][a], nOrg, Q, result);
                }
                else if (foundone)
                {
                    simd_eval_end(this->pOrgsSoA[l][a], nOrg, Q, result);
                }
            }
            else
            {
                size_t nOrg = this->pOrgs[l][a].size();
                // Lemma 1
                if (!foundzero)
                {
                    simd_eval_end(this->pOrgsSoA[l][a], nOrg, Q, result);
                }
                else
                {
                    simd_eval_none(this->pOrgsSoA[l][a], nOrg, result);
                }
            }

            // Lemma 1, 3
            size_t nRep = this->pReps[l][a].size();
            if (!foundzero)
            {
                simd_eval_end(this->pRepsSoA[l][a], nRep, Q, result);
            }
            else
            {
                simd_eval_none(this->pRepsSoA[l][a], nRep, result);
            }

            if (a < b)
            {
                if (!foundone)
                {
                    // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                    for (auto j = a + 1; j < b; j++)
                    {
                        size_t n = this->pOrgs[l][j].size();
                        simd_eval_none(this->pOrgsSoA[l][j], n, result);
                    }

                    // Handle the partition that contains b: consider only originals, comparisons needed
                    size_t nB = this->pOrgs[l][b].size();
                    simd_eval_start(this->pOrgsSoA[l][b], nB, Q, result);
                }
                else
                {
                    for (auto j = a + 1; j <= b; j++)
                    {
                        size_t n = this->pOrgs[l][j].size();
                        simd_eval_none(this->pOrgsSoA[l][j], n, result);
                    }
                }
            }

            if ((!foundone) && (b % 2)) // last bit of b is 1
                foundone = 1;
            if ((!foundzero) && (!(a % 2))) // last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }

    // Handle root.
    size_t nRoot = this->pOrgs[this->numBits][0].size();
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        simd_eval_none(this->pOrgsSoA[this->numBits][0], nRoot, result);
    }
    else
    {
        // Comparisons needed
        simd_eval_both(this->pOrgsSoA[this->numBits][0], nRoot, Q, result);
    }

    return result;
}
