.echo on
.mode box
-- HINT+ DB Wrapper Test Cases
-- Run this using: ./build/release/duckdb < test_wrapper.sql

-- 1. Insert initial intervals (These go directly into the Delta Log)
SELECT '--- 1. Inserting initial intervals (10-50, 20-60, 30-70) ---' as step;
SELECT hint_insert(10, 50);
SELECT hint_insert(20, 60);
SELECT hint_insert(30, 70);

-- 2. Search before merging
-- Expected overlaps for [15, 25]: [10, 50] and [20, 60] -> 2
SELECT '--- 2. Searching Delta Log for [15, 25] ---' as step;
SELECT * FROM hint_search(15, 25);

-- 3. Merge Delta Log into Main Index
SELECT '--- 3. Merging Delta Log into Main Index ---' as step;
SELECT hint_merge();

-- 4. Search after merge (Queries the Main Index)
SELECT '--- 4. Searching Main Index for [15, 25] ---' as step;
SELECT * FROM hint_search(15, 25);

-- 5. Insert more intervals (New Delta Log items)
SELECT '--- 5. Inserting new intervals (15-20, 100-200) ---' as step;
SELECT hint_insert(15, 20);
SELECT hint_insert(100, 200);

-- 6. Search across both Main Index and Delta Log concurrently
-- Expected overlaps for [15, 25]: [10, 50], [20, 60] from Main Index + [15, 20] from Delta Log -> 3
SELECT '--- 6. Searching both Main Index & Delta Log for [15, 25] ---' as step;
SELECT * FROM hint_search(15, 25);
