# HINT+ DB Wrapper for DuckDB

This extension integrates the **HINT+** (Hierarchical Interval) index architecture as a Database Wrapper for DuckDB. It implements a **Horizontal Update-Driven (HUD)** storage layout to support fast, LSM-like interval inserts without constantly reconstructing the static HINT main index.

## Architecture Highlights
- **Delta Log:** A fast append-only `std::vector` (in-memory) that ingests newly inserted intervals instantly in $O(1)$ time. 
- **Main Index:** A static HINT `HINT_M` index (written in C++) that holds the widely queried historical data.
- **Concurrent Search:** Querying for interval overlaps dynamically retrieves results by combining the static Main Index matches with the active Delta Log matches. 
- **Deferred Merging:** Manual merge triggers recalculate and flush the active Delta Log structurally into a brand-new updated Main Index to keep search performance optimal over time.

## Cloning and Building

Since this extension relies on the DuckDB source code via submodules, ensure you clone the repository recursively:

```sh
git clone --recursive <your_repo_url>
# If already cloned without submodules, run:
# git submodule update --init --recursive
```

### Build steps
To build the extension, run:
```sh
make
```

## Running the Extension

Start the DuckDB shell with the compiled extension loaded:
```sh
./build/release/duckdb
```

### SQL API

Because we are creating a DB shell around a specialized C++ memory structure without altering DuckDB's deeply internal storage engine catalog, all HINT+ operations are mapped to DuckDB **Scalar** and **Table Functions** using `SELECT`.

#### 1. Inserting Data (Delta Log)
Inserts data into the fast-append Delta Log buffer. Returns the assigned item ID.
```sql
SELECT hint_insert(start_interval, end_interval);
```
Example:
```sql
SELECT hint_insert(10, 50);
```

#### 2. Querying Overlaps (Concurrent Index Search)
Returns the total count of intervals that overlap with the queried `[start, end]`. This seamlessly triggers a scan of both the static Main Index and the active Delta Log.
```sql
SELECT * FROM hint_search(query_start, query_end);
```

#### 3. Merging (Compaction)
Compacts the active Delta Log into the Main HINT Index and clears the Delta Log. Useful to execute periodically for deep query performance.
```sql
SELECT hint_merge();
```

## Running Tests
You can run the end-to-end integration test file locally to verify the DB wrapper logic:
```sh
./build/release/duckdb < test_wrapper.sql
```
This tests Inserts, searches out of the Delta Log, the Merge runtime, and resolving Mixed Searches automatically.
