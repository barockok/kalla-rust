# Ballista Distributed Execution — Remove Staging Phase

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the staging-to-S3 pipeline with Ballista standalone distributed execution, where each source type (CSV, Postgres) provides its own partitioned scan directly — no intermediate Parquet materialization.

**Architecture:** Use Ballista's `SessionContext::standalone()` mode (scheduler + executor embedded in-process) so every kalla-worker binary gains distributed execution with zero additional deployment components. Each data source implements a custom `TableProvider` that defines how to partition and fetch data natively: Postgres uses `LIMIT/OFFSET` partitioned queries, S3 CSV uses byte-range splitting via `object_store`. The existing NATS job queue orchestrates which worker runs the exec job; Ballista distributes the scan/join within that worker's process.

**Tech Stack:** DataFusion 44, Ballista 44 (standalone feature), `object_store` 0.11 (aws), `sqlx` 0.8 (postgres), `async-nats` 0.38

---

## Overview

### What Changes

1. **ReconciliationEngine** gets a new `new_distributed()` constructor that creates a Ballista standalone `SessionContext` instead of a plain `SessionContext`
2. **PostgresConnector** gains a `PostgresPartitionedTable` — a custom `TableProvider` that returns N partitions, each executing `SELECT * FROM t LIMIT x OFFSET y` directly against Postgres
3. **S3Connector** gains a `CsvByteRangeTable` — a custom `TableProvider` that splits S3 CSV files by byte ranges and parses each range independently
4. **Exec handler** (scaled mode) switches from loading staged Parquet to registering source-native `TableProvider`s with the distributed engine
5. **Stage handler** becomes optional — only used as a fallback or when `STAGE_TO_S3=true`

### What Stays the Same

- Single mode HTTP path — unchanged (already loads sources directly)
- NATS job queue for orchestrating scaled-mode jobs
- Postgres job tracking tables (`jobs`, `run_staging_tracker`)
- The `match_sql` / `Recipe` format
- Financial UDFs (`tolerance_match`)
- Evidence writing

### Dependency Versions

```
ballista = { version = "44", features = ["standalone"] }
datafusion = "44"     # unchanged
arrow = "53"          # unchanged
object_store = "0.11" # unchanged
```

---

## Task 1: Add Ballista Dependency

**Files:**
- Modify: `Cargo.toml` (workspace deps, line 19)
- Modify: `crates/kalla-core/Cargo.toml`

**Step 1: Add ballista to workspace dependencies**

In `Cargo.toml`, add after the `datafusion = "44"` line:

```toml
ballista = { version = "44", default-features = false, features = ["standalone"] }
```

**Step 2: Add ballista to kalla-core**

In `crates/kalla-core/Cargo.toml`, add:

```toml
ballista.workspace = true
```

**Step 3: Verify it compiles**

Run: `cargo check --workspace`
Expected: Compiles successfully (ballista 44 is compatible with datafusion 44)

**Step 4: Commit**

```bash
git add Cargo.toml crates/kalla-core/Cargo.toml Cargo.lock
git commit -m "deps: add ballista 44 with standalone feature"
```

---

## Task 2: Add Distributed Engine Constructor

**Files:**
- Modify: `crates/kalla-core/src/engine.rs`

**Step 1: Write the failing test**

Add to `engine.rs` tests:

```rust
#[tokio::test]
async fn test_distributed_engine_creation() {
    let engine = ReconciliationEngine::new_distributed().await.unwrap();
    // Verify UDFs are registered
    let result = engine.sql("SELECT tolerance_match(1.0, 1.005, 0.01)").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_distributed_engine_csv_query() {
    let csv = "id,val\n1,10\n2,20\n3,30\n";
    let (_f, path) = write_temp_csv(csv);

    let engine = ReconciliationEngine::new_distributed().await.unwrap();
    engine.register_csv("dist_t", &path).await.unwrap();

    let df = engine.sql("SELECT COUNT(*) AS cnt FROM dist_t").await.unwrap();
    let batches = df.collect().await.unwrap();
    let cnt = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 3);
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p kalla-core test_distributed_engine -- --nocapture`
Expected: FAIL — `new_distributed` doesn't exist

**Step 3: Implement `new_distributed`**

Add to `ReconciliationEngine`:

```rust
use anyhow::Result as AnyhowResult;

/// Create a distributed reconciliation engine using Ballista standalone mode.
/// Embeds scheduler + executor in-process for distributed query execution.
pub async fn new_distributed() -> AnyhowResult<Self> {
    let config = datafusion::prelude::SessionConfig::new_with_ballista()
        .with_information_schema(true);
    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::standalone_with_state(state).await?;

    // Register financial UDFs
    udf::register_financial_udfs(&ctx);

    info!("ReconciliationEngine (distributed/Ballista standalone) initialized");
    Ok(Self { ctx })
}
```

Note: The `SessionConfig::new_with_ballista()` and `SessionContext::standalone_with_state()` APIs come from the `ballista` crate's prelude. Add the necessary import:

```rust
use ballista::prelude::*;
```

This may conflict with `datafusion::prelude::*` — if so, use qualified paths:
```rust
let config = ballista::prelude::SessionConfig::new_with_ballista();
let ctx = ballista::prelude::SessionContext::standalone_with_state(state).await?;
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p kalla-core test_distributed_engine -- --nocapture`
Expected: PASS

**Step 5: Run full test suite**

Run: `cargo test --workspace`
Expected: All existing tests still pass

**Step 6: Commit**

```bash
git add crates/kalla-core/
git commit -m "feat: add distributed ReconciliationEngine via Ballista standalone"
```

---

## Task 3: PostgresPartitionedTable — Custom TableProvider

**Files:**
- Create: `crates/kalla-connectors/src/postgres_partitioned.rs`
- Modify: `crates/kalla-connectors/src/lib.rs`
- Modify: `crates/kalla-connectors/Cargo.toml`

This is the core of the "skip staging" approach. Instead of extracting Postgres → Parquet → S3, we create a `TableProvider` that tells DataFusion how to partition reads directly against Postgres.

**Step 1: Add required dependencies to kalla-connectors**

In `crates/kalla-connectors/Cargo.toml`, ensure these are present:

```toml
datafusion.workspace = true
arrow.workspace = true
tokio.workspace = true
futures = "0.3"
```

**Step 2: Write the failing test**

Create test in `postgres_partitioned.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn test_partition_ranges() {
        let ranges = compute_partition_ranges(100, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], (0, 25));
        assert_eq!(ranges[1], (25, 25));
        assert_eq!(ranges[2], (50, 25));
        assert_eq!(ranges[3], (75, 25));
    }

    #[test]
    fn test_partition_ranges_uneven() {
        let ranges = compute_partition_ranges(10, 3);
        assert_eq!(ranges.len(), 3);
        // First two get 3 rows, last gets remainder (4)
        assert_eq!(ranges[0], (0, 3));
        assert_eq!(ranges[1], (3, 3));
        assert_eq!(ranges[2], (6, 4));
    }

    #[test]
    fn test_partition_ranges_single() {
        let ranges = compute_partition_ranges(50, 1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 50));
    }

    #[test]
    fn test_partition_ranges_more_partitions_than_rows() {
        let ranges = compute_partition_ranges(2, 10);
        // Should cap at row_count partitions
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], (0, 1));
        assert_eq!(ranges[1], (1, 1));
    }
}
```

**Step 3: Run test to verify it fails**

Run: `cargo test -p kalla-connectors test_partition_ranges -- --nocapture`
Expected: FAIL — module doesn't exist

**Step 4: Implement PostgresPartitionedTable**

Create `crates/kalla-connectors/src/postgres_partitioned.rs`:

```rust
//! Partitioned Postgres TableProvider for DataFusion.
//!
//! Instead of loading an entire table into memory (MemTable), this provider
//! tells DataFusion to read from Postgres in N parallel partitions using
//! LIMIT/OFFSET queries. Each partition is a separate ExecutionPlan that
//! independently connects to Postgres and streams rows.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{SchemaRef, DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Column, Row};
use tracing::{debug, info};

use crate::postgres::pg_type_to_arrow;

/// Compute (offset, limit) ranges for N partitions over `total_rows`.
pub fn compute_partition_ranges(total_rows: u64, num_partitions: usize) -> Vec<(u64, u64)> {
    let n = (num_partitions as u64).min(total_rows).max(1);
    let chunk = total_rows / n;
    let mut ranges = Vec::with_capacity(n as usize);
    for i in 0..n {
        let offset = i * chunk;
        let limit = if i == n - 1 {
            total_rows - offset
        } else {
            chunk
        };
        ranges.push((offset, limit));
    }
    ranges
}

/// A TableProvider that reads from PostgreSQL in parallel partitions.
///
/// Each partition executes `SELECT * FROM <table> ORDER BY <order_col> LIMIT x OFFSET y`
/// independently. The ORDER BY ensures deterministic partitioning.
pub struct PostgresPartitionedTable {
    conn_string: String,
    pg_table: String,
    schema: SchemaRef,
    total_rows: u64,
    num_partitions: usize,
    /// Column to ORDER BY for deterministic offset-based partitioning.
    /// If None, uses the first column.
    order_column: Option<String>,
}

impl PostgresPartitionedTable {
    /// Create a new partitioned table provider by introspecting the Postgres table.
    ///
    /// This connects to Postgres, infers schema, counts rows, and stores metadata.
    /// No data is fetched until DataFusion calls `scan()`.
    pub async fn new(
        conn_string: &str,
        pg_table: &str,
        num_partitions: usize,
        order_column: Option<String>,
    ) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(conn_string)
            .await?;

        // Infer schema from a LIMIT 0 query
        let schema = infer_schema(&pool, pg_table).await?;

        // Count rows
        let count_sql = format!("SELECT COUNT(*) FROM \"{}\"", pg_table);
        let row: (i64,) = sqlx::query_as(&count_sql).fetch_one(&pool).await?;
        let total_rows = row.0 as u64;

        info!(
            "PostgresPartitionedTable: {} rows in '{}', {} partitions",
            total_rows, pg_table, num_partitions
        );

        Ok(Self {
            conn_string: conn_string.to_string(),
            pg_table: pg_table.to_string(),
            schema: Arc::new(schema),
            total_rows,
            num_partitions,
            order_column,
        })
    }

    /// Register this table provider with a DataFusion SessionContext.
    pub async fn register(
        self,
        ctx: &datafusion::prelude::SessionContext,
        table_name: &str,
    ) -> anyhow::Result<()> {
        ctx.register_table(table_name, Arc::new(self))?;
        Ok(())
    }
}

impl fmt::Debug for PostgresPartitionedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresPartitionedTable")
            .field("pg_table", &self.pg_table)
            .field("total_rows", &self.total_rows)
            .field("num_partitions", &self.num_partitions)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for PostgresPartitionedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let ranges = compute_partition_ranges(self.total_rows, self.num_partitions);
        let order_col = self.order_column.clone().unwrap_or_else(|| {
            self.schema.fields()[0].name().clone()
        });

        // Build projected schema
        let projected_schema = match projection {
            Some(indices) => {
                let fields: Vec<_> = indices.iter().map(|&i| self.schema.field(i).clone()).collect();
                Arc::new(Schema::new(fields))
            }
            None => self.schema.clone(),
        };

        // Select columns
        let select_cols = match projection {
            Some(indices) => indices
                .iter()
                .map(|&i| format!("\"{}\"", self.schema.field(i).name()))
                .collect::<Vec<_>>()
                .join(", "),
            None => "*".to_string(),
        };

        // Fetch each partition's data (Ballista standalone runs in-process,
        // so we can use async here)
        let mut partitions: Vec<Vec<RecordBatch>> = Vec::with_capacity(ranges.len());

        for (offset, limit) in &ranges {
            let sql = format!(
                "SELECT {} FROM \"{}\" ORDER BY \"{}\" LIMIT {} OFFSET {}",
                select_cols, self.pg_table, order_col, limit, offset
            );
            debug!("Partition query: {}", sql);

            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&self.conn_string)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let rows: Vec<PgRow> = sqlx::query(&sql)
                .fetch_all(&pool)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            if rows.is_empty() {
                partitions.push(vec![]);
                continue;
            }

            let batch = crate::postgres::rows_to_record_batch(&rows, projected_schema.clone())
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            partitions.push(vec![batch]);
        }

        Ok(Arc::new(MemoryExec::try_new(
            &partitions,
            projected_schema,
            None,
        )?))
    }
}

/// Infer Arrow schema from a Postgres table using a LIMIT 0 query.
async fn infer_schema(pool: &PgPool, table: &str) -> anyhow::Result<Schema> {
    // Use information_schema to get column types without fetching data
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, udt_name FROM information_schema.columns \
         WHERE table_name = $1 ORDER BY ordinal_position"
    )
    .bind(table)
    .fetch_all(pool)
    .await?;

    anyhow::ensure!(!rows.is_empty(), "Table '{}' not found or has no columns", table);

    let fields: Vec<Field> = rows
        .iter()
        .map(|(name, udt)| {
            let dt = pg_type_to_arrow(&udt.to_uppercase());
            Field::new(name, dt, true)
        })
        .collect();

    Ok(Schema::new(fields))
}
```

**Important:** The `pg_type_to_arrow` and `rows_to_record_batch` functions in `postgres.rs` are currently private. We need to make them `pub(crate)`:

In `crates/kalla-connectors/src/postgres.rs`, change:
- `fn pg_type_to_arrow(...)` → `pub(crate) fn pg_type_to_arrow(...)`
- `fn rows_to_record_batch(...)` → `pub(crate) fn rows_to_record_batch(...)`

**Step 5: Add module to lib.rs**

In `crates/kalla-connectors/src/lib.rs`, add:

```rust
pub mod postgres_partitioned;
pub use postgres_partitioned::PostgresPartitionedTable;
```

**Step 6: Run tests**

Run: `cargo test -p kalla-connectors test_partition_ranges -- --nocapture`
Expected: PASS

**Step 7: Run full suite**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 8: Commit**

```bash
git add crates/kalla-connectors/
git commit -m "feat: add PostgresPartitionedTable for direct partitioned reads"
```

---

## Task 4: CsvByteRangeTable — Custom TableProvider for S3 CSV

**Files:**
- Create: `crates/kalla-connectors/src/csv_partitioned.rs`
- Modify: `crates/kalla-connectors/src/lib.rs`

S3 CSV files can be split by byte ranges. Each partition reads a byte range via `object_store` `GetOptions::range`, handles partial first/last lines, and parses CSV independently.

**Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_range_partitions() {
        let ranges = compute_byte_ranges(1000, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], (0, 250));
        assert_eq!(ranges[1], (250, 500));
        assert_eq!(ranges[2], (500, 750));
        assert_eq!(ranges[3], (750, 1000));
    }

    #[test]
    fn test_byte_range_single_partition() {
        let ranges = compute_byte_ranges(500, 1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 500));
    }

    #[test]
    fn test_byte_range_small_file() {
        let ranges = compute_byte_ranges(10, 5);
        // Should cap partitions to avoid empty ranges
        assert!(ranges.len() <= 5);
        // First range starts at 0, last ends at 10
        assert_eq!(ranges.first().unwrap().0, 0);
        assert_eq!(ranges.last().unwrap().1, 10);
    }

    #[test]
    fn test_split_csv_chunk_handles_partial_lines() {
        let data = b"id,name,amount\n1,Alice,100\n2,Bob,200\n3,Carol,300\n";
        // Simulate reading from middle of file (byte 15 = start of "1,Alice...")
        let chunk = &data[20..];  // "ice,100\n2,Bob,200\n3,Carol,300\n"
        let (skip_first, lines) = split_csv_chunk(chunk, false);
        assert!(skip_first); // first line is partial
        // Should have 2 complete lines: "2,Bob,200" and "3,Carol,300"
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_split_csv_chunk_first_partition() {
        let data = b"1,Alice,100\n2,Bob,200\n";
        let (skip_first, lines) = split_csv_chunk(data, true);
        assert!(!skip_first); // first partition keeps first line
        assert_eq!(lines.len(), 2);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p kalla-connectors test_byte_range -- --nocapture`
Expected: FAIL

**Step 3: Implement CsvByteRangeTable**

Create `crates/kalla-connectors/src/csv_partitioned.rs`:

```rust
//! Byte-range partitioned CSV reader for S3.
//!
//! Splits a CSV file on S3 into byte ranges. Each partition reads its byte
//! range via object_store, discards the first partial line (except partition 0),
//! and reads past the end to complete the last line.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::csv::ReaderBuilder;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore};
use tracing::{debug, info};
use url::Url;

use crate::s3::S3Config;

/// Compute byte-range partitions as (start_byte, end_byte) exclusive.
pub fn compute_byte_ranges(file_size: u64, num_partitions: usize) -> Vec<(u64, u64)> {
    let n = (num_partitions as u64).min(file_size).max(1);
    let chunk = file_size / n;
    let mut ranges = Vec::with_capacity(n as usize);
    for i in 0..n {
        let start = i * chunk;
        let end = if i == n - 1 { file_size } else { (i + 1) * chunk };
        ranges.push((start, end));
    }
    ranges
}

/// Split a raw CSV byte chunk into lines, handling partial first/last lines.
///
/// Returns (skip_first_line, complete_lines_as_bytes).
/// `is_first_partition`: if true, the first line is NOT partial.
pub fn split_csv_chunk(data: &[u8], is_first_partition: bool) -> (bool, Vec<&[u8]>) {
    let mut lines: Vec<&[u8]> = Vec::new();
    let mut start = 0;
    for (i, &byte) in data.iter().enumerate() {
        if byte == b'\n' {
            if i > start {
                lines.push(&data[start..i]);
            }
            start = i + 1;
        }
    }
    // Handle last line without trailing newline
    if start < data.len() {
        lines.push(&data[start..]);
    }

    let skip_first = !is_first_partition && !lines.is_empty();
    if skip_first {
        lines.remove(0); // discard partial first line
    }

    (skip_first, lines)
}

/// A TableProvider for byte-range partitioned CSV on S3.
pub struct CsvByteRangeTable {
    s3_uri: String,
    bucket: String,
    key: String,
    schema: SchemaRef,
    file_size: u64,
    num_partitions: usize,
    s3_config: S3Config,
    has_header: bool,
}

impl CsvByteRangeTable {
    /// Create by reading the CSV header from S3 to infer schema and get file size.
    pub async fn new(
        s3_uri: &str,
        num_partitions: usize,
        s3_config: S3Config,
    ) -> anyhow::Result<Self> {
        let (bucket, key) = crate::s3::S3Connector::parse_s3_uri(s3_uri)?;
        let store = build_s3_store(&s3_config, &bucket)?;
        let path = ObjectPath::from(key.as_str());

        // Get file metadata for size
        let meta = store.head(&path).await?;
        let file_size = meta.size as u64;

        // Read first 8KB to infer schema from header
        let header_range = GetOptions {
            range: Some(GetRange::Bounded(std::ops::Range {
                start: 0,
                end: 8192.min(file_size as usize),
            })),
            ..Default::default()
        };
        let header_bytes = store.get_opts(&path, header_range).await?.bytes().await?;

        // Parse header line
        let header_end = header_bytes.iter().position(|&b| b == b'\n')
            .unwrap_or(header_bytes.len());
        let header_line = std::str::from_utf8(&header_bytes[..header_end])?;
        let columns: Vec<&str> = header_line.split(',').collect();

        // For now, all columns are Utf8 — schema refinement can come later
        // when we parse actual data. The CSV reader will handle type inference.
        let fields: Vec<arrow::datatypes::Field> = columns
            .iter()
            .map(|name| arrow::datatypes::Field::new(name.trim().trim_matches('"'), arrow::datatypes::DataType::Utf8, true))
            .collect();
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));

        info!(
            "CsvByteRangeTable: {} bytes, {} partitions for '{}'",
            file_size, num_partitions, s3_uri
        );

        Ok(Self {
            s3_uri: s3_uri.to_string(),
            bucket,
            key,
            schema,
            file_size,
            num_partitions,
            s3_config,
            has_header: true,
        })
    }

    /// Register with DataFusion context.
    pub async fn register(
        self,
        ctx: &datafusion::prelude::SessionContext,
        table_name: &str,
    ) -> anyhow::Result<()> {
        ctx.register_table(table_name, Arc::new(self))?;
        Ok(())
    }
}

impl fmt::Debug for CsvByteRangeTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvByteRangeTable")
            .field("s3_uri", &self.s3_uri)
            .field("file_size", &self.file_size)
            .field("num_partitions", &self.num_partitions)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for CsvByteRangeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let ranges = compute_byte_ranges(self.file_size, self.num_partitions);
        let store = build_s3_store(&self.s3_config, &self.bucket)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let path = ObjectPath::from(self.key.as_str());

        // Find header line end byte offset
        let header_opts = GetOptions {
            range: Some(GetRange::Bounded(std::ops::Range { start: 0, end: 8192.min(self.file_size as usize) })),
            ..Default::default()
        };
        let header_data = store.get_opts(&path, header_opts).await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            .bytes().await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let header_end = header_data.iter().position(|&b| b == b'\n')
            .map(|p| p + 1)
            .unwrap_or(0) as u64;

        let projected_schema = match projection {
            Some(indices) => {
                let fields: Vec<_> = indices.iter().map(|&i| self.schema.field(i).clone()).collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => self.schema.clone(),
        };

        let mut partitions: Vec<Vec<RecordBatch>> = Vec::with_capacity(ranges.len());

        for (i, &(start, end)) in ranges.iter().enumerate() {
            // Skip header for all partitions; adjust first partition start
            let actual_start = if i == 0 { header_end } else { start };
            // Read slightly past end to complete last line (up to 64KB extra)
            let actual_end = if i == ranges.len() - 1 {
                self.file_size
            } else {
                (end + 65536).min(self.file_size)
            };

            if actual_start >= actual_end {
                partitions.push(vec![]);
                continue;
            }

            let opts = GetOptions {
                range: Some(GetRange::Bounded(std::ops::Range {
                    start: actual_start as usize,
                    end: actual_end as usize,
                })),
                ..Default::default()
            };
            let data = store.get_opts(&path, opts).await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                .bytes().await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            // For non-first partitions: skip to first complete line
            // For non-last partitions: only read up to the original end boundary
            let is_first = i == 0;
            let (_skip, lines) = split_csv_chunk(&data, is_first);

            // If non-last partition, we need to truncate lines that start past `end`
            // For simplicity, parse all lines in the range and let DataFusion handle it

            // Reconstruct CSV with header for arrow csv reader
            let header_str = std::str::from_utf8(&header_data[..header_end as usize - 1])
                .unwrap_or("");
            let mut csv_buf = Vec::new();
            csv_buf.extend_from_slice(header_str.as_bytes());
            csv_buf.push(b'\n');
            for line in &lines {
                csv_buf.extend_from_slice(line);
                csv_buf.push(b'\n');
            }

            // Parse with Arrow CSV reader
            let cursor = std::io::Cursor::new(csv_buf);
            let reader = ReaderBuilder::new(self.schema.clone())
                .with_header(true)
                .build(cursor)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut batch_vec = Vec::new();
            for batch in reader {
                let batch = batch.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                // Apply projection if needed
                if let Some(indices) = projection {
                    let columns: Vec<_> = indices.iter().map(|&i| batch.column(i).clone()).collect();
                    let projected = RecordBatch::try_new(projected_schema.clone(), columns)?;
                    batch_vec.push(projected);
                } else {
                    batch_vec.push(batch);
                }
            }
            partitions.push(batch_vec);
        }

        Ok(Arc::new(MemoryExec::try_new(
            &partitions,
            projected_schema,
            None,
        )?))
    }
}

fn build_s3_store(config: &S3Config, bucket: &str) -> anyhow::Result<impl ObjectStore> {
    let mut builder = AmazonS3Builder::new()
        .with_region(&config.region)
        .with_bucket_name(bucket)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key);
    if let Some(ref endpoint) = config.endpoint_url {
        builder = builder.with_endpoint(endpoint);
    }
    if config.allow_http {
        builder = builder.with_allow_http(true);
    }
    Ok(builder.build()?)
}
```

**Step 5: Add to lib.rs**

```rust
pub mod csv_partitioned;
pub use csv_partitioned::CsvByteRangeTable;
```

**Step 6: Run tests**

Run: `cargo test -p kalla-connectors test_byte_range -- --nocapture`
Run: `cargo test -p kalla-connectors test_split_csv -- --nocapture`
Expected: PASS

**Step 7: Commit**

```bash
git add crates/kalla-connectors/
git commit -m "feat: add CsvByteRangeTable for partitioned S3 CSV reads"
```

---

## Task 5: Update Exec Handler to Use Distributed Engine + Native Providers

**Files:**
- Modify: `crates/kalla-worker/src/exec.rs`
- Modify: `crates/kalla-worker/Cargo.toml`

**Step 1: Add ballista dep to kalla-worker**

In `crates/kalla-worker/Cargo.toml`:

```toml
ballista.workspace = true
```

**Step 2: Update handle_exec for distributed execution**

Modify `handle_exec` in `exec.rs` to optionally use the distributed engine and native table providers. Add a `use_distributed: bool` parameter (controlled by env var `BALLISTA_ENABLED`):

```rust
/// Execute the reconciliation run (scaled mode — NATS).
///
/// If `BALLISTA_ENABLED=true`, uses distributed engine with native table providers
/// (no staging required). Otherwise falls back to staged Parquet path.
pub async fn handle_exec(
    pool: &PgPool,
    run_id: Uuid,
    job_id: Uuid,
    recipe_json: &str,
    staged_sources: &[StagedSource],
    callback_url: Option<&str>,
) -> Result<ExecResult> {
    let use_distributed = std::env::var("BALLISTA_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if use_distributed {
        handle_exec_distributed(pool, run_id, job_id, recipe_json, staged_sources, callback_url).await
    } else {
        handle_exec_staged(pool, run_id, job_id, recipe_json, staged_sources, callback_url).await
    }
}
```

Where `handle_exec_staged` is the current implementation renamed, and `handle_exec_distributed` is the new path that:

1. Creates `ReconciliationEngine::new_distributed().await?`
2. For each source, checks the source URI:
   - `postgres://` → creates `PostgresPartitionedTable` and registers it
   - `s3://*.csv` → creates `CsvByteRangeTable` and registers it
   - `s3://*.parquet` or staged paths → falls back to existing `register_parquet`
3. Executes `match_sql` via the distributed engine (same SQL, distributed scan)
4. Counts unmatched, writes evidence, callbacks — same as before

**Step 3: Verify compilation**

Run: `cargo check --workspace`
Expected: Compiles

**Step 4: Run tests**

Run: `cargo test --workspace`
Expected: All pass (distributed path not activated in tests since `BALLISTA_ENABLED` defaults to false)

**Step 5: Commit**

```bash
git add crates/kalla-worker/
git commit -m "feat: add distributed execution path with BALLISTA_ENABLED flag"
```

---

## Task 6: Update Scaled Job Flow — Direct Exec Without Staging

**Files:**
- Modify: `crates/kalla-worker/src/queue.rs` — add `source_uris` to Exec message
- Modify: `crates/kalla-worker/src/job_loop.rs` — skip staging when distributed
- Modify: `benchmarks/inject_scaled_job.py` — support direct exec injection

When `BALLISTA_ENABLED=true`, the scaled-mode flow changes:

**Old flow:** StagePlan → StageChunk(s) → (completion gate) → Exec (reads Parquet from S3)
**New flow:** Exec directly (reads from Postgres/S3 CSV natively)

**Step 1: Add source URIs to Exec message**

In `queue.rs`, add an optional field to `JobMessage::Exec`:

```rust
Exec {
    job_id: Uuid,
    run_id: Uuid,
    recipe_json: String,
    staged_sources: Vec<StagedSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    callback_url: Option<String>,
    /// Original source URIs for direct (non-staged) execution.
    /// When present, the exec handler can read directly from sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_uris: Option<Vec<SourceUri>>,
},
```

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceUri {
    pub alias: String,
    pub uri: String,
}
```

**Step 2: Update inject_scaled_job.py**

Add `--direct-exec` flag that skips StagePlan jobs and publishes an Exec message directly to `kalla.exec` with `source_uris` populated.

**Step 3: Run tests**

Run: `cargo test --workspace`
Expected: All pass (serde backward compatible due to `default`)

**Step 4: Commit**

```bash
git add crates/kalla-worker/ benchmarks/
git commit -m "feat: support direct exec without staging via source_uris"
```

---

## Task 7: Add Config for Distributed Mode

**Files:**
- Modify: `crates/kalla-worker/src/config.rs`

**Step 1: Add distributed mode config**

Add to `WorkerConfig`:

```rust
/// Enable Ballista distributed execution (skips staging).
pub ballista_enabled: bool,
/// Number of partitions per source for distributed reads.
pub ballista_partitions: usize,
```

Read from env:
- `BALLISTA_ENABLED` → `ballista_enabled` (default: false)
- `BALLISTA_PARTITIONS` → `ballista_partitions` (default: 4)

**Step 2: Write test**

```rust
#[test]
fn from_env_ballista_config() {
    let _lock = ENV_LOCK.lock().unwrap();
    clear_env();

    unsafe {
        std::env::set_var("BALLISTA_ENABLED", "true");
        std::env::set_var("BALLISTA_PARTITIONS", "8");
    }

    let config = WorkerConfig::from_env().unwrap();
    assert!(config.ballista_enabled);
    assert_eq!(config.ballista_partitions, 8);

    clear_env();
}
```

**Step 3: Implement and verify**

Run: `cargo test -p kalla-worker from_env_ballista -- --nocapture`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/kalla-worker/src/config.rs
git commit -m "feat: add BALLISTA_ENABLED and BALLISTA_PARTITIONS config"
```

---

## Task 8: Update Benchmarks for Direct Execution

**Files:**
- Modify: `benchmarks/inject_scaled_job.py`
- Create: `benchmarks/scenarios/direct_postgres_100k.json`
- Modify: `benchmarks/run_scaled_benchmark.sh`

**Step 1: Add direct exec support to inject_scaled_job.py**

Add `--direct-exec` flag that:
1. Skips StagePlan jobs
2. Publishes Exec message directly to NATS with `source_uris` field
3. Workers read from Postgres directly using `PostgresPartitionedTable`

**Step 2: Create scenario file**

```json
{
    "name": "direct_postgres_100k",
    "mode": "scaled",
    "source_type": "postgres",
    "rows": 100000,
    "workers": 2,
    "direct_exec": true,
    "match_sql": "SELECT i.*, p.* FROM left_src i JOIN right_src p ON i.invoice_id = p.reference_number AND tolerance_match(i.amount, p.paid_amount, 0.02)"
}
```

**Step 3: Update run_scaled_benchmark.sh**

Pass `--direct-exec` flag to `inject_scaled_job.py` when scenario has `"direct_exec": true`.
Set `BALLISTA_ENABLED=true` when starting workers for direct exec scenarios.

**Step 4: Run tests**

Run: `cargo test --workspace`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/
git commit -m "feat: add direct exec benchmark scenario (no staging)"
```

---

## Task 9: Update CI for Distributed Benchmarks

**Files:**
- Modify: `.github/workflows/ci.yml`

**Step 1: Add ballista benchmark variant**

In the `benchmark-scaled` job, add a step that runs the direct-exec scenarios with `BALLISTA_ENABLED=true`:

```yaml
- name: Run direct-exec benchmarks
  env:
    PG_URL: postgresql://kalla:kalla_secret@localhost:5432/kalla
    WORKER_BINARY: ./target/release/kalla-worker
    NUM_WORKERS: "2"
    BALLISTA_ENABLED: "true"
    BALLISTA_PARTITIONS: "4"
  run: bash benchmarks/run_scaled_benchmark.sh benchmarks/scenarios/direct_postgres_100k.json
```

**Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add direct-exec benchmark with Ballista"
```

---

## Task 10: Final Verification

**Step 1: Full test suite**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 2: Clippy + fmt**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Run: `cargo fmt --all -- --check`
Expected: Clean

**Step 3: Build release**

Run: `cargo build --release --bin kalla-worker`
Expected: Compiles successfully

**Step 4: Final commit and push**

```bash
git push origin main
```

---

## Key Design Decisions

1. **Feature flag, not replacement:** `BALLISTA_ENABLED` lets you switch between staged (existing) and distributed (new) execution. Both paths coexist — no breaking changes.

2. **Standalone mode, not cluster:** Ballista scheduler + executor run in-process within each worker. No new deployment components. Same binary, same Docker image.

3. **Source-specific providers:** Each data source type gets its own `TableProvider` implementation that knows how to partition reads natively. Postgres uses LIMIT/OFFSET, CSV uses byte ranges.

4. **Backward-compatible messages:** New `source_uris` field on Exec message is optional (`serde(default)`). Old messages without it still work via the staged path.

5. **Staging not removed:** The staging path remains as fallback. It's still useful for cases where you want to persist data on S3 for reuse across multiple reconciliation runs.
