# Ballista Cluster Mode — True Distributed Partition Execution

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace Ballista standalone mode (in-process scheduler+executor) with cluster mode (external scheduler + multiple executors), enabling partition-level distribution across multiple physical workers.

**Architecture:** Currently, `postgres_partitioned` and `csv_partitioned` TableProviders eagerly fetch all partition data into `MemoryExec` during `scan()` — this runs entirely within a single process. In cluster mode, Ballista's scheduler distributes `ExecutionPlan` partitions across remote executor processes. This requires replacing `MemoryExec` with lazy custom `ExecutionPlan` nodes (`PostgresScanExec`, `CsvRangeScanExec`) that serialize via `PhysicalExtensionCodec` and fetch data locally on each executor. The kalla-worker connects to the Ballista scheduler via `SessionContext::remote("df://scheduler:50050")` and submits queries — the scheduler handles partition assignment.

**Tech Stack:** DataFusion 44, Ballista 44, `sqlx` 0.8, `object_store` 0.11, `async-nats` 0.38, `prost` (protobuf serialization for codec)

---

## Overview

### Current State (Standalone)

```
kalla-worker process
├── Ballista standalone (scheduler + executor in-process)
├── PostgresPartitionedTable.scan()
│   └── for each partition: SELECT ... LIMIT/OFFSET → MemoryExec
└── All partitions fetched and processed in ONE process
```

Benchmark result: scaled 5M rows → 44,771 rows/sec (single worker does all work).

### Target State (Cluster)

```
Ballista Scheduler (separate process)
├── Receives query from kalla-worker
├── Plans execution, identifies partitions
└── Distributes partition tasks to executors

Executor 1                    Executor 2
├── PostgresScanExec(p0)      ├── PostgresScanExec(p2)
├── PostgresScanExec(p1)      ├── PostgresScanExec(p3)
└── Fetches LIMIT/OFFSET      └── Fetches LIMIT/OFFSET
    from Postgres directly         from Postgres directly
```

Each executor only fetches its assigned partitions. True parallel I/O across machines.

### Key Constraint: PhysicalExtensionCodec

Ballista serializes `ExecutionPlan` nodes as protobuf to send them to remote executors. Custom nodes (like our `PostgresScanExec`) must implement `PhysicalExtensionCodec` for serialization/deserialization. This is the core technical challenge.

### What Changes

1. **New crate `kalla-ballista`** — contains codec, custom scheduler/executor binaries
2. **`PostgresScanExec`** — lazy `ExecutionPlan` that fetches a single partition from Postgres on the executor
3. **`CsvRangeScanExec`** — lazy `ExecutionPlan` that fetches a byte range from S3 on the executor
4. **`KallaPhysicalCodec`** — `PhysicalExtensionCodec` implementation for our custom nodes
5. **`PostgresPartitionedTable.scan()`** — returns `PostgresScanExec` nodes instead of `MemoryExec`
6. **`CsvByteRangeTable.scan()`** — returns `CsvRangeScanExec` nodes instead of `MemoryExec`
7. **`ReconciliationEngine::new_cluster()`** — connects to external Ballista scheduler
8. **`handle_exec()`** — uses cluster engine when `BALLISTA_SCHEDULER_URL` is set

### What Stays the Same

- Single mode HTTP path — unchanged
- NATS job queue for job distribution
- `match_sql` / `Recipe` format
- Financial UDFs (`tolerance_match`)
- Evidence writing
- Benchmark infrastructure (inject_scaled_job.py publishes to NATS as before)

### Dependency Changes

```toml
# Workspace Cargo.toml — update ballista features
ballista = { version = "44", default-features = false, features = ["standalone"] }
ballista-scheduler = "44"   # scheduler binary support
ballista-executor = "44"    # executor binary support
datafusion-proto = "44"     # PhysicalExtensionCodec trait
```

### Verified Ballista 44 API

- `SessionContextExt::remote("df://host:50050")` — connects to external scheduler
- `SessionContextExt::standalone()` — in-process scheduler+executor (current)
- `PhysicalExtensionCodec` — from `datafusion_proto::physical_plan`, required methods:
  - `try_decode(&self, buf: &[u8], inputs: &[Arc<dyn ExecutionPlan>], registry: &dyn FunctionRegistry) -> Result<Arc<dyn ExecutionPlan>>`
  - `try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()>`
  - Optional: `try_decode_udf`/`try_encode_udf` for custom UDF propagation
- `SchedulerConfig.override_physical_codec: Option<Arc<dyn PhysicalExtensionCodec>>`
- `ExecutorProcessConfig.override_physical_codec: Option<Arc<dyn PhysicalExtensionCodec>>`
- Client-side: `SessionConfigExt::with_ballista_physical_extension_codec()` to inject codec

---

## Task 1: Create `kalla-ballista` Crate

**Files:**
- Create: `crates/kalla-ballista/Cargo.toml`
- Create: `crates/kalla-ballista/src/lib.rs`
- Modify: `Cargo.toml` (workspace members)

This crate will hold the codec, custom execution plans, and scheduler/executor binaries.

**Step 1: Create crate directory and Cargo.toml**

```toml
[package]
name = "kalla-ballista"
version.workspace = true
edition.workspace = true

[dependencies]
datafusion.workspace = true
ballista.workspace = true
ballista-scheduler.workspace = true
ballista-executor.workspace = true
datafusion-proto.workspace = true
arrow.workspace = true
sqlx.workspace = true
tokio.workspace = true
anyhow.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
object_store = { version = "0.11", features = ["aws"] }
async-trait = "0.1"
futures = "0.3"
serde.workspace = true
serde_json.workspace = true

kalla-connectors.workspace = true
kalla-core.workspace = true
```

**Step 2: Create `lib.rs` with module stubs**

```rust
pub mod codec;
pub mod postgres_scan_exec;
pub mod csv_range_scan_exec;
```

**Step 3: Add to workspace**

In root `Cargo.toml`, add `"crates/kalla-ballista"` to `workspace.members` and add:
```toml
kalla-ballista = { path = "crates/kalla-ballista" }
```

Also add workspace deps:
```toml
prost = "0.13"
prost-types = "0.13"
```

**Step 4: Verify it compiles**

Run: `cargo check --workspace`
Expected: Compiles (empty modules)

**Step 5: Commit**

```bash
git add crates/kalla-ballista/ Cargo.toml Cargo.lock
git commit -m "feat: add kalla-ballista crate skeleton"
```

---

## Task 2: Implement `PostgresScanExec` — Lazy Partition Execution Plan

**Files:**
- Create: `crates/kalla-ballista/src/postgres_scan_exec.rs`

This is the heart of cluster mode. Unlike `MemoryExec` which holds pre-fetched data, `PostgresScanExec` is a *lazy* execution plan that connects to Postgres and fetches rows when `execute()` is called — which happens on the remote executor.

**Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_postgres_scan_exec_properties() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let exec = PostgresScanExec::new(
            "postgres://localhost/test".to_string(),
            "users".to_string(),
            schema.clone(),
            0,   // offset
            100, // limit
            None,
        );

        assert_eq!(exec.schema(), schema);
        // Single output partition (this node represents one partition)
        assert_eq!(exec.output_partitioning().partition_count(), 1);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let exec = PostgresScanExec::new(
            "postgres://localhost/test".to_string(),
            "users".to_string(),
            schema,
            50,
            100,
            Some("id".to_string()),
        );

        let bytes = exec.serialize();
        let deserialized = PostgresScanExec::deserialize(&bytes).unwrap();

        assert_eq!(deserialized.conn_string, "postgres://localhost/test");
        assert_eq!(deserialized.pg_table, "users");
        assert_eq!(deserialized.offset, 50);
        assert_eq!(deserialized.limit, 100);
        assert_eq!(deserialized.order_column, Some("id".to_string()));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p kalla-ballista test_postgres_scan_exec -- --nocapture`
Expected: FAIL — module doesn't exist

**Step 3: Implement PostgresScanExec**

```rust
//! Lazy PostgreSQL partition scan — fetches data on the executor.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DFResult;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::Stream;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::Row;
use tracing::debug;

use kalla_connectors::postgres::rows_to_record_batch;

/// A lazy execution plan node that fetches a single partition from PostgreSQL.
///
/// When Ballista sends this node to a remote executor, `execute()` is called
/// there — connecting to Postgres and fetching LIMIT/OFFSET rows on-demand.
#[derive(Debug, Clone)]
pub struct PostgresScanExec {
    pub conn_string: String,
    pub pg_table: String,
    pub schema: SchemaRef,
    pub offset: u64,
    pub limit: u64,
    pub order_column: Option<String>,
    properties: PlanProperties,
}

impl PostgresScanExec {
    pub fn new(
        conn_string: String,
        pg_table: String,
        schema: SchemaRef,
        offset: u64,
        limit: u64,
        order_column: Option<String>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            conn_string,
            pg_table,
            schema,
            offset,
            limit,
            order_column,
            properties,
        }
    }

    /// Serialize this node to bytes for Ballista transport.
    pub fn serialize(&self) -> Vec<u8> {
        // Use JSON for simplicity; could switch to protobuf for performance.
        let payload = serde_json::json!({
            "conn_string": self.conn_string,
            "pg_table": self.pg_table,
            "offset": self.offset,
            "limit": self.limit,
            "order_column": self.order_column,
            "schema": serialize_schema(&self.schema),
        });
        serde_json::to_vec(&payload).expect("PostgresScanExec serialization failed")
    }

    /// Deserialize from bytes.
    pub fn deserialize(bytes: &[u8]) -> anyhow::Result<Self> {
        let val: serde_json::Value = serde_json::from_slice(bytes)?;
        let conn_string = val["conn_string"].as_str().unwrap().to_string();
        let pg_table = val["pg_table"].as_str().unwrap().to_string();
        let offset = val["offset"].as_u64().unwrap();
        let limit = val["limit"].as_u64().unwrap();
        let order_column = val["order_column"].as_str().map(|s| s.to_string());
        let schema = deserialize_schema(&val["schema"])?;

        Ok(Self::new(conn_string, pg_table, schema, offset, limit, order_column))
    }
}

impl DisplayAs for PostgresScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PostgresScanExec: table={}, offset={}, limit={}",
            self.pg_table, self.offset, self.limit
        )
    }
}

impl ExecutionPlan for PostgresScanExec {
    fn name(&self) -> &str {
        "PostgresScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self) // leaf node, no children to replace
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let conn_string = self.conn_string.clone();
        let pg_table = self.pg_table.clone();
        let schema = Arc::clone(&self.schema);
        let offset = self.offset;
        let limit = self.limit;
        let order_column = self.order_column.clone();

        let columns_sql = schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let query = match &order_column {
            Some(col) => format!(
                "SELECT {} FROM \"{}\" ORDER BY \"{}\" LIMIT {} OFFSET {}",
                columns_sql, pg_table, col, limit, offset
            ),
            None => format!(
                "SELECT {} FROM \"{}\" LIMIT {} OFFSET {}",
                columns_sql, pg_table, limit, offset
            ),
        };

        debug!("PostgresScanExec executing: {}", query);

        // Spawn async fetch; return a stream wrapper
        Ok(Box::pin(PostgresScanStream::new(conn_string, query, schema)))
    }
}

/// Stream that lazily connects to Postgres and returns RecordBatches.
struct PostgresScanStream {
    schema: SchemaRef,
    inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
}

impl PostgresScanStream {
    fn new(conn_string: String, query: String, schema: SchemaRef) -> Self {
        let schema_clone = Arc::clone(&schema);
        let stream = futures::stream::once(async move {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&conn_string)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let rows: Vec<PgRow> = sqlx::query(&query)
                .fetch_all(&pool)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            pool.close().await;

            if rows.is_empty() {
                return Ok(RecordBatch::new_empty(schema_clone));
            }

            rows_to_record_batch(&rows, schema_clone)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
        });

        Self {
            schema,
            inner: Box::pin(stream),
        }
    }
}

impl Stream for PostgresScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl datafusion::physical_plan::RecordBatchStream for PostgresScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// -- Schema serialization helpers (JSON) --

fn serialize_schema(schema: &SchemaRef) -> serde_json::Value {
    let fields: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "data_type": format!("{:?}", f.data_type()),
                "nullable": f.is_nullable(),
            })
        })
        .collect();
    serde_json::Value::Array(fields)
}

fn deserialize_schema(val: &serde_json::Value) -> anyhow::Result<SchemaRef> {
    use arrow::datatypes::{DataType, Field};
    let fields: Vec<Field> = val
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("expected array for schema"))?
        .iter()
        .map(|f| {
            let name = f["name"].as_str().unwrap();
            let dt_str = f["data_type"].as_str().unwrap();
            let nullable = f["nullable"].as_bool().unwrap_or(true);
            let dt = parse_data_type(dt_str);
            Field::new(name, dt, nullable)
        })
        .collect();
    Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
}

fn parse_data_type(s: &str) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType;
    match s {
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Boolean" => DataType::Boolean,
        "Binary" => DataType::Binary,
        _ => DataType::Utf8, // default fallback
    }
}
```

**Step 4: Run tests**

Run: `cargo test -p kalla-ballista test_postgres_scan_exec -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/kalla-ballista/src/postgres_scan_exec.rs
git commit -m "feat: add PostgresScanExec lazy execution plan for Ballista cluster"
```

---

## Task 3: Implement `CsvRangeScanExec` — Lazy S3 Byte-Range Execution Plan

**Files:**
- Create: `crates/kalla-ballista/src/csv_range_scan_exec.rs`

Same pattern as `PostgresScanExec` but for S3 CSV byte ranges.

**Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_csv_range_scan_exec_properties() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("amount", DataType::Utf8, true),
        ]));

        let exec = CsvRangeScanExec::new(
            "s3://bucket/data.csv".to_string(),
            schema.clone(),
            1024,  // start_byte
            2048,  // end_byte
            false, // is_first_partition
            "id,amount".to_string(),
            S3Config::default(),
        );

        assert_eq!(exec.schema(), schema);
        assert_eq!(exec.output_partitioning().partition_count(), 1);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
        ]));

        let exec = CsvRangeScanExec::new(
            "s3://bucket/data.csv".to_string(),
            schema,
            512,
            1024,
            false,
            "id".to_string(),
            S3Config::default(),
        );

        let bytes = exec.serialize();
        let deserialized = CsvRangeScanExec::deserialize(&bytes).unwrap();

        assert_eq!(deserialized.s3_uri, "s3://bucket/data.csv");
        assert_eq!(deserialized.start_byte, 512);
        assert_eq!(deserialized.end_byte, 1024);
        assert!(!deserialized.is_first_partition);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p kalla-ballista test_csv_range_scan_exec -- --nocapture`
Expected: FAIL

**Step 3: Implement CsvRangeScanExec**

Follow the same pattern as `PostgresScanExec`:
- Lazy execution: `execute()` reads the byte range from S3, handles partial lines, parses CSV
- Serialization: JSON with S3 config, byte range, schema
- One output partition per node

**Step 4: Run tests**

Run: `cargo test -p kalla-ballista -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/kalla-ballista/src/csv_range_scan_exec.rs
git commit -m "feat: add CsvRangeScanExec lazy execution plan for Ballista cluster"
```

---

## Task 4: Implement `KallaPhysicalCodec`

**Files:**
- Create: `crates/kalla-ballista/src/codec.rs`

Ballista requires a `PhysicalExtensionCodec` to serialize/deserialize custom `ExecutionPlan` nodes when distributing them to remote executors.

**Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_scan_exec::PostgresScanExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::ExecutionPlan;

    #[tokio::test]
    async fn test_codec_roundtrip_postgres() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let exec = PostgresScanExec::new(
            "postgres://localhost/test".to_string(),
            "users".to_string(),
            schema,
            0,
            100,
            Some("id".to_string()),
        );

        let codec = KallaPhysicalCodec::new();
        let mut buf = Vec::new();
        codec
            .try_encode(Arc::new(exec.clone()) as Arc<dyn ExecutionPlan>, &mut buf)
            .unwrap();

        let decoded = codec.try_decode(buf.as_slice()).unwrap();
        let pg_exec = decoded.as_any().downcast_ref::<PostgresScanExec>().unwrap();
        assert_eq!(pg_exec.conn_string, "postgres://localhost/test");
        assert_eq!(pg_exec.pg_table, "users");
        assert_eq!(pg_exec.offset, 0);
        assert_eq!(pg_exec.limit, 100);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p kalla-ballista test_codec -- --nocapture`
Expected: FAIL

**Step 3: Implement KallaPhysicalCodec**

```rust
//! PhysicalExtensionCodec for Ballista cluster mode.
//!
//! Enables serialization of custom ExecutionPlan nodes (PostgresScanExec,
//! CsvRangeScanExec) so Ballista can send them to remote executors.
//!
//! The trait lives in `datafusion_proto::physical_plan::PhysicalExtensionCodec`.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::csv_range_scan_exec::CsvRangeScanExec;
use crate::postgres_scan_exec::PostgresScanExec;

/// Magic bytes for discriminating node types in the wire format.
const POSTGRES_SCAN_TAG: u8 = 1;
const CSV_RANGE_SCAN_TAG: u8 = 2;

/// Codec for serializing/deserializing Kalla's custom ExecutionPlan nodes.
///
/// Both the scheduler and executor binaries must register this codec so
/// custom plan nodes can be sent over the wire.
#[derive(Debug)]
pub struct KallaPhysicalCodec;

impl KallaPhysicalCodec {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalExtensionCodec for KallaPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if buf.is_empty() {
            return Err(DataFusionError::Plan("Empty codec buffer".to_string()));
        }

        match buf[0] {
            POSTGRES_SCAN_TAG => {
                let exec = PostgresScanExec::deserialize(&buf[1..])
                    .map_err(|e| DataFusionError::Plan(format!("PostgresScanExec decode: {}", e)))?;
                Ok(Arc::new(exec))
            }
            CSV_RANGE_SCAN_TAG => {
                let exec = CsvRangeScanExec::deserialize(&buf[1..])
                    .map_err(|e| DataFusionError::Plan(format!("CsvRangeScanExec decode: {}", e)))?;
                Ok(Arc::new(exec))
            }
            other => Err(DataFusionError::Plan(format!(
                "Unknown KallaPhysicalCodec tag: {}",
                other
            ))),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> DFResult<()> {
        if let Some(pg) = node.as_any().downcast_ref::<PostgresScanExec>() {
            buf.push(POSTGRES_SCAN_TAG);
            buf.extend_from_slice(&pg.serialize());
            Ok(())
        } else if let Some(csv) = node.as_any().downcast_ref::<CsvRangeScanExec>() {
            buf.push(CSV_RANGE_SCAN_TAG);
            buf.extend_from_slice(&csv.serialize());
            Ok(())
        } else {
            Err(DataFusionError::Plan(format!(
                "KallaPhysicalCodec cannot encode: {}",
                node.name()
            )))
        }
    }

    // Optional: implement try_decode_udf/try_encode_udf to propagate
    // tolerance_match UDF to executors. This avoids needing to register
    // UDFs manually in each executor binary.
}
```

**Step 4: Run tests**

Run: `cargo test -p kalla-ballista test_codec -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/kalla-ballista/src/codec.rs
git commit -m "feat: add KallaPhysicalCodec for Ballista cluster serialization"
```

---

## Task 5: Update `PostgresPartitionedTable.scan()` to Return Lazy Nodes

**Files:**
- Modify: `crates/kalla-connectors/src/postgres_partitioned.rs`

Currently `scan()` eagerly fetches all partitions into `MemoryExec`. We need a mode that returns `PostgresScanExec` leaf nodes instead — one per partition — so Ballista can distribute them.

**Step 1: Write the failing test**

Add to `postgres_partitioned.rs` tests:

```rust
#[test]
fn test_partition_ranges_produce_correct_count() {
    // Verify that N partitions produce N ranges for the lazy scan path
    let ranges = compute_partition_ranges(1_000_000, 8);
    assert_eq!(ranges.len(), 8);
    let total: u64 = ranges.iter().map(|(_, limit)| limit).sum();
    assert_eq!(total, 1_000_000);
}
```

**Step 2: Add `scan_lazy()` method**

Add a new method to `PostgresPartitionedTable` that returns a `UnionExec` wrapping N `PostgresScanExec` nodes:

```rust
use kalla_ballista::postgres_scan_exec::PostgresScanExec;
use datafusion::physical_plan::union::UnionExec;

/// Create a lazy execution plan — each partition is a `PostgresScanExec`
/// that fetches data when executed (suitable for Ballista cluster mode).
pub fn scan_lazy(&self) -> DFResult<Arc<dyn ExecutionPlan>> {
    let ranges = compute_partition_ranges(self.total_rows, self.num_partitions);
    let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(ranges.len());

    for (offset, limit) in &ranges {
        plans.push(Arc::new(PostgresScanExec::new(
            self.conn_string.clone(),
            self.pg_table.clone(),
            Arc::clone(&self.schema),
            *offset,
            *limit,
            self.order_column.clone(),
        )));
    }

    if plans.len() == 1 {
        Ok(plans.into_iter().next().unwrap())
    } else {
        Ok(Arc::new(UnionExec::new(plans)))
    }
}
```

**Step 3: Update `scan()` to use lazy path when codec is available**

The `TableProvider::scan()` method needs to detect whether it's running in cluster mode. Use a session config flag:

```rust
async fn scan(&self, state: &dyn Session, ...) -> DFResult<Arc<dyn ExecutionPlan>> {
    // Check if cluster mode is enabled via session config
    let use_lazy = state
        .config()
        .options()
        .extensions
        .get::<KallaClusterConfig>()
        .map(|c| c.enabled)
        .unwrap_or(false);

    if use_lazy {
        return self.scan_lazy();
    }

    // ... existing MemoryExec path for standalone/single mode ...
}
```

Alternatively, since detecting cluster mode from `scan()` is complex, add a `lazy_scan: bool` field to the table provider set during construction.

**Step 4: Run tests**

Run: `cargo test -p kalla-connectors -- --nocapture`
Expected: All existing tests pass, new test passes

**Step 5: Commit**

```bash
git add crates/kalla-connectors/src/postgres_partitioned.rs
git commit -m "feat: add scan_lazy() for Ballista cluster partition distribution"
```

---

## Task 6: Update `CsvByteRangeTable.scan()` for Lazy Nodes

**Files:**
- Modify: `crates/kalla-connectors/src/csv_partitioned.rs`

Same pattern as Task 5 but for CSV byte ranges.

**Step 1: Add `scan_lazy()` method**

Returns a `UnionExec` wrapping N `CsvRangeScanExec` nodes.

**Step 2: Run tests**

Run: `cargo test -p kalla-connectors -- --nocapture`
Expected: All pass

**Step 3: Commit**

```bash
git add crates/kalla-connectors/src/csv_partitioned.rs
git commit -m "feat: add scan_lazy() for CsvByteRangeTable cluster mode"
```

---

## Task 7: Add `ReconciliationEngine::new_cluster()` Constructor

**Files:**
- Modify: `crates/kalla-core/src/engine.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn test_cluster_engine_constructor() {
    // This test requires a running Ballista scheduler, so mark it #[ignore]
    // For unit testing, verify the constructor signature exists.
    // Integration test will be in CI with docker-compose.
}
```

**Step 2: Implement `new_cluster()`**

```rust
/// Create a cluster-mode engine that connects to an external Ballista scheduler.
///
/// Queries submitted to this engine are distributed across Ballista executors.
/// The scheduler URL should be in the form `df://host:port`.
pub async fn new_cluster(scheduler_url: &str) -> anyhow::Result<Self> {
    use ballista::prelude::{SessionConfigExt as _, SessionContextExt as _};
    use kalla_ballista::codec::KallaPhysicalCodec;

    // Build session state with our custom codec so the client can serialize
    // custom plan nodes when submitting queries to the scheduler.
    let config = datafusion::prelude::SessionConfig::new()
        .with_information_schema(true)
        .with_ballista_physical_extension_codec(Arc::new(KallaPhysicalCodec::new()));

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx: SessionContext = SessionContext::remote_with_state(scheduler_url, state).await?;
    udf::register_financial_udfs(&ctx);

    info!(
        "ReconciliationEngine (cluster mode, scheduler={}) initialized",
        scheduler_url
    );

    Ok(Self { ctx })
}
```

**Step 3: Run tests**

Run: `cargo test -p kalla-core -- --nocapture`
Expected: All existing tests pass

**Step 4: Commit**

```bash
git add crates/kalla-core/src/engine.rs
git commit -m "feat: add ReconciliationEngine::new_cluster() for Ballista cluster mode"
```

---

## Task 8: Update `handle_exec()` to Support Cluster Mode

**Files:**
- Modify: `crates/kalla-worker/src/exec.rs`
- Modify: `crates/kalla-worker/src/config.rs`

**Step 1: Add `ballista_scheduler_url` to config**

In `config.rs`:

```rust
/// Optional Ballista scheduler URL for cluster mode (e.g., "df://localhost:50050").
/// When set, uses cluster mode. When None, uses standalone/single mode.
pub ballista_scheduler_url: Option<String>,
```

Parse from `BALLISTA_SCHEDULER_URL` env var.

**Step 2: Update `handle_exec()`**

```rust
pub async fn handle_exec(
    config: &WorkerConfig,
    pool: &PgPool,
    run_id: Uuid,
    job_id: Uuid,
    recipe_json: &str,
    source_uris: &[SourceUri],
    callback_url: Option<&str>,
) -> Result<ExecResult> {
    let engine = if let Some(ref scheduler_url) = config.ballista_scheduler_url {
        // Cluster mode: connect to external Ballista scheduler
        ReconciliationEngine::new_cluster(scheduler_url).await?
    } else {
        // Standalone mode: in-process execution
        ReconciliationEngine::new()
    };

    // Register sources with lazy_scan=true when in cluster mode
    let use_lazy = config.ballista_scheduler_url.is_some();
    // ... register sources with lazy flag ...
    // ... rest of execution unchanged ...
}
```

**Step 3: Write config test**

```rust
#[test]
fn from_env_ballista_scheduler_url() {
    let _lock = ENV_LOCK.lock().unwrap();
    clear_env();

    unsafe {
        std::env::set_var("BALLISTA_SCHEDULER_URL", "df://scheduler:50050");
    }

    let config = WorkerConfig::from_env().unwrap();
    assert_eq!(
        config.ballista_scheduler_url,
        Some("df://scheduler:50050".to_string())
    );

    clear_env();
}
```

**Step 4: Run tests**

Run: `cargo test --workspace`
Expected: All pass

**Step 5: Commit**

```bash
git add crates/kalla-worker/src/exec.rs crates/kalla-worker/src/config.rs
git commit -m "feat: use Ballista cluster mode when BALLISTA_SCHEDULER_URL is set"
```

---

## Task 9: Build Custom Scheduler Binary

**Files:**
- Create: `crates/kalla-ballista/src/bin/kalla-scheduler.rs`

Ballista executors need the `KallaPhysicalCodec` registered to deserialize our custom nodes. The scheduler also needs it to understand the plan it's distributing.

**Step 1: Write the scheduler binary**

```rust
//! Kalla Ballista scheduler — distributes queries to executors with custom codec.
//!
//! The scheduler needs KallaPhysicalCodec registered so it can serialize
//! custom plan nodes (PostgresScanExec, CsvRangeScanExec) when sending
//! them to executors.

use std::sync::Arc;

use anyhow::Result;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use kalla_ballista::codec::KallaPhysicalCodec;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // SchedulerConfig has override_physical_codec field
    let mut config = SchedulerConfig::default();
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));

    // start_server(cluster_backend, addr, config)
    // The exact start_server signature may need cluster + addr params:
    //   start_server(cluster, "0.0.0.0:50050", Arc::new(config))
    // Verify from ballista-scheduler-44 source.
    start_server(Arc::new(config)).await?;

    Ok(())
}
```

**Step 2: Verify it compiles**

Run: `cargo build -p kalla-ballista --bin kalla-scheduler`
Expected: Compiles

**Step 3: Commit**

```bash
git add crates/kalla-ballista/src/bin/
git commit -m "feat: add kalla-scheduler binary with custom codec"
```

---

## Task 10: Build Custom Executor Binary

**Files:**
- Create: `crates/kalla-ballista/src/bin/kalla-executor.rs`

**Step 1: Write the executor binary**

```rust
//! Kalla Ballista executor — runs partition tasks with custom codec.
//!
//! The executor needs KallaPhysicalCodec to deserialize custom plan nodes
//! received from the scheduler. It also needs tolerance_match UDF registered.

use std::sync::Arc;

use anyhow::Result;
use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use kalla_ballista::codec::KallaPhysicalCodec;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // ExecutorProcessConfig has override_physical_codec field
    let mut config = ExecutorProcessConfig::default();
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));

    // start_executor_process(config) connects to the scheduler and
    // starts executing partition tasks.
    start_executor_process(Arc::new(config)).await?;

    Ok(())
}
```

**Step 2: Verify it compiles**

Run: `cargo build -p kalla-ballista --bin kalla-executor`
Expected: Compiles

**Step 3: Commit**

```bash
git add crates/kalla-ballista/src/bin/
git commit -m "feat: add kalla-executor binary with custom codec"
```

---

## Task 11: Add Docker Compose for Cluster Mode

**Files:**
- Create: `docker-compose.cluster.yml`

**Step 1: Write docker-compose for cluster deployment**

```yaml
version: "3.8"
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: kalla
      POSTGRES_PASSWORD: kalla_secret
      POSTGRES_DB: kalla
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U kalla"]
      interval: 5s
      timeout: 3s
      retries: 5

  nats:
    image: nats:2.10
    command: ["--jetstream"]
    ports:
      - "4222:4222"

  scheduler:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["kalla-scheduler"]
    ports:
      - "50050:50050"
    environment:
      RUST_LOG: info

  executor-1:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["kalla-executor"]
    environment:
      RUST_LOG: info
      SCHEDULER_URL: "df://scheduler:50050"

  executor-2:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["kalla-executor"]
    environment:
      RUST_LOG: info
      SCHEDULER_URL: "df://scheduler:50050"

  worker:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["kalla-worker"]
    ports:
      - "9090:9090"
    environment:
      NATS_URL: "nats://nats:4222"
      DATABASE_URL: "postgresql://kalla:kalla_secret@postgres:5432/kalla"
      BALLISTA_SCHEDULER_URL: "df://scheduler:50050"
      BALLISTA_PARTITIONS: "8"
      RUST_LOG: info
    depends_on:
      postgres:
        condition: service_healthy
      nats:
        condition: service_started
      scheduler:
        condition: service_started
```

**Step 2: Commit**

```bash
git add docker-compose.cluster.yml
git commit -m "feat: add docker-compose.cluster.yml for Ballista cluster deployment"
```

---

## Task 12: Update Benchmarks for Cluster Mode

**Files:**
- Create: `benchmarks/scenarios/cluster_postgres_100k.json`
- Create: `benchmarks/scenarios/cluster_postgres_1m.json`
- Create: `benchmarks/scenarios/cluster_postgres_5m.json`
- Create: `benchmarks/run_cluster_benchmark.sh`

**Step 1: Create cluster benchmark scenarios**

```json
{
    "name": "cluster_postgres_100k",
    "mode": "cluster",
    "source_type": "postgres",
    "rows": 100000,
    "workers": 2,
    "executors": 2,
    "match_sql": "SELECT i.*, p.* FROM left_src i JOIN right_src p ON i.invoice_id = p.reference_number AND tolerance_match(i.amount, p.paid_amount, 0.02)"
}
```

**Step 2: Write `run_cluster_benchmark.sh`**

Script that:
1. Starts Ballista scheduler + 2 executors
2. Starts kalla-worker with `BALLISTA_SCHEDULER_URL`
3. Seeds Postgres data
4. Injects job via NATS
5. Polls for completion
6. Reports elapsed time + rows/sec

**Step 3: Commit**

```bash
git add benchmarks/
git commit -m "feat: add cluster-mode benchmark scenarios and runner"
```

---

## Task 13: Update CI for Cluster Mode Benchmarks

**Files:**
- Modify: `.github/workflows/ci.yml`

**Step 1: Add benchmark-cluster job**

Add a new CI job that:
1. Builds kalla-scheduler, kalla-executor, kalla-worker
2. Starts Postgres + NATS + scheduler + 2 executors
3. Runs cluster benchmark scenarios
4. Reports results

**Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add Ballista cluster mode benchmark job"
```

---

## Task 14: UDF Registration on Executors

**Files:**
- Modify: `crates/kalla-ballista/src/bin/kalla-executor.rs`
- Possibly modify: `crates/kalla-core/src/udf.rs`

**Critical issue:** The `tolerance_match` UDF must be available on each executor. Ballista executors don't automatically share UDFs from the client session. The executor binary must register UDFs at startup.

**Step 1: Verify UDF availability**

Run a test query through cluster mode that uses `tolerance_match`. If it fails with "function not found", the executor needs UDF registration.

**Step 2: Register UDFs in executor binary**

Add UDF registration to the executor's session config or use Ballista's `LogicalExtensionCodec` for UDF serialization.

**Step 3: Test end-to-end**

Run benchmark with `tolerance_match` in match_sql through cluster mode.

**Step 4: Commit**

```bash
git add crates/kalla-ballista/ crates/kalla-core/
git commit -m "fix: register tolerance_match UDF on Ballista executors"
```

---

## Task 15: Final Verification

**Step 1: Full test suite**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 2: Clippy + fmt**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Run: `cargo fmt --all -- --check`
Expected: Clean

**Step 3: Build all binaries**

Run: `cargo build --release --bin kalla-worker --bin kalla-scheduler --bin kalla-executor`
Expected: Compiles

**Step 4: Push with benchmark trigger**

```bash
git push origin main  # with [perform-benchmark] in commit message
```

Expected: Single-mode, scaled-mode, and cluster-mode benchmarks all pass. Cluster mode shows significantly higher rows/sec for 5M rows compared to scaled standalone.

---

## Key Design Decisions

1. **Separate crate for Ballista integration:** `kalla-ballista` keeps Ballista cluster concerns (codec, custom binaries) isolated. The core connectors don't need Ballista as a dependency.

2. **Lazy `ExecutionPlan` nodes:** `PostgresScanExec` and `CsvRangeScanExec` only fetch data when `execute()` is called on the remote executor. This is the fundamental change from the current eager `MemoryExec` approach.

3. **JSON serialization for codec:** Simpler than protobuf, good enough for the metadata payload (connection strings, offsets, schema). Can migrate to protobuf later if serialization overhead matters.

4. **`scan_lazy()` vs modifying `scan()`:** Adding a separate method avoids breaking the existing standalone path. The `handle_exec` function chooses which path based on config.

5. **Custom scheduler/executor binaries:** Required because Ballista executors need the `KallaPhysicalCodec` registered at startup. Can't use stock Ballista binaries.

6. **UDF registration challenge:** `tolerance_match` must be available on every executor. This is a known Ballista limitation — UDFs registered on the client session aren't automatically propagated. The executor binary must register them explicitly.

7. **Backward compatible:** All three modes coexist:
   - Single mode (no NATS, no Ballista) — HTTP jobs, `ReconciliationEngine::new()`
   - Scaled standalone (NATS, no scheduler URL) — NATS jobs, `ReconciliationEngine::new()` with partitioned providers
   - Cluster mode (NATS + scheduler URL) — NATS jobs, `ReconciliationEngine::new_cluster()` with lazy providers

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Ballista 44 cluster API differs from docs | High | Verify API in `ballista/examples/` before Task 9-10 |
| UDFs not propagating to executors | Medium | Task 14 explicitly addresses this; may need `LogicalExtensionCodec` |
| Postgres connection from executor containers | Low | Ensure executor containers can reach Postgres (network config) |
| Schema serialization edge cases | Low | JSON fallback to Utf8; test with all pg types |
| Ballista scheduler overhead for small queries | Low | Only use cluster mode for large datasets (config-driven) |
