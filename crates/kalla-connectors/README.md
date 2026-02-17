# kalla-connectors

Data source connectors for the Kalla reconciliation engine. Each connector registers a `TableProvider` with DataFusion, enabling SQL queries across heterogeneous sources.

## Connector Types

| Connector | Module | Partitioning | Use Case |
|-----------|--------|-------------|----------|
| `PostgresConnector` | `postgres` | None (full load) | Small tables, scoped queries |
| `PostgresPartitionedTable` | `postgres_partitioned` | `LIMIT/OFFSET` rows | Large Postgres tables, parallel reads |
| `CsvByteRangeTable` | `csv_partitioned` | Byte ranges | CSV files on S3, parallel reads |
| `S3Connector` | `s3` | None | Parquet files on S3 |
| `BigQueryConnector` | `bigquery` | None | BigQuery (stub) |

## SourceConnector Trait

The base trait for non-partitioned connectors:

```rust
#[async_trait]
pub trait SourceConnector: Send + Sync {
    async fn register_table(&self, ctx, table_name, source_table, where_clause) -> Result<()>;
    async fn register_scoped(&self, ctx, table_name, source_table, conditions, limit) -> Result<usize>;
    async fn stream_table(&self, ctx, table_name) -> Result<SendableRecordBatchStream>;
}
```

- `register_table` loads the full table (or filtered subset) into a `MemTable`
- `register_scoped` applies `FilterCondition`s and optional `LIMIT`
- `stream_table` returns a streaming reader over an already-registered table

## Partitioned Connectors

### PostgresPartitionedTable

Implements `TableProvider` directly. Divides a Postgres table into row-based partitions using `LIMIT/OFFSET`.

**Construction:**

```rust
let table = PostgresPartitionedTable::new(
    "postgres://user:pass@host/db",
    "invoices",          // table name
    4,                   // num_partitions
    Some("ctid".into()), // order_column — REQUIRED for correctness
).await?;
ctx.register_table("left_src", Arc::new(table))?;
```

At construction time:
1. Connects to Postgres, infers the Arrow schema from `information_schema.columns`
2. Runs `SELECT COUNT(*) FROM "table"` to get `total_rows`
3. No data is fetched — data is loaded lazily in `scan()`

**Query construction in `scan()`:**

Each partition generates one SQL query:

```sql
-- With order_column (correct):
SELECT "col1", "col2" FROM "table" ORDER BY "ctid" LIMIT 250000 OFFSET 0
SELECT "col1", "col2" FROM "table" ORDER BY "ctid" LIMIT 250000 OFFSET 250000

-- Without order_column (UNSAFE for partitioned reads):
SELECT "col1", "col2" FROM "table" LIMIT 250000 OFFSET 0
SELECT "col1", "col2" FROM "table" LIMIT 250000 OFFSET 250000
```

**Partition range computation:**

`compute_partition_ranges(total_rows, num_partitions)` returns `Vec<(offset, limit)>`:

- Divides rows evenly; last partition absorbs remainder
- Caps partition count to `total_rows` if fewer rows than partitions
- Returns empty vec for 0 rows or 0 partitions

Example: `compute_partition_ranges(1_000_000, 4)` produces:

| Partition | Offset | Limit |
|-----------|--------|-------|
| 0 | 0 | 250,000 |
| 1 | 250,000 | 250,000 |
| 2 | 500,000 | 250,000 |
| 3 | 750,000 | 250,000 |

**Remote reconstruction:**

For distributed execution (Ballista), tables are serialized/deserialized via `from_parts()`:

```rust
PostgresPartitionedTable::from_parts(
    conn_string, pg_table, schema, total_rows, num_partitions, order_column,
)
```

No database connection is needed — schema and row count are carried in the serialized plan.

### CsvByteRangeTable

Implements `TableProvider` directly. Divides an S3 CSV file into byte-range partitions.

**Construction:**

```rust
let table = CsvByteRangeTable::new(
    "s3://bucket/path/to/file.csv",
    4,        // num_partitions
    s3_config,
).await?;
ctx.register_table("right_src", Arc::new(table))?;
```

At construction time:
1. `HEAD` request to get file size
2. Reads first 8KB to infer column names from the CSV header
3. All columns are typed as `Utf8` — consumer casts as needed

**Byte range computation:**

`compute_byte_ranges(file_size, num_partitions)` returns `Vec<(start_byte, end_byte)>`:

- Divides bytes evenly; last partition extends to `file_size`
- Caps partition count to `file_size` if fewer bytes than partitions

**Partition boundary handling:**

CSV records don't align to byte boundaries. Each partition handles this:

- **First partition**: Skips the header line, keeps all data lines
- **Non-first partitions**: Discards the first (partial) line via `split_csv_chunk(data, is_first=false)`
- Each partition prepends the header before parsing with Arrow's CSV reader

This ensures every record is read exactly once with no duplicates or gaps.

## Critical Invariant: ORDER BY for LIMIT/OFFSET

**PostgreSQL does not guarantee row ordering without an explicit `ORDER BY` clause.**

When using `LIMIT/OFFSET` for partitioning, concurrent partition queries without `ORDER BY` can return overlapping or missing rows because the planner is free to choose any row ordering.

**Always provide `order_column` when constructing `PostgresPartitionedTable`.**

Recommended values:
- `"ctid"` — PostgreSQL's physical tuple identifier. Stable within a single transaction/snapshot. No index required. Suitable for read-only reconciliation workloads.
- A primary key column (e.g., `"id"`) — if the table has one and an index exists.

**What happens without ORDER BY:**

With 1M rows and 4 partitions, we observed:
- Local (single connection): 1,070,774 matches (rows duplicated across partitions)
- Cluster (4 parallel connections): 378,652 matches (rows skipped between partitions)
- Ground truth (single query): 600,014 matches

After adding `ORDER BY ctid`, all execution modes produce 600,014 — matching the ground truth.

## Filter System

The `filter` module provides SQL WHERE clause construction with input sanitization:

```rust
use kalla_connectors::{FilterCondition, FilterOp, FilterValue, build_where_clause};

let conditions = vec![
    FilterCondition {
        column: "invoice_date".into(),
        op: FilterOp::Between,
        value: FilterValue::Range(["2024-01-01".into(), "2024-01-31".into()]),
    },
    FilterCondition {
        column: "amount".into(),
        op: FilterOp::Gte,
        value: FilterValue::Number(100.0),
    },
];

let clause = build_where_clause(&conditions);
// => " WHERE "invoice_date" BETWEEN '2024-01-01' AND '2024-01-31' AND "amount" >= 100"
```

Supported operators: `Eq`, `Neq`, `Gt`, `Gte`, `Lt`, `Lte`, `Between`, `In`, `Like`.

String values are sanitized (single quotes escaped). Column names are double-quote escaped.

## Type Mapping

### From `information_schema.columns` (partitioned connector)

| PostgreSQL Type | Arrow Type |
|----------------|------------|
| `smallint` | `Int16` |
| `integer` | `Int32` |
| `bigint` | `Int64` |
| `real` | `Float32` |
| `double precision` | `Float64` |
| `numeric`, `decimal` | `Float64` |
| `boolean` | `Boolean` |
| `text`, `character varying`, `character` | `Utf8` |
| `bytea` | `Binary` |
| `date`, `timestamp *` | `Utf8` |
| `uuid`, `json`, `jsonb` | `Utf8` |

### From `pg_type` (non-partitioned connector)

| PostgreSQL Type | Arrow Type |
|----------------|------------|
| `INT2`, `SMALLINT` | `Int16` |
| `INT4`, `INTEGER`, `INT` | `Int32` |
| `INT8`, `BIGINT` | `Int64` |
| `FLOAT4`, `REAL` | `Float32` |
| `FLOAT8`, `DOUBLE PRECISION`, `NUMERIC` | `Float64` |
| `BOOL`, `BOOLEAN` | `Boolean` |
| `TEXT`, `VARCHAR`, `CHAR`, `BPCHAR` | `Utf8` |
| `BYTEA` | `Binary` |
| `DATE`, `TIMESTAMP`, `TIMESTAMPTZ` | `Utf8` |
| Unknown types | `Utf8` |

## Writing a New Connector

To add a new partitioned connector:

1. **Implement `TableProvider`** with a `scan()` that returns one `RecordBatch` per partition
2. **Provide a partition range function** (like `compute_partition_ranges` or `compute_byte_ranges`)
3. **Ensure deterministic partitioning** — every source row must appear in exactly one partition
4. **Provide `from_parts()` constructor** for remote reconstruction without a live connection
5. **Expose metadata accessors** (`total_rows()`, `num_partitions()`, `schema()`) for serialization
6. **Register via `ctx.register_table()`** to make the table available for SQL queries

For databases using `LIMIT/OFFSET`:
- **Always require an `order_column`** parameter
- Use `ORDER BY` in every partition query
- Prefer `ctid` (Postgres) or equivalent physical row ID for tables without a natural ordering column
