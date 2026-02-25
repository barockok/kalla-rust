//! PostgreSQL partitioned table provider for DataFusion
//!
//! Implements `TableProvider` to support partitioned reads from PostgreSQL
//! using `LIMIT/OFFSET` queries, enabling parallel partition-level execution.
//!
//! Contains both the `PostgresPartitionedTable` (the `TableProvider`) and
//! `PostgresScanExec` (the lazy `ExecutionPlan` that fetches a single
//! partition). Used by both local and cluster (Ballista) modes.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::{Expr, SessionContext};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::Row;
use tracing::{debug, info};

// ===========================================================================
// Partition helpers
// ===========================================================================

/// Compute (offset, limit) ranges for partitioned reads.
///
/// Divides `total_rows` evenly across `num_partitions`. The last partition
/// receives any remainder rows. If `num_partitions` exceeds `total_rows`,
/// the partition count is capped to `total_rows`.
pub fn compute_partition_ranges(total_rows: u64, num_partitions: usize) -> Vec<(u64, u64)> {
    if total_rows == 0 || num_partitions == 0 {
        return vec![];
    }

    let effective_partitions = std::cmp::min(num_partitions as u64, total_rows) as usize;
    let base_size = total_rows / effective_partitions as u64;
    let remainder = total_rows % effective_partitions as u64;

    let mut ranges = Vec::with_capacity(effective_partitions);
    let mut offset = 0u64;

    for i in 0..effective_partitions {
        let limit = if i == effective_partitions - 1 {
            base_size + remainder
        } else {
            base_size
        };
        ranges.push((offset, limit));
        offset += limit;
    }

    ranges
}

// ===========================================================================
// PostgresPartitionedTable — the TableProvider
// ===========================================================================

/// A DataFusion `TableProvider` that reads from PostgreSQL using partitioned
/// `LIMIT/OFFSET` queries.
///
/// On `scan()`, the table is divided into partitions and each partition is
/// fetched independently, enabling parallel reads when used with DataFusion
/// or Ballista executors.
pub struct PostgresPartitionedTable {
    conn_string: String,
    pg_table: String,
    schema: SchemaRef,
    total_rows: u64,
    num_partitions: usize,
    order_column: Option<String>,
    where_clause: Option<String>,
}

impl fmt::Debug for PostgresPartitionedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresPartitionedTable")
            .field("pg_table", &self.pg_table)
            .field("total_rows", &self.total_rows)
            .field("num_partitions", &self.num_partitions)
            .field("order_column", &self.order_column)
            .field("where_clause", &self.where_clause)
            .finish()
    }
}

impl PostgresPartitionedTable {
    /// Create a new `PostgresPartitionedTable` by connecting to Postgres,
    /// inferring the schema from `information_schema.columns`, and counting rows.
    ///
    /// No data is fetched at construction time; data is fetched lazily in `scan()`.
    pub async fn new(
        conn_string: &str,
        pg_table: &str,
        num_partitions: usize,
        order_column: Option<String>,
        where_clause: Option<String>,
    ) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(conn_string)
            .await?;

        // Infer schema from information_schema.columns
        let schema = infer_schema(&pool, pg_table).await?;

        // Count total rows (with optional WHERE clause)
        let wc = where_clause.as_deref().unwrap_or("");
        let count_query = format!("SELECT COUNT(*) AS cnt FROM \"{}\"{}", pg_table, wc);
        let row: (i64,) = sqlx::query_as(&count_query).fetch_one(&pool).await?;
        let total_rows = row.0 as u64;

        info!(
            "PostgresPartitionedTable: table='{}', rows={}, partitions={}, order_col={:?}, where={:?}",
            pg_table, total_rows, num_partitions, order_column, where_clause
        );

        pool.close().await;

        Ok(Self {
            conn_string: conn_string.to_string(),
            pg_table: pg_table.to_string(),
            schema,
            total_rows,
            num_partitions,
            order_column,
            where_clause,
        })
    }

    /// Reconstruct from pre-computed parts (no database connection required).
    /// Used by the logical codec to deserialize a table provider on remote executors.
    pub fn from_parts(
        conn_string: String,
        pg_table: String,
        schema: SchemaRef,
        total_rows: u64,
        num_partitions: usize,
        order_column: Option<String>,
        where_clause: Option<String>,
    ) -> Self {
        Self {
            conn_string,
            pg_table,
            schema,
            total_rows,
            num_partitions,
            order_column,
            where_clause,
        }
    }

    /// Access the connection string (e.g. for serializing partition metadata).
    pub fn conn_string(&self) -> &str {
        &self.conn_string
    }

    /// Access the Postgres table name.
    pub fn pg_table(&self) -> &str {
        &self.pg_table
    }

    /// Access the total row count discovered at construction time.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Access the configured number of partitions.
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Access the order column, if set.
    pub fn order_column(&self) -> Option<&str> {
        self.order_column.as_deref()
    }

    /// Access the inferred Arrow schema.
    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Access the optional WHERE clause.
    pub fn where_clause(&self) -> Option<&str> {
        self.where_clause.as_deref()
    }

    /// Serialize this table provider to bytes for the wire codec.
    pub fn wire_serialize(&self) -> Vec<u8> {
        let info = serde_json::json!({
            "conn_string": self.conn_string,
            "pg_table": self.pg_table,
            "total_rows": self.total_rows,
            "num_partitions": self.num_partitions,
            "order_column": self.order_column,
            "where_clause": self.where_clause,
        });
        serde_json::to_vec(&info).expect("PostgresPartitionedTable serialization cannot fail")
    }

    /// Deserialize from bytes + schema into a `PostgresPartitionedTable`.
    pub fn wire_deserialize(buf: &[u8], schema: SchemaRef) -> datafusion::error::Result<Self> {
        let info: serde_json::Value = serde_json::from_slice(buf).map_err(|e| {
            datafusion::error::DataFusionError::Internal(format!(
                "failed to deserialize PostgresPartitionedTable: {e}"
            ))
        })?;

        let conn_string = info["conn_string"]
            .as_str()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal("missing conn_string".into())
            })?
            .to_string();
        let pg_table = info["pg_table"]
            .as_str()
            .ok_or_else(|| datafusion::error::DataFusionError::Internal("missing pg_table".into()))?
            .to_string();
        let total_rows = info["total_rows"].as_u64().unwrap_or(0);
        let num_partitions = info["num_partitions"].as_u64().unwrap_or(1) as usize;
        let order_column = info["order_column"].as_str().map(|s| s.to_string());
        let where_clause = info["where_clause"].as_str().map(|s| s.to_string());

        Ok(Self::from_parts(
            conn_string,
            pg_table,
            schema,
            total_rows,
            num_partitions,
            order_column,
            where_clause,
        ))
    }
}

#[async_trait]
impl TableProvider for PostgresPartitionedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(Statistics {
            num_rows: Precision::Exact(self.total_rows as usize),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        })
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
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
                self.where_clause.clone(),
            )));
        }

        if plans.len() == 1 {
            Ok(plans.into_iter().next().unwrap())
        } else {
            Ok(Arc::new(UnionExec::new(plans)))
        }
    }
}

/// Register a `PostgresPartitionedTable` with a DataFusion `SessionContext`.
pub async fn register(
    ctx: &SessionContext,
    table_name: &str,
    conn_string: &str,
    pg_table: &str,
    num_partitions: usize,
    order_column: Option<String>,
    where_clause: Option<String>,
) -> anyhow::Result<()> {
    let table = PostgresPartitionedTable::new(
        conn_string,
        pg_table,
        num_partitions,
        order_column,
        where_clause,
    )
    .await?;
    ctx.register_table(table_name, Arc::new(table))?;
    info!(
        "Registered PostgresPartitionedTable '{}' -> '{}'",
        pg_table, table_name
    );
    Ok(())
}

/// Infer an Arrow schema from Postgres `information_schema.columns`.
async fn infer_schema(pool: &sqlx::PgPool, table_name: &str) -> anyhow::Result<SchemaRef> {
    let query = r#"
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position
    "#;

    let rows: Vec<(String, String)> = sqlx::query_as(query)
        .bind(table_name)
        .fetch_all(pool)
        .await?;

    if rows.is_empty() {
        anyhow::bail!(
            "No columns found for table '{}' in information_schema.columns",
            table_name
        );
    }

    let fields: Vec<Field> = rows
        .iter()
        .map(|(col_name, data_type)| {
            let arrow_type = info_schema_type_to_arrow(data_type);
            Field::new(col_name, arrow_type, true)
        })
        .collect();

    Ok(Arc::new(Schema::new(fields)))
}

/// Convert PostgreSQL rows to Arrow RecordBatch.
fn rows_to_record_batch(
    rows: &[PgRow],
    schema: Arc<Schema>,
) -> anyhow::Result<arrow::array::RecordBatch> {
    use arrow::array::*;

    let mut columns: Vec<ArrayRef> = Vec::new();

    for (i, field) in schema.fields().iter().enumerate() {
        let array: ArrayRef = match field.data_type() {
            DataType::Int16 => {
                let values: Vec<Option<i16>> = rows
                    .iter()
                    .map(|row| row.try_get::<i16, _>(i).ok())
                    .collect();
                Arc::new(Int16Array::from(values))
            }
            DataType::Int32 => {
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|row| row.try_get::<i32, _>(i).ok())
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| row.try_get::<i64, _>(i).ok())
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float32 => {
                let values: Vec<Option<f32>> = rows
                    .iter()
                    .map(|row| row.try_get::<f32, _>(i).ok())
                    .collect();
                Arc::new(Float32Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|row| row.try_get::<f64, _>(i).ok())
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            DataType::Boolean => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|row| row.try_get::<bool, _>(i).ok())
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            _ => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| row.try_get::<String, _>(i).ok())
                    .collect();
                Arc::new(StringArray::from(values))
            }
        };
        columns.push(array);
    }

    Ok(arrow::array::RecordBatch::try_new(schema, columns)?)
}

/// Map `information_schema.columns.data_type` values to Arrow DataType.
///
/// The `information_schema` uses SQL standard type names (e.g. "integer",
/// "character varying") rather than the shorter Postgres type names.
fn info_schema_type_to_arrow(data_type: &str) -> DataType {
    match data_type.to_lowercase().as_str() {
        "smallint" => DataType::Int16,
        "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "double precision" => DataType::Float64,
        "numeric" | "decimal" => DataType::Float64,
        "boolean" => DataType::Boolean,
        "text" | "character varying" | "character" | "name" => DataType::Utf8,
        "bytea" => DataType::Binary,
        "date" | "timestamp without time zone" | "timestamp with time zone" => DataType::Utf8,
        "uuid" => DataType::Utf8,
        "json" | "jsonb" => DataType::Utf8,
        "array" | "user-defined" => DataType::Utf8,
        other => {
            debug!("Unknown PostgreSQL type '{}', defaulting to Utf8", other);
            DataType::Utf8
        }
    }
}

// ===========================================================================
// PostgresScanExec — the lazy ExecutionPlan for a single partition
// ===========================================================================

/// A lazy DataFusion `ExecutionPlan` that fetches a single LIMIT/OFFSET
/// partition from PostgreSQL when `execute()` is called.
///
/// This node is a leaf node (no children) with exactly 1 output partition.
/// The actual Postgres query is deferred until the returned stream is polled.
#[derive(Debug)]
pub struct PostgresScanExec {
    pub conn_string: String,
    pub pg_table: String,
    pub schema: SchemaRef,
    pub offset: u64,
    pub limit: u64,
    pub order_column: Option<String>,
    pub where_clause: Option<String>,
    properties: PlanProperties,
}

impl PostgresScanExec {
    /// Create a new `PostgresScanExec`.
    pub fn new(
        conn_string: String,
        pg_table: String,
        schema: SchemaRef,
        offset: u64,
        limit: u64,
        order_column: Option<String>,
        where_clause: Option<String>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            conn_string,
            pg_table,
            schema,
            offset,
            limit,
            order_column,
            where_clause,
            properties,
        }
    }

    // -- Serialization -------------------------------------------------------

    /// Serialize this execution plan to bytes (JSON).
    pub fn serialize(&self) -> Vec<u8> {
        let dto = PostgresScanExecDto {
            conn_string: self.conn_string.clone(),
            pg_table: self.pg_table.clone(),
            offset: self.offset,
            limit: self.limit,
            order_column: self.order_column.clone(),
            where_clause: self.where_clause.clone(),
            schema_fields: self
                .schema
                .fields()
                .iter()
                .map(|f| PgFieldDto {
                    name: f.name().clone(),
                    data_type: format!("{:?}", f.data_type()),
                    nullable: f.is_nullable(),
                })
                .collect(),
        };
        serde_json::to_vec(&dto).expect("PostgresScanExecDto serialization cannot fail")
    }

    /// Deserialize from bytes (JSON) back into a `PostgresScanExec`.
    pub fn deserialize(bytes: &[u8]) -> anyhow::Result<Self> {
        let dto: PostgresScanExecDto = serde_json::from_slice(bytes)?;
        let fields: Vec<Field> = dto
            .schema_fields
            .iter()
            .map(|f| Field::new(&f.name, parse_data_type(&f.data_type), f.nullable))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        Ok(Self::new(
            dto.conn_string,
            dto.pg_table,
            schema,
            dto.offset,
            dto.limit,
            dto.order_column,
            dto.where_clause,
        ))
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "PostgresScanExec is a leaf node and cannot have children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "PostgresScanExec only supports partition 0, got {}",
                partition
            )));
        }

        let conn_string = self.conn_string.clone();
        let pg_table = self.pg_table.clone();
        let schema = Arc::clone(&self.schema);
        let offset = self.offset;
        let limit = self.limit;
        let order_column = self.order_column.clone();
        let where_clause = self.where_clause.clone();

        let stream = futures::stream::once(async move {
            let result = fetch_partition(
                &conn_string,
                &pg_table,
                &schema,
                offset,
                limit,
                order_column.as_deref(),
                where_clause.as_deref(),
            )
            .await;
            result.map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

impl DisplayAs for PostgresScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PostgresScanExec: table={}, offset={}, limit={}",
            self.pg_table, self.offset, self.limit
        )?;
        if let Some(wc) = &self.where_clause {
            write!(f, ", where={}", wc)?;
        }
        Ok(())
    }
}

/// Execute a LIMIT/OFFSET query against Postgres and return a `RecordBatch`.
async fn fetch_partition(
    conn_string: &str,
    pg_table: &str,
    schema: &SchemaRef,
    offset: u64,
    limit: u64,
    order_column: Option<&str>,
    where_clause: Option<&str>,
) -> anyhow::Result<arrow::array::RecordBatch> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(conn_string)
        .await?;

    let columns_sql: String = schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    let wc = where_clause.unwrap_or("");
    let query = match order_column {
        Some(col) => format!(
            "SELECT {} FROM \"{}\"{} ORDER BY \"{}\" LIMIT {} OFFSET {}",
            columns_sql, pg_table, wc, col, limit, offset
        ),
        None => format!(
            "SELECT {} FROM \"{}\"{} LIMIT {} OFFSET {}",
            columns_sql, pg_table, wc, limit, offset
        ),
    };

    debug!("PostgresScanExec query: {}", query);

    let rows: Vec<PgRow> = sqlx::query(&query).fetch_all(&pool).await?;
    pool.close().await;

    if rows.is_empty() {
        Ok(arrow::array::RecordBatch::new_empty(Arc::clone(schema)))
    } else {
        rows_to_record_batch(&rows, Arc::clone(schema))
    }
}

// ---------------------------------------------------------------------------
// Serialization DTOs
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct PostgresScanExecDto {
    conn_string: String,
    pg_table: String,
    offset: u64,
    limit: u64,
    order_column: Option<String>,
    #[serde(default)]
    where_clause: Option<String>,
    schema_fields: Vec<PgFieldDto>,
}

#[derive(Serialize, Deserialize)]
struct PgFieldDto {
    name: String,
    data_type: String,
    nullable: bool,
}

/// Parse a `DataType` from its `Debug` representation string.
fn parse_data_type(s: &str) -> DataType {
    match s {
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Boolean" => DataType::Boolean,
        "Utf8" => DataType::Utf8,
        "Binary" => DataType::Binary,
        _ => DataType::Utf8,
    }
}

// ===========================================================================
// Wire codec entries
// ===========================================================================

/// Wire tag for [`PostgresScanExec`].
pub const WIRE_TAG_POSTGRES_EXEC: u8 = 0x01;

/// Wire tag for [`PostgresPartitionedTable`].
pub const WIRE_TAG_POSTGRES_TABLE: u8 = 0x01;

/// Codec entry for serializing/deserializing [`PostgresScanExec`].
pub fn postgres_exec_codec_entry() -> crate::wire::ExecCodecEntry {
    crate::wire::ExecCodecEntry {
        tag: WIRE_TAG_POSTGRES_EXEC,
        type_name: "PostgresScanExec",
        try_encode: |any| {
            any.downcast_ref::<PostgresScanExec>()
                .map(|pg| pg.serialize())
        },
        try_decode: |buf| {
            PostgresScanExec::deserialize(buf)
                .map(|e| Arc::new(e) as Arc<dyn datafusion::physical_plan::ExecutionPlan>)
                .map_err(|e| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "failed to deserialize PostgresScanExec: {e}"
                    ))
                })
        },
    }
}

/// Codec entry for serializing/deserializing [`PostgresPartitionedTable`].
pub fn postgres_table_codec_entry() -> crate::wire::TableCodecEntry {
    crate::wire::TableCodecEntry {
        tag: WIRE_TAG_POSTGRES_TABLE,
        type_name: "PostgresPartitionedTable",
        try_encode: |any| {
            any.downcast_ref::<PostgresPartitionedTable>()
                .map(|pg| pg.wire_serialize())
        },
        try_decode: |buf, schema| {
            PostgresPartitionedTable::wire_deserialize(buf, schema)
                .map(|t| Arc::new(t) as Arc<dyn datafusion::catalog::TableProvider>)
        },
    }
}

// ===========================================================================
// Scoped load (ephemeral pool, filtered SELECT, text-cast rows)
// ===========================================================================

/// Column metadata returned by `load_db_scoped`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Load filtered rows from a Postgres table using a dedicated connection.
///
/// Creates an ephemeral pool from `conn_string`, queries `information_schema`
/// for column metadata, builds a filtered SELECT, and returns structured data.
/// Returns `(columns, rows_as_strings, row_count)`.
pub async fn load_db_scoped(
    conn_string: &str,
    table_name: &str,
    conditions: &[crate::filter::FilterCondition],
    limit: usize,
) -> anyhow::Result<(Vec<ColumnMeta>, Vec<Vec<String>>, usize)> {
    // Validate table name to prevent SQL injection (it is interpolated into format!)
    if !table_name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_')
    {
        anyhow::bail!("Invalid table name: '{}'", table_name);
    }

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(conn_string)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to source DB: {}", e))?;

    // Query column metadata
    let meta_rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT column_name, data_type, is_nullable \
         FROM information_schema.columns \
         WHERE table_name = $1 AND table_schema = 'public' \
         ORDER BY ordinal_position",
    )
    .bind(table_name)
    .fetch_all(&pool)
    .await?;

    if meta_rows.is_empty() {
        pool.close().await;
        anyhow::bail!("Table '{}' not found or has no columns", table_name);
    }

    let columns: Vec<ColumnMeta> = meta_rows
        .iter()
        .map(|(name, dt, nullable)| ColumnMeta {
            name: name.clone(),
            data_type: dt.clone(),
            nullable: nullable == "YES",
        })
        .collect();

    // Build SELECT with all columns cast to ::text
    let select_cols: String = columns
        .iter()
        .map(|c| format!("\"{}\"::text", c.name))
        .collect::<Vec<_>>()
        .join(", ");

    let where_clause = crate::filter::build_where_clause(conditions);

    let sql = format!(
        "SELECT {} FROM \"{}\" {} LIMIT {}",
        select_cols, table_name, where_clause, limit
    );

    debug!("load_db_scoped query: {}", sql);

    let data_rows: Vec<PgRow> = sqlx::query(&sql).fetch_all(&pool).await?;
    pool.close().await;

    let rows: Vec<Vec<String>> = data_rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    row.try_get::<Option<String>, _>(i)
                        .unwrap_or(None)
                        .unwrap_or_default()
                })
                .collect()
        })
        .collect();

    let count = rows.len();
    Ok((columns, rows, count))
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- partition range tests -----------------------------------------------

    #[test]
    fn test_partition_ranges_even_division() {
        let ranges = compute_partition_ranges(100, 4);
        assert_eq!(ranges, vec![(0, 25), (25, 25), (50, 25), (75, 25)]);
    }

    #[test]
    fn test_partition_ranges_uneven_division() {
        let ranges = compute_partition_ranges(10, 3);
        assert_eq!(ranges, vec![(0, 3), (3, 3), (6, 4)]);
    }

    #[test]
    fn test_partition_ranges_single_partition() {
        let ranges = compute_partition_ranges(50, 1);
        assert_eq!(ranges, vec![(0, 50)]);
    }

    #[test]
    fn test_partition_ranges_more_partitions_than_rows() {
        let ranges = compute_partition_ranges(2, 10);
        assert_eq!(ranges, vec![(0, 1), (1, 1)]);
    }

    #[test]
    fn test_partition_ranges_zero_rows() {
        let ranges = compute_partition_ranges(0, 4);
        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_partition_ranges_zero_partitions() {
        let ranges = compute_partition_ranges(100, 0);
        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_partition_ranges_one_row() {
        let ranges = compute_partition_ranges(1, 4);
        assert_eq!(ranges, vec![(0, 1)]);
    }

    #[test]
    fn test_partition_ranges_large_table() {
        let ranges = compute_partition_ranges(1_000_000, 8);
        assert_eq!(ranges.len(), 8);
        let total: u64 = ranges.iter().map(|(_, limit)| limit).sum();
        assert_eq!(total, 1_000_000);
        let mut expected_offset = 0u64;
        for (offset, limit) in &ranges {
            assert_eq!(*offset, expected_offset);
            expected_offset += limit;
        }
    }

    #[test]
    fn test_partition_ranges_exact_match() {
        let ranges = compute_partition_ranges(5, 5);
        assert_eq!(ranges, vec![(0, 1), (1, 1), (2, 1), (3, 1), (4, 1)]);
    }

    #[test]
    fn test_info_schema_type_to_arrow() {
        assert_eq!(info_schema_type_to_arrow("integer"), DataType::Int32);
        assert_eq!(info_schema_type_to_arrow("bigint"), DataType::Int64);
        assert_eq!(info_schema_type_to_arrow("smallint"), DataType::Int16);
        assert_eq!(info_schema_type_to_arrow("real"), DataType::Float32);
        assert_eq!(
            info_schema_type_to_arrow("double precision"),
            DataType::Float64
        );
        assert_eq!(info_schema_type_to_arrow("numeric"), DataType::Float64);
        assert_eq!(info_schema_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(info_schema_type_to_arrow("text"), DataType::Utf8);
        assert_eq!(
            info_schema_type_to_arrow("character varying"),
            DataType::Utf8
        );
        assert_eq!(info_schema_type_to_arrow("uuid"), DataType::Utf8);
        assert_eq!(
            info_schema_type_to_arrow("timestamp without time zone"),
            DataType::Utf8
        );
        assert_eq!(
            info_schema_type_to_arrow("timestamp with time zone"),
            DataType::Utf8
        );
        assert_eq!(info_schema_type_to_arrow("jsonb"), DataType::Utf8);
    }

    // -- PostgresScanExec tests ----------------------------------------------

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]))
    }

    #[test]
    fn test_postgres_scan_exec_properties() {
        let schema = sample_schema();
        let exec = PostgresScanExec::new(
            "postgres://localhost/test".to_string(),
            "my_table".to_string(),
            Arc::clone(&schema),
            100,
            50,
            Some("id".to_string()),
            None,
        );
        assert_eq!(exec.schema(), schema);
        assert_eq!(exec.properties().partitioning.partition_count(), 1);
        assert!(exec.children().is_empty());
        let display_str = format!(
            "{}",
            datafusion::physical_plan::displayable(&exec).one_line()
        );
        assert!(
            display_str.contains("PostgresScanExec: table=my_table, offset=100, limit=50"),
            "unexpected display: {}",
            display_str
        );
    }

    #[test]
    fn test_serialization_roundtrip() {
        let schema = sample_schema();
        let exec = PostgresScanExec::new(
            "postgres://user:pass@host:5432/db".to_string(),
            "invoices".to_string(),
            Arc::clone(&schema),
            500,
            250,
            Some("invoice_id".to_string()),
            None,
        );
        let bytes = exec.serialize();
        let restored =
            PostgresScanExec::deserialize(&bytes).expect("deserialization should succeed");
        assert_eq!(restored.conn_string, exec.conn_string);
        assert_eq!(restored.pg_table, exec.pg_table);
        assert_eq!(restored.offset, exec.offset);
        assert_eq!(restored.limit, exec.limit);
        assert_eq!(restored.order_column, exec.order_column);
        assert_eq!(restored.where_clause, exec.where_clause);
        assert_eq!(restored.schema.fields().len(), exec.schema.fields().len());
        for (orig, rest) in exec
            .schema
            .fields()
            .iter()
            .zip(restored.schema.fields().iter())
        {
            assert_eq!(orig.name(), rest.name());
            assert_eq!(orig.data_type(), rest.data_type());
            assert_eq!(orig.is_nullable(), rest.is_nullable());
        }
    }

    #[test]
    fn test_serialization_roundtrip_no_order_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Binary, true),
        ]));
        let exec = PostgresScanExec::new(
            "postgres://localhost/test".to_string(),
            "points".to_string(),
            schema,
            0,
            1000,
            None,
            None,
        );
        let bytes = exec.serialize();
        let restored =
            PostgresScanExec::deserialize(&bytes).expect("deserialization should succeed");
        assert_eq!(restored.order_column, None);
        assert_eq!(restored.pg_table, "points");
        assert_eq!(restored.offset, 0);
        assert_eq!(restored.limit, 1000);
    }

    #[test]
    fn test_serialization_roundtrip_with_where_clause() {
        let schema = sample_schema();
        let exec = PostgresScanExec::new(
            "postgres://user:pass@host:5432/db".to_string(),
            "invoices".to_string(),
            Arc::clone(&schema),
            0,
            500,
            Some("id".to_string()),
            Some(" WHERE \"status\" = 'active' AND \"amount\" >= 50".to_string()),
        );
        let bytes = exec.serialize();
        let restored =
            PostgresScanExec::deserialize(&bytes).expect("deserialization should succeed");
        assert_eq!(
            restored.where_clause,
            Some(" WHERE \"status\" = 'active' AND \"amount\" >= 50".to_string())
        );
        assert_eq!(restored.conn_string, exec.conn_string);
        assert_eq!(restored.pg_table, exec.pg_table);
        assert_eq!(restored.offset, exec.offset);
        assert_eq!(restored.limit, exec.limit);
        assert_eq!(restored.order_column, exec.order_column);
    }

    #[test]
    fn test_parse_data_type_known_types() {
        assert_eq!(parse_data_type("Int16"), DataType::Int16);
        assert_eq!(parse_data_type("Int32"), DataType::Int32);
        assert_eq!(parse_data_type("Int64"), DataType::Int64);
        assert_eq!(parse_data_type("Float32"), DataType::Float32);
        assert_eq!(parse_data_type("Float64"), DataType::Float64);
        assert_eq!(parse_data_type("Boolean"), DataType::Boolean);
        assert_eq!(parse_data_type("Utf8"), DataType::Utf8);
        assert_eq!(parse_data_type("Binary"), DataType::Binary);
    }

    #[test]
    fn test_parse_data_type_unknown_defaults_to_utf8() {
        assert_eq!(parse_data_type("LargeUtf8"), DataType::Utf8);
        assert_eq!(
            parse_data_type("Timestamp(Nanosecond, None)"),
            DataType::Utf8
        );
        assert_eq!(parse_data_type("SomeWeirdType"), DataType::Utf8);
    }
}
