//! Distributed execution node for PostgreSQL table scans.
//!
//! `PostgresScanExec` is a lazy DataFusion `ExecutionPlan` that fetches a single
//! LIMIT/OFFSET partition from PostgreSQL when `execute()` is called on a remote
//! Ballista executor. Unlike the eager `PostgresPartitionedTable` which loads all
//! partitions into `MemoryExec` at scan time, this plan defers the Postgres query
//! until the stream is actually polled.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use tracing::debug;

use kalla_connectors::postgres::rows_to_record_batch;

// ---------------------------------------------------------------------------
// PostgresScanExec
// ---------------------------------------------------------------------------

/// A lazy DataFusion `ExecutionPlan` that fetches a single LIMIT/OFFSET
/// partition from PostgreSQL when `execute()` is called.
///
/// This node is intended to run on a remote Ballista executor. It is a leaf
/// node (no children) with exactly 1 output partition. The actual Postgres
/// query is deferred until the returned stream is polled.
#[derive(Debug)]
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
    /// Create a new `PostgresScanExec`.
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
            schema_fields: self
                .schema
                .fields()
                .iter()
                .map(|f| FieldDto {
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
        ))
    }
}

// ---------------------------------------------------------------------------
// ExecutionPlan
// ---------------------------------------------------------------------------

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
        // Leaf node — no children.
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

        // Build the stream lazily — the async Postgres work happens inside.
        let stream = futures::stream::once(async move {
            let result = fetch_partition(
                &conn_string,
                &pg_table,
                &schema,
                offset,
                limit,
                order_column.as_deref(),
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

// ---------------------------------------------------------------------------
// DisplayAs
// ---------------------------------------------------------------------------

impl DisplayAs for PostgresScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PostgresScanExec: table={}, offset={}, limit={}",
            self.pg_table, self.offset, self.limit
        )
    }
}

// ---------------------------------------------------------------------------
// Internal async fetch
// ---------------------------------------------------------------------------

/// Actually execute the LIMIT/OFFSET query against Postgres and return a
/// `RecordBatch`.
async fn fetch_partition(
    conn_string: &str,
    pg_table: &str,
    schema: &SchemaRef,
    offset: u64,
    limit: u64,
    order_column: Option<&str>,
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

    let query = match order_column {
        Some(col) => format!(
            "SELECT {} FROM \"{}\" ORDER BY \"{}\" LIMIT {} OFFSET {}",
            columns_sql, pg_table, col, limit, offset
        ),
        None => format!(
            "SELECT {} FROM \"{}\" LIMIT {} OFFSET {}",
            columns_sql, pg_table, limit, offset
        ),
    };

    debug!("PostgresScanExec query: {}", query);

    let rows: Vec<sqlx::postgres::PgRow> = sqlx::query(&query).fetch_all(&pool).await?;
    pool.close().await;

    if rows.is_empty() {
        // Return an empty RecordBatch with the correct schema.
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
    schema_fields: Vec<FieldDto>,
}

#[derive(Serialize, Deserialize)]
struct FieldDto {
    name: String,
    data_type: String,
    nullable: bool,
}

/// Parse a `DataType` from its `Debug` representation string.
///
/// Handles the common types produced by `format!("{:?}", dt)`.  Unknown
/// strings default to `DataType::Utf8`.
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
        );

        // Schema should match.
        assert_eq!(exec.schema(), schema);

        // Must have exactly 1 output partition.
        assert_eq!(exec.properties().partitioning.partition_count(), 1);

        // Leaf node — no children.
        assert!(exec.children().is_empty());

        // Display
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
        );

        let bytes = exec.serialize();
        let restored =
            PostgresScanExec::deserialize(&bytes).expect("deserialization should succeed");

        assert_eq!(restored.conn_string, exec.conn_string);
        assert_eq!(restored.pg_table, exec.pg_table);
        assert_eq!(restored.offset, exec.offset);
        assert_eq!(restored.limit, exec.limit);
        assert_eq!(restored.order_column, exec.order_column);
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
