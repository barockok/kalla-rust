//! PostgreSQL partitioned table provider for DataFusion
//!
//! Implements `TableProvider` to support partitioned reads from PostgreSQL
//! using `LIMIT/OFFSET` queries, enabling parallel partition-level execution.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::prelude::{Expr, SessionContext};
use sqlx::postgres::{PgPoolOptions, PgRow};
use tracing::{debug, info};

use crate::postgres::{pg_type_to_arrow, rows_to_record_batch};

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
}

impl fmt::Debug for PostgresPartitionedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresPartitionedTable")
            .field("pg_table", &self.pg_table)
            .field("total_rows", &self.total_rows)
            .field("num_partitions", &self.num_partitions)
            .field("order_column", &self.order_column)
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
    ) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(conn_string)
            .await?;

        // Infer schema from information_schema.columns
        let schema = infer_schema(&pool, pg_table).await?;

        // Count total rows
        let count_query = format!("SELECT COUNT(*) AS cnt FROM \"{}\"", pg_table);
        let row: (i64,) = sqlx::query_as(&count_query).fetch_one(&pool).await?;
        let total_rows = row.0 as u64;

        info!(
            "PostgresPartitionedTable: table='{}', rows={}, partitions={}, order_col={:?}",
            pg_table, total_rows, num_partitions, order_column
        );

        pool.close().await;

        Ok(Self {
            conn_string: conn_string.to_string(),
            pg_table: pg_table.to_string(),
            schema,
            total_rows,
            num_partitions,
            order_column,
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
    ) -> Self {
        Self {
            conn_string,
            pg_table,
            schema,
            total_rows,
            num_partitions,
            order_column,
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
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let ranges = compute_partition_ranges(self.total_rows, self.num_partitions);

        // Determine which columns to SELECT
        let all_fields: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        let selected_fields: Vec<&str> = match projection {
            Some(indices) => indices.iter().map(|&i| all_fields[i]).collect(),
            None => all_fields.clone(),
        };

        let columns_sql = selected_fields
            .iter()
            .map(|name| format!("\"{}\"", name))
            .collect::<Vec<_>>()
            .join(", ");

        // Build projected schema
        let projected_schema = match projection {
            Some(indices) => {
                let projected_fields: Vec<Field> = indices
                    .iter()
                    .map(|&i| self.schema.field(i).clone())
                    .collect();
                Arc::new(Schema::new(projected_fields))
            }
            None => Arc::clone(&self.schema),
        };

        // Connect and fetch each partition
        let pool = PgPoolOptions::new()
            .max_connections(self.num_partitions.max(2) as u32)
            .connect(&self.conn_string)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let mut partitions: Vec<Vec<arrow::array::RecordBatch>> = Vec::with_capacity(ranges.len());

        for (offset, limit) in &ranges {
            let query = match &self.order_column {
                Some(col) => format!(
                    "SELECT {} FROM \"{}\" ORDER BY \"{}\" LIMIT {} OFFSET {}",
                    columns_sql, self.pg_table, col, limit, offset
                ),
                None => format!(
                    "SELECT {} FROM \"{}\" LIMIT {} OFFSET {}",
                    columns_sql, self.pg_table, limit, offset
                ),
            };

            debug!("Partition query: {}", query);

            let rows: Vec<PgRow> = sqlx::query(&query)
                .fetch_all(&pool)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            if rows.is_empty() {
                partitions.push(vec![]);
            } else {
                let batch = rows_to_record_batch(&rows, Arc::clone(&projected_schema))
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
                partitions.push(vec![batch]);
            }
        }

        pool.close().await;

        // MemoryExec expects partitions as &[Vec<RecordBatch>]
        // Pass None for projection since we already projected the columns in SQL
        let exec = MemoryExec::try_new(&partitions, projected_schema, None)?;

        Ok(Arc::new(exec))
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
) -> anyhow::Result<()> {
    let table =
        PostgresPartitionedTable::new(conn_string, pg_table, num_partitions, order_column).await?;
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

/// Map `information_schema.columns.data_type` values to Arrow DataType.
///
/// The `information_schema` uses SQL standard type names (e.g. "integer",
/// "character varying") rather than the shorter Postgres type names.
fn info_schema_type_to_arrow(data_type: &str) -> DataType {
    // information_schema uses SQL-standard names, so we handle those
    // and also fall back to pg_type_to_arrow for Postgres-native names.
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
            // Fall back to the existing pg_type_to_arrow helper
            pg_type_to_arrow(other)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // 2 rows, 10 partitions -> capped to 2 partitions
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

        // Verify all rows are covered
        let total: u64 = ranges.iter().map(|(_, limit)| limit).sum();
        assert_eq!(total, 1_000_000);

        // Verify no gaps
        let mut expected_offset = 0u64;
        for (offset, limit) in &ranges {
            assert_eq!(*offset, expected_offset);
            expected_offset += limit;
        }
    }

    #[test]
    fn test_partition_ranges_exact_match() {
        // rows == partitions
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
}
