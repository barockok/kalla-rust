//! PostgreSQL connector for DataFusion

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Column, Row};
use std::sync::Arc;
use tracing::{debug, info};

use crate::filter::{build_where_clause, FilterCondition};
use crate::SourceConnector;

/// PostgreSQL connector that can register tables with DataFusion
pub struct PostgresConnector {
    pool: PgPool,
}

impl PostgresConnector {
    /// Create a new PostgreSQL connector
    pub async fn new(connection_string: &str) -> Result<Self> {
        info!("Connecting to PostgreSQL...");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await?;
        info!("Connected to PostgreSQL successfully");
        Ok(Self { pool })
    }

    /// Register a PostgreSQL table with the DataFusion context
    ///
    /// This loads the table data into memory as a MemTable.
    /// For large tables, consider using filters or pagination.
    pub async fn register_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        pg_table: &str,
        where_clause: Option<&str>,
    ) -> Result<()> {
        let query = match where_clause {
            Some(clause) => format!("SELECT * FROM {} WHERE {}", pg_table, clause),
            None => format!("SELECT * FROM {}", pg_table),
        };

        debug!("Executing query: {}", query);

        // Get column info from the query
        let rows: Vec<PgRow> = sqlx::query(&query).fetch_all(&self.pool).await?;

        if rows.is_empty() {
            // Register empty table with inferred schema
            info!("No rows returned, registering empty table");
            return Ok(());
        }

        // Build schema from first row
        let first_row = &rows[0];
        let columns = first_row.columns();

        let fields: Vec<Field> = columns
            .iter()
            .map(|col| {
                let data_type = pg_type_to_arrow(col.type_info().to_string().as_str());
                Field::new(col.name(), data_type, true)
            })
            .collect();

        let schema = Arc::new(Schema::new(fields));

        // Convert rows to Arrow RecordBatch
        let batch = rows_to_record_batch(&rows, schema.clone())?;
        let batches = vec![batch];

        // Create MemTable and register
        let mem_table = MemTable::try_new(schema, vec![batches])?;
        ctx.register_table(table_name, Arc::new(mem_table))?;

        info!(
            "Registered PostgreSQL table '{}' as '{}' with {} rows",
            pg_table,
            table_name,
            rows.len()
        );

        Ok(())
    }

    /// Register a scoped (filtered) subset of a PostgreSQL table with DataFusion.
    pub async fn register_scoped(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        pg_table: &str,
        conditions: &[FilterCondition],
        limit: Option<usize>,
    ) -> Result<usize> {
        // Deregister existing table if present to reload with new scope
        let _ = ctx.deregister_table(table_name);

        let query = build_scoped_query(pg_table, conditions, limit);
        debug!("Scoped query: {}", query);

        let rows: Vec<PgRow> = sqlx::query(&query).fetch_all(&self.pool).await?;
        let row_count = rows.len();

        if rows.is_empty() {
            info!("Scoped query returned 0 rows for '{}'", table_name);
            return Ok(0);
        }

        // Reuse existing row-to-RecordBatch logic
        let first_row = &rows[0];
        let columns = first_row.columns();
        let fields: Vec<Field> = columns
            .iter()
            .map(|col| {
                let data_type = pg_type_to_arrow(col.type_info().to_string().as_str());
                Field::new(col.name(), data_type, true)
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let batch = rows_to_record_batch(&rows, schema.clone())?;
        let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table(table_name, Arc::new(mem_table))?;

        info!(
            "Registered scoped table '{}' with {} rows",
            table_name, row_count
        );
        Ok(row_count)
    }

    /// Get the connection pool for direct queries
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl SourceConnector for PostgresConnector {
    async fn register_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        source_table: &str,
        where_clause: Option<&str>,
    ) -> Result<()> {
        // Delegate to the existing inherent method
        PostgresConnector::register_table(self, ctx, table_name, source_table, where_clause).await
    }

    async fn register_scoped(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        source_table: &str,
        conditions: &[FilterCondition],
        limit: Option<usize>,
    ) -> Result<usize> {
        PostgresConnector::register_scoped(self, ctx, table_name, source_table, conditions, limit)
            .await
    }

    async fn stream_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
    ) -> Result<SendableRecordBatchStream> {
        let df = ctx
            .sql(&format!("SELECT * FROM \"{}\"", table_name))
            .await?;
        Ok(df.execute_stream().await?)
    }
}

/// Build a scoped SELECT query from table, conditions, and optional limit.
pub fn build_scoped_query(
    table: &str,
    conditions: &[FilterCondition],
    limit: Option<usize>,
) -> String {
    let mut query = format!("SELECT * FROM \"{}\"", table.replace('"', "\"\""));
    query.push_str(&build_where_clause(conditions));
    if let Some(lim) = limit {
        query.push_str(&format!(" LIMIT {}", lim));
    }
    query
}

/// Convert PostgreSQL type name to Arrow DataType
fn pg_type_to_arrow(pg_type: &str) -> DataType {
    match pg_type.to_uppercase().as_str() {
        "INT2" | "SMALLINT" => DataType::Int16,
        "INT4" | "INTEGER" | "INT" => DataType::Int32,
        "INT8" | "BIGINT" => DataType::Int64,
        "FLOAT4" | "REAL" => DataType::Float32,
        "FLOAT8" | "DOUBLE PRECISION" | "NUMERIC" | "DECIMAL" => DataType::Float64,
        "BOOL" | "BOOLEAN" => DataType::Boolean,
        "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" | "NAME" => DataType::Utf8,
        "BYTEA" => DataType::Binary,
        // Store dates and timestamps as strings for simplicity
        "DATE" | "TIMESTAMP" | "TIMESTAMPTZ" => DataType::Utf8,
        "UUID" => DataType::Utf8, // Store as string
        _ => {
            debug!("Unknown PostgreSQL type '{}', defaulting to Utf8", pg_type);
            DataType::Utf8
        }
    }
}

/// Convert PostgreSQL rows to Arrow RecordBatch
fn rows_to_record_batch(rows: &[PgRow], schema: Arc<Schema>) -> Result<RecordBatch> {
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
            DataType::Utf8 | _ => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| row.try_get::<String, _>(i).ok())
                    .collect();
                Arc::new(StringArray::from(values))
            }
        };
        columns.push(array);
    }

    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::{FilterCondition, FilterOp, FilterValue};

    fn fc(column: &str, op: FilterOp, value: FilterValue) -> FilterCondition {
        FilterCondition {
            column: column.to_string(),
            op,
            value,
        }
    }

    #[test]
    fn test_build_scoped_query_with_conditions_and_limit() {
        let conditions = vec![
            fc(
                "invoice_date",
                FilterOp::Between,
                FilterValue::Range(["2024-01-01".to_string(), "2024-01-31".to_string()]),
            ),
            fc("amount", FilterOp::Gte, FilterValue::Number(100.0)),
        ];
        let query = build_scoped_query("invoices", &conditions, Some(50));
        assert_eq!(
            query,
            "SELECT * FROM \"invoices\" WHERE \"invoice_date\" BETWEEN '2024-01-01' AND '2024-01-31' AND \"amount\" >= 100 LIMIT 50"
        );
    }

    #[test]
    fn test_build_scoped_query_no_conditions() {
        let query = build_scoped_query("payments", &[], Some(200));
        assert_eq!(query, "SELECT * FROM \"payments\" LIMIT 200");
    }

    #[test]
    fn test_build_scoped_query_no_limit() {
        let conditions = vec![fc(
            "status",
            FilterOp::Eq,
            FilterValue::String("active".to_string()),
        )];
        let query = build_scoped_query("orders", &conditions, None);
        assert_eq!(
            query,
            "SELECT * FROM \"orders\" WHERE \"status\" = 'active'"
        );
    }

    #[test]
    fn test_build_scoped_query_no_conditions_no_limit() {
        let query = build_scoped_query("my_table", &[], None);
        assert_eq!(query, "SELECT * FROM \"my_table\"");
    }

    #[test]
    fn test_build_scoped_query_multiple_conditions() {
        let conditions = vec![
            fc("a", FilterOp::Eq, FilterValue::Number(1.0)),
            fc("b", FilterOp::Gt, FilterValue::Number(2.0)),
            fc(
                "c",
                FilterOp::Like,
                FilterValue::String("%test%".to_string()),
            ),
        ];
        let query = build_scoped_query("t", &conditions, Some(10));
        assert!(query.contains("\"a\" = 1"));
        assert!(query.contains("\"b\" > 2"));
        assert!(query.contains("\"c\" LIKE '%test%'"));
        assert!(query.contains("LIMIT 10"));
    }

    #[test]
    fn test_build_scoped_query_table_name_with_quotes() {
        let query = build_scoped_query("my\"table", &[], None);
        assert_eq!(query, "SELECT * FROM \"my\"\"table\"");
    }

    #[test]
    fn test_pg_type_to_arrow_int_types() {
        assert_eq!(pg_type_to_arrow("INT2"), DataType::Int16);
        assert_eq!(pg_type_to_arrow("SMALLINT"), DataType::Int16);
        assert_eq!(pg_type_to_arrow("INT4"), DataType::Int32);
        assert_eq!(pg_type_to_arrow("INTEGER"), DataType::Int32);
        assert_eq!(pg_type_to_arrow("INT"), DataType::Int32);
        assert_eq!(pg_type_to_arrow("INT8"), DataType::Int64);
        assert_eq!(pg_type_to_arrow("BIGINT"), DataType::Int64);
    }

    #[test]
    fn test_pg_type_to_arrow_float_types() {
        assert_eq!(pg_type_to_arrow("FLOAT4"), DataType::Float32);
        assert_eq!(pg_type_to_arrow("REAL"), DataType::Float32);
        assert_eq!(pg_type_to_arrow("FLOAT8"), DataType::Float64);
        assert_eq!(pg_type_to_arrow("DOUBLE PRECISION"), DataType::Float64);
        assert_eq!(pg_type_to_arrow("NUMERIC"), DataType::Float64);
        assert_eq!(pg_type_to_arrow("DECIMAL"), DataType::Float64);
    }

    #[test]
    fn test_pg_type_to_arrow_bool() {
        assert_eq!(pg_type_to_arrow("BOOL"), DataType::Boolean);
        assert_eq!(pg_type_to_arrow("BOOLEAN"), DataType::Boolean);
    }

    #[test]
    fn test_pg_type_to_arrow_text_types() {
        assert_eq!(pg_type_to_arrow("TEXT"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("VARCHAR"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("CHAR"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("BPCHAR"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("NAME"), DataType::Utf8);
    }

    #[test]
    fn test_pg_type_to_arrow_binary() {
        assert_eq!(pg_type_to_arrow("BYTEA"), DataType::Binary);
    }

    #[test]
    fn test_pg_type_to_arrow_temporal() {
        assert_eq!(pg_type_to_arrow("DATE"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("TIMESTAMP"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("TIMESTAMPTZ"), DataType::Utf8);
    }

    #[test]
    fn test_pg_type_to_arrow_uuid() {
        assert_eq!(pg_type_to_arrow("UUID"), DataType::Utf8);
    }

    #[test]
    fn test_pg_type_to_arrow_unknown_defaults_to_utf8() {
        assert_eq!(pg_type_to_arrow("JSONB"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("CIDR"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("UNKNOWN_TYPE"), DataType::Utf8);
    }

    #[test]
    fn test_pg_type_to_arrow_case_insensitive() {
        assert_eq!(pg_type_to_arrow("int4"), DataType::Int32);
        assert_eq!(pg_type_to_arrow("Bool"), DataType::Boolean);
        assert_eq!(pg_type_to_arrow("text"), DataType::Utf8);
    }
}
