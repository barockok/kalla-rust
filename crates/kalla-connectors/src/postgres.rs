//! PostgreSQL connector for DataFusion

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Column, Row};
use std::sync::Arc;
use tracing::{debug, info};

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

    /// Get the connection pool for direct queries
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
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
