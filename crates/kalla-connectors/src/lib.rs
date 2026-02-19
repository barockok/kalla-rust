//! Kalla Connectors - Data source adapters
//!
//! This crate provides connectors for various data sources:
//! - PostgreSQL
//! - S3 (Parquet files via object_store)
//! - BigQuery (stub)

pub mod bigquery;
pub mod csv_partitioned;
pub mod error;
pub mod factory;
pub mod filter;
pub mod postgres;
pub mod postgres_partitioned;
pub mod s3;

pub use bigquery::BigQueryConnector;
pub use csv_partitioned::CsvByteRangeTable;
pub use error::ConnectorError;
pub use factory::{ConnectorFactory, ConnectorRegistry};
pub use filter::{build_where_clause, FilterCondition, FilterOp, FilterValue};
pub use postgres::PostgresConnector;
pub use postgres_partitioned::PostgresPartitionedTable;
pub use s3::{S3Config, S3Connector};

use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;

/// Trait for data source connectors that can register tables and provide streams.
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// Register a full table with the DataFusion context.
    async fn register_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        source_table: &str,
        where_clause: Option<&str>,
    ) -> anyhow::Result<()>;

    /// Register a scoped (filtered) subset of a table with DataFusion.
    async fn register_scoped(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        source_table: &str,
        conditions: &[FilterCondition],
        limit: Option<usize>,
    ) -> anyhow::Result<usize>;

    /// Return a streaming RecordBatch reader for the given table.
    /// This avoids loading the full dataset into memory.
    async fn stream_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
    ) -> anyhow::Result<SendableRecordBatchStream>;
}
