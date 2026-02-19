//! Kalla Connectors - Data source adapters
//!
//! This crate provides connectors for various data sources:
//! - PostgreSQL
//! - S3 (CSV byte-range, Parquet via object_store)
//! - Local CSV / Parquet files

pub mod bigquery;
pub mod csv_partitioned;
pub mod error;
pub mod factory;
pub mod filter;
pub mod postgres;
pub mod s3;

pub use csv_partitioned::{CsvByteRangeTable, CsvRangeScanExec};
pub use error::ConnectorError;
pub use factory::register_source;
pub use filter::{build_where_clause, FilterCondition, FilterOp, FilterValue};
pub use postgres::{PostgresPartitionedTable, PostgresScanExec};
pub use s3::{S3Config, S3Connector};
