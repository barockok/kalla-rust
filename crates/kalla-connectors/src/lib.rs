//! Kalla Connectors - Data source adapters
//!
//! This crate provides connectors for various data sources:
//! - PostgreSQL
//! - S3 CSV (byte-range partitioned)
//! - Local CSV / Parquet files

pub mod csv_connector;
pub mod error;
pub mod factory;
pub mod filter;
pub mod postgres_connector;
pub mod s3;
pub mod wire;

pub use csv_connector::{CsvByteRangeTable, CsvRangeScanExec};
pub use error::ConnectorError;
pub use factory::register_source;
pub use filter::{build_where_clause, FilterCondition, FilterOp, FilterValue};
pub use postgres_connector::{PostgresPartitionedTable, PostgresScanExec};
pub use s3::{parse_s3_uri, S3Config};
pub use wire::{exec_codecs, table_codecs, ExecCodecEntry, TableCodecEntry};
