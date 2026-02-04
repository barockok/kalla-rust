//! Kalla Connectors - Data source adapters
//!
//! This crate provides connectors for various data sources:
//! - PostgreSQL
//! - S3 (future)
//! - BigQuery (future)

pub mod filter;
pub mod postgres;

pub use filter::{build_where_clause, FilterCondition, FilterOp, FilterValue};
pub use postgres::PostgresConnector;
