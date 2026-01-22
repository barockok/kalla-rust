//! Kalla Connectors - Data source adapters
//!
//! This crate provides connectors for various data sources:
//! - PostgreSQL
//! - S3 (future)
//! - BigQuery (future)

pub mod postgres;

pub use postgres::PostgresConnector;
