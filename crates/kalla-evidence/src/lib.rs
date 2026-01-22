//! Kalla Evidence Store - Parquet-based evidence logging
//!
//! This crate provides storage and retrieval for reconciliation results.

pub mod store;
pub mod schema;

pub use store::EvidenceStore;
pub use schema::{MatchedRecord, UnmatchedRecord, RunMetadata, RunStatus};
