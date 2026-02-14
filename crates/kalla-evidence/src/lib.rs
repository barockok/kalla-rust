//! Kalla Evidence Store - Parquet-based evidence logging
//!
//! This crate provides storage and retrieval for reconciliation results.

pub mod schema;
pub mod store;

pub use schema::{MatchedRecord, RunMetadata, RunStatus, UnmatchedRecord};
pub use store::EvidenceStore;
