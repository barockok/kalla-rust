//! Kalla Core - DataFusion engine and financial UDFs
//!
//! This crate provides the core reconciliation engine built on Apache Arrow DataFusion.

pub mod engine;
pub mod partitioned;
pub mod udf;

pub use engine::ReconciliationEngine;

// Re-export for downstream consumers
pub use datafusion::physical_plan::SendableRecordBatchStream;
