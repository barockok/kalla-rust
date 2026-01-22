//! Kalla Core - DataFusion engine and financial UDFs
//!
//! This crate provides the core reconciliation engine built on Apache Arrow DataFusion.

pub mod engine;
pub mod udf;

pub use engine::ReconciliationEngine;
