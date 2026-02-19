//! Wire codec registry for serializing/deserializing custom DataFusion nodes.
//!
//! Provides [`ExecCodecEntry`] and [`TableCodecEntry`] — self-contained
//! encode/decode descriptors for each connector's execution plan and table
//! provider types.  The Ballista codec consumes these registries instead of
//! hard-coding match arms for each concrete type.
//!
//! ## Adding a new connector
//!
//! 1. Implement `serialize()` / `deserialize()` on your `ExecutionPlan`.
//! 2. Implement `wire_serialize()` / `wire_deserialize()` on your `TableProvider`.
//! 3. Create entry constructor functions (see `postgres::postgres_exec_entry`).
//! 4. Add one line each to [`exec_codecs()`] and [`table_codecs()`].

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::ExecutionPlan;

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

/// Encoder function: tries to downcast and serialize, returns `None` if wrong type.
pub type EncodeFn = fn(&dyn std::any::Any) -> Option<Vec<u8>>;

/// Decoder function for execution plans (tag byte already stripped).
pub type ExecDecodeFn = fn(&[u8]) -> DFResult<Arc<dyn ExecutionPlan>>;

/// Decoder function for table providers (tag byte already stripped).
pub type TableDecodeFn = fn(&[u8], SchemaRef) -> DFResult<Arc<dyn TableProvider>>;

// ---------------------------------------------------------------------------
// Entry types
// ---------------------------------------------------------------------------

/// A self-contained codec entry for a custom [`ExecutionPlan`] node.
pub struct ExecCodecEntry {
    /// Wire tag byte (unique per exec plan type).
    pub tag: u8,
    /// Human-readable name for error messages.
    pub type_name: &'static str,
    /// Try to encode a plan node.  Returns `Some(payload)` if the node
    /// matches this entry's concrete type, `None` otherwise.
    pub try_encode: EncodeFn,
    /// Decode a payload (after the tag byte has been stripped) into an
    /// execution plan.
    pub try_decode: ExecDecodeFn,
}

/// A self-contained codec entry for a custom [`TableProvider`] node.
pub struct TableCodecEntry {
    /// Wire tag byte (unique per table provider type).
    pub tag: u8,
    /// Human-readable name for error messages.
    pub type_name: &'static str,
    /// Try to encode a table provider.  Returns `Some(payload)` if the
    /// provider matches this entry's concrete type, `None` otherwise.
    pub try_encode: EncodeFn,
    /// Decode a payload (after the tag byte has been stripped) into a
    /// table provider.  The `SchemaRef` is provided by the framework.
    pub try_decode: TableDecodeFn,
}

// ---------------------------------------------------------------------------
// Registries
// ---------------------------------------------------------------------------

/// Returns codec entries for all known [`ExecutionPlan`] types.
pub fn exec_codecs() -> Vec<ExecCodecEntry> {
    vec![
        crate::postgres::postgres_exec_codec_entry(),
        crate::csv_partitioned::csv_exec_codec_entry(),
    ]
}

/// Returns codec entries for all known [`TableProvider`] types.
pub fn table_codecs() -> Vec<TableCodecEntry> {
    vec![
        crate::postgres::postgres_table_codec_entry(),
        crate::csv_partitioned::csv_table_codec_entry(),
    ]
}
