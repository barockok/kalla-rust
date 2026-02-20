//! Ballista codec for serializing/deserializing custom execution plans.
//!
//! Wraps Ballista's `BallistaPhysicalExtensionCodec` to handle both
//! Ballista-internal nodes (ShuffleWriterExec, ShuffleReaderExec, etc.)
//! and Kalla's custom nodes.
//!
//! ## Wire format for Kalla nodes
//!
//! Each serialized payload is prefixed with a single tag byte followed by
//! a JSON payload.  Tag assignments and encode/decode logic are defined by
//! each connector via [`kalla_connectors::wire`] — this codec is a generic
//! dispatcher that iterates the registry.
//!
//! Adding a new connector requires **zero changes** to this file.

use std::fmt::Debug;
use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use kalla_connectors::{ExecCodecEntry, TableCodecEntry};

// ---------------------------------------------------------------------------
// KallaPhysicalCodec
// ---------------------------------------------------------------------------

/// A [`PhysicalExtensionCodec`] that handles serialization and deserialization
/// of Kalla's custom `ExecutionPlan` nodes for Ballista cluster mode.
///
/// Delegates to [`BallistaPhysicalExtensionCodec`] for Ballista-internal nodes
/// like `ShuffleWriterExec` and `ShuffleReaderExec`.
pub struct KallaPhysicalCodec {
    inner: BallistaPhysicalExtensionCodec,
    exec_codecs: Vec<ExecCodecEntry>,
}

impl Debug for KallaPhysicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KallaPhysicalCodec")
            .field("exec_codecs_count", &self.exec_codecs.len())
            .finish()
    }
}

impl KallaPhysicalCodec {
    pub fn new() -> Self {
        Self {
            inner: BallistaPhysicalExtensionCodec::default(),
            exec_codecs: kalla_connectors::exec_codecs(),
        }
    }
}

impl Default for KallaPhysicalCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalExtensionCodec for KallaPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if buf.is_empty() {
            return Err(DataFusionError::Internal(
                "KallaPhysicalCodec: empty buffer".to_string(),
            ));
        }

        let tag = buf[0];
        let payload = &buf[1..];

        // Try Kalla custom nodes first (identified by tag byte)
        for entry in &self.exec_codecs {
            if entry.tag == tag {
                return (entry.try_decode)(payload);
            }
        }

        // Delegate to Ballista's codec for internal nodes (ShuffleWriter, etc.)
        self.inner.try_decode(buf, inputs, registry)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        // Try Kalla custom nodes first
        for entry in &self.exec_codecs {
            if let Some(bytes) = (entry.try_encode)(node.as_any()) {
                buf.push(entry.tag);
                buf.extend_from_slice(&bytes);
                return Ok(());
            }
        }

        // Delegate to Ballista's codec for internal nodes
        self.inner.try_encode(node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> DFResult<Arc<ScalarUDF>> {
        match name {
            "tolerance_match" => Ok(Arc::new(kalla_core::udf::tolerance_match_udf())),
            _ => self.inner.try_decode_udf(name, buf),
        }
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> DFResult<()> {
        // No payload needed — the UDF is identified by name alone
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// KallaLogicalCodec
// ---------------------------------------------------------------------------

/// A [`LogicalExtensionCodec`] that handles serialization of Kalla's custom
/// `TableProvider` implementations so Ballista can ship logical plans to the
/// scheduler.
///
/// Delegates to [`BallistaLogicalExtensionCodec`] for Ballista-internal nodes.
pub struct KallaLogicalCodec {
    inner: ballista_core::serde::BallistaLogicalExtensionCodec,
    table_codecs: Vec<TableCodecEntry>,
}

impl Debug for KallaLogicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KallaLogicalCodec")
            .field("table_codecs_count", &self.table_codecs.len())
            .finish()
    }
}

impl KallaLogicalCodec {
    pub fn new() -> Self {
        Self {
            inner: ballista_core::serde::BallistaLogicalExtensionCodec::default(),
            table_codecs: kalla_connectors::table_codecs(),
        }
    }
}

impl Default for KallaLogicalCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl datafusion_proto::logical_plan::LogicalExtensionCodec for KallaLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &datafusion::prelude::SessionContext,
    ) -> DFResult<datafusion::logical_expr::Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> DFResult<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &datafusion::sql::TableReference,
        schema: arrow::datatypes::SchemaRef,
        _ctx: &datafusion::prelude::SessionContext,
    ) -> DFResult<Arc<dyn datafusion::catalog::TableProvider>> {
        if buf.is_empty() {
            return Err(DataFusionError::Internal(
                "KallaLogicalCodec: empty buffer for table provider".to_string(),
            ));
        }

        let tag = buf[0];
        let payload = &buf[1..];

        for entry in &self.table_codecs {
            if entry.tag == tag {
                return (entry.try_decode)(payload, schema);
            }
        }

        Err(DataFusionError::Internal(format!(
            "KallaLogicalCodec: unknown table provider tag 0x{tag:02x}"
        )))
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &datafusion::sql::TableReference,
        node: Arc<dyn datafusion::catalog::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> DFResult<()> {
        for entry in &self.table_codecs {
            if let Some(bytes) = (entry.try_encode)(node.as_any()) {
                buf.push(entry.tag);
                buf.extend_from_slice(&bytes);
                return Ok(());
            }
        }

        // Delegate to Ballista's inner codec for any other table providers
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> DFResult<Arc<ScalarUDF>> {
        match name {
            "tolerance_match" => Ok(Arc::new(kalla_core::udf::tolerance_match_udf())),
            _ => self.inner.try_decode_udf(name, buf),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use kalla_connectors::PostgresScanExec;

    /// Provides a minimal `FunctionRegistry` for decoding tests.
    fn empty_registry() -> Arc<datafusion::prelude::SessionContext> {
        Arc::new(datafusion::prelude::SessionContext::new())
    }

    /// Build a minimal encoded buffer via the codec for dispatch tests.
    fn encode_sample_postgres(codec: &KallaPhysicalCodec) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let exec = PostgresScanExec::new(
            "postgres://localhost/test".to_string(),
            "t".to_string(),
            schema,
            0,
            10,
            None,
            None,
        );
        let mut buf = Vec::new();
        codec
            .try_encode(Arc::new(exec), &mut buf)
            .expect("encode should succeed");
        buf
    }

    #[test]
    fn test_codec_dispatches_by_tag() {
        let codec = KallaPhysicalCodec::new();
        let buf = encode_sample_postgres(&codec);

        // Tag byte routes to the correct connector decoder
        let registry = empty_registry();
        let decoded = codec
            .try_decode(&buf, &[], registry.as_ref())
            .expect("decode should succeed");
        assert!(decoded
            .as_any()
            .downcast_ref::<PostgresScanExec>()
            .is_some());
    }

    #[test]
    fn test_codec_unknown_tag_delegates_to_ballista() {
        let codec = KallaPhysicalCodec::new();
        let registry = empty_registry();

        let buf = vec![0xFF, 0x00, 0x01]; // unknown tag
        let result = codec.try_decode(&buf, &[], registry.as_ref());
        // Delegated to BallistaPhysicalExtensionCodec which fails on garbage
        assert!(result.is_err());
    }

    #[test]
    fn test_codec_empty_buffer() {
        let codec = KallaPhysicalCodec::new();
        let registry = empty_registry();

        let result = codec.try_decode(&[], &[], registry.as_ref());
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("empty buffer"), "unexpected: {err_msg}");
    }

    #[test]
    fn test_codec_corrupt_payload() {
        let codec = KallaPhysicalCodec::new();
        let registry = empty_registry();

        // Valid tag, garbage payload
        let tag = kalla_connectors::postgres_connector::WIRE_TAG_POSTGRES_EXEC;
        let buf = vec![tag, 0x00, 0x01, 0x02];
        let result = codec.try_decode(&buf, &[], registry.as_ref());
        assert!(result.is_err());
    }

    #[test]
    fn test_codec_udf_decode() {
        let codec = KallaPhysicalCodec::new();
        let udf = codec.try_decode_udf("tolerance_match", &[]).unwrap();
        assert_eq!(udf.name(), "tolerance_match");
    }

    #[test]
    fn test_codec_udf_unknown_delegates() {
        let codec = KallaPhysicalCodec::new();
        assert!(codec.try_decode_udf("nonexistent", &[]).is_err());
    }

    #[test]
    fn test_codec_udf_encode_noop() {
        let codec = KallaPhysicalCodec::new();
        let udf = kalla_core::udf::tolerance_match_udf();
        let mut buf = Vec::new();
        codec.try_encode_udf(&udf, &mut buf).unwrap();
        assert!(buf.is_empty());
    }
}
