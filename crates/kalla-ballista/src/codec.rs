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
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use kalla_connectors::s3::S3Config;
    use kalla_connectors::{CsvRangeScanExec, PostgresScanExec};

    fn pg_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]))
    }

    fn csv_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]))
    }

    fn sample_s3_config() -> S3Config {
        S3Config {
            region: "us-east-1".to_string(),
            access_key_id: "test-key".to_string(),
            secret_access_key: "test-secret".to_string(),
            endpoint_url: Some("http://localhost:9000".to_string()),
            allow_http: true,
        }
    }

    /// Provides a minimal `FunctionRegistry` for decoding tests.
    fn empty_registry() -> Arc<datafusion::prelude::SessionContext> {
        Arc::new(datafusion::prelude::SessionContext::new())
    }

    #[test]
    fn test_codec_roundtrip_postgres() {
        let codec = KallaPhysicalCodec::new();
        let schema = pg_schema();

        let exec = PostgresScanExec::new(
            "postgres://user:pass@host:5432/db".to_string(),
            "invoices".to_string(),
            Arc::clone(&schema),
            500,
            250,
            Some("invoice_id".to_string()),
            None,
        );

        // Encode
        let mut buf = Vec::new();
        codec
            .try_encode(Arc::new(exec), &mut buf)
            .expect("encode should succeed");

        // First byte should be the postgres tag.
        assert_eq!(
            buf[0],
            kalla_connectors::postgres::WIRE_TAG_POSTGRES_EXEC
        );

        // Decode
        let registry = empty_registry();
        let decoded = codec
            .try_decode(&buf, &[], registry.as_ref())
            .expect("decode should succeed");

        let restored = decoded
            .as_any()
            .downcast_ref::<PostgresScanExec>()
            .expect("should downcast to PostgresScanExec");

        assert_eq!(restored.conn_string, "postgres://user:pass@host:5432/db");
        assert_eq!(restored.pg_table, "invoices");
        assert_eq!(restored.offset, 500);
        assert_eq!(restored.limit, 250);
        assert_eq!(restored.order_column, Some("invoice_id".to_string()));
        assert_eq!(restored.schema.fields().len(), 4);
        assert_eq!(restored.schema.field(0).name(), "id");
        assert_eq!(*restored.schema.field(0).data_type(), DataType::Int64);
        assert_eq!(restored.where_clause, None);
    }

    #[test]
    fn test_codec_roundtrip_postgres_with_where_clause() {
        let codec = KallaPhysicalCodec::new();
        let schema = pg_schema();

        let exec = PostgresScanExec::new(
            "postgres://user:pass@host:5432/db".to_string(),
            "invoices".to_string(),
            Arc::clone(&schema),
            0,
            1000,
            Some("invoice_id".to_string()),
            Some(" WHERE \"status\" = 'active'".to_string()),
        );

        // Encode
        let mut buf = Vec::new();
        codec
            .try_encode(Arc::new(exec), &mut buf)
            .expect("encode should succeed");

        // Decode
        let registry = empty_registry();
        let decoded = codec
            .try_decode(&buf, &[], registry.as_ref())
            .expect("decode should succeed");

        let restored = decoded
            .as_any()
            .downcast_ref::<PostgresScanExec>()
            .expect("should downcast to PostgresScanExec");

        assert_eq!(
            restored.where_clause,
            Some(" WHERE \"status\" = 'active'".to_string())
        );
        assert_eq!(restored.pg_table, "invoices");
        assert_eq!(restored.offset, 0);
        assert_eq!(restored.limit, 1000);
    }

    #[test]
    fn test_codec_roundtrip_csv() {
        let codec = KallaPhysicalCodec::new();
        let schema = csv_schema();

        let exec = CsvRangeScanExec::new(
            "s3://my-bucket/data.csv".to_string(),
            Arc::clone(&schema),
            1000,
            5000,
            true,
            "id,value,score".to_string(),
            sample_s3_config(),
        );

        // Encode
        let mut buf = Vec::new();
        codec
            .try_encode(Arc::new(exec), &mut buf)
            .expect("encode should succeed");

        // First byte should be the csv tag.
        assert_eq!(
            buf[0],
            kalla_connectors::csv_partitioned::WIRE_TAG_CSV_EXEC
        );

        // Decode
        let registry = empty_registry();
        let decoded = codec
            .try_decode(&buf, &[], registry.as_ref())
            .expect("decode should succeed");

        let restored = decoded
            .as_any()
            .downcast_ref::<CsvRangeScanExec>()
            .expect("should downcast to CsvRangeScanExec");

        assert_eq!(restored.s3_uri, "s3://my-bucket/data.csv");
        assert_eq!(restored.start_byte, 1000);
        assert_eq!(restored.end_byte, 5000);
        assert!(restored.is_first_partition);
        assert_eq!(restored.header_line, "id,value,score");
        assert_eq!(restored.s3_config.region, "us-east-1");
        assert_eq!(restored.s3_config.access_key_id, "test-key");
        assert_eq!(
            restored.s3_config.endpoint_url,
            Some("http://localhost:9000".to_string())
        );
        assert!(restored.s3_config.allow_http);
        assert_eq!(restored.schema.fields().len(), 3);
        assert_eq!(restored.schema.field(0).name(), "id");
    }

    #[test]
    fn test_codec_unknown_tag() {
        let codec = KallaPhysicalCodec::new();
        let registry = empty_registry();

        let buf = vec![0xFF, 0x00, 0x01]; // unknown tag 0xFF
        let result = codec.try_decode(&buf, &[], registry.as_ref());

        // Unknown tags are delegated to BallistaPhysicalExtensionCodec, which
        // fails to parse the bytes as a protobuf message.
        assert!(result.is_err());
    }

    #[test]
    fn test_codec_empty_buffer() {
        let codec = KallaPhysicalCodec::new();
        let registry = empty_registry();

        let buf: Vec<u8> = vec![];
        let result = codec.try_decode(&buf, &[], registry.as_ref());

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("empty buffer"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_codec_corrupt_payload() {
        let codec = KallaPhysicalCodec::new();
        let registry = empty_registry();

        // Valid tag but garbage payload
        let buf = vec![
            kalla_connectors::postgres::WIRE_TAG_POSTGRES_EXEC,
            0x00,
            0x01,
            0x02,
        ];
        let result = codec.try_decode(&buf, &[], registry.as_ref());

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("failed to deserialize PostgresScanExec"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_codec_udf_roundtrip() {
        let codec = KallaPhysicalCodec::new();
        let udf = codec.try_decode_udf("tolerance_match", &[]).unwrap();
        assert_eq!(udf.name(), "tolerance_match");
    }

    #[test]
    fn test_codec_udf_unknown() {
        let codec = KallaPhysicalCodec::new();
        let result = codec.try_decode_udf("nonexistent_udf", &[]);
        // Unknown UDFs are delegated to BallistaPhysicalExtensionCodec
        assert!(result.is_err());
    }

    #[test]
    fn test_codec_udf_encode_noop() {
        let codec = KallaPhysicalCodec::new();
        let udf = kalla_core::udf::tolerance_match_udf();
        let mut buf = Vec::new();
        codec.try_encode_udf(&udf, &mut buf).unwrap();
        // Encoding writes no payload for name-only UDFs
        assert!(buf.is_empty());
    }
}
