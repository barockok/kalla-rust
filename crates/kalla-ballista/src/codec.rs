//! Ballista codec for serializing/deserializing custom execution plans.
//!
//! Enables serialization of custom `ExecutionPlan` nodes (`PostgresScanExec`,
//! `CsvRangeScanExec`) so Ballista can send them to remote executors.
//!
//! ## Wire format
//!
//! Each serialized payload is prefixed with a single tag byte:
//!
//! - `0x01` = `PostgresScanExec`
//! - `0x02` = `CsvRangeScanExec`
//!
//! The remaining bytes are the JSON payload produced by each node's
//! `serialize()` method.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::csv_range_scan_exec::CsvRangeScanExec;
use crate::postgres_scan_exec::PostgresScanExec;

// ---------------------------------------------------------------------------
// Tag bytes
// ---------------------------------------------------------------------------

const TAG_POSTGRES_SCAN: u8 = 0x01;
const TAG_CSV_RANGE_SCAN: u8 = 0x02;

// ---------------------------------------------------------------------------
// KallaPhysicalCodec
// ---------------------------------------------------------------------------

/// A [`PhysicalExtensionCodec`] that handles serialization and deserialization
/// of Kalla's custom `ExecutionPlan` nodes for Ballista cluster mode.
#[derive(Debug)]
pub struct KallaPhysicalCodec;

impl KallaPhysicalCodec {
    pub fn new() -> Self {
        Self
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
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if buf.is_empty() {
            return Err(DataFusionError::Internal(
                "KallaPhysicalCodec: empty buffer".to_string(),
            ));
        }

        let tag = buf[0];
        let payload = &buf[1..];

        match tag {
            TAG_POSTGRES_SCAN => {
                let exec = PostgresScanExec::deserialize(payload).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "KallaPhysicalCodec: failed to deserialize PostgresScanExec: {e}"
                    ))
                })?;
                Ok(Arc::new(exec))
            }
            TAG_CSV_RANGE_SCAN => {
                let exec = CsvRangeScanExec::deserialize(payload).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "KallaPhysicalCodec: failed to deserialize CsvRangeScanExec: {e}"
                    ))
                })?;
                Ok(Arc::new(exec))
            }
            other => Err(DataFusionError::Internal(format!(
                "KallaPhysicalCodec: unknown tag byte 0x{other:02x}"
            ))),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> DFResult<()> {
        if let Some(pg) = node.as_any().downcast_ref::<PostgresScanExec>() {
            buf.push(TAG_POSTGRES_SCAN);
            buf.extend_from_slice(&pg.serialize());
            Ok(())
        } else if let Some(csv) = node.as_any().downcast_ref::<CsvRangeScanExec>() {
            buf.push(TAG_CSV_RANGE_SCAN);
            buf.extend_from_slice(&csv.serialize());
            Ok(())
        } else {
            Err(DataFusionError::Internal(format!(
                "KallaPhysicalCodec: unrecognized ExecutionPlan node: {}",
                node.name()
            )))
        }
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> DFResult<Arc<ScalarUDF>> {
        match name {
            "tolerance_match" => Ok(Arc::new(kalla_core::udf::tolerance_match_udf())),
            _ => Err(DataFusionError::Internal(format!(
                "KallaPhysicalCodec: unknown UDF: {name}"
            ))),
        }
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> DFResult<()> {
        // No payload needed — the UDF is identified by name alone
        Ok(())
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
        );

        // Encode
        let mut buf = Vec::new();
        codec
            .try_encode(Arc::new(exec), &mut buf)
            .expect("encode should succeed");

        // First byte should be the postgres tag.
        assert_eq!(buf[0], TAG_POSTGRES_SCAN);

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
        assert_eq!(buf[0], TAG_CSV_RANGE_SCAN);

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
        assert_eq!(restored.s3_config.endpoint_url, Some("http://localhost:9000".to_string()));
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

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown tag byte 0xff"),
            "unexpected error: {err_msg}"
        );
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
        let buf = vec![TAG_POSTGRES_SCAN, 0x00, 0x01, 0x02];
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
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown UDF: nonexistent_udf"),
            "unexpected error: {err_msg}"
        );
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
