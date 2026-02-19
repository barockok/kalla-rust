//! Ballista codec for serializing/deserializing custom execution plans.
//!
//! Wraps Ballista's `BallistaPhysicalExtensionCodec` to handle both
//! Ballista-internal nodes (ShuffleWriterExec, ShuffleReaderExec, etc.)
//! and Kalla's custom nodes (`PostgresScanExec`, `CsvRangeScanExec`).
//!
//! ## Wire format for Kalla nodes
//!
//! Each serialized payload is prefixed with a single tag byte:
//!
//! - `0x01` = `PostgresScanExec`
//! - `0x02` = `CsvRangeScanExec`
//!
//! The remaining bytes are the JSON payload produced by each node's
//! `serialize()` method. Ballista-internal nodes are delegated to
//! `BallistaPhysicalExtensionCodec`.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::catalog::Session;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{ScalarUDF, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::csv_range_scan_exec::CsvRangeScanExec;
use crate::postgres_scan_exec::PostgresScanExec;
use crate::scan_lazy::ScanLazy;

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
///
/// Delegates to [`BallistaPhysicalExtensionCodec`] for Ballista-internal nodes
/// like `ShuffleWriterExec` and `ShuffleReaderExec`.
#[derive(Debug)]
pub struct KallaPhysicalCodec {
    inner: BallistaPhysicalExtensionCodec,
}

impl KallaPhysicalCodec {
    pub fn new() -> Self {
        Self {
            inner: BallistaPhysicalExtensionCodec::default(),
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
        match tag {
            TAG_POSTGRES_SCAN => {
                let exec = PostgresScanExec::deserialize(payload).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "KallaPhysicalCodec: failed to deserialize PostgresScanExec: {e}"
                    ))
                })?;
                return Ok(Arc::new(exec));
            }
            TAG_CSV_RANGE_SCAN => {
                let exec = CsvRangeScanExec::deserialize(payload).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "KallaPhysicalCodec: failed to deserialize CsvRangeScanExec: {e}"
                    ))
                })?;
                return Ok(Arc::new(exec));
            }
            _ => {}
        }

        // Delegate to Ballista's codec for internal nodes (ShuffleWriter, etc.)
        self.inner.try_decode(buf, inputs, registry)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        // Try Kalla custom nodes first
        if let Some(pg) = node.as_any().downcast_ref::<PostgresScanExec>() {
            buf.push(TAG_POSTGRES_SCAN);
            buf.extend_from_slice(&pg.serialize());
            Ok(())
        } else if let Some(csv) = node.as_any().downcast_ref::<CsvRangeScanExec>() {
            buf.push(TAG_CSV_RANGE_SCAN);
            buf.extend_from_slice(&csv.serialize());
            Ok(())
        } else {
            // Delegate to Ballista's codec for internal nodes
            self.inner.try_encode(node, buf)
        }
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
// Lazy table wrappers for cluster-mode deserialization
// ---------------------------------------------------------------------------

/// Wraps a `PostgresPartitionedTable` so that `scan()` returns serializable
/// `PostgresScanExec` nodes (via `scan_lazy()`) instead of the eager `MemoryExec`.
///
/// Created when the scheduler deserializes a logical plan containing a Postgres
/// table reference. The lazy scan nodes are distributed to remote executors.
struct LazyPostgresTable(kalla_connectors::postgres_partitioned::PostgresPartitionedTable);

impl std::fmt::Debug for LazyPostgresTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LazyPostgresTable({:?})", self.0)
    }
}

#[async_trait]
impl datafusion::datasource::TableProvider for LazyPostgresTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        self.0.table_type()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.0.scan_lazy()
    }
}

/// Wraps a `CsvByteRangeTable` so that `scan()` returns serializable
/// `CsvRangeScanExec` nodes (via `scan_lazy()`) instead of the eager in-memory scan.
struct LazyCsvByteRangeTable(kalla_connectors::csv_partitioned::CsvByteRangeTable);

impl std::fmt::Debug for LazyCsvByteRangeTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LazyCsvByteRangeTable({:?})", self.0)
    }
}

#[async_trait]
impl datafusion::datasource::TableProvider for LazyCsvByteRangeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        self.0.table_type()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.0.scan_lazy()
    }
}

// ---------------------------------------------------------------------------
// KallaLogicalCodec
// ---------------------------------------------------------------------------

/// Tag bytes for logical-level serialization of custom table providers.
const LOGICAL_TAG_POSTGRES: u8 = 0x01;
const LOGICAL_TAG_CSV_BYTE_RANGE: u8 = 0x02;

/// A [`LogicalExtensionCodec`] that handles serialization of Kalla's custom
/// `TableProvider` implementations (`PostgresPartitionedTable`, `CsvByteRangeTable`)
/// so Ballista can ship logical plans to the scheduler.
///
/// Delegates to [`BallistaLogicalExtensionCodec`] for Ballista-internal nodes.
#[derive(Debug)]
pub struct KallaLogicalCodec {
    inner: ballista_core::serde::BallistaLogicalExtensionCodec,
}

impl KallaLogicalCodec {
    pub fn new() -> Self {
        Self {
            inner: ballista_core::serde::BallistaLogicalExtensionCodec::default(),
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

        match tag {
            LOGICAL_TAG_POSTGRES => {
                let info: serde_json::Value = serde_json::from_slice(payload).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "KallaLogicalCodec: failed to deserialize PostgresPartitionedTable: {e}"
                    ))
                })?;

                let conn_string = info["conn_string"]
                    .as_str()
                    .ok_or_else(|| DataFusionError::Internal("missing conn_string".into()))?
                    .to_string();
                let pg_table = info["pg_table"]
                    .as_str()
                    .ok_or_else(|| DataFusionError::Internal("missing pg_table".into()))?
                    .to_string();
                let total_rows = info["total_rows"].as_u64().unwrap_or(0);
                let num_partitions = info["num_partitions"].as_u64().unwrap_or(1) as usize;
                let order_column = info["order_column"].as_str().map(|s| s.to_string());
                let where_clause = info["where_clause"].as_str().map(|s| s.to_string());

                // Reconstruct without connecting — schema and row count are provided.
                // Wrap in LazyPostgresTable so scan() returns serializable
                // PostgresScanExec nodes for distribution to remote executors.
                let table =
                    kalla_connectors::postgres_partitioned::PostgresPartitionedTable::from_parts(
                        conn_string,
                        pg_table,
                        schema,
                        total_rows,
                        num_partitions,
                        order_column,
                        where_clause,
                    );
                Ok(Arc::new(LazyPostgresTable(table)))
            }
            LOGICAL_TAG_CSV_BYTE_RANGE => {
                let info: serde_json::Value = serde_json::from_slice(payload).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "KallaLogicalCodec: failed to deserialize CsvByteRangeTable: {e}"
                    ))
                })?;

                let s3_uri = info["s3_uri"]
                    .as_str()
                    .ok_or_else(|| DataFusionError::Internal("missing s3_uri".into()))?
                    .to_string();
                let total_size = info["total_size"].as_u64().unwrap_or(0);
                let num_partitions = info["num_partitions"].as_u64().unwrap_or(1) as usize;
                let header_line = info["header_line"]
                    .as_str()
                    .ok_or_else(|| DataFusionError::Internal("missing header_line".into()))?
                    .to_string();
                let s3_config: kalla_connectors::s3::S3Config =
                    serde_json::from_value(info["s3_config"].clone()).map_err(|e| {
                        DataFusionError::Internal(format!(
                            "KallaLogicalCodec: failed to deserialize S3Config: {e}"
                        ))
                    })?;

                // Wrap in LazyCsvByteRangeTable so scan() returns serializable
                // CsvRangeScanExec nodes for distribution to remote executors.
                let table = kalla_connectors::csv_partitioned::CsvByteRangeTable::from_parts(
                    s3_uri,
                    schema,
                    total_size,
                    num_partitions,
                    header_line,
                    s3_config,
                );
                Ok(Arc::new(LazyCsvByteRangeTable(table)))
            }
            _ => Err(DataFusionError::Internal(format!(
                "KallaLogicalCodec: unknown table provider tag 0x{tag:02x}"
            ))),
        }
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &datafusion::sql::TableReference,
        node: Arc<dyn datafusion::catalog::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> DFResult<()> {
        if let Some(pg) =
            node.as_any()
                .downcast_ref::<kalla_connectors::postgres_partitioned::PostgresPartitionedTable>()
        {
            buf.push(LOGICAL_TAG_POSTGRES);
            let info = serde_json::json!({
                "conn_string": pg.conn_string(),
                "pg_table": pg.pg_table(),
                "total_rows": pg.total_rows(),
                "num_partitions": pg.num_partitions(),
                "order_column": pg.order_column(),
                "where_clause": pg.where_clause(),
            });
            buf.extend_from_slice(
                serde_json::to_vec(&info)
                    .map_err(|e| DataFusionError::Internal(format!("serialize error: {e}")))?
                    .as_slice(),
            );
            Ok(())
        } else if let Some(csv) = node
            .as_any()
            .downcast_ref::<kalla_connectors::csv_partitioned::CsvByteRangeTable>()
        {
            buf.push(LOGICAL_TAG_CSV_BYTE_RANGE);
            let info = serde_json::json!({
                "s3_uri": csv.s3_uri(),
                "total_size": csv.total_size(),
                "num_partitions": csv.num_partitions(),
                "header_line": csv.header_line(),
                "s3_config": csv.s3_config(),
            });
            buf.extend_from_slice(
                serde_json::to_vec(&info)
                    .map_err(|e| DataFusionError::Internal(format!("serialize error: {e}")))?
                    .as_slice(),
            );
            Ok(())
        } else {
            // Delegate to Ballista's inner codec for any other table providers
            self.inner.try_encode_table_provider(table_ref, node, buf)
        }
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
