//! Distributed execution node for CSV range-based scans.
//!
//! `CsvRangeScanExec` is a lazy DataFusion `ExecutionPlan` that fetches a single
//! byte range from an S3 CSV file when `execute()` is called on a remote
//! Ballista executor. Unlike the eager `CsvByteRangeTable` which reads all
//! partitions into `MemoryExec` at scan time, this plan defers the S3 fetch
//! until the stream is actually polled.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore};
use serde::{Deserialize, Serialize};
use tracing::debug;

use kalla_connectors::s3::{S3Config, S3Connector};

// ---------------------------------------------------------------------------
// CsvRangeScanExec
// ---------------------------------------------------------------------------

/// A lazy DataFusion `ExecutionPlan` that fetches a single byte range from
/// an S3 CSV file when `execute()` is called.
///
/// This node is intended to run on a remote Ballista executor. It is a leaf
/// node (no children) with exactly 1 output partition. The actual S3 read is
/// deferred until the returned stream is polled.
#[derive(Debug)]
pub struct CsvRangeScanExec {
    pub s3_uri: String,
    pub schema: SchemaRef,
    pub start_byte: u64,
    pub end_byte: u64,
    pub is_first_partition: bool,
    pub header_line: String,
    pub s3_config: S3Config,
    properties: PlanProperties,
}

impl CsvRangeScanExec {
    /// Create a new `CsvRangeScanExec`.
    pub fn new(
        s3_uri: String,
        schema: SchemaRef,
        start_byte: u64,
        end_byte: u64,
        is_first_partition: bool,
        header_line: String,
        s3_config: S3Config,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            s3_uri,
            schema,
            start_byte,
            end_byte,
            is_first_partition,
            header_line,
            s3_config,
            properties,
        }
    }

    // -- Serialization -------------------------------------------------------

    /// Serialize this execution plan to bytes (JSON).
    pub fn serialize(&self) -> Vec<u8> {
        let dto = CsvRangeScanExecDto {
            s3_uri: self.s3_uri.clone(),
            start_byte: self.start_byte,
            end_byte: self.end_byte,
            is_first_partition: self.is_first_partition,
            header_line: self.header_line.clone(),
            s3_config: self.s3_config.clone(),
            schema_fields: self
                .schema
                .fields()
                .iter()
                .map(|f| FieldDto {
                    name: f.name().clone(),
                    data_type: format!("{:?}", f.data_type()),
                    nullable: f.is_nullable(),
                })
                .collect(),
        };
        serde_json::to_vec(&dto).expect("CsvRangeScanExecDto serialization cannot fail")
    }

    /// Deserialize from bytes (JSON) back into a `CsvRangeScanExec`.
    pub fn deserialize(bytes: &[u8]) -> anyhow::Result<Self> {
        let dto: CsvRangeScanExecDto = serde_json::from_slice(bytes)?;
        let fields: Vec<Field> = dto
            .schema_fields
            .iter()
            .map(|f| Field::new(&f.name, parse_data_type(&f.data_type), f.nullable))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        Ok(Self::new(
            dto.s3_uri,
            schema,
            dto.start_byte,
            dto.end_byte,
            dto.is_first_partition,
            dto.header_line,
            dto.s3_config,
        ))
    }
}

// ---------------------------------------------------------------------------
// ExecutionPlan
// ---------------------------------------------------------------------------

impl ExecutionPlan for CsvRangeScanExec {
    fn name(&self) -> &str {
        "CsvRangeScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Leaf node — no children.
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "CsvRangeScanExec is a leaf node and cannot have children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "CsvRangeScanExec only supports partition 0, got {}",
                partition
            )));
        }

        let s3_uri = self.s3_uri.clone();
        let schema = Arc::clone(&self.schema);
        let start_byte = self.start_byte;
        let end_byte = self.end_byte;
        let is_first_partition = self.is_first_partition;
        let header_line = self.header_line.clone();
        let s3_config = self.s3_config.clone();

        // Build the stream lazily — the async S3 work happens inside.
        let stream = futures::stream::once(async move {
            let result = fetch_csv_range(
                &s3_uri,
                &schema,
                start_byte,
                end_byte,
                is_first_partition,
                &header_line,
                &s3_config,
            )
            .await;
            result.map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

// ---------------------------------------------------------------------------
// DisplayAs
// ---------------------------------------------------------------------------

impl DisplayAs for CsvRangeScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CsvRangeScanExec: uri={}, range={}..{}",
            self.s3_uri, self.start_byte, self.end_byte
        )
    }
}

// ---------------------------------------------------------------------------
// Internal async fetch
// ---------------------------------------------------------------------------

/// Read a byte range from S3, handle partial lines at boundaries, and parse
/// the resulting CSV into a `RecordBatch`.
async fn fetch_csv_range(
    s3_uri: &str,
    schema: &SchemaRef,
    start_byte: u64,
    end_byte: u64,
    is_first_partition: bool,
    header_line: &str,
    s3_config: &S3Config,
) -> anyhow::Result<arrow::array::RecordBatch> {
    let (bucket, key) = S3Connector::parse_s3_uri(s3_uri)?;
    let store = build_store(s3_config, &bucket)?;
    let path = ObjectPath::from(key.as_str());

    // Read the byte range from S3
    let opts = GetOptions {
        range: Some(GetRange::Bounded(start_byte as usize..end_byte as usize)),
        ..Default::default()
    };
    let result = store.get_opts(&path, opts).await.map_err(|e| {
        anyhow::anyhow!(
            "failed to read S3 byte range {}..{}: {}",
            start_byte,
            end_byte,
            e
        )
    })?;
    let raw_bytes = result
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("failed to read bytes: {}", e))?;

    debug!(
        "CsvRangeScanExec: fetched {} bytes from {} (range {}..{})",
        raw_bytes.len(),
        s3_uri,
        start_byte,
        end_byte
    );

    // Handle partial lines at partition boundaries.
    // For the first partition: skip the header line from the raw data.
    // For non-first partitions: skip the first (partial) line.
    let lines: Vec<&[u8]> = if is_first_partition {
        raw_bytes
            .split(|&b| b == b'\n')
            .skip(1) // skip header
            .filter(|l| !l.is_empty())
            .collect()
    } else {
        let all_lines: Vec<&[u8]> = raw_bytes
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .collect();
        // Skip first (partial) line
        if all_lines.is_empty() {
            vec![]
        } else {
            all_lines[1..].to_vec()
        }
    };

    if lines.is_empty() {
        return Ok(arrow::array::RecordBatch::new_empty(Arc::clone(schema)));
    }

    // Reconstruct CSV with header for the Arrow CSV reader
    let mut csv_data = Vec::new();
    csv_data.extend_from_slice(header_line.as_bytes());
    csv_data.push(b'\n');
    for line in &lines {
        csv_data.extend_from_slice(line);
        csv_data.push(b'\n');
    }

    debug!(
        "CsvRangeScanExec: parsing {} data lines, {} csv bytes",
        lines.len(),
        csv_data.len()
    );

    // Parse CSV data into a RecordBatch
    let cursor = std::io::Cursor::new(csv_data);
    let reader = ReaderBuilder::new(Arc::clone(schema))
        .with_header(true)
        .build(cursor)?;

    // Collect all batches and concatenate
    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }

    if batches.is_empty() {
        Ok(arrow::array::RecordBatch::new_empty(Arc::clone(schema)))
    } else if batches.len() == 1 {
        Ok(batches.into_iter().next().unwrap())
    } else {
        // Concatenate multiple batches into one
        let batch = arrow::compute::concat_batches(schema, &batches)?;
        Ok(batch)
    }
}

/// Build an `object_store::aws::AmazonS3` instance for the given bucket.
fn build_store(config: &S3Config, bucket: &str) -> anyhow::Result<object_store::aws::AmazonS3> {
    let mut builder = AmazonS3Builder::new()
        .with_region(&config.region)
        .with_bucket_name(bucket)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key);

    if let Some(ref endpoint) = config.endpoint_url {
        builder = builder.with_endpoint(endpoint);
    }
    if config.allow_http {
        builder = builder.with_allow_http(true);
    }

    builder
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build S3 object store: {}", e))
}

// ---------------------------------------------------------------------------
// Serialization DTOs
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct CsvRangeScanExecDto {
    s3_uri: String,
    start_byte: u64,
    end_byte: u64,
    is_first_partition: bool,
    header_line: String,
    s3_config: S3Config,
    schema_fields: Vec<FieldDto>,
}

#[derive(Serialize, Deserialize)]
struct FieldDto {
    name: String,
    data_type: String,
    nullable: bool,
}

/// Parse a `DataType` from its `Debug` representation string.
///
/// Handles the common types produced by `format!("{:?}", dt)`. Unknown
/// strings default to `DataType::Utf8`.
fn parse_data_type(s: &str) -> DataType {
    match s {
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Boolean" => DataType::Boolean,
        "Utf8" => DataType::Utf8,
        "Binary" => DataType::Binary,
        _ => DataType::Utf8,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
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

    #[test]
    fn test_csv_range_scan_exec_properties() {
        let schema = sample_schema();
        let exec = CsvRangeScanExec::new(
            "s3://bucket/data.csv".to_string(),
            Arc::clone(&schema),
            1000,
            2000,
            false,
            "id,name,amount".to_string(),
            sample_s3_config(),
        );

        // Schema should match.
        assert_eq!(exec.schema(), schema);

        // Must have exactly 1 output partition.
        assert_eq!(exec.properties().partitioning.partition_count(), 1);

        // Leaf node — no children.
        assert!(exec.children().is_empty());

        // Display
        let display_str = format!(
            "{}",
            datafusion::physical_plan::displayable(&exec).one_line()
        );
        assert!(
            display_str.contains("CsvRangeScanExec: uri=s3://bucket/data.csv, range=1000..2000"),
            "unexpected display: {}",
            display_str
        );
    }

    #[test]
    fn test_serialization_roundtrip() {
        let schema = sample_schema();
        let exec = CsvRangeScanExec::new(
            "s3://my-bucket/path/to/file.csv".to_string(),
            Arc::clone(&schema),
            500,
            1500,
            true,
            "id,name,amount".to_string(),
            sample_s3_config(),
        );

        let bytes = exec.serialize();
        let restored =
            CsvRangeScanExec::deserialize(&bytes).expect("deserialization should succeed");

        assert_eq!(restored.s3_uri, exec.s3_uri);
        assert_eq!(restored.start_byte, exec.start_byte);
        assert_eq!(restored.end_byte, exec.end_byte);
        assert_eq!(restored.is_first_partition, exec.is_first_partition);
        assert_eq!(restored.header_line, exec.header_line);
        assert_eq!(restored.s3_config.region, exec.s3_config.region);
        assert_eq!(
            restored.s3_config.access_key_id,
            exec.s3_config.access_key_id
        );
        assert_eq!(
            restored.s3_config.secret_access_key,
            exec.s3_config.secret_access_key
        );
        assert_eq!(restored.s3_config.endpoint_url, exec.s3_config.endpoint_url);
        assert_eq!(restored.s3_config.allow_http, exec.s3_config.allow_http);
        assert_eq!(restored.schema.fields().len(), exec.schema.fields().len());

        for (orig, rest) in exec
            .schema
            .fields()
            .iter()
            .zip(restored.schema.fields().iter())
        {
            assert_eq!(orig.name(), rest.name());
            assert_eq!(orig.data_type(), rest.data_type());
            assert_eq!(orig.is_nullable(), rest.is_nullable());
        }
    }

    #[test]
    fn test_serialization_roundtrip_non_first_partition() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, true),
        ]));
        let exec = CsvRangeScanExec::new(
            "s3://data-bucket/large.csv".to_string(),
            schema,
            10000,
            20000,
            false,
            "x,y".to_string(),
            S3Config {
                region: "eu-west-1".to_string(),
                access_key_id: "ak".to_string(),
                secret_access_key: "sk".to_string(),
                endpoint_url: None,
                allow_http: false,
            },
        );

        let bytes = exec.serialize();
        let restored =
            CsvRangeScanExec::deserialize(&bytes).expect("deserialization should succeed");

        assert!(!restored.is_first_partition);
        assert_eq!(restored.s3_uri, "s3://data-bucket/large.csv");
        assert_eq!(restored.start_byte, 10000);
        assert_eq!(restored.end_byte, 20000);
        assert_eq!(restored.header_line, "x,y");
        assert!(restored.s3_config.endpoint_url.is_none());
        assert!(!restored.s3_config.allow_http);
    }

    #[test]
    fn test_parse_data_type_known_types() {
        assert_eq!(parse_data_type("Int16"), DataType::Int16);
        assert_eq!(parse_data_type("Int32"), DataType::Int32);
        assert_eq!(parse_data_type("Int64"), DataType::Int64);
        assert_eq!(parse_data_type("Float32"), DataType::Float32);
        assert_eq!(parse_data_type("Float64"), DataType::Float64);
        assert_eq!(parse_data_type("Boolean"), DataType::Boolean);
        assert_eq!(parse_data_type("Utf8"), DataType::Utf8);
        assert_eq!(parse_data_type("Binary"), DataType::Binary);
    }

    #[test]
    fn test_parse_data_type_unknown_defaults_to_utf8() {
        assert_eq!(parse_data_type("LargeUtf8"), DataType::Utf8);
        assert_eq!(
            parse_data_type("Timestamp(Nanosecond, None)"),
            DataType::Utf8
        );
        assert_eq!(parse_data_type("SomeWeirdType"), DataType::Utf8);
    }
}
