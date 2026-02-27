//! CSV byte-range partitioned table provider for DataFusion
//!
//! Implements `TableProvider` to support partitioned reads of CSV files on S3.
//! The file is split into byte ranges, each partition reads its range via
//! `object_store`, handles partial first/last lines at boundaries, and parses
//! CSV independently.  This enables parallel reads across Ballista executors.
//!
//! Contains both the `CsvByteRangeTable` (the `TableProvider`) and
//! `CsvRangeScanExec` (the lazy `ExecutionPlan` that fetches a single byte
//! range). Used by both local and cluster (Ballista) modes.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::{Expr, SessionContext};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::s3::S3Config;

// ===========================================================================
// Partition helpers
// ===========================================================================

/// Compute byte ranges for partitioned reads of a file.
///
/// Returns a vector of `(start_byte, end_byte)` tuples where `end_byte` is
/// exclusive.  Partitions are sized evenly; the last partition absorbs any
/// remainder.  If `num_partitions` exceeds `file_size`, the partition count
/// is capped to `file_size`.
pub fn compute_byte_ranges(file_size: u64, num_partitions: usize) -> Vec<(u64, u64)> {
    if file_size == 0 || num_partitions == 0 {
        return vec![];
    }

    let effective = std::cmp::min(num_partitions as u64, file_size) as usize;
    let chunk = file_size / effective as u64;
    let remainder = file_size % effective as u64;

    let mut ranges = Vec::with_capacity(effective);
    let mut start = 0u64;

    for i in 0..effective {
        let end = if i == effective - 1 {
            file_size
        } else {
            start + chunk
        };
        ranges.push((start, end));
        start = end;
    }

    // Sanity: absorb rounding dust into last range
    if let Some(last) = ranges.last_mut() {
        last.1 = file_size;
    }

    let _ = remainder; // used implicitly via last-partition logic

    ranges
}

/// Split a raw byte chunk into CSV lines, handling partition boundaries.
///
/// When `is_first_partition` is `false` the first line is assumed to be a
/// partial continuation of the previous partition's last line and is
/// discarded.
///
/// Returns `(skipped_first, lines)` where `skipped_first` indicates whether
/// the first line was dropped.
pub fn split_csv_chunk(data: &[u8], is_first_partition: bool) -> (bool, Vec<&[u8]>) {
    if data.is_empty() {
        return (false, vec![]);
    }

    // Split on newlines and remove empty slices (from leading/trailing newlines)
    let mut lines: Vec<&[u8]> = data
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();

    if !is_first_partition && !lines.is_empty() {
        lines.remove(0);
        return (true, lines);
    }

    (false, lines)
}

// ===========================================================================
// CsvByteRangeTable — the TableProvider
// ===========================================================================

/// A DataFusion `TableProvider` that reads a CSV file from S3 using
/// byte-range partitioned reads.
///
/// On construction, the file's size and header are retrieved.  On `scan()`,
/// the file is divided into byte ranges and each range becomes a lazy
/// `CsvRangeScanExec` node that fetches data when polled.
pub struct CsvByteRangeTable {
    s3_uri: String,
    schema: SchemaRef,
    file_size: u64,
    num_partitions: usize,
    s3_config: S3Config,
}

impl fmt::Debug for CsvByteRangeTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvByteRangeTable")
            .field("s3_uri", &self.s3_uri)
            .field("file_size", &self.file_size)
            .field("num_partitions", &self.num_partitions)
            .finish()
    }
}

impl CsvByteRangeTable {
    /// Create a new `CsvByteRangeTable` by connecting to S3, reading the
    /// file metadata (size) and the first few KB to infer column names.
    ///
    /// All columns are typed as `Utf8` — the consumer can cast as needed.
    pub async fn new(
        s3_uri: &str,
        num_partitions: usize,
        s3_config: S3Config,
    ) -> anyhow::Result<Self> {
        let (bucket, key) = crate::s3::parse_s3_uri(s3_uri)?;
        let store = build_store(&s3_config, &bucket)?;
        let path = ObjectPath::from(key.as_str());

        // Get file metadata for size
        let meta = store
            .head(&path)
            .await
            .map_err(|e| anyhow::anyhow!("failed to HEAD S3 object: {}", e))?;
        let file_size = meta.size as u64;

        // Read first 8KB to infer header / column names
        let header_bytes = {
            let sample_end = std::cmp::min(8192, file_size);
            let opts = GetOptions {
                range: Some(GetRange::Bounded(0..sample_end as usize)),
                ..Default::default()
            };
            let result = store
                .get_opts(&path, opts)
                .await
                .map_err(|e| anyhow::anyhow!("failed to read CSV header from S3: {}", e))?;
            result
                .bytes()
                .await
                .map_err(|e| anyhow::anyhow!("failed to read header bytes: {}", e))?
        };

        // Extract the first line as CSV header
        let header_line_end = header_bytes
            .iter()
            .position(|&b| b == b'\n')
            .unwrap_or(header_bytes.len());
        let header_str = std::str::from_utf8(&header_bytes[..header_line_end])
            .map_err(|e| anyhow::anyhow!("CSV header is not valid UTF-8: {}", e))?;

        let column_names: Vec<&str> = header_str.split(',').map(|s| s.trim()).collect();
        let fields: Vec<Field> = column_names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        info!(
            "CsvByteRangeTable: uri='{}', size={}, partitions={}, cols={}",
            s3_uri,
            file_size,
            num_partitions,
            column_names.len()
        );

        Ok(Self {
            s3_uri: s3_uri.to_string(),
            schema,
            file_size,
            num_partitions,
            s3_config,
        })
    }

    /// Reconstruct from pre-computed parts (no S3 connection required).
    /// Used by the logical codec to deserialize a table provider on remote executors.
    pub fn from_parts(
        s3_uri: String,
        schema: SchemaRef,
        total_size: u64,
        num_partitions: usize,
        _header_line: String,
        s3_config: S3Config,
    ) -> Self {
        Self {
            s3_uri,
            schema,
            file_size: total_size,
            num_partitions,
            s3_config,
        }
    }

    /// Access the total file size (alias for file_size, used by codec).
    pub fn total_size(&self) -> u64 {
        self.file_size
    }

    /// Access the S3 URI.
    pub fn s3_uri(&self) -> &str {
        &self.s3_uri
    }

    /// Access the file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Access the configured number of partitions.
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Access the S3 configuration.
    pub fn s3_config(&self) -> &S3Config {
        &self.s3_config
    }

    /// Extract the header line from the schema (reconstructed from field names).
    pub fn header_line(&self) -> String {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Serialize this table provider to bytes for the wire codec.
    pub fn wire_serialize(&self) -> Vec<u8> {
        let info = serde_json::json!({
            "s3_uri": self.s3_uri,
            "total_size": self.file_size,
            "num_partitions": self.num_partitions,
            "header_line": self.header_line(),
            "s3_config": self.s3_config,
        });
        serde_json::to_vec(&info).expect("CsvByteRangeTable serialization cannot fail")
    }

    /// Deserialize from bytes + schema into a `CsvByteRangeTable`.
    pub fn wire_deserialize(buf: &[u8], schema: SchemaRef) -> datafusion::error::Result<Self> {
        let info: serde_json::Value = serde_json::from_slice(buf).map_err(|e| {
            datafusion::error::DataFusionError::Internal(format!(
                "failed to deserialize CsvByteRangeTable: {e}"
            ))
        })?;

        let s3_uri = info["s3_uri"]
            .as_str()
            .ok_or_else(|| datafusion::error::DataFusionError::Internal("missing s3_uri".into()))?
            .to_string();
        let total_size = info["total_size"].as_u64().unwrap_or(0);
        let num_partitions = info["num_partitions"].as_u64().unwrap_or(1) as usize;
        let header_line = info["header_line"]
            .as_str()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal("missing header_line".into())
            })?
            .to_string();
        let s3_config: S3Config =
            serde_json::from_value(info["s3_config"].clone()).map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "failed to deserialize S3Config: {e}"
                ))
            })?;

        Ok(Self::from_parts(
            s3_uri,
            schema,
            total_size,
            num_partitions,
            header_line,
            s3_config,
        ))
    }
}

#[async_trait]
impl TableProvider for CsvByteRangeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let ranges = compute_byte_ranges(self.file_size, self.num_partitions);
        let header = self.header_line();
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(ranges.len());

        for (i, (start, end)) in ranges.iter().enumerate() {
            plans.push(Arc::new(CsvRangeScanExec::new(
                self.s3_uri.clone(),
                Arc::clone(&self.schema),
                *start,
                *end,
                i == 0,
                header.clone(),
                self.s3_config.clone(),
            )));
        }

        if plans.len() == 1 {
            Ok(plans.into_iter().next().unwrap())
        } else {
            Ok(Arc::new(UnionExec::new(plans)))
        }
    }
}

/// Build an `object_store::aws::AmazonS3` instance for the given bucket.
pub(crate) fn build_store(
    config: &S3Config,
    bucket: &str,
) -> anyhow::Result<object_store::aws::AmazonS3> {
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

/// Register a `CsvByteRangeTable` with a DataFusion `SessionContext`.
pub async fn register(
    ctx: &SessionContext,
    table_name: &str,
    s3_uri: &str,
    num_partitions: usize,
    s3_config: S3Config,
) -> anyhow::Result<()> {
    let table = CsvByteRangeTable::new(s3_uri, num_partitions, s3_config).await?;
    ctx.register_table(table_name, Arc::new(table))?;
    info!(
        "Registered CsvByteRangeTable '{}' from '{}'",
        table_name, s3_uri
    );
    Ok(())
}

// ===========================================================================
// CsvRangeScanExec — the lazy ExecutionPlan for a single byte range
// ===========================================================================

/// A lazy DataFusion `ExecutionPlan` that fetches a single byte range from
/// an S3 CSV file when `execute()` is called.
///
/// This node is a leaf node (no children) with exactly 1 output partition.
/// The actual S3 read is deferred until the returned stream is polled.
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
                .map(|f| CsvFieldDto {
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

impl DisplayAs for CsvRangeScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CsvRangeScanExec: uri={}, range={}..{}",
            self.s3_uri, self.start_byte, self.end_byte
        )
    }
}

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
    let (bucket, key) = crate::s3::parse_s3_uri(s3_uri)?;
    let store = build_store(s3_config, &bucket)?;
    let path = ObjectPath::from(key.as_str());

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

    let lines: Vec<&[u8]> = if is_first_partition {
        raw_bytes
            .split(|&b| b == b'\n')
            .skip(1)
            .filter(|l| !l.is_empty())
            .collect()
    } else {
        let all_lines: Vec<&[u8]> = raw_bytes
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .collect();
        if all_lines.is_empty() {
            vec![]
        } else {
            all_lines[1..].to_vec()
        }
    };

    if lines.is_empty() {
        return Ok(arrow::array::RecordBatch::new_empty(Arc::clone(schema)));
    }

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

    let cursor = std::io::Cursor::new(csv_data);
    let reader = ReaderBuilder::new(Arc::clone(schema))
        .with_header(true)
        .build(cursor)?;

    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }

    if batches.is_empty() {
        Ok(arrow::array::RecordBatch::new_empty(Arc::clone(schema)))
    } else if batches.len() == 1 {
        Ok(batches.into_iter().next().unwrap())
    } else {
        let batch = arrow::compute::concat_batches(schema, &batches)?;
        Ok(batch)
    }
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
    schema_fields: Vec<CsvFieldDto>,
}

#[derive(Serialize, Deserialize)]
struct CsvFieldDto {
    name: String,
    data_type: String,
    nullable: bool,
}

/// Parse a `DataType` from its `Debug` representation string.
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

// ===========================================================================
// Wire codec entries
// ===========================================================================

/// Wire tag for [`CsvRangeScanExec`].
pub const WIRE_TAG_CSV_EXEC: u8 = 0x02;

/// Wire tag for [`CsvByteRangeTable`].
pub const WIRE_TAG_CSV_TABLE: u8 = 0x02;

/// Codec entry for serializing/deserializing [`CsvRangeScanExec`].
pub fn csv_exec_codec_entry() -> crate::wire::ExecCodecEntry {
    crate::wire::ExecCodecEntry {
        tag: WIRE_TAG_CSV_EXEC,
        type_name: "CsvRangeScanExec",
        try_encode: |any| {
            any.downcast_ref::<CsvRangeScanExec>()
                .map(|csv| csv.serialize())
        },
        try_decode: |buf| {
            CsvRangeScanExec::deserialize(buf)
                .map(|e| Arc::new(e) as Arc<dyn datafusion::physical_plan::ExecutionPlan>)
                .map_err(|e| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "failed to deserialize CsvRangeScanExec: {e}"
                    ))
                })
        },
    }
}

/// Codec entry for serializing/deserializing [`CsvByteRangeTable`].
pub fn csv_table_codec_entry() -> crate::wire::TableCodecEntry {
    crate::wire::TableCodecEntry {
        tag: WIRE_TAG_CSV_TABLE,
        type_name: "CsvByteRangeTable",
        try_encode: |any| {
            any.downcast_ref::<CsvByteRangeTable>()
                .map(|csv| csv.wire_serialize())
        },
        try_decode: |buf, schema| {
            CsvByteRangeTable::wire_deserialize(buf, schema)
                .map(|t| Arc::new(t) as Arc<dyn datafusion::catalog::TableProvider>)
        },
    }
}

// ===========================================================================
// Scoped CSV loading (used by the load-scoped HTTP endpoint)
// ===========================================================================

use crate::filter::{FilterCondition, FilterOp, FilterValue};

/// Read a CSV file from S3, apply in-memory filters, and return structured data.
///
/// Returns `(column_names, rows_as_strings, total_filtered_count)`.
/// Each row is a `Vec<String>` with values in column order.
pub async fn load_csv_scoped(
    s3_uri: &str,
    s3_config: &S3Config,
    conditions: &[FilterCondition],
    limit: usize,
) -> anyhow::Result<(Vec<String>, Vec<Vec<String>>, usize)> {
    let (bucket, key) = crate::s3::parse_s3_uri(s3_uri)?;
    let store = build_store(s3_config, &bucket)?;
    let path = ObjectPath::from(key.as_str());

    // Read full CSV file
    let result = store
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("failed to read CSV from S3: {}", e))?;
    let bytes = result
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("failed to read CSV bytes: {}", e))?;

    let csv_text = std::str::from_utf8(&bytes)
        .map_err(|e| anyhow::anyhow!("CSV is not valid UTF-8: {}", e))?;

    // Parse CSV into records
    let mut rdr = csv::ReaderBuilder::new().from_reader(csv_text.as_bytes());
    let headers: Vec<String> = rdr
        .headers()
        .map_err(|e| anyhow::anyhow!("failed to read CSV headers: {}", e))?
        .iter()
        .map(|h| h.trim().to_string())
        .collect();

    let mut all_rows: Vec<Vec<String>> = Vec::new();
    for record in rdr.records() {
        let record = record.map_err(|e| anyhow::anyhow!("CSV parse error: {}", e))?;
        let row: Vec<String> = record.iter().map(|f| f.to_string()).collect();
        all_rows.push(row);
    }

    // Apply in-memory filters
    let filtered: Vec<Vec<String>> = all_rows
        .into_iter()
        .filter(|row| {
            conditions.iter().all(|cond| {
                let col_idx = match headers.iter().position(|h| h == &cond.column) {
                    Some(idx) => idx,
                    None => return true, // skip unknown columns
                };
                let cell = row.get(col_idx).map(|s| s.as_str()).unwrap_or("");
                matches_filter(cell, &cond.op, &cond.value)
            })
        })
        .collect();

    let total = filtered.len();
    let limited: Vec<Vec<String>> = filtered.into_iter().take(limit).collect();

    Ok((headers, limited, total))
}

/// Check whether a cell value matches a filter condition (string comparison).
fn matches_filter(cell: &str, op: &FilterOp, value: &FilterValue) -> bool {
    match (op, value) {
        (FilterOp::Eq, FilterValue::String(v)) => cell == v,
        (FilterOp::Eq, FilterValue::Number(v)) => cell == format_num(*v).as_str(),
        (FilterOp::Neq, FilterValue::String(v)) => cell != v,
        (FilterOp::Neq, FilterValue::Number(v)) => cell != format_num(*v).as_str(),
        (FilterOp::Gt, FilterValue::String(v)) => cell > v.as_str(),
        (FilterOp::Gt, FilterValue::Number(v)) => cell > format_num(*v).as_str(),
        (FilterOp::Gte, FilterValue::String(v)) => cell >= v.as_str(),
        (FilterOp::Gte, FilterValue::Number(v)) => cell >= format_num(*v).as_str(),
        (FilterOp::Lt, FilterValue::String(v)) => cell < v.as_str(),
        (FilterOp::Lt, FilterValue::Number(v)) => cell < format_num(*v).as_str(),
        (FilterOp::Lte, FilterValue::String(v)) => cell <= v.as_str(),
        (FilterOp::Lte, FilterValue::Number(v)) => cell <= format_num(*v).as_str(),
        (FilterOp::Between, FilterValue::StringArray(vals)) if vals.len() == 2 => {
            cell >= vals[0].as_str() && cell <= vals[1].as_str()
        }
        (FilterOp::In, FilterValue::StringArray(vals)) => vals.iter().any(|v| cell == v),
        (FilterOp::Like, FilterValue::String(pattern)) => sql_like_match(cell, pattern),
        _ => true, // fallback for mismatched op/value
    }
}

/// Simple SQL LIKE pattern matching (case-insensitive).
/// `%` matches any sequence, `_` matches any single character.
fn sql_like_match(text: &str, pattern: &str) -> bool {
    let text = text.to_lowercase();
    let pattern = pattern.to_lowercase();
    let t = text.as_bytes();
    let p = pattern.as_bytes();
    let (tlen, plen) = (t.len(), p.len());

    // DP: dp[j] = can pattern[..j] match text[..i]
    let mut dp = vec![false; plen + 1];
    dp[0] = true;
    // Leading %'s match empty string
    for j in 0..plen {
        if p[j] == b'%' {
            dp[j + 1] = dp[j];
        } else {
            break;
        }
    }

    for i in 0..tlen {
        let mut new_dp = vec![false; plen + 1];
        for j in 0..plen {
            if p[j] == b'%' {
                // % matches zero (new_dp[j]) or one+ chars (dp[j+1])
                new_dp[j + 1] = new_dp[j] || dp[j + 1];
            } else if p[j] == b'_' || p[j] == t[i] {
                new_dp[j + 1] = dp[j];
            }
        }
        dp = new_dp;
    }
    dp[plen]
}

fn format_num(n: f64) -> String {
    if n == n.floor() {
        format!("{}", n as i64)
    } else {
        format!("{}", n)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- byte range tests ----------------------------------------------------

    #[test]
    fn test_byte_range_partitions() {
        let ranges = compute_byte_ranges(1000, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], (0, 250));
        assert_eq!(ranges[1], (250, 500));
        assert_eq!(ranges[2], (500, 750));
        assert_eq!(ranges[3], (750, 1000));
    }

    #[test]
    fn test_byte_range_single_partition() {
        let ranges = compute_byte_ranges(500, 1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 500));
    }

    #[test]
    fn test_byte_range_small_file() {
        let ranges = compute_byte_ranges(10, 5);
        assert!(ranges.len() <= 5);
        assert_eq!(ranges.first().unwrap().0, 0);
        assert_eq!(ranges.last().unwrap().1, 10);
    }

    #[test]
    fn test_byte_range_zero_size() {
        let ranges = compute_byte_ranges(0, 4);
        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_byte_range_zero_partitions() {
        let ranges = compute_byte_ranges(100, 0);
        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_byte_range_more_partitions_than_bytes() {
        let ranges = compute_byte_ranges(3, 10);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], (0, 1));
        assert_eq!(ranges[1], (1, 2));
        assert_eq!(ranges[2], (2, 3));
    }

    #[test]
    fn test_byte_range_uneven() {
        let ranges = compute_byte_ranges(10, 3);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], (0, 3));
        assert_eq!(ranges[1], (3, 6));
        assert_eq!(ranges[2], (6, 10));
        let total: u64 = ranges.iter().map(|(s, e)| e - s).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_byte_range_one_byte() {
        let ranges = compute_byte_ranges(1, 4);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 1));
    }

    #[test]
    fn test_split_csv_chunk_first_partition() {
        let data = b"1,Alice,100\n2,Bob,200\n";
        let (skip, lines) = split_csv_chunk(data, true);
        assert!(!skip);
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_split_csv_chunk_middle_partition() {
        let data = b"ice,100\n2,Bob,200\n3,Carol,300\n";
        let (skip, lines) = split_csv_chunk(data, false);
        assert!(skip);
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_split_csv_chunk_no_trailing_newline() {
        let data = b"1,A\n2,B";
        let (skip, lines) = split_csv_chunk(data, true);
        assert!(!skip);
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_split_csv_chunk_empty() {
        let data = b"";
        let (skip, lines) = split_csv_chunk(data, true);
        assert!(!skip);
        assert_eq!(lines.len(), 0);
    }

    #[test]
    fn test_split_csv_chunk_single_line_no_newline() {
        let data = b"partial_data";
        let (skip, lines) = split_csv_chunk(data, false);
        assert!(skip);
        assert_eq!(lines.len(), 0);
    }

    #[test]
    fn test_split_csv_chunk_only_newline() {
        let data = b"\n";
        let (skip, lines) = split_csv_chunk(data, true);
        assert!(!skip);
        assert_eq!(lines.len(), 0);
    }

    // -- CsvRangeScanExec tests ----------------------------------------------

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
        assert_eq!(exec.schema(), schema);
        assert_eq!(exec.properties().partitioning.partition_count(), 1);
        assert!(exec.children().is_empty());
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
    fn test_csv_serialization_roundtrip() {
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
    fn test_csv_serialization_roundtrip_non_first_partition() {
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
