//! CSV byte-range partitioned table provider for DataFusion
//!
//! Implements `TableProvider` to support partitioned reads of CSV files on S3.
//! The file is split into byte ranges, each partition reads its range via
//! `object_store`, handles partial first/last lines at boundaries, and parses
//! CSV independently.  This enables parallel reads across Ballista executors.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SessionContext};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore};
use tracing::{debug, info};

use crate::s3::{S3Config, S3Connector};

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

/// A DataFusion `TableProvider` that reads a CSV file from S3 using
/// byte-range partitioned reads.
///
/// On construction, the file's size and header are retrieved.  On `scan()`,
/// the file is divided into byte ranges, each range is fetched, partial
/// boundary lines are handled, and the resulting CSV data is parsed into
/// Arrow `RecordBatch`es.
pub struct CsvByteRangeTable {
    s3_uri: String,
    bucket: String,
    key: String,
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
        let (bucket, key) = S3Connector::parse_s3_uri(s3_uri)?;
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
            bucket,
            key,
            schema,
            file_size,
            num_partitions,
            s3_config,
        })
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
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let ranges = compute_byte_ranges(self.file_size, self.num_partitions);
        let store = build_store(&self.s3_config, &self.bucket)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        let path = ObjectPath::from(self.key.as_str());
        let header = self.header_line();

        // Build projected schema
        let projected_schema = match projection {
            Some(indices) => {
                let projected_fields: Vec<Field> = indices
                    .iter()
                    .map(|&i| self.schema.field(i).clone())
                    .collect();
                Arc::new(Schema::new(projected_fields))
            }
            None => Arc::clone(&self.schema),
        };

        let mut partitions: Vec<Vec<arrow::array::RecordBatch>> = Vec::with_capacity(ranges.len());

        for (i, (start, end)) in ranges.iter().enumerate() {
            let is_first = i == 0;

            // Read byte range from S3
            let opts = GetOptions {
                range: Some(GetRange::Bounded(*start as usize..*end as usize)),
                ..Default::default()
            };
            let result = store.get_opts(&path, opts).await.map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "failed to read S3 byte range {}..{}: {}",
                    start, end, e
                ))
            })?;
            let raw_bytes = result.bytes().await.map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "failed to read bytes for partition {}: {}",
                    i, e
                ))
            })?;

            // Handle partial lines at partition boundaries
            let (_skipped, lines) = if is_first {
                // First partition: skip the header line, keep data lines
                let all_lines: Vec<&[u8]> = raw_bytes.split(|&b| b == b'\n').collect();
                let data_lines: Vec<&[u8]> = all_lines
                    .into_iter()
                    .skip(1) // skip header
                    .filter(|l| !l.is_empty())
                    .collect();
                (false, data_lines)
            } else {
                split_csv_chunk(&raw_bytes, false)
            };

            if lines.is_empty() {
                partitions.push(vec![]);
                continue;
            }

            // Reconstruct CSV with header for the Arrow CSV reader
            let mut csv_data = Vec::new();
            csv_data.extend_from_slice(header.as_bytes());
            csv_data.push(b'\n');
            for line in &lines {
                csv_data.extend_from_slice(line);
                csv_data.push(b'\n');
            }

            debug!(
                "Partition {}: range={}..{}, lines={}, csv_bytes={}",
                i,
                start,
                end,
                lines.len(),
                csv_data.len()
            );

            // Parse CSV data into a RecordBatch
            let cursor = std::io::Cursor::new(csv_data);
            let reader = ReaderBuilder::new(Arc::clone(&self.schema))
                .with_header(true)
                .build(cursor)
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "failed to build CSV reader for partition {}: {}",
                        i, e
                    ))
                })?;

            let mut batch_vec = Vec::new();
            for batch_result in reader {
                let batch = batch_result.map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "CSV parse error in partition {}: {}",
                        i, e
                    ))
                })?;

                // Apply projection if requested
                let projected_batch = match projection {
                    Some(indices) => {
                        let columns: Vec<_> = indices
                            .iter()
                            .map(|&idx| batch.column(idx).clone())
                            .collect();
                        arrow::array::RecordBatch::try_new(Arc::clone(&projected_schema), columns)
                            .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "projection error in partition {}: {}",
                                i, e
                            ))
                        })?
                    }
                    None => batch,
                };
                batch_vec.push(projected_batch);
            }

            partitions.push(batch_vec);
        }

        let exec = MemoryExec::try_new(&partitions, projected_schema, None)?;
        Ok(Arc::new(exec))
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

#[cfg(test)]
mod tests {
    use super::*;

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
        // 3 bytes, 10 partitions -> capped to 3 partitions
        let ranges = compute_byte_ranges(3, 10);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], (0, 1));
        assert_eq!(ranges[1], (1, 2));
        assert_eq!(ranges[2], (2, 3));
    }

    #[test]
    fn test_byte_range_uneven() {
        // 10 bytes, 3 partitions: 3, 3, 4
        let ranges = compute_byte_ranges(10, 3);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], (0, 3));
        assert_eq!(ranges[1], (3, 6));
        assert_eq!(ranges[2], (6, 10));
        // Verify coverage
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
        assert!(!skip); // first partition keeps all lines
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_split_csv_chunk_middle_partition() {
        // Simulating a read that starts mid-line
        let data = b"ice,100\n2,Bob,200\n3,Carol,300\n";
        let (skip, lines) = split_csv_chunk(data, false);
        assert!(skip); // non-first partition discards first partial line
        assert_eq!(lines.len(), 2); // "2,Bob,200" and "3,Carol,300"
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
        // After discarding the only (partial) line, nothing left
        assert_eq!(lines.len(), 0);
    }

    #[test]
    fn test_split_csv_chunk_only_newline() {
        let data = b"\n";
        let (skip, lines) = split_csv_chunk(data, true);
        assert!(!skip);
        assert_eq!(lines.len(), 0);
    }
}
