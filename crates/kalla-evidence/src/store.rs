//! Evidence store implementation

use anyhow::Result;
use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use crate::schema::{MatchedRecord, RunMetadata, UnmatchedRecord};

/// Evidence store for persisting reconciliation results
pub struct EvidenceStore {
    base_path: PathBuf,
}

impl EvidenceStore {
    /// Create a new evidence store at the given path
    pub fn new(base_path: impl AsRef<Path>) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }

    /// Get the path for a specific run
    pub fn run_path(&self, run_id: &Uuid) -> PathBuf {
        self.base_path.join("runs").join(run_id.to_string())
    }

    /// Initialize a new run directory
    pub fn init_run(&self, metadata: &RunMetadata) -> Result<PathBuf> {
        let run_path = self.run_path(&metadata.run_id);
        fs::create_dir_all(&run_path)?;

        // Write metadata
        let metadata_path = run_path.join("metadata.json");
        let metadata_json = serde_json::to_string_pretty(metadata)?;
        fs::write(&metadata_path, metadata_json)?;

        info!("Initialized run at {:?}", run_path);
        Ok(run_path)
    }

    /// Update run metadata
    pub fn update_metadata(&self, metadata: &RunMetadata) -> Result<()> {
        let run_path = self.run_path(&metadata.run_id);
        let metadata_path = run_path.join("metadata.json");
        let metadata_json = serde_json::to_string_pretty(metadata)?;
        fs::write(&metadata_path, metadata_json)?;
        Ok(())
    }

    /// Write matched records to parquet
    pub fn write_matched(
        &self,
        run_id: &Uuid,
        records: &[MatchedRecord],
    ) -> Result<PathBuf> {
        let run_path = self.run_path(run_id);
        let output_path = run_path.join("matched.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("match_id", DataType::Utf8, false),
            Field::new("left_key", DataType::Utf8, false),
            Field::new("right_key", DataType::Utf8, false),
            Field::new("rule_name", DataType::Utf8, false),
            Field::new("confidence", DataType::Float64, false),
            Field::new(
                "matched_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]));

        let match_id_strings: Vec<String> = records.iter().map(|r| r.match_id.to_string()).collect();
        let left_keys: Vec<&str> = records.iter().map(|r| r.left_key.as_str()).collect();
        let right_keys: Vec<&str> = records.iter().map(|r| r.right_key.as_str()).collect();
        let rule_names: Vec<&str> = records.iter().map(|r| r.rule_name.as_str()).collect();
        let confidences: Vec<f64> = records.iter().map(|r| r.confidence).collect();
        let timestamps: Vec<i64> = records
            .iter()
            .map(|r| r.matched_at.timestamp_micros())
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(match_id_strings.iter().map(|s| s.as_str()).collect::<Vec<_>>())) as ArrayRef,
                Arc::new(StringArray::from(left_keys)) as ArrayRef,
                Arc::new(StringArray::from(right_keys)) as ArrayRef,
                Arc::new(StringArray::from(rule_names)) as ArrayRef,
                Arc::new(Float64Array::from(confidences)) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC")) as ArrayRef,
            ],
        )?;

        let file = File::create(&output_path)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        info!(
            "Wrote {} matched records to {:?}",
            records.len(),
            output_path
        );
        Ok(output_path)
    }

    /// Write unmatched records to parquet
    pub fn write_unmatched(
        &self,
        run_id: &Uuid,
        records: &[UnmatchedRecord],
        side: &str, // "left" or "right"
    ) -> Result<PathBuf> {
        let run_path = self.run_path(run_id);
        let filename = format!("unmatched_{}.parquet", side);
        let output_path = run_path.join(&filename);

        let schema = Arc::new(Schema::new(vec![
            Field::new("record_key", DataType::Utf8, false),
            Field::new("attempted_rules", DataType::Utf8, false), // JSON array as string
            Field::new("closest_candidate", DataType::Utf8, true),
            Field::new("rejection_reason", DataType::Utf8, false),
        ]));

        let record_keys: Vec<&str> = records.iter().map(|r| r.record_key.as_str()).collect();
        let attempted_rules: Vec<String> = records
            .iter()
            .map(|r| serde_json::to_string(&r.attempted_rules).unwrap_or_default())
            .collect();
        let closest_candidates: Vec<Option<&str>> = records
            .iter()
            .map(|r| r.closest_candidate.as_deref())
            .collect();
        let rejection_reasons: Vec<&str> = records
            .iter()
            .map(|r| r.rejection_reason.as_str())
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(record_keys)) as ArrayRef,
                Arc::new(StringArray::from(attempted_rules.iter().map(|s| s.as_str()).collect::<Vec<_>>())) as ArrayRef,
                Arc::new(StringArray::from(closest_candidates)) as ArrayRef,
                Arc::new(StringArray::from(rejection_reasons)) as ArrayRef,
            ],
        )?;

        let file = File::create(&output_path)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        info!(
            "Wrote {} unmatched {} records to {:?}",
            records.len(),
            side,
            output_path
        );
        Ok(output_path)
    }

    /// Get the latest run path
    pub fn latest_run(&self) -> Result<Option<PathBuf>> {
        let runs_path = self.base_path.join("runs");
        if !runs_path.exists() {
            return Ok(None);
        }

        let mut entries: Vec<_> = fs::read_dir(&runs_path)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();

        // Sort by modification time (newest first)
        entries.sort_by(|a, b| {
            b.metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                .cmp(
                    &a.metadata()
                        .and_then(|m| m.modified())
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                )
        });

        Ok(entries.first().map(|e| e.path()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use crate::schema::RunStatus;
    use tempfile::tempdir;

    fn test_metadata() -> RunMetadata {
        RunMetadata::new(
            "test-recipe".to_string(),
            "file://left.csv".to_string(),
            "file://right.csv".to_string(),
        )
    }

    fn test_matched_records(n: usize) -> Vec<MatchedRecord> {
        (0..n)
            .map(|i| {
                MatchedRecord::new(
                    format!("LEFT-{}", i),
                    format!("RIGHT-{}", i),
                    "rule1".to_string(),
                    1.0 - (i as f64 * 0.1),
                )
            })
            .collect()
    }

    fn test_unmatched_records(n: usize) -> Vec<UnmatchedRecord> {
        (0..n)
            .map(|i| UnmatchedRecord {
                record_key: format!("ORPHAN-{}", i),
                attempted_rules: vec!["rule1".to_string(), "rule2".to_string()],
                closest_candidate: if i % 2 == 0 { Some(format!("NEAR-{}", i)) } else { None },
                rejection_reason: format!("No match within tolerance for record {}", i),
            })
            .collect()
    }

    #[test]
    fn test_evidence_store_creation() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        assert!(store.base_path.exists());
    }

    #[test]
    fn test_evidence_store_creates_directory() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("deep").join("nested").join("evidence");
        let store = EvidenceStore::new(&nested).unwrap();
        assert!(store.base_path.exists());
    }

    #[test]
    fn test_init_run() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();

        let run_path = store.init_run(&metadata).unwrap();
        assert!(run_path.exists());
        assert!(run_path.join("metadata.json").exists());
    }

    #[test]
    fn test_init_run_metadata_content() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        let run_path = store.init_run(&metadata).unwrap();

        let json = std::fs::read_to_string(run_path.join("metadata.json")).unwrap();
        let parsed: RunMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.recipe_id, "test-recipe");
        assert_eq!(parsed.left_source, "file://left.csv");
        assert_eq!(parsed.right_source, "file://right.csv");
        assert_eq!(parsed.status, RunStatus::Running);
        assert!(parsed.completed_at.is_none());
    }

    #[test]
    fn test_update_metadata() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let mut metadata = test_metadata();
        store.init_run(&metadata).unwrap();

        metadata.matched_count = 42;
        metadata.complete();
        store.update_metadata(&metadata).unwrap();

        let json = std::fs::read_to_string(store.run_path(&metadata.run_id).join("metadata.json")).unwrap();
        let parsed: RunMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.matched_count, 42);
        assert_eq!(parsed.status, RunStatus::Completed);
        assert!(parsed.completed_at.is_some());
    }

    #[test]
    fn test_run_path() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let id = Uuid::new_v4();
        let path = store.run_path(&id);
        assert!(path.to_str().unwrap().contains(&id.to_string()));
    }

    #[test]
    fn test_write_matched_records() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        store.init_run(&metadata).unwrap();

        let records = test_matched_records(3);
        let path = store.write_matched(&metadata.run_id, &records).unwrap();

        assert!(path.exists());
        assert!(path.to_str().unwrap().ends_with("matched.parquet"));
    }

    #[test]
    fn test_write_matched_records_parquet_readable() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        store.init_run(&metadata).unwrap();

        let records = test_matched_records(5);
        let path = store.write_matched(&metadata.run_id, &records).unwrap();

        // Read back the parquet file
        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();

        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();
            // Verify schema
            assert_eq!(batch.num_columns(), 6);
            assert_eq!(batch.schema().field(0).name(), "match_id");
            assert_eq!(batch.schema().field(1).name(), "left_key");
            assert_eq!(batch.schema().field(2).name(), "right_key");
            assert_eq!(batch.schema().field(3).name(), "rule_name");
            assert_eq!(batch.schema().field(4).name(), "confidence");
            assert_eq!(batch.schema().field(5).name(), "matched_at");
        }
        assert_eq!(total_rows, 5);
    }

    #[test]
    fn test_write_unmatched_left_records() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        store.init_run(&metadata).unwrap();

        let records = test_unmatched_records(4);
        let path = store.write_unmatched(&metadata.run_id, &records, "left").unwrap();

        assert!(path.exists());
        assert!(path.to_str().unwrap().ends_with("unmatched_left.parquet"));
    }

    #[test]
    fn test_write_unmatched_right_records() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        store.init_run(&metadata).unwrap();

        let records = test_unmatched_records(2);
        let path = store.write_unmatched(&metadata.run_id, &records, "right").unwrap();

        assert!(path.exists());
        assert!(path.to_str().unwrap().ends_with("unmatched_right.parquet"));
    }

    #[test]
    fn test_write_unmatched_parquet_readable() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        store.init_run(&metadata).unwrap();

        let records = test_unmatched_records(3);
        let path = store.write_unmatched(&metadata.run_id, &records, "left").unwrap();

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();

        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();
            assert_eq!(batch.num_columns(), 4);
            assert_eq!(batch.schema().field(0).name(), "record_key");
            assert_eq!(batch.schema().field(1).name(), "attempted_rules");
            assert_eq!(batch.schema().field(2).name(), "closest_candidate");
            assert_eq!(batch.schema().field(3).name(), "rejection_reason");
        }
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_write_empty_matched_records() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        store.init_run(&metadata).unwrap();

        let path = store.write_matched(&metadata.run_id, &[]).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_write_empty_unmatched_records() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        store.init_run(&metadata).unwrap();

        let path = store.write_unmatched(&metadata.run_id, &[], "left").unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_latest_run_empty() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let result = store.latest_run().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_latest_run_single() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        let run_path = store.init_run(&metadata).unwrap();

        let latest = store.latest_run().unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap(), run_path);
    }

    #[test]
    fn test_latest_run_multiple() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();

        // Create first run
        let meta1 = test_metadata();
        store.init_run(&meta1).unwrap();

        // Brief sleep to ensure different modification times
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Create second run
        let meta2 = test_metadata();
        let run_path2 = store.init_run(&meta2).unwrap();

        let latest = store.latest_run().unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap(), run_path2);
    }

    #[test]
    fn test_matched_record_round_trip() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        store.init_run(&metadata).unwrap();

        let records = vec![
            MatchedRecord::new("INV-001".to_string(), "PAY-001".to_string(), "exact_match".to_string(), 1.0),
            MatchedRecord::new("INV-002".to_string(), "PAY-002".to_string(), "fuzzy_match".to_string(), 0.85),
        ];
        let path = store.write_matched(&metadata.run_id, &records).unwrap();

        // Read back and verify content
        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();

        let batch = &batches[0];
        let left_keys = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(left_keys.value(0), "INV-001");
        assert_eq!(left_keys.value(1), "INV-002");

        let rule_names = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(rule_names.value(0), "exact_match");
        assert_eq!(rule_names.value(1), "fuzzy_match");

        let confidences = batch.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((confidences.value(0) - 1.0).abs() < f64::EPSILON);
        assert!((confidences.value(1) - 0.85).abs() < f64::EPSILON);
    }

    #[test]
    fn test_unmatched_with_nullable_closest_candidate() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        store.init_run(&metadata).unwrap();

        let records = vec![
            UnmatchedRecord {
                record_key: "K1".to_string(),
                attempted_rules: vec!["r1".to_string()],
                closest_candidate: Some("NEAR-1".to_string()),
                rejection_reason: "tolerance exceeded".to_string(),
            },
            UnmatchedRecord {
                record_key: "K2".to_string(),
                attempted_rules: vec!["r1".to_string()],
                closest_candidate: None,
                rejection_reason: "no candidates".to_string(),
            },
        ];
        let path = store.write_unmatched(&metadata.run_id, &records, "left").unwrap();

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();

        let batch = &batches[0];
        let candidates = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(candidates.value(0), "NEAR-1");
        assert!(arrow::array::Array::is_null(candidates, 1));
    }

    #[test]
    fn test_large_batch_write() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        let metadata = test_metadata();
        store.init_run(&metadata).unwrap();

        let records = test_matched_records(1000);
        let path = store.write_matched(&metadata.run_id, &records).unwrap();
        assert!(path.exists());

        // Verify row count
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();
        let total: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(total, 1000);
    }
}
