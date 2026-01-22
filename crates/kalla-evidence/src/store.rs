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
    use tempfile::tempdir;

    #[test]
    fn test_evidence_store_creation() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();
        assert!(store.base_path.exists());
    }

    #[test]
    fn test_init_run() {
        let dir = tempdir().unwrap();
        let store = EvidenceStore::new(dir.path()).unwrap();

        let metadata = RunMetadata::new(
            "test-recipe".to_string(),
            "file://left.csv".to_string(),
            "file://right.csv".to_string(),
        );

        let run_path = store.init_run(&metadata).unwrap();
        assert!(run_path.exists());
        assert!(run_path.join("metadata.json").exists());
    }
}
