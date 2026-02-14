//! Evidence store schema definitions

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Metadata for a reconciliation run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetadata {
    /// Unique run identifier
    pub run_id: Uuid,

    /// Recipe ID used for this run
    pub recipe_id: String,

    /// When the run started
    pub started_at: DateTime<Utc>,

    /// When the run completed
    pub completed_at: Option<DateTime<Utc>>,

    /// Left source URI
    pub left_source: String,

    /// Right source URI
    pub right_source: String,

    /// Total records in left source
    pub left_record_count: u64,

    /// Total records in right source
    pub right_record_count: u64,

    /// Number of matched records
    pub matched_count: u64,

    /// Number of unmatched left records
    pub unmatched_left_count: u64,

    /// Number of unmatched right records
    pub unmatched_right_count: u64,

    /// Run status
    pub status: RunStatus,
}

/// Status of a reconciliation run
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
}

/// A matched record entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchedRecord {
    /// Unique match identifier
    pub match_id: Uuid,

    /// Primary key from left source
    pub left_key: String,

    /// Primary key from right source
    pub right_key: String,

    /// Name of the rule that triggered the match
    pub rule_name: String,

    /// Confidence score (0.0 - 1.0, where 1.0 = exact match)
    pub confidence: f64,

    /// Timestamp when matched
    pub matched_at: DateTime<Utc>,
}

/// An unmatched record entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnmatchedRecord {
    /// Primary key of the orphan record
    pub record_key: String,

    /// Rules that were attempted
    pub attempted_rules: Vec<String>,

    /// Closest candidate key (if any near-match was found)
    pub closest_candidate: Option<String>,

    /// Reason for rejection/non-match
    pub rejection_reason: String,
}

impl RunMetadata {
    pub fn new(recipe_id: String, left_source: String, right_source: String) -> Self {
        Self {
            run_id: Uuid::new_v4(),
            recipe_id,
            started_at: Utc::now(),
            completed_at: None,
            left_source,
            right_source,
            left_record_count: 0,
            right_record_count: 0,
            matched_count: 0,
            unmatched_left_count: 0,
            unmatched_right_count: 0,
            status: RunStatus::Running,
        }
    }

    pub fn complete(&mut self) {
        self.completed_at = Some(Utc::now());
        self.status = RunStatus::Completed;
    }

    pub fn fail(&mut self) {
        self.completed_at = Some(Utc::now());
        self.status = RunStatus::Failed;
    }
}

impl MatchedRecord {
    pub fn new(left_key: String, right_key: String, rule_name: String, confidence: f64) -> Self {
        Self {
            match_id: Uuid::new_v4(),
            left_key,
            right_key,
            rule_name,
            confidence,
            matched_at: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_metadata_new() {
        let meta = RunMetadata::new(
            "recipe-1".to_string(),
            "file://left.csv".to_string(),
            "file://right.csv".to_string(),
        );
        assert_eq!(meta.recipe_id, "recipe-1");
        assert_eq!(meta.left_source, "file://left.csv");
        assert_eq!(meta.right_source, "file://right.csv");
        assert_eq!(meta.status, RunStatus::Running);
        assert!(meta.completed_at.is_none());
        assert_eq!(meta.left_record_count, 0);
        assert_eq!(meta.right_record_count, 0);
        assert_eq!(meta.matched_count, 0);
        assert_eq!(meta.unmatched_left_count, 0);
        assert_eq!(meta.unmatched_right_count, 0);
    }

    #[test]
    fn test_run_metadata_complete() {
        let mut meta = RunMetadata::new("r".to_string(), "l".to_string(), "r".to_string());
        assert_eq!(meta.status, RunStatus::Running);
        meta.complete();
        assert_eq!(meta.status, RunStatus::Completed);
        assert!(meta.completed_at.is_some());
    }

    #[test]
    fn test_run_metadata_fail() {
        let mut meta = RunMetadata::new("r".to_string(), "l".to_string(), "r".to_string());
        meta.fail();
        assert_eq!(meta.status, RunStatus::Failed);
        assert!(meta.completed_at.is_some());
    }

    #[test]
    fn test_run_metadata_serialization() {
        let meta = RunMetadata::new("recipe".to_string(), "left".to_string(), "right".to_string());
        let json = serde_json::to_string(&meta).unwrap();
        let parsed: RunMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.recipe_id, meta.recipe_id);
        assert_eq!(parsed.run_id, meta.run_id);
        assert_eq!(parsed.status, RunStatus::Running);
    }

    #[test]
    fn test_run_status_serialization() {
        let json = serde_json::to_string(&RunStatus::Running).unwrap();
        assert_eq!(json, "\"running\"");
        let json = serde_json::to_string(&RunStatus::Completed).unwrap();
        assert_eq!(json, "\"completed\"");
        let json = serde_json::to_string(&RunStatus::Failed).unwrap();
        assert_eq!(json, "\"failed\"");

        let parsed: RunStatus = serde_json::from_str("\"running\"").unwrap();
        assert_eq!(parsed, RunStatus::Running);
    }

    #[test]
    fn test_matched_record_new() {
        let record = MatchedRecord::new(
            "LEFT-1".to_string(),
            "RIGHT-1".to_string(),
            "exact_match".to_string(),
            0.95,
        );
        assert_eq!(record.left_key, "LEFT-1");
        assert_eq!(record.right_key, "RIGHT-1");
        assert_eq!(record.rule_name, "exact_match");
        assert!((record.confidence - 0.95).abs() < f64::EPSILON);
    }

    #[test]
    fn test_matched_record_serialization() {
        let record = MatchedRecord::new("L".to_string(), "R".to_string(), "rule".to_string(), 1.0);
        let json = serde_json::to_string(&record).unwrap();
        let parsed: MatchedRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.left_key, "L");
        assert_eq!(parsed.match_id, record.match_id);
    }

    #[test]
    fn test_unmatched_record_serialization() {
        let record = UnmatchedRecord {
            record_key: "ORPHAN-1".to_string(),
            attempted_rules: vec!["r1".to_string(), "r2".to_string()],
            closest_candidate: Some("NEAR-1".to_string()),
            rejection_reason: "Outside tolerance".to_string(),
        };
        let json = serde_json::to_string(&record).unwrap();
        let parsed: UnmatchedRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.record_key, "ORPHAN-1");
        assert_eq!(parsed.attempted_rules.len(), 2);
        assert_eq!(parsed.closest_candidate, Some("NEAR-1".to_string()));
    }

    #[test]
    fn test_unmatched_record_no_candidate() {
        let record = UnmatchedRecord {
            record_key: "K".to_string(),
            attempted_rules: vec![],
            closest_candidate: None,
            rejection_reason: "No candidates".to_string(),
        };
        let json = serde_json::to_string(&record).unwrap();
        let parsed: UnmatchedRecord = serde_json::from_str(&json).unwrap();
        assert!(parsed.closest_candidate.is_none());
    }

    #[test]
    fn test_run_metadata_unique_ids() {
        let m1 = RunMetadata::new("r".to_string(), "l".to_string(), "r".to_string());
        let m2 = RunMetadata::new("r".to_string(), "l".to_string(), "r".to_string());
        assert_ne!(m1.run_id, m2.run_id);
    }

    #[test]
    fn test_matched_record_unique_ids() {
        let r1 = MatchedRecord::new("L".to_string(), "R".to_string(), "r".to_string(), 1.0);
        let r2 = MatchedRecord::new("L".to_string(), "R".to_string(), "r".to_string(), 1.0);
        assert_ne!(r1.match_id, r2.match_id);
    }
}
