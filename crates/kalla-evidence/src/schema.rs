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
