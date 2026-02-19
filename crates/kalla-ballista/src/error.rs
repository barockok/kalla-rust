//! Typed errors for the ballista runner crate.

use std::fmt;

/// Errors that can occur during job execution in the runner.
#[derive(Debug)]
pub enum RunnerError {
    /// Failed to register a data source with the engine.
    SourceRegistrationFailed(String),
    /// The match SQL query failed during execution.
    MatchSqlFailed(String),
    /// Writing evidence (matched/unmatched records) failed.
    EvidenceWriteFailed(String),
    /// A callback to the API server failed.
    CallbackFailed(String),
    /// The DataFusion/Ballista engine encountered an error.
    EngineFailed(String),
}

impl fmt::Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunnerError::SourceRegistrationFailed(msg) => {
                write!(f, "source registration failed: {}", msg)
            }
            RunnerError::MatchSqlFailed(msg) => write!(f, "match SQL failed: {}", msg),
            RunnerError::EvidenceWriteFailed(msg) => {
                write!(f, "evidence write failed: {}", msg)
            }
            RunnerError::CallbackFailed(msg) => write!(f, "callback failed: {}", msg),
            RunnerError::EngineFailed(msg) => write!(f, "engine failed: {}", msg),
        }
    }
}

impl std::error::Error for RunnerError {}

impl From<anyhow::Error> for RunnerError {
    fn from(e: anyhow::Error) -> Self {
        RunnerError::EngineFailed(e.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for RunnerError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        RunnerError::EngineFailed(e.to_string())
    }
}
