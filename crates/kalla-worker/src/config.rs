//! Worker configuration from environment variables.

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub nats_url: String,
    pub database_url: String,
    pub metrics_port: u16,
    // Staging config
    pub max_parallel_chunks: usize,
    pub chunk_threshold_rows: u64,
    // Job health
    pub heartbeat_interval_secs: u64,
    pub reaper_interval_secs: u64,
    // S3 / staging
    pub staging_bucket: String,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            worker_id: std::env::var("WORKER_ID")
                .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string()),
            nats_url: std::env::var("NATS_URL").context("NATS_URL required")?,
            database_url: std::env::var("DATABASE_URL").context("DATABASE_URL required")?,
            metrics_port: std::env::var("METRICS_PORT")
                .unwrap_or_else(|_| "9090".to_string())
                .parse()
                .context("Invalid METRICS_PORT")?,
            max_parallel_chunks: std::env::var("MAX_PARALLEL_CHUNKS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .context("Invalid MAX_PARALLEL_CHUNKS")?,
            chunk_threshold_rows: std::env::var("CHUNK_THRESHOLD_ROWS")
                .unwrap_or_else(|_| "1000000".to_string())
                .parse()
                .context("Invalid CHUNK_THRESHOLD_ROWS")?,
            heartbeat_interval_secs: std::env::var("HEARTBEAT_INTERVAL_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .context("Invalid HEARTBEAT_INTERVAL_SECS")?,
            reaper_interval_secs: std::env::var("REAPER_INTERVAL_SECS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .context("Invalid REAPER_INTERVAL_SECS")?,
            staging_bucket: std::env::var("STAGING_BUCKET")
                .unwrap_or_else(|_| "kalla-staging".to_string()),
        })
    }
}
