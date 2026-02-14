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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serialize env-mutating tests to avoid races.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env() {
        for key in [
            "WORKER_ID",
            "NATS_URL",
            "DATABASE_URL",
            "METRICS_PORT",
            "MAX_PARALLEL_CHUNKS",
            "CHUNK_THRESHOLD_ROWS",
            "HEARTBEAT_INTERVAL_SECS",
            "REAPER_INTERVAL_SECS",
            "STAGING_BUCKET",
        ] {
            unsafe { std::env::remove_var(key) };
        }
    }

    #[test]
    fn from_env_with_all_vars() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        unsafe {
            std::env::set_var("WORKER_ID", "test-worker-1");
            std::env::set_var("NATS_URL", "nats://localhost:4222");
            std::env::set_var("DATABASE_URL", "postgres://localhost/test");
            std::env::set_var("METRICS_PORT", "8080");
            std::env::set_var("MAX_PARALLEL_CHUNKS", "5");
            std::env::set_var("CHUNK_THRESHOLD_ROWS", "500000");
            std::env::set_var("HEARTBEAT_INTERVAL_SECS", "15");
            std::env::set_var("REAPER_INTERVAL_SECS", "45");
            std::env::set_var("STAGING_BUCKET", "my-bucket");
        }

        let config = WorkerConfig::from_env().unwrap();
        assert_eq!(config.worker_id, "test-worker-1");
        assert_eq!(config.nats_url, "nats://localhost:4222");
        assert_eq!(config.database_url, "postgres://localhost/test");
        assert_eq!(config.metrics_port, 8080);
        assert_eq!(config.max_parallel_chunks, 5);
        assert_eq!(config.chunk_threshold_rows, 500000);
        assert_eq!(config.heartbeat_interval_secs, 15);
        assert_eq!(config.reaper_interval_secs, 45);
        assert_eq!(config.staging_bucket, "my-bucket");

        clear_env();
    }

    #[test]
    fn from_env_uses_defaults() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        unsafe {
            std::env::set_var("NATS_URL", "nats://localhost:4222");
            std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        }

        let config = WorkerConfig::from_env().unwrap();
        // worker_id is a random UUID — just check it's non-empty
        assert!(!config.worker_id.is_empty());
        assert_eq!(config.metrics_port, 9090);
        assert_eq!(config.max_parallel_chunks, 10);
        assert_eq!(config.chunk_threshold_rows, 1_000_000);
        assert_eq!(config.heartbeat_interval_secs, 30);
        assert_eq!(config.reaper_interval_secs, 60);
        assert_eq!(config.staging_bucket, "kalla-staging");

        clear_env();
    }

    #[test]
    fn from_env_missing_nats_url() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        unsafe {
            std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        }

        let result = WorkerConfig::from_env();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("NATS_URL"),
            "Expected NATS_URL error, got: {err_msg}"
        );

        clear_env();
    }

    #[test]
    fn from_env_missing_database_url() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        unsafe {
            std::env::set_var("NATS_URL", "nats://localhost:4222");
        }

        let result = WorkerConfig::from_env();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("DATABASE_URL"),
            "Expected DATABASE_URL error, got: {err_msg}"
        );

        clear_env();
    }

    #[test]
    fn from_env_invalid_metrics_port() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        unsafe {
            std::env::set_var("NATS_URL", "nats://localhost:4222");
            std::env::set_var("DATABASE_URL", "postgres://localhost/test");
            std::env::set_var("METRICS_PORT", "not-a-number");
        }

        let result = WorkerConfig::from_env();
        assert!(result.is_err());

        clear_env();
    }
}
