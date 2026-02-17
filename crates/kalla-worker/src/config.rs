//! Worker configuration from environment variables.

use anyhow::{Context, Result};

/// Worker mode determined by configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerMode {
    /// Single mode — accepts jobs via HTTP, no NATS/Postgres app-DB.
    Single,
    /// Scaled mode — consumes jobs from NATS JetStream, uses app-DB for job tracking.
    Scaled,
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    /// None = single mode (HTTP). Some = scaled mode (NATS).
    pub nats_url: Option<String>,
    /// App database URL — only used in scaled mode for job tracking.
    pub database_url: Option<String>,
    /// Number of partitions per source for distributed reads.
    pub ballista_partitions: usize,
    pub metrics_port: u16,
    // Job health (scaled mode only)
    pub heartbeat_interval_secs: u64,
    pub reaper_interval_secs: u64,
    /// Local directory for staging files (single mode).
    pub staging_path: String,
    /// Optional Ballista scheduler URL for cluster mode (e.g., "df://localhost:50050").
    /// When set, uses cluster mode with true distributed execution.
    pub ballista_scheduler_url: Option<String>,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            worker_id: std::env::var("WORKER_ID")
                .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string()),
            nats_url: std::env::var("NATS_URL").ok(),
            database_url: std::env::var("DATABASE_URL").ok(),
            ballista_partitions: std::env::var("BALLISTA_PARTITIONS")
                .unwrap_or_else(|_| "4".to_string())
                .parse()
                .context("Invalid BALLISTA_PARTITIONS")?,
            metrics_port: std::env::var("METRICS_PORT")
                .unwrap_or_else(|_| "9090".to_string())
                .parse()
                .context("Invalid METRICS_PORT")?,
            heartbeat_interval_secs: std::env::var("HEARTBEAT_INTERVAL_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .context("Invalid HEARTBEAT_INTERVAL_SECS")?,
            reaper_interval_secs: std::env::var("REAPER_INTERVAL_SECS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .context("Invalid REAPER_INTERVAL_SECS")?,
            staging_path: std::env::var("STAGING_PATH").unwrap_or_else(|_| "./staging".to_string()),
            ballista_scheduler_url: std::env::var("BALLISTA_SCHEDULER_URL").ok(),
        })
    }

    /// Determine worker mode from configuration.
    pub fn mode(&self) -> WorkerMode {
        if self.nats_url.is_some() {
            WorkerMode::Scaled
        } else {
            WorkerMode::Single
        }
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
            "BALLISTA_PARTITIONS",
            "METRICS_PORT",
            "HEARTBEAT_INTERVAL_SECS",
            "REAPER_INTERVAL_SECS",
            "STAGING_PATH",
            "BALLISTA_SCHEDULER_URL",
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
            std::env::set_var("HEARTBEAT_INTERVAL_SECS", "15");
            std::env::set_var("REAPER_INTERVAL_SECS", "45");
            std::env::set_var("STAGING_PATH", "/data/staging");
        }

        let config = WorkerConfig::from_env().unwrap();
        assert_eq!(config.worker_id, "test-worker-1");
        assert_eq!(config.nats_url, Some("nats://localhost:4222".to_string()));
        assert_eq!(
            config.database_url,
            Some("postgres://localhost/test".to_string())
        );
        assert_eq!(config.metrics_port, 8080);
        assert_eq!(config.heartbeat_interval_secs, 15);
        assert_eq!(config.reaper_interval_secs, 45);
        assert_eq!(config.staging_path, "/data/staging");
        assert_eq!(config.mode(), WorkerMode::Scaled);

        clear_env();
    }

    #[test]
    fn from_env_single_mode_defaults() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        // No NATS_URL, no DATABASE_URL => single mode
        let config = WorkerConfig::from_env().unwrap();
        assert!(!config.worker_id.is_empty());
        assert_eq!(config.nats_url, None);
        assert_eq!(config.database_url, None);
        assert_eq!(config.metrics_port, 9090);
        assert_eq!(config.staging_path, "./staging");
        assert_eq!(config.ballista_partitions, 4);
        assert_eq!(config.mode(), WorkerMode::Single);

        clear_env();
    }

    #[test]
    fn from_env_ballista_partitions() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        let config = WorkerConfig::from_env().unwrap();
        assert_eq!(config.ballista_partitions, 4);

        clear_env();
    }

    #[test]
    fn from_env_scaled_mode() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        unsafe {
            std::env::set_var("NATS_URL", "nats://localhost:4222");
            std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        }

        let config = WorkerConfig::from_env().unwrap();
        assert_eq!(config.nats_url, Some("nats://localhost:4222".to_string()));
        assert_eq!(config.mode(), WorkerMode::Scaled);

        clear_env();
    }

    #[test]
    fn from_env_invalid_metrics_port() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        unsafe {
            std::env::set_var("METRICS_PORT", "not-a-number");
        }

        let result = WorkerConfig::from_env();
        assert!(result.is_err());

        clear_env();
    }

    #[test]
    fn from_env_ballista_scheduler_url() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        unsafe {
            std::env::set_var("BALLISTA_SCHEDULER_URL", "df://scheduler:50050");
        }

        let config = WorkerConfig::from_env().unwrap();
        assert_eq!(
            config.ballista_scheduler_url,
            Some("df://scheduler:50050".to_string())
        );

        clear_env();
    }
}
