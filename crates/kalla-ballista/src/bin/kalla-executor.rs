//! Custom Ballista executor binary with Kalla's physical codec registered.
//!
//! This binary wraps the standard Ballista executor and injects
//! [`KallaPhysicalCodec`] so that custom execution plan nodes
//! (`PostgresScanExec`, `CsvRangeScanExec`) received from the scheduler
//! can be deserialized and executed on remote workers.
//!
//! # Environment variables
//!
//! | Variable         | Default     | Description                              |
//! |-----------------|-------------|------------------------------------------|
//! | `BIND_HOST`     | `0.0.0.0`  | Host address the executor binds to        |
//! | `BIND_PORT`     | `50051`     | Flight port the executor listens on       |
//! | `BIND_GRPC_PORT`| `50052`     | gRPC port the executor listens on         |
//! | `SCHEDULER_HOST`| `localhost` | Hostname of the Ballista scheduler        |
//! | `SCHEDULER_PORT`| `50050`     | Port of the Ballista scheduler            |
//! | `EXTERNAL_HOST` | (auto)      | Hostname executors advertise to scheduler |

use std::sync::Arc;

use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};

use kalla_ballista::codec::KallaPhysicalCodec;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Build executor config with our custom physical codec.
    let mut config = ExecutorProcessConfig::default();
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));

    // Allow overriding network settings via env vars (useful for Docker).
    if let Ok(host) = std::env::var("BIND_HOST") {
        config.bind_host = host;
    } else {
        // Default to 0.0.0.0 for container deployments.
        config.bind_host = "0.0.0.0".into();
    }
    if let Ok(port) = std::env::var("BIND_PORT") {
        config.port = port.parse()?;
    }
    if let Ok(port) = std::env::var("BIND_GRPC_PORT") {
        config.grpc_port = port.parse()?;
    }
    if let Ok(host) = std::env::var("SCHEDULER_HOST") {
        config.scheduler_host = host;
    }
    if let Ok(port) = std::env::var("SCHEDULER_PORT") {
        config.scheduler_port = port.parse()?;
    }
    if let Ok(host) = std::env::var("EXTERNAL_HOST") {
        config.external_host = Some(host);
    }

    tracing::info!(
        scheduler = %format!("{}:{}", config.scheduler_host, config.scheduler_port),
        bind = %format!("{}:{}", config.bind_host, config.port),
        "Starting kalla-executor"
    );

    // Start the executor process (blocks until shutdown).
    start_executor_process(Arc::new(config)).await?;

    Ok(())
}
