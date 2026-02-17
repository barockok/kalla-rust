//! Custom Ballista scheduler binary with Kalla's physical codec registered.
//!
//! This binary wraps the standard Ballista scheduler and injects
//! [`KallaPhysicalCodec`] so that custom execution plan nodes
//! (`PostgresScanExec`, `CsvRangeScanExec`) can be serialized and
//! deserialized when distributing work across the cluster.
//!
//! # Environment variables
//!
//! | Variable     | Default     | Description                        |
//! |-------------|-------------|------------------------------------|
//! | `BIND_HOST` | `0.0.0.0`  | Host address the scheduler binds to |
//! | `BIND_PORT` | `50050`     | Port the scheduler listens on       |

use std::net::SocketAddr;
use std::sync::Arc;

use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;

use kalla_ballista::codec::KallaPhysicalCodec;

#[tokio::main]
#[allow(clippy::field_reassign_with_default)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Build scheduler config with our custom physical codec.
    let mut config = SchedulerConfig::default();
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));

    // Allow overriding bind address via env vars (useful for Docker).
    if let Ok(host) = std::env::var("BIND_HOST") {
        config.bind_host = host;
    } else {
        // Default to 0.0.0.0 for container deployments.
        config.bind_host = "0.0.0.0".into();
    }
    if let Ok(port) = std::env::var("BIND_PORT") {
        config.bind_port = port.parse()?;
    }

    // Derive the bind address from the (possibly overridden) config.
    let addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port).parse()?;

    // Create an in-memory cluster backend (the default).
    let cluster = BallistaCluster::new_from_config(&config).await?;

    tracing::info!("Starting kalla-scheduler on {addr}");

    // Start the scheduler gRPC server (blocks until shutdown).
    start_server(cluster, addr, Arc::new(config)).await?;

    Ok(())
}
