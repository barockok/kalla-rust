//! Custom Ballista scheduler binary with Kalla's physical codec registered.
//!
//! This binary wraps the standard Ballista scheduler and injects
//! [`KallaPhysicalCodec`] so that custom execution plan nodes
//! (`PostgresScanExec`, `CsvRangeScanExec`) can be serialized and
//! deserialized when distributing work across the cluster.

use std::net::SocketAddr;
use std::sync::Arc;

use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;

use kalla_ballista::codec::KallaPhysicalCodec;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Build scheduler config with our custom physical codec.
    let mut config = SchedulerConfig::default();
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));

    // Derive the bind address from the config defaults.
    let addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port).parse()?;

    // Create an in-memory cluster backend (the default).
    let cluster = BallistaCluster::new_from_config(&config).await?;

    // Start the scheduler gRPC server (blocks until shutdown).
    start_server(cluster, addr, Arc::new(config)).await?;

    Ok(())
}
