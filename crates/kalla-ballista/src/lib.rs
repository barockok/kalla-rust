pub mod codec;
pub mod csv_range_scan_exec;
pub mod postgres_scan_exec;
pub mod scan_lazy;

use std::net::SocketAddr;
use std::sync::Arc;

use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;

use codec::KallaPhysicalCodec;

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

/// Options for starting the Ballista scheduler.
pub struct SchedulerOpts {
    pub bind_host: String,
    pub grpc_port: u16,
}

/// Start the Ballista scheduler with the Kalla physical codec.
///
/// Blocks until the scheduler shuts down.
#[allow(clippy::field_reassign_with_default)]
pub async fn start_scheduler(opts: SchedulerOpts) -> anyhow::Result<()> {
    let mut config = SchedulerConfig::default();
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));
    config.bind_host = opts.bind_host;
    config.bind_port = opts.grpc_port;

    let addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port).parse()?;
    let cluster = BallistaCluster::new_from_config(&config).await?;

    tracing::info!("Starting kalla-scheduler on {addr}");
    start_server(cluster, addr, Arc::new(config)).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

/// Options for starting a Ballista executor.
pub struct ExecutorOpts {
    pub bind_host: String,
    pub flight_port: u16,
    pub grpc_port: u16,
    pub scheduler_host: String,
    pub scheduler_port: u16,
    pub external_host: Option<String>,
}

/// Start a Ballista executor with the Kalla physical codec.
///
/// Blocks until the executor shuts down.
#[allow(clippy::field_reassign_with_default)]
pub async fn start_executor(opts: ExecutorOpts) -> anyhow::Result<()> {
    let mut config = ExecutorProcessConfig::default();
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));
    config.bind_host = opts.bind_host;
    config.port = opts.flight_port;
    config.grpc_port = opts.grpc_port;
    config.scheduler_host = opts.scheduler_host;
    config.scheduler_port = opts.scheduler_port;
    config.external_host = opts.external_host;

    tracing::info!(
        scheduler = %format!("{}:{}", config.scheduler_host, config.scheduler_port),
        bind = %format!("{}:{}", config.bind_host, config.port),
        "Starting kalla-executor"
    );

    start_executor_process(Arc::new(config)).await?;

    Ok(())
}
