pub mod codec;
pub mod error;
pub mod runner;
pub(crate) mod sources;

use std::net::SocketAddr;
use std::sync::Arc;

use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use datafusion::execution::FunctionRegistry;

use codec::KallaPhysicalCodec;

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

/// Options for starting the Ballista scheduler with the embedded HTTP runner.
pub struct SchedulerOpts {
    pub bind_host: String,
    pub grpc_port: u16,
    pub http_port: u16,
    pub partitions: usize,
    pub staging_path: String,
    /// Maximum number of jobs that can execute concurrently (default: 4).
    pub max_concurrent_jobs: usize,
}

/// Start the Ballista scheduler with the Kalla physical codec **and** the
/// embedded HTTP runner.  Both services run concurrently via `tokio::select!`.
///
/// Blocks until either service shuts down.
#[allow(clippy::field_reassign_with_default)]
pub async fn start_scheduler(opts: SchedulerOpts) -> anyhow::Result<()> {
    let mut config = SchedulerConfig::default();
    config.override_logical_codec = Some(Arc::new(codec::KallaLogicalCodec::new()));
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));
    config.override_session_builder = Some(Arc::new(|session_config| {
        let mut state = ballista_core::utils::default_session_builder(session_config)?;
        state.register_udf(Arc::new(kalla_core::udf::tolerance_match_udf()))?;
        Ok(state)
    }));
    config.bind_host = opts.bind_host.clone();
    config.bind_port = opts.grpc_port;

    let addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port).parse()?;
    let cluster = BallistaCluster::new_from_config(&config).await?;

    tracing::info!("Starting kalla-scheduler (gRPC) on {addr}");

    let runner_config = runner::RunnerConfig {
        grpc_port: opts.grpc_port,
        partitions: opts.partitions,
        staging_path: opts.staging_path,
        max_concurrent_jobs: opts.max_concurrent_jobs,
    };
    let http_addr = format!("{}:{}", opts.bind_host, opts.http_port);

    tokio::select! {
        res = start_server(cluster, addr, Arc::new(config)) => {
            res?;
        }
        res = runner::start_runner(&http_addr, runner_config) => {
            res?;
        }
    }

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
    config.override_logical_codec = Some(Arc::new(codec::KallaLogicalCodec::new()));
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));

    // Register Kalla UDFs in the executor's function registry so they are
    // available in the TaskContext when deserializing physical plans.
    let mut fn_registry = ballista_core::registry::BallistaFunctionRegistry::default();
    fn_registry.scalar_functions.insert(
        "tolerance_match".to_string(),
        Arc::new(kalla_core::udf::tolerance_match_udf()),
    );
    config.override_function_registry = Some(Arc::new(fn_registry));

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
