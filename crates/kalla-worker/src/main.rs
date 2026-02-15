//! kalla-worker binary — dual-mode job worker for Kalla.
//!
//! - **Single mode** (no NATS_URL): Axum HTTP server accepting jobs at POST /api/jobs
//! - **Scaled mode** (NATS_URL set): Consumes jobs from NATS JetStream

mod config;
mod exec;
mod health;
mod heartbeat;
mod http_api;
mod job_loop;
mod metrics;
mod queue;
mod reaper;
mod stage;

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use config::{WorkerConfig, WorkerMode};
use health::HealthState;
use http_api::CallbackClient;
use metrics::WorkerMetrics;
use queue::QueueClient;

/// Shared state for the HTTP job submission endpoint (single mode).
pub struct WorkerState {
    pub job_tx: mpsc::Sender<http_api::JobRequest>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let config = WorkerConfig::from_env()?;
    let mode = config.mode();
    info!(
        "Starting kalla-worker {} in {:?} mode",
        config.worker_id, mode
    );

    // Metrics
    let worker_metrics = WorkerMetrics::new();

    // Health/readiness state
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let health_state = Arc::new(HealthState {
        metrics: worker_metrics.clone(),
        ready: ready.clone(),
    });

    match mode {
        WorkerMode::Single => run_single_mode(config, health_state, worker_metrics).await,
        WorkerMode::Scaled => run_scaled_mode(config, health_state, worker_metrics).await,
    }
}

/// Single mode: Axum server with health + job submission, process via mpsc channel.
async fn run_single_mode(
    config: WorkerConfig,
    health_state: Arc<HealthState>,
    _metrics: WorkerMetrics,
) -> Result<()> {
    let (job_tx, mut job_rx) = mpsc::channel::<http_api::JobRequest>(32);

    let worker_state = Arc::new(WorkerState { job_tx });

    // Build combined router: health routes + job submission
    let health_router = health::health_router(health_state);
    let job_router = http_api::job_router(worker_state);
    let app = health_router.merge(job_router);

    let port = config.metrics_port;
    let addr = format!("0.0.0.0:{}", port);
    info!("Single-mode HTTP server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;

    // Spawn the HTTP server
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let callback = CallbackClient::new();

    // Process jobs from the channel
    while let Some(job) = job_rx.recv().await {
        let run_id = job.run_id;
        info!("Processing job for run {}", run_id);

        match exec::handle_http_job(&config, &callback, job).await {
            Ok(result) => {
                info!(
                    "Run {} completed: {} matched, {} unmatched_left, {} unmatched_right",
                    run_id, result.matched, result.unmatched_left, result.unmatched_right
                );
            }
            Err(e) => {
                tracing::error!("Run {} failed: {}", run_id, e);
            }
        }
    }

    Ok(())
}

/// Scaled mode: Connect to NATS + Postgres, run job loop (existing behavior).
async fn run_scaled_mode(
    config: WorkerConfig,
    health_state: Arc<HealthState>,
    worker_metrics: WorkerMetrics,
) -> Result<()> {
    let nats_url = config
        .nats_url
        .as_ref()
        .expect("NATS_URL required for scaled mode");
    let database_url = config
        .database_url
        .as_ref()
        .expect("DATABASE_URL required for scaled mode");

    // Connect to Postgres
    let pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?,
    );
    info!("Connected to database");

    // Connect to NATS
    let queue = Arc::new(QueueClient::connect(nats_url).await?);
    info!("Connected to NATS at {}", nats_url);

    // Start metrics/health HTTP server
    let metrics_port = config.metrics_port;
    let health_router = health::health_router(health_state);
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{}", metrics_port);
        info!("Metrics server listening on {}", addr);
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
        axum::serve(listener, health_router).await.unwrap();
    });

    // Start reaper background task
    reaper::spawn_reaper(
        pool.clone(),
        queue.clone(),
        worker_metrics.clone(),
        config.reaper_interval_secs,
    );
    info!(
        "Reaper started (interval: {}s)",
        config.reaper_interval_secs
    );

    // Run main job loop (blocks forever)
    job_loop::run_job_loop(config, pool, queue, worker_metrics).await?;

    Ok(())
}
