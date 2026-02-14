//! kalla-worker binary — distributed job worker for Kalla.

mod config;
mod exec;
mod health;
mod heartbeat;
mod job_loop;
mod metrics;
mod queue;
mod reaper;
mod stage;

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use config::WorkerConfig;
use health::HealthState;
use metrics::WorkerMetrics;
use queue::QueueClient;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let config = WorkerConfig::from_env()?;
    info!("Starting kalla-worker {}", config.worker_id);

    // Connect to Postgres
    let pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(5)
            .connect(&config.database_url)
            .await?,
    );
    info!("Connected to database");

    // Connect to NATS
    let queue = Arc::new(QueueClient::connect(&config.nats_url).await?);
    info!("Connected to NATS at {}", config.nats_url);

    // Metrics
    let worker_metrics = WorkerMetrics::new();

    // Health/readiness state
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let health_state = Arc::new(HealthState {
        metrics: worker_metrics.clone(),
        ready: ready.clone(),
    });

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
