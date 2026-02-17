//! Reaper — background task that reclaims stale (heartbeat-expired) jobs.

use sqlx::PgPool;
use std::sync::Arc;
use tracing::{info, warn};

use crate::metrics::WorkerMetrics;
use crate::queue::QueueClient;

/// Spawn reaper as a background tokio task.
pub fn spawn_reaper(
    pool: Arc<PgPool>,
    _queue: Arc<QueueClient>,
    metrics: WorkerMetrics,
    interval_secs: u64,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            if let Err(e) = reap_stale_jobs(&pool, &metrics).await {
                warn!("Reaper error: {}", e);
            }
        }
    });
}

async fn reap_stale_jobs(pool: &PgPool, metrics: &WorkerMetrics) -> anyhow::Result<()> {
    // Reclaim retryable jobs
    let reclaimed: Vec<(uuid::Uuid, uuid::Uuid, String)> = sqlx::query_as(
        "UPDATE jobs
         SET status = 'pending', claimed_by = NULL
         WHERE status = 'claimed'
           AND last_heartbeat < now() - (timeout_seconds || ' seconds')::interval
           AND attempts < max_attempts
         RETURNING job_id, run_id, job_type",
    )
    .fetch_all(pool)
    .await?;

    for (job_id, _run_id, job_type) in &reclaimed {
        info!("Reaper reclaimed stale job {} (type: {})", job_id, job_type);
        metrics.reaper_reclaimed.inc();
    }

    // Fail jobs that exceeded max attempts
    let failed: Vec<(uuid::Uuid,)> = sqlx::query_as(
        "UPDATE jobs SET status = 'failed'
         WHERE status = 'claimed'
           AND last_heartbeat < now() - (timeout_seconds || ' seconds')::interval
           AND attempts >= max_attempts
         RETURNING job_id",
    )
    .fetch_all(pool)
    .await?;

    for (job_id,) in &failed {
        warn!("Reaper failed job {}", job_id);
        metrics.reaper_failed.inc();
    }

    Ok(())
}
