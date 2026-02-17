//! Job loop — pulls messages from NATS queue and dispatches to handler.

use anyhow::Result;
use futures::StreamExt;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::WorkerConfig;
use crate::exec;
use crate::heartbeat::spawn_heartbeat;
use crate::metrics::{JobTypeLabel, WorkerMetrics};
use crate::queue::{JobMessage, QueueClient};

/// Run the main job loop. Pulls from the exec queue.
pub async fn run_job_loop(
    config: WorkerConfig,
    pool: Arc<PgPool>,
    queue: Arc<QueueClient>,
    metrics: WorkerMetrics,
) -> Result<()> {
    let exec_consumer = queue.exec_consumer(&config.worker_id).await?;

    info!("Job loop started for worker {}", config.worker_id);

    let mut exec_messages = exec_consumer.messages().await?;

    loop {
        // Update queue depth metric
        if let Ok(depth) = queue.exec_queue_depth().await {
            metrics.exec_queue_depth.set(depth as i64);
        }

        match exec_messages.next().await {
            Some(Ok(msg)) => {
                let payload: Result<JobMessage, _> = serde_json::from_slice(&msg.payload);
                match payload {
                    Ok(job) => {
                        metrics.active_jobs.inc();
                        if let Err(e) = handle_job(&config, &pool, &metrics, job).await {
                            error!("Job failed: {}", e);
                        }
                        metrics.active_jobs.dec();
                        msg.ack().await.ok();
                    }
                    Err(e) => {
                        warn!("Invalid exec message: {}", e);
                        msg.ack().await.ok();
                    }
                }
            }
            Some(Err(e)) => warn!("Exec consumer error: {}", e),
            None => break,
        }
    }

    Ok(())
}

async fn handle_job(
    config: &WorkerConfig,
    pool: &PgPool,
    metrics: &WorkerMetrics,
    job: JobMessage,
) -> Result<()> {
    match job {
        JobMessage::Exec {
            job_id,
            run_id,
            ref recipe_json,
            ref source_uris,
            ref callback_url,
        } => {
            claim_job(pool, job_id, &config.worker_id).await?;
            let _heartbeat = spawn_heartbeat(
                Arc::new(pool.clone()),
                job_id,
                config.heartbeat_interval_secs,
            );

            exec::handle_exec(
                config,
                pool,
                run_id,
                job_id,
                recipe_json,
                source_uris,
                callback_url.as_deref(),
            )
            .await?;

            metrics
                .jobs_completed
                .get_or_create(&JobTypeLabel("exec".to_string()))
                .inc();
        }
    }
    Ok(())
}

async fn claim_job(pool: &PgPool, job_id: Uuid, worker_id: &str) -> Result<()> {
    sqlx::query(
        "UPDATE jobs SET status = 'claimed', claimed_by = $2,
         claimed_at = now(), last_heartbeat = now(), attempts = attempts + 1
         WHERE job_id = $1",
    )
    .bind(job_id)
    .bind(worker_id)
    .execute(pool)
    .await?;
    Ok(())
}
