//! Job loop — pulls messages from NATS queues and dispatches to handlers.

use anyhow::Result;
use futures::StreamExt;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::WorkerConfig;
use crate::heartbeat::spawn_heartbeat;
use crate::metrics::{JobTypeLabel, WorkerMetrics};
use crate::queue::{JobMessage, QueueClient};
use crate::{exec, stage};

/// Run the main job loop. Pulls from both stage and exec queues.
pub async fn run_job_loop(
    config: WorkerConfig,
    pool: Arc<PgPool>,
    queue: Arc<QueueClient>,
    metrics: WorkerMetrics,
) -> Result<()> {
    let stage_consumer = queue.stage_consumer(&config.worker_id).await?;
    let exec_consumer = queue.exec_consumer(&config.worker_id).await?;

    info!("Job loop started for worker {}", config.worker_id);

    // Pull from both consumers concurrently
    let mut stage_messages = stage_consumer.messages().await?;
    let mut exec_messages = exec_consumer.messages().await?;

    loop {
        // Update queue depth metrics
        if let Ok(depth) = queue.stage_queue_depth().await {
            metrics.stage_queue_depth.set(depth as i64);
        }
        if let Ok(depth) = queue.exec_queue_depth().await {
            metrics.exec_queue_depth.set(depth as i64);
        }

        tokio::select! {
            Some(msg) = stage_messages.next() => {
                match msg {
                    Ok(msg) => {
                        let payload: Result<JobMessage, _> = serde_json::from_slice(&msg.payload);
                        match payload {
                            Ok(job) => {
                                metrics.active_jobs.inc();
                                if let Err(e) = handle_job(&config, &pool, &queue, &metrics, job).await {
                                    error!("Job failed: {}", e);
                                }
                                metrics.active_jobs.dec();
                                msg.ack().await.ok();
                            }
                            Err(e) => {
                                warn!("Invalid stage message: {}", e);
                                msg.ack().await.ok();
                            }
                        }
                    }
                    Err(e) => warn!("Stage consumer error: {}", e),
                }
            }
            Some(msg) = exec_messages.next() => {
                match msg {
                    Ok(msg) => {
                        let payload: Result<JobMessage, _> = serde_json::from_slice(&msg.payload);
                        match payload {
                            Ok(job) => {
                                metrics.active_jobs.inc();
                                if let Err(e) = handle_job(&config, &pool, &queue, &metrics, job).await {
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
                    Err(e) => warn!("Exec consumer error: {}", e),
                }
            }
        }
    }
}

async fn handle_job(
    config: &WorkerConfig,
    pool: &PgPool,
    queue: &QueueClient,
    metrics: &WorkerMetrics,
    job: JobMessage,
) -> Result<()> {
    match job {
        JobMessage::StagePlan {
            job_id,
            run_id,
            ref source_uri,
            ref source_alias,
            ref partition_key,
        } => {
            claim_job(pool, job_id, &config.worker_id).await?;
            let _heartbeat = spawn_heartbeat(
                Arc::new(pool.clone()),
                job_id,
                config.heartbeat_interval_secs,
            );

            stage::handle_stage_plan(
                pool,
                queue,
                config,
                run_id,
                job_id,
                source_uri,
                source_alias,
                partition_key.as_deref(),
            )
            .await?;

            metrics
                .jobs_completed
                .get_or_create(&JobTypeLabel("stage_plan".to_string()))
                .inc();
        }
        JobMessage::StageChunk {
            job_id,
            run_id,
            ref source_uri,
            ref source_alias,
            offset,
            limit,
            ref output_path,
            ..
        } => {
            claim_job(pool, job_id, &config.worker_id).await?;
            let _heartbeat = spawn_heartbeat(
                Arc::new(pool.clone()),
                job_id,
                config.heartbeat_interval_secs,
            );

            let rows = stage::handle_stage_chunk(
                pool,
                queue,
                config,
                run_id,
                job_id,
                source_uri,
                source_alias,
                offset,
                limit,
                output_path,
            )
            .await?;

            metrics.rows_processed.inc_by(rows);
            metrics
                .jobs_completed
                .get_or_create(&JobTypeLabel("stage_chunk".to_string()))
                .inc();
        }
        JobMessage::Exec {
            job_id,
            run_id,
            ref recipe_json,
            ref staged_sources,
            ref callback_url,
            ref source_uris,
        } => {
            claim_job(pool, job_id, &config.worker_id).await?;
            let _heartbeat = spawn_heartbeat(
                Arc::new(pool.clone()),
                job_id,
                config.heartbeat_interval_secs,
            );

            exec::handle_exec(
                pool,
                run_id,
                job_id,
                recipe_json,
                staged_sources,
                callback_url.as_deref(),
                source_uris.as_deref(),
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
