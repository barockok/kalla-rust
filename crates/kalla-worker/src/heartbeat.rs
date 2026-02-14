//! Heartbeat loop — updates last_heartbeat in Postgres for active jobs.

use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{debug, warn};
use uuid::Uuid;

/// Spawns a heartbeat loop that updates the job's last_heartbeat column
/// every `interval_secs` seconds. Returns a `watch::Sender` — drop it to
/// stop the heartbeat.
pub fn spawn_heartbeat(pool: Arc<PgPool>, job_id: Uuid, interval_secs: u64) -> watch::Sender<()> {
    let (stop_tx, mut stop_rx) = watch::channel(());

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let result = sqlx::query(
                        "UPDATE jobs SET last_heartbeat = now() WHERE job_id = $1"
                    )
                    .bind(job_id)
                    .execute(pool.as_ref())
                    .await;

                    match result {
                        Ok(_) => debug!("Heartbeat sent for job {}", job_id),
                        Err(e) => warn!("Heartbeat failed for job {}: {}", job_id, e),
                    }
                }
                _ = stop_rx.changed() => {
                    debug!("Heartbeat stopped for job {}", job_id);
                    break;
                }
            }
        }
    });

    stop_tx
}
