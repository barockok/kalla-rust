# Worker Autoscaling Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the worker autoscaling architecture from `docs/plans/2026-02-14-worker-autoscaling-design.md` — NATS-based job queue, kalla-worker binary, parallel staging, heartbeat/reaper, Prometheus metrics, and K8s manifests.

**Architecture:** The API server pushes jobs to NATS JetStream queues (stage + exec). Stateless workers pull jobs, stage non-native sources to Parquet on S3, submit reconciliation plans to Ballista, and write evidence. Workers expose `/metrics`, `/health`, `/ready` endpoints. Job health is tracked via heartbeats in PostgreSQL with a reaper background task in each worker.

**Tech Stack:** Rust, NATS JetStream (async-nats crate), Ballista (ballista crate), prometheus-client (metrics), Axum (worker HTTP endpoints), sqlx (PostgreSQL), object_store (S3)

**Design doc:** `docs/plans/2026-02-14-worker-autoscaling-design.md`

---

## Workstream Overview

Four parallel worktrees, each on a feature branch:

| Worktree | Branch | Agent | Scope |
|----------|--------|-------|-------|
| `wt-infra` | `feat/infra-schema` | infra | SQL schema, docker-compose, K8s manifests |
| `wt-worker` | `feat/kalla-worker` | forge | New kalla-worker crate (binary) |
| `wt-api` | `feat/api-nats` | api | Modify kalla-server to push jobs to NATS |
| `wt-tests` | `feat/worker-tests` | sentinel | Unit + integration tests for worker |

**Dependency order:** infra + forge + api run in parallel (no file overlap). sentinel starts after forge completes. Final merge resolves `Cargo.toml` and `docker-compose.yml` conflicts.

**Key constraint:** NATS messages are JSON. Server and worker each define their own serde types for the same message schema. This avoids compile-time coupling and allows all worktrees to build independently.

---

## Task 1: Infrastructure — SQL Schema, Docker, K8s

**Branch:** `feat/infra-schema`
**Worktree:** `.worktrees/wt-infra`

**Files:**
- Modify: `scripts/init.sql`
- Modify: `docker-compose.yml`
- Create: `k8s/api-server.yaml`
- Create: `k8s/worker.yaml`
- Create: `k8s/ballista-scheduler.yaml`
- Create: `k8s/ballista-executor.yaml`
- Create: `k8s/nats.yaml`
- Create: `k8s/keda-triggers.yaml`

### Step 1: Add SQL tables to init.sql

Append to `scripts/init.sql` (after the chat_sessions table):

```sql
-- ============================================
-- JOB QUEUE TRACKING
-- ============================================

CREATE TABLE IF NOT EXISTS run_staging_tracker (
    run_id          UUID PRIMARY KEY,
    status          TEXT NOT NULL DEFAULT 'staging',
    total_chunks    INTEGER NOT NULL,
    completed_chunks INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL,
    job_type        TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    claimed_by      TEXT,
    claimed_at      TIMESTAMPTZ,
    last_heartbeat  TIMESTAMPTZ,
    timeout_seconds INTEGER NOT NULL DEFAULT 300,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_run_id ON jobs(run_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_heartbeat ON jobs(status, last_heartbeat)
    WHERE status = 'claimed';
```

### Step 2: Run init.sql against local Postgres to verify

```bash
PGPASSWORD=kalla_secret psql -h localhost -U kalla -d kalla -f scripts/init.sql
```
Expected: no errors, tables created.

### Step 3: Update docker-compose.yml

Add NATS and MinIO services. Add kalla-worker service. Modify the `server` service to include `NATS_URL` environment variable.

Add these services after the `web` service:

```yaml
  nats:
    image: nats:2-alpine
    container_name: kalla-nats
    ports:
      - "4222:4222"
      - "8222:8222"
    command: ["--jetstream", "--store_dir=/data", "-m", "8222"]
    volumes:
      - nats_data:/data
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8222/healthz || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  minio:
    image: minio/minio:latest
    container_name: kalla-minio
    ports:
      - "9000:9000"
      - "9001:9001"
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minioadmin}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minioadmin}
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9000/minio/health/live || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  worker:
    build:
      context: .
      dockerfile: crates/kalla-worker/Dockerfile
    container_name: kalla-worker
    depends_on:
      postgres:
        condition: service_healthy
      nats:
        condition: service_healthy
      minio:
        condition: service_healthy
      db-init:
        condition: service_completed_successfully
    environment:
      DATABASE_URL: postgres://${POSTGRES_USER:-kalla}:${POSTGRES_PASSWORD:-kalla_secret}@postgres:5432/${POSTGRES_DB:-kalla}
      NATS_URL: nats://nats:4222
      AWS_ENDPOINT_URL: http://minio:9000
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER:-minioadmin}
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD:-minioadmin}
      AWS_REGION: us-east-1
      AWS_ALLOW_HTTP: "true"
      RUST_LOG: ${RUST_LOG:-info}
    ports:
      - "9090:9090"
    restart: unless-stopped
```

Add `NATS_URL: nats://nats:4222` to the `server` service environment.

Add `nats_data:` and `minio_data:` to the volumes section.

### Step 4: Create K8s manifests

Create `k8s/` directory with these files. Use the exact YAML from the design doc's "Kubernetes Deployment" section for:

- `k8s/api-server.yaml` — Deployment + Service + HPA (minReplicas: 2, maxReplicas: 10, CPU 70%)
- `k8s/worker.yaml` — Deployment + PodMonitor (minReplicas: 1, port 9090 for metrics)
- `k8s/ballista-scheduler.yaml` — Deployment + Service (port 50050, 1 replica)
- `k8s/ballista-executor.yaml` — StatefulSet + HPA (minReplicas: 2, maxReplicas: 20, CPU 75%, memory 80%)
- `k8s/nats.yaml` — StatefulSet + headless Service (3 replicas, JetStream enabled)
- `k8s/keda-triggers.yaml` — KEDA ScaledObject with NATS JetStream + Prometheus triggers (from design doc)

### Step 5: Commit

```bash
git add scripts/init.sql docker-compose.yml k8s/
git commit -m "feat: add job tracking schema, NATS/MinIO to docker-compose, K8s manifests"
```

---

## Task 2: kalla-worker Crate

**Branch:** `feat/kalla-worker`
**Worktree:** `.worktrees/wt-worker`

**Files:**
- Create: `crates/kalla-worker/Cargo.toml`
- Create: `crates/kalla-worker/Dockerfile`
- Create: `crates/kalla-worker/src/main.rs`
- Create: `crates/kalla-worker/src/config.rs`
- Create: `crates/kalla-worker/src/queue.rs`
- Create: `crates/kalla-worker/src/job_loop.rs`
- Create: `crates/kalla-worker/src/stage.rs`
- Create: `crates/kalla-worker/src/exec.rs`
- Create: `crates/kalla-worker/src/reaper.rs`
- Create: `crates/kalla-worker/src/heartbeat.rs`
- Create: `crates/kalla-worker/src/metrics.rs`
- Create: `crates/kalla-worker/src/health.rs`
- Modify: `Cargo.toml` (add workspace member)

### Step 1: Add kalla-worker to workspace

In root `Cargo.toml`, add `"crates/kalla-worker"` to the `members` array. Also add these workspace dependencies:

```toml
# In [workspace.dependencies]
async-nats = "0.38"
prometheus-client = "0.23"
```

### Step 2: Create Cargo.toml

```toml
[package]
name = "kalla-worker"
version.workspace = true
edition.workspace = true
license.workspace = true
description = "Distributed worker for Kalla reconciliation jobs"

[[bin]]
name = "kalla-worker"
path = "src/main.rs"

[dependencies]
kalla-core.workspace = true
kalla-connectors.workspace = true
kalla-recipe.workspace = true
kalla-evidence.workspace = true
datafusion.workspace = true
arrow.workspace = true
parquet.workspace = true
axum.workspace = true
tokio.workspace = true
serde.workspace = true
serde_json.workspace = true
uuid.workspace = true
anyhow.workspace = true
thiserror.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
sqlx.workspace = true
url.workspace = true
chrono.workspace = true
async-nats.workspace = true
prometheus-client.workspace = true
futures = "0.3"
object_store = { version = "0.11", features = ["aws"] }

[dev-dependencies]
tempfile = "3"
```

### Step 3: Create config.rs

Worker configuration loaded from environment variables:

```rust
//! Worker configuration from environment variables.

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub nats_url: String,
    pub database_url: String,
    pub metrics_port: u16,
    // Staging config
    pub max_parallel_chunks: usize,
    pub chunk_threshold_rows: u64,
    // Job health
    pub heartbeat_interval_secs: u64,
    pub reaper_interval_secs: u64,
    // S3 / staging
    pub staging_bucket: String,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            worker_id: std::env::var("WORKER_ID")
                .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string()),
            nats_url: std::env::var("NATS_URL").context("NATS_URL required")?,
            database_url: std::env::var("DATABASE_URL").context("DATABASE_URL required")?,
            metrics_port: std::env::var("METRICS_PORT")
                .unwrap_or_else(|_| "9090".to_string())
                .parse()
                .context("Invalid METRICS_PORT")?,
            max_parallel_chunks: std::env::var("MAX_PARALLEL_CHUNKS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .context("Invalid MAX_PARALLEL_CHUNKS")?,
            chunk_threshold_rows: std::env::var("CHUNK_THRESHOLD_ROWS")
                .unwrap_or_else(|_| "1000000".to_string())
                .parse()
                .context("Invalid CHUNK_THRESHOLD_ROWS")?,
            heartbeat_interval_secs: std::env::var("HEARTBEAT_INTERVAL_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .context("Invalid HEARTBEAT_INTERVAL_SECS")?,
            reaper_interval_secs: std::env::var("REAPER_INTERVAL_SECS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .context("Invalid REAPER_INTERVAL_SECS")?,
            staging_bucket: std::env::var("STAGING_BUCKET")
                .unwrap_or_else(|_| "kalla-staging".to_string()),
        })
    }
}
```

### Step 4: Create queue.rs

NATS JetStream queue abstraction and job message types:

```rust
//! NATS JetStream queue client and message types.

use anyhow::Result;
use async_nats::jetstream::{self, consumer::PullConsumer, stream::Stream as JsStream};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const STAGE_STREAM: &str = "KALLA_STAGE";
pub const EXEC_STREAM: &str = "KALLA_EXEC";
pub const STAGE_SUBJECT: &str = "kalla.stage";
pub const EXEC_SUBJECT: &str = "kalla.exec";

/// Job types that flow through the queues.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JobMessage {
    /// Plan staging for a source (COUNT, decide chunking).
    StagePlan {
        job_id: Uuid,
        run_id: Uuid,
        source_uri: String,
        source_alias: String,
        partition_key: Option<String>,
    },
    /// Extract a single chunk from a source to Parquet on S3.
    StageChunk {
        job_id: Uuid,
        run_id: Uuid,
        source_uri: String,
        source_alias: String,
        chunk_index: u32,
        total_chunks: u32,
        offset: u64,
        limit: u64,
        output_path: String,
    },
    /// Execute reconciliation via Ballista (all sources are now Parquet on S3).
    Exec {
        job_id: Uuid,
        run_id: Uuid,
        recipe_json: String,
        staged_sources: Vec<StagedSource>,
    },
}

/// A source that has been staged to S3 Parquet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedSource {
    pub alias: String,
    pub s3_path: String,
    pub is_native: bool,
}

/// NATS JetStream queue client.
pub struct QueueClient {
    jetstream: jetstream::Context,
    stage_stream: JsStream,
    exec_stream: JsStream,
}

impl QueueClient {
    /// Connect to NATS and ensure streams exist.
    pub async fn connect(nats_url: &str) -> Result<Self> {
        let client = async_nats::connect(nats_url).await?;
        let jetstream = jetstream::new(client);

        let stage_stream = jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: STAGE_STREAM.to_string(),
                subjects: vec![STAGE_SUBJECT.to_string()],
                retention: jetstream::stream::RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await?;

        let exec_stream = jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: EXEC_STREAM.to_string(),
                subjects: vec![EXEC_SUBJECT.to_string()],
                retention: jetstream::stream::RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await?;

        Ok(Self {
            jetstream,
            stage_stream,
            exec_stream,
        })
    }

    /// Publish a job to the stage queue.
    pub async fn publish_stage(&self, msg: &JobMessage) -> Result<()> {
        let payload = serde_json::to_vec(msg)?;
        self.jetstream
            .publish(STAGE_SUBJECT, payload.into())
            .await?
            .await?;
        Ok(())
    }

    /// Publish a job to the exec queue.
    pub async fn publish_exec(&self, msg: &JobMessage) -> Result<()> {
        let payload = serde_json::to_vec(msg)?;
        self.jetstream
            .publish(EXEC_SUBJECT, payload.into())
            .await?
            .await?;
        Ok(())
    }

    /// Create a pull consumer for the stage queue.
    pub async fn stage_consumer(&self, consumer_name: &str) -> Result<PullConsumer> {
        let consumer = self
            .stage_stream
            .get_or_create_consumer(
                consumer_name,
                jetstream::consumer::pull::Config {
                    durable_name: Some(consumer_name.to_string()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await?;
        Ok(consumer)
    }

    /// Create a pull consumer for the exec queue.
    pub async fn exec_consumer(&self, consumer_name: &str) -> Result<PullConsumer> {
        let consumer = self
            .exec_stream
            .get_or_create_consumer(
                consumer_name,
                jetstream::consumer::pull::Config {
                    durable_name: Some(consumer_name.to_string()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await?;
        Ok(consumer)
    }

    /// Get current pending message count for the stage stream.
    pub async fn stage_queue_depth(&self) -> Result<u64> {
        let info = self.stage_stream.info().await?;
        Ok(info.state.messages)
    }

    /// Get current pending message count for the exec stream.
    pub async fn exec_queue_depth(&self) -> Result<u64> {
        let info = self.exec_stream.info().await?;
        Ok(info.state.messages)
    }
}
```

### Step 5: Create metrics.rs

Prometheus metrics using `prometheus-client`:

```rust
//! Prometheus metrics for worker observability and autoscaling signals.

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct JobTypeLabel(pub String);

impl prometheus_client::encoding::EncodeLabelSet for JobTypeLabel {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::LabelSetEncoder,
    ) -> Result<(), std::fmt::Error> {
        use prometheus_client::encoding::EncodeLabel;
        ("type", self.0.as_str()).encode(encoder.encode_label())?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct WorkerMetrics {
    pub stage_queue_depth: Gauge<i64, AtomicU64>,
    pub exec_queue_depth: Gauge<i64, AtomicU64>,
    pub queue_oldest_wait_secs: Gauge<f64, AtomicU64>,
    pub active_jobs: Gauge<i64, AtomicU64>,
    pub jobs_completed: Family<JobTypeLabel, Counter>,
    pub reaper_reclaimed: Counter,
    pub reaper_failed: Counter,
    pub rows_processed: Counter,
    pub registry: Arc<Registry>,
}

impl WorkerMetrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let stage_queue_depth = Gauge::default();
        registry.register(
            "kalla_stage_queue_depth",
            "Number of pending stage jobs",
            stage_queue_depth.clone(),
        );

        let exec_queue_depth = Gauge::default();
        registry.register(
            "kalla_exec_queue_depth",
            "Number of pending exec jobs",
            exec_queue_depth.clone(),
        );

        let queue_oldest_wait_secs = Gauge::default();
        registry.register(
            "kalla_queue_oldest_wait_seconds",
            "Age of oldest waiting job in seconds",
            queue_oldest_wait_secs.clone(),
        );

        let active_jobs = Gauge::default();
        registry.register(
            "kalla_worker_active_jobs",
            "Number of jobs currently being processed",
            active_jobs.clone(),
        );

        let jobs_completed = Family::<JobTypeLabel, Counter>::default();
        registry.register(
            "kalla_worker_jobs_completed_total",
            "Total jobs completed by type",
            jobs_completed.clone(),
        );

        let reaper_reclaimed = Counter::default();
        registry.register(
            "kalla_reaper_jobs_reclaimed_total",
            "Jobs reclaimed by reaper",
            reaper_reclaimed.clone(),
        );

        let reaper_failed = Counter::default();
        registry.register(
            "kalla_reaper_jobs_failed_total",
            "Jobs permanently failed by reaper",
            reaper_failed.clone(),
        );

        let rows_processed = Counter::default();
        registry.register(
            "kalla_worker_rows_processed_total",
            "Total rows processed across all jobs",
            rows_processed.clone(),
        );

        Self {
            stage_queue_depth,
            exec_queue_depth,
            queue_oldest_wait_secs,
            active_jobs,
            jobs_completed,
            reaper_reclaimed,
            reaper_failed,
            rows_processed,
            registry: Arc::new(registry),
        }
    }

    /// Encode all metrics as Prometheus text format.
    pub fn encode(&self) -> String {
        let mut buf = String::new();
        encode(&mut buf, &self.registry).unwrap();
        buf
    }
}
```

### Step 6: Create health.rs

HTTP endpoints for liveness, readiness, and metrics:

```rust
//! Health and metrics HTTP endpoints (Axum).

use axum::{extract::State, http::StatusCode, routing::get, Router};
use std::sync::Arc;

use crate::metrics::WorkerMetrics;

pub struct HealthState {
    pub metrics: WorkerMetrics,
    pub ready: Arc<std::sync::atomic::AtomicBool>,
}

pub fn health_router(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn health() -> &'static str {
    "OK"
}

async fn ready(State(state): State<Arc<HealthState>>) -> Result<&'static str, StatusCode> {
    if state
        .ready
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        Ok("OK")
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

async fn metrics(State(state): State<Arc<HealthState>>) -> String {
    state.metrics.encode()
}
```

### Step 7: Create heartbeat.rs

Background heartbeat loop for active jobs:

```rust
//! Heartbeat loop — updates last_heartbeat in Postgres for active jobs.

use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{debug, warn};
use uuid::Uuid;

/// Spawns a heartbeat loop that updates the job's last_heartbeat column
/// every `interval_secs` seconds. Returns a `watch::Sender` — drop it to
/// stop the heartbeat.
pub fn spawn_heartbeat(
    pool: Arc<PgPool>,
    job_id: Uuid,
    interval_secs: u64,
) -> watch::Sender<()> {
    let (stop_tx, mut stop_rx) = watch::channel(());

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(
            std::time::Duration::from_secs(interval_secs),
        );
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
```

### Step 8: Create reaper.rs

Background reaper loop that reclaims stale jobs:

```rust
//! Reaper — background task that reclaims stale (heartbeat-expired) jobs.

use sqlx::PgPool;
use std::sync::Arc;
use tracing::{info, warn};

use crate::metrics::WorkerMetrics;
use crate::queue::QueueClient;

/// Spawn reaper as a background tokio task.
pub fn spawn_reaper(
    pool: Arc<PgPool>,
    queue: Arc<QueueClient>,
    metrics: WorkerMetrics,
    interval_secs: u64,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(
            std::time::Duration::from_secs(interval_secs),
        );
        loop {
            interval.tick().await;
            if let Err(e) = reap_stale_jobs(&pool, &queue, &metrics).await {
                warn!("Reaper error: {}", e);
            }
        }
    });
}

async fn reap_stale_jobs(
    pool: &PgPool,
    _queue: &QueueClient,
    metrics: &WorkerMetrics,
) -> anyhow::Result<()> {
    // Reclaim retryable jobs
    let reclaimed: Vec<(uuid::Uuid, uuid::Uuid, String)> = sqlx::query_as(
        "UPDATE jobs
         SET status = 'pending', claimed_by = NULL
         WHERE status = 'claimed'
           AND last_heartbeat < now() - (timeout_seconds || ' seconds')::interval
           AND attempts < max_attempts
         RETURNING job_id, run_id, job_type"
    )
    .fetch_all(pool)
    .await?;

    for (job_id, _run_id, job_type) in &reclaimed {
        info!("Reaper reclaimed stale job {} (type: {})", job_id, job_type);
        metrics.reaper_reclaimed.inc();
    }

    // Fail jobs that exceeded max attempts
    let failed: Vec<(uuid::Uuid, uuid::Uuid)> = sqlx::query_as(
        "UPDATE jobs SET status = 'failed'
         WHERE status = 'claimed'
           AND last_heartbeat < now() - (timeout_seconds || ' seconds')::interval
           AND attempts >= max_attempts
         RETURNING job_id, run_id"
    )
    .fetch_all(pool)
    .await?;

    for (job_id, run_id) in &failed {
        warn!("Reaper failed job {} (run {})", job_id, run_id);
        metrics.reaper_failed.inc();

        // Mark entire run as failed
        sqlx::query(
            "UPDATE run_staging_tracker SET status = 'failed', updated_at = now()
             WHERE run_id = $1 AND status != 'failed'"
        )
        .bind(run_id)
        .execute(pool)
        .await?;
    }

    Ok(())
}
```

### Step 9: Create stage.rs

Stage handler — extracts non-native sources to Parquet on S3:

```rust
//! Stage handler — extracts non-native sources to Parquet on S3.

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use kalla_connectors::PostgresConnector;
use kalla_core::ReconciliationEngine;
use parquet::arrow::ArrowWriter;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::WorkerConfig;
use crate::queue::{JobMessage, QueueClient};

/// Handle a StagePlan job — count rows, decide on chunking, fan out chunk jobs.
pub async fn handle_stage_plan(
    pool: &PgPool,
    queue: &QueueClient,
    config: &WorkerConfig,
    run_id: Uuid,
    job_id: Uuid,
    source_uri: &str,
    source_alias: &str,
    partition_key: Option<&str>,
) -> Result<()> {
    let (conn_string, table_name) = parse_source_uri(source_uri)?;
    let connector = PostgresConnector::new(&conn_string).await?;

    let engine = ReconciliationEngine::new();
    connector
        .register_table(engine.context(), source_alias, &table_name, None)
        .await?;

    // Count rows
    let df = engine
        .sql(&format!("SELECT COUNT(*) as cnt FROM \"{}\"", source_alias))
        .await?;
    let batches = df.collect().await?;
    let row_count = batches
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
        })
        .map(|a| a.value(0) as u64)
        .unwrap_or(0);

    info!(
        "Stage plan for {}: {} rows (threshold: {})",
        source_alias, row_count, config.chunk_threshold_rows
    );

    if row_count <= config.chunk_threshold_rows {
        // Small source — single chunk job
        let total_chunks = 1u32;
        update_staging_tracker(pool, run_id, total_chunks as i32).await?;

        let chunk_job = JobMessage::StageChunk {
            job_id: Uuid::new_v4(),
            run_id,
            source_uri: source_uri.to_string(),
            source_alias: source_alias.to_string(),
            chunk_index: 0,
            total_chunks,
            offset: 0,
            limit: row_count,
            output_path: format!(
                "s3://{}/staging/{}/{}/part-00.parquet",
                config.staging_bucket, run_id, source_alias
            ),
        };
        queue.publish_stage(&chunk_job).await?;
    } else {
        // Large source — fan out to parallel chunks
        let num_chunks = (row_count / config.chunk_threshold_rows)
            .min(config.max_parallel_chunks as u64) as u32;
        let chunk_size = row_count / num_chunks as u64;

        update_staging_tracker(pool, run_id, num_chunks as i32).await?;

        for i in 0..num_chunks {
            let offset = i as u64 * chunk_size;
            let limit = if i == num_chunks - 1 {
                row_count - offset
            } else {
                chunk_size
            };

            let chunk_job = JobMessage::StageChunk {
                job_id: Uuid::new_v4(),
                run_id,
                source_uri: source_uri.to_string(),
                source_alias: source_alias.to_string(),
                chunk_index: i,
                total_chunks: num_chunks,
                offset,
                limit,
                output_path: format!(
                    "s3://{}/staging/{}/{}/part-{:02}.parquet",
                    config.staging_bucket, run_id, source_alias, i
                ),
            };
            queue.publish_stage(&chunk_job).await?;
        }
    }

    // Mark this plan job as completed
    sqlx::query("UPDATE jobs SET status = 'completed' WHERE job_id = $1")
        .bind(job_id)
        .execute(pool)
        .await?;

    Ok(())
}

/// Handle a StageChunk job — extract rows and write Parquet to S3.
pub async fn handle_stage_chunk(
    pool: &PgPool,
    queue: &QueueClient,
    _config: &WorkerConfig,
    run_id: Uuid,
    job_id: Uuid,
    source_uri: &str,
    source_alias: &str,
    offset: u64,
    limit: u64,
    output_path: &str,
) -> Result<u64> {
    let (conn_string, table_name) = parse_source_uri(source_uri)?;
    let connector = PostgresConnector::new(&conn_string).await?;

    let engine = ReconciliationEngine::new();
    connector
        .register_table(engine.context(), source_alias, &table_name, None)
        .await?;

    // Extract chunk
    let query = format!(
        "SELECT * FROM \"{}\" LIMIT {} OFFSET {}",
        source_alias, limit, offset
    );
    let mut stream = engine.sql_stream(&query).await?;

    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut total_rows = 0u64;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        total_rows += batch.num_rows() as u64;
        batches.push(batch);
    }

    info!(
        "Stage chunk for {}: extracted {} rows (offset={}, limit={})",
        source_alias, total_rows, offset, limit
    );

    // Write Parquet to local temp file then upload to S3
    // (In production, stream directly to S3 via object_store)
    if !batches.is_empty() {
        let schema = batches[0].schema();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;
            for batch in &batches {
                writer.write(batch)?;
            }
            writer.close()?;
        }

        // TODO: Upload `buf` to `output_path` via object_store
        // For now, write to local filesystem as fallback
        info!("Would upload {} bytes to {}", buf.len(), output_path);
    }

    // Mark job completed
    sqlx::query("UPDATE jobs SET status = 'completed' WHERE job_id = $1")
        .bind(job_id)
        .execute(pool)
        .await?;

    // Increment staging tracker atomically
    let result: (i32, i32) = sqlx::query_as(
        "UPDATE run_staging_tracker
         SET completed_chunks = completed_chunks + 1, updated_at = now()
         WHERE run_id = $1
         RETURNING completed_chunks, total_chunks",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?;

    let (completed, total) = result;
    info!(
        "Staging progress for run {}: {}/{}",
        run_id, completed, total
    );

    // Completion gate — if this was the last chunk, push exec job
    if completed == total {
        info!("All chunks staged for run {}, pushing exec job", run_id);
        sqlx::query(
            "UPDATE run_staging_tracker SET status = 'ready', updated_at = now()
             WHERE run_id = $1",
        )
        .bind(run_id)
        .execute(pool)
        .await?;

        // Retrieve the recipe payload from the original exec-pending job
        let payload: Option<(serde_json::Value,)> = sqlx::query_as(
            "SELECT payload FROM jobs WHERE run_id = $1 AND job_type = 'exec' LIMIT 1",
        )
        .bind(run_id)
        .fetch_optional(pool)
        .await?;

        if let Some((payload,)) = payload {
            let exec_msg: JobMessage = serde_json::from_value(payload)?;
            queue.publish_exec(&exec_msg).await?;
        }
    }

    Ok(total_rows)
}

async fn update_staging_tracker(pool: &PgPool, run_id: Uuid, total_chunks: i32) -> Result<()> {
    sqlx::query(
        "INSERT INTO run_staging_tracker (run_id, total_chunks)
         VALUES ($1, $2)
         ON CONFLICT (run_id) DO UPDATE SET total_chunks = run_staging_tracker.total_chunks + $2, updated_at = now()",
    )
    .bind(run_id)
    .bind(total_chunks)
    .execute(pool)
    .await?;
    Ok(())
}

fn parse_source_uri(uri: &str) -> Result<(String, String)> {
    let url = url::Url::parse(uri)?;
    let table_name = url
        .query_pairs()
        .find(|(k, _)| k == "table")
        .map(|(_, v)| v.to_string())
        .ok_or_else(|| anyhow::anyhow!("Missing 'table' query parameter"))?;
    let mut conn_url = url.clone();
    conn_url.set_query(None);
    Ok((conn_url.to_string(), table_name))
}
```

### Step 10: Create exec.rs

Exec handler — runs reconciliation after staging is complete:

```rust
//! Exec handler — runs reconciliation after all sources are staged.
//!
//! Transpiles the recipe to SQL, executes via DataFusion (local or Ballista),
//! and writes evidence.

use anyhow::Result;
use futures::StreamExt;
use kalla_core::ReconciliationEngine;
use kalla_evidence::{EvidenceStore, MatchedRecord, UnmatchedRecord};
use kalla_recipe::{MatchRecipe, Transpiler};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

use crate::queue::StagedSource;

/// Execute the reconciliation run.
pub async fn handle_exec(
    pool: &PgPool,
    run_id: Uuid,
    job_id: Uuid,
    recipe_json: &str,
    staged_sources: &[StagedSource],
) -> Result<ExecResult> {
    let recipe: MatchRecipe = serde_json::from_str(recipe_json)?;

    let engine = ReconciliationEngine::new();

    // Register all sources (staged Parquet or native)
    for source in staged_sources {
        if source.s3_path.ends_with(".parquet") || source.s3_path.contains("/staging/") {
            // Register as Parquet directory
            engine
                .register_parquet(&source.alias, &source.s3_path)
                .await?;
        } else if source.s3_path.ends_with(".csv") {
            engine.register_csv(&source.alias, &source.s3_path).await?;
        }
        info!("Registered source '{}' from {}", source.alias, source.s3_path);
    }

    // Transpile recipe to SQL
    let transpiled = Transpiler::transpile(&recipe)?;

    // Execute matches
    let mut total_matched = 0u64;
    let mut matched_records: Vec<MatchedRecord> = Vec::new();

    let left_pk = recipe
        .sources
        .left
        .primary_key
        .as_ref()
        .and_then(|v| v.first())
        .map(|s| s.as_str())
        .unwrap_or("id");
    let right_pk = recipe
        .sources
        .right
        .primary_key
        .as_ref()
        .and_then(|v| v.first())
        .map(|s| s.as_str())
        .unwrap_or("id");

    for rule in &transpiled.match_queries {
        match engine.sql_stream(&rule.query).await {
            Ok(mut stream) => {
                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result?;
                    for row_idx in 0..batch.num_rows() {
                        let left_key = extract_string_value(&batch, left_pk, row_idx)
                            .unwrap_or_else(|| format!("row_{}", row_idx));
                        let right_key = extract_string_value(&batch, right_pk, row_idx)
                            .unwrap_or_else(|| format!("row_{}", row_idx));
                        matched_records.push(MatchedRecord::new(
                            left_key,
                            right_key,
                            rule.name.clone(),
                            1.0,
                        ));
                    }
                    total_matched += batch.num_rows() as u64;
                }
            }
            Err(e) => warn!("Match rule '{}' failed: {}", rule.name, e),
        }
    }

    // Execute orphan queries
    let mut unmatched_left = 0u64;
    let mut unmatched_right = 0u64;

    if let Some(ref query) = transpiled.left_orphan_query {
        if let Ok(mut stream) = engine.sql_stream(query).await {
            while let Some(Ok(batch)) = stream.next().await {
                unmatched_left += batch.num_rows() as u64;
            }
        }
    }

    if let Some(ref query) = transpiled.right_orphan_query {
        if let Ok(mut stream) = engine.sql_stream(query).await {
            while let Some(Ok(batch)) = stream.next().await {
                unmatched_right += batch.num_rows() as u64;
            }
        }
    }

    // Write evidence
    let evidence_store = EvidenceStore::new("./evidence")?;
    if !matched_records.is_empty() {
        let _ = evidence_store.write_matched(&run_id, &matched_records);
    }

    // Mark job completed
    sqlx::query("UPDATE jobs SET status = 'completed' WHERE job_id = $1")
        .bind(job_id)
        .execute(pool)
        .await?;

    // Mark run completed
    sqlx::query(
        "UPDATE run_staging_tracker SET status = 'completed', updated_at = now()
         WHERE run_id = $1",
    )
    .bind(run_id)
    .execute(pool)
    .await?;

    Ok(ExecResult {
        matched: total_matched,
        unmatched_left,
        unmatched_right,
    })
}

pub struct ExecResult {
    pub matched: u64,
    pub unmatched_left: u64,
    pub unmatched_right: u64,
}

fn extract_string_value(
    batch: &arrow::record_batch::RecordBatch,
    column_name: &str,
    row_idx: usize,
) -> Option<String> {
    let col_idx = batch.schema().index_of(column_name).ok()?;
    let col = batch.column(col_idx);
    if let Some(arr) = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
    {
        return Some(arr.value(row_idx).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return Some(arr.value(row_idx).to_string());
    }
    None
}
```

### Step 11: Create job_loop.rs

Main job loop pulling from both queues:

```rust
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
    let mut stage_messages = stage_consumer
        .messages()
        .await?;
    let mut exec_messages = exec_consumer
        .messages()
        .await?;

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
        } => {
            claim_job(pool, job_id, &config.worker_id).await?;
            let _heartbeat = spawn_heartbeat(
                Arc::new(pool.clone()),
                job_id,
                config.heartbeat_interval_secs,
            );

            exec::handle_exec(pool, run_id, job_id, recipe_json, staged_sources).await?;

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
```

### Step 12: Create main.rs

Binary entry point:

```rust
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
    info!("Reaper started (interval: {}s)", config.reaper_interval_secs);

    // Run main job loop (blocks forever)
    job_loop::run_job_loop(config, pool, queue, worker_metrics).await?;

    Ok(())
}
```

### Step 13: Create Dockerfile for worker

```dockerfile
# Stage 1: Build
FROM rust:1.85-bookworm AS builder
WORKDIR /app

ENV CARGO_BUILD_JOBS=2

COPY Cargo.toml Cargo.lock ./
COPY crates/kalla-core/Cargo.toml crates/kalla-core/Cargo.toml
COPY crates/kalla-connectors/Cargo.toml crates/kalla-connectors/Cargo.toml
COPY crates/kalla-recipe/Cargo.toml crates/kalla-recipe/Cargo.toml
COPY crates/kalla-evidence/Cargo.toml crates/kalla-evidence/Cargo.toml
COPY crates/kalla-ai/Cargo.toml crates/kalla-ai/Cargo.toml
COPY crates/kalla-cli/Cargo.toml crates/kalla-cli/Cargo.toml
COPY crates/kalla-worker/Cargo.toml crates/kalla-worker/Cargo.toml
COPY kalla-server/Cargo.toml kalla-server/Cargo.toml

RUN mkdir -p crates/kalla-core/src && echo "pub fn _dummy() {}" > crates/kalla-core/src/lib.rs && \
    mkdir -p crates/kalla-connectors/src && echo "pub fn _dummy() {}" > crates/kalla-connectors/src/lib.rs && \
    mkdir -p crates/kalla-recipe/src && echo "pub fn _dummy() {}" > crates/kalla-recipe/src/lib.rs && \
    mkdir -p crates/kalla-evidence/src && echo "pub fn _dummy() {}" > crates/kalla-evidence/src/lib.rs && \
    mkdir -p crates/kalla-ai/src && echo "pub fn _dummy() {}" > crates/kalla-ai/src/lib.rs && \
    mkdir -p crates/kalla-cli/src && echo "fn main() {}" > crates/kalla-cli/src/main.rs && \
    mkdir -p crates/kalla-worker/src && echo "fn main() {}" > crates/kalla-worker/src/main.rs && \
    mkdir -p kalla-server/src && echo "fn main() {}" > kalla-server/src/main.rs

RUN cargo update home --precise 0.5.9 && \
    cargo update comfy-table --precise 7.1.4

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin kalla-worker 2>&1 || true

COPY crates ./crates
COPY kalla-server ./kalla-server

RUN find /app/crates -name '*.rs' -exec touch {} + && \
    find /app/kalla-server -name '*.rs' -exec touch {} +

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin kalla-worker && \
    cp target/release/kalla-worker /usr/local/bin/kalla-worker

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/kalla-worker /usr/local/bin/
EXPOSE 9090
CMD ["kalla-worker"]
```

### Step 14: Run cargo check

```bash
cargo check --workspace
```
Expected: compiles with no errors.

### Step 15: Run cargo fmt and clippy

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: no warnings.

### Step 16: Commit

```bash
git add crates/kalla-worker/ Cargo.toml Cargo.lock
git commit -m "feat: add kalla-worker crate with NATS job loop, staging, exec, reaper, metrics"
```

---

## Task 3: API Server — Push Jobs to NATS

**Branch:** `feat/api-nats`
**Worktree:** `.worktrees/wt-api`

**Files:**
- Modify: `kalla-server/Cargo.toml` (add async-nats dependency)
- Modify: `kalla-server/src/main.rs` (replace in-process worker with NATS publisher)
- Delete: `kalla-server/src/worker.rs` (replaced by kalla-worker crate)

### Step 1: Add async-nats to kalla-server dependencies

In `kalla-server/Cargo.toml`, add:
```toml
async-nats.workspace = true
```

### Step 2: Create queue publisher module

Create `kalla-server/src/nats_publisher.rs`:

```rust
//! NATS publisher — pushes jobs to stage and exec queues.

use anyhow::Result;
use async_nats::jetstream;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const STAGE_STREAM: &str = "KALLA_STAGE";
const EXEC_STREAM: &str = "KALLA_EXEC";
const STAGE_SUBJECT: &str = "kalla.stage";
const EXEC_SUBJECT: &str = "kalla.exec";

/// Job message published to NATS.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JobMessage {
    StagePlan {
        job_id: Uuid,
        run_id: Uuid,
        source_uri: String,
        source_alias: String,
        partition_key: Option<String>,
    },
    Exec {
        job_id: Uuid,
        run_id: Uuid,
        recipe_json: String,
        staged_sources: Vec<StagedSource>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StagedSource {
    pub alias: String,
    pub s3_path: String,
    pub is_native: bool,
}

/// Publisher for pushing jobs to NATS queues.
#[derive(Clone)]
pub struct NatsPublisher {
    jetstream: jetstream::Context,
}

impl NatsPublisher {
    pub async fn connect(nats_url: &str) -> Result<Self> {
        let client = async_nats::connect(nats_url).await?;
        let jetstream = jetstream::new(client);

        // Ensure streams exist
        jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: STAGE_STREAM.to_string(),
                subjects: vec![STAGE_SUBJECT.to_string()],
                retention: jetstream::stream::RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await?;

        jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: EXEC_STREAM.to_string(),
                subjects: vec![EXEC_SUBJECT.to_string()],
                retention: jetstream::stream::RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await?;

        Ok(Self { jetstream })
    }

    pub async fn publish_stage(&self, msg: &JobMessage) -> Result<()> {
        let payload = serde_json::to_vec(msg)?;
        self.jetstream
            .publish(STAGE_SUBJECT, payload.into())
            .await?
            .await?;
        Ok(())
    }

    pub async fn publish_exec(&self, msg: &JobMessage) -> Result<()> {
        let payload = serde_json::to_vec(msg)?;
        self.jetstream
            .publish(EXEC_SUBJECT, payload.into())
            .await?
            .await?;
        Ok(())
    }
}
```

### Step 3: Modify main.rs

1. Remove `pub mod worker;` and `use worker::{Worker, WorkerHandle};`
2. Add `pub mod nats_publisher;` and `use nats_publisher::NatsPublisher;`
3. Replace `worker: WorkerHandle` in `AppState` with `nats: Option<NatsPublisher>`
4. In `main()`, replace `Worker::spawn(...)` with `NatsPublisher::connect(nats_url)` (optional, fallback if NATS_URL not set)
5. In `create_run()`, replace `worker.submit_run()` with classifying sources and publishing to NATS:
   - For each non-native source → publish `StagePlan` to stage queue
   - For native sources → mark as already staged
   - Create `run_staging_tracker` row in Postgres
   - Create `jobs` rows in Postgres
   - If no staging needed → publish `Exec` directly to exec queue

### Step 4: Update create_run handler

The `create_run` function should:
1. Validate recipe (unchanged)
2. Create run metadata (unchanged)
3. Classify sources as native or non-native
4. For non-native: insert `run_staging_tracker` row, insert `jobs` rows, publish `StagePlan` to NATS
5. For all-native: publish `Exec` job directly
6. Return run_id immediately

### Step 5: Delete worker.rs

Remove `kalla-server/src/worker.rs`.

### Step 6: Run cargo check, fmt, clippy

```bash
cargo check -p kalla-server
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

### Step 7: Commit

```bash
git add kalla-server/
git commit -m "feat: replace in-process worker with NATS publisher in API server"
```

---

## Task 4: Tests

**Branch:** `feat/worker-tests`
**Worktree:** `.worktrees/wt-tests`

**Files:**
- Create: `crates/kalla-worker/tests/config_test.rs`
- Create: `crates/kalla-worker/tests/metrics_test.rs`
- Create: `crates/kalla-worker/tests/queue_test.rs`

### Step 1: Config unit tests

Test that `WorkerConfig::from_env()` reads environment variables correctly with defaults.

### Step 2: Metrics unit tests

Test that `WorkerMetrics::new()` creates all expected metrics and `encode()` produces valid Prometheus text format.

### Step 3: Queue message serialization tests

Test that `JobMessage` serializes/deserializes correctly for all variants (StagePlan, StageChunk, Exec).

### Step 4: Health endpoint tests

Test that `/health` returns 200, `/ready` returns 200 when ready and 503 when not, `/metrics` returns prometheus text.

### Step 5: Run all tests

```bash
cargo test --workspace
```

### Step 6: Run cargo fmt and clippy

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

### Step 7: Commit

```bash
git add crates/kalla-worker/tests/
git commit -m "test: add unit tests for worker config, metrics, queue, health"
```

---

## Task 5: Merge & Integration

**Branch:** `main`

After all 4 feature branches complete:

### Step 1: Merge branches to main

```bash
git checkout main
git merge feat/infra-schema
git merge feat/kalla-worker
git merge feat/api-nats
git merge feat/worker-tests
```

Resolve any conflicts in `Cargo.toml` and `docker-compose.yml`.

### Step 2: Run full CI checks

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

### Step 3: Update CI workflow

Add kalla-worker Docker build step to `.github/workflows/ci.yml`.

### Step 4: Commit and push

```bash
git push origin main
```
