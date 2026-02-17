# Unified `kallad` Binary Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace three binaries (`kalla-worker`, `kalla-scheduler`, `kalla-executor`) with a single `kallad` binary. Embed an HTTP job runner in the scheduler. Remove NATS and worker-side Postgres job tracking.

**Architecture:** Single `kallad` binary with two clap subcommands: `scheduler` (Ballista gRPC + Axum HTTP runner) and `executor` (Ballista executor). The scheduler auto-detects executor presence — with executors it distributes via Ballista, without it falls back to local DataFusion. NATS is removed; API calls the scheduler HTTP endpoint directly.

**Tech Stack:** Rust, clap 4, Axum 0.7, Ballista 44, DataFusion 44, tokio

**Design doc:** `docs/plans/2026-02-17-unified-kallad-binary-design.md`

---

## Dependency Graph

```
Task 1 (kallad crate skeleton)
  └─> Task 2 (extract scheduler/executor to lib)
       └─> Task 3 (HTTP runner module)
            └─> Task 4 (wire runner into scheduler + CLI)
                 └─> Task 5 (delete kalla-worker + NATS)
                      └─> Task 6 (update Docker + compose)
                           └─> Task 7 (update benchmarks)
                                └─> Task 8 (update CI + docs + init.sql)
                                     └─> Task 9 (final verification)
```

---

### Task 1: Create `kallad` crate skeleton with clap CLI

**Files:**
- Create: `crates/kallad/Cargo.toml`
- Create: `crates/kallad/src/main.rs`
- Modify: `Cargo.toml` (workspace root — add member)

**Step 1: Create Cargo.toml**

Create `crates/kallad/Cargo.toml`:

```toml
[package]
name = "kallad"
version.workspace = true
edition.workspace = true
license.workspace = true
description = "Kalla reconciliation daemon — scheduler and executor"

[[bin]]
name = "kallad"
path = "src/main.rs"

[dependencies]
kalla-ballista.workspace = true
clap.workspace = true
tokio.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
anyhow.workspace = true
```

**Step 2: Create main.rs with clap subcommands**

Create `crates/kallad/src/main.rs`:

```rust
//! kallad — Kalla reconciliation daemon.
//!
//! Single binary with two subcommands:
//! - `kallad scheduler` — Ballista scheduler + HTTP job runner
//! - `kallad executor`  — Ballista executor (scales independently)

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "kallad", about = "Kalla reconciliation daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the scheduler (Ballista gRPC + HTTP job runner)
    Scheduler {
        /// HTTP port for job submission API
        #[arg(long, default_value = "8080", env = "HTTP_PORT")]
        http_port: u16,
        /// gRPC port for Ballista scheduler
        #[arg(long, default_value = "50050", env = "GRPC_PORT")]
        grpc_port: u16,
        /// Bind host address
        #[arg(long, default_value = "0.0.0.0", env = "BIND_HOST")]
        bind_host: String,
        /// Number of partitions per source
        #[arg(long, default_value = "4", env = "BALLISTA_PARTITIONS")]
        partitions: usize,
        /// Local staging path for evidence files
        #[arg(long, default_value = "./staging", env = "STAGING_PATH")]
        staging_path: String,
    },
    /// Start a Ballista executor
    Executor {
        /// Scheduler host
        #[arg(long, default_value = "localhost", env = "SCHEDULER_HOST")]
        scheduler_host: String,
        /// Scheduler gRPC port
        #[arg(long, default_value = "50050", env = "SCHEDULER_PORT")]
        scheduler_port: u16,
        /// Flight port
        #[arg(long, default_value = "50051", env = "BIND_PORT")]
        flight_port: u16,
        /// gRPC port
        #[arg(long, default_value = "50052", env = "BIND_GRPC_PORT")]
        grpc_port: u16,
        /// Bind host address
        #[arg(long, default_value = "0.0.0.0", env = "BIND_HOST")]
        bind_host: String,
        /// External hostname advertised to scheduler
        #[arg(long, env = "EXTERNAL_HOST")]
        external_host: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Scheduler {
            http_port,
            grpc_port,
            bind_host,
            partitions,
            staging_path,
        } => {
            todo!("scheduler: http={http_port}, grpc={grpc_port}")
        }
        Command::Executor {
            scheduler_host,
            scheduler_port,
            flight_port,
            grpc_port,
            bind_host,
            external_host,
        } => {
            todo!("executor: scheduler={scheduler_host}:{scheduler_port}")
        }
    }
}
```

**Step 3: Add to workspace**

In root `Cargo.toml`, add `"crates/kallad"` to workspace members list.

**Step 4: Verify it builds**

Run: `cargo build --bin kallad`
Expected: Builds successfully (but panics at runtime with todo!).

**Step 5: Commit**

```bash
git add crates/kallad/ Cargo.toml Cargo.lock
git commit -m "feat: add kallad crate skeleton with clap subcommands"
```

---

### Task 2: Extract scheduler and executor startup into library functions

**Files:**
- Modify: `crates/kalla-ballista/src/lib.rs`
- Modify: `crates/kalla-ballista/Cargo.toml` (remove [[bin]] sections)
- Delete: `crates/kalla-ballista/src/bin/kalla-scheduler.rs`
- Delete: `crates/kalla-ballista/src/bin/kalla-executor.rs`
- Modify: `crates/kallad/src/main.rs` (wire subcommands)

**Step 1: Add library functions to kalla-ballista/src/lib.rs**

Replace `crates/kalla-ballista/src/lib.rs` with:

```rust
pub mod codec;
pub mod csv_range_scan_exec;
pub mod postgres_scan_exec;
pub mod scan_lazy;

use std::net::SocketAddr;
use std::sync::Arc;

use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;

use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};

use codec::KallaPhysicalCodec;

/// Configuration for the scheduler subprocess.
pub struct SchedulerOpts {
    pub bind_host: String,
    pub grpc_port: u16,
}

/// Start the Ballista scheduler gRPC server. Blocks until shutdown.
#[allow(clippy::field_reassign_with_default)]
pub async fn start_scheduler(opts: SchedulerOpts) -> anyhow::Result<()> {
    let mut config = SchedulerConfig::default();
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));
    config.bind_host = opts.bind_host;
    config.bind_port = opts.grpc_port;

    let addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port).parse()?;
    let cluster = BallistaCluster::new_from_config(&config).await?;

    tracing::info!("Ballista scheduler listening on {addr}");
    start_server(cluster, addr, Arc::new(config)).await?;
    Ok(())
}

/// Configuration for the executor subprocess.
pub struct ExecutorOpts {
    pub bind_host: String,
    pub flight_port: u16,
    pub grpc_port: u16,
    pub scheduler_host: String,
    pub scheduler_port: u16,
    pub external_host: Option<String>,
}

/// Start the Ballista executor. Blocks until shutdown.
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
        "Starting executor"
    );

    start_executor_process(Arc::new(config)).await?;
    Ok(())
}
```

**Step 2: Remove [[bin]] sections from kalla-ballista/Cargo.toml**

Delete these lines from `crates/kalla-ballista/Cargo.toml`:

```toml
[[bin]]
name = "kalla-scheduler"
path = "src/bin/kalla-scheduler.rs"

[[bin]]
name = "kalla-executor"
path = "src/bin/kalla-executor.rs"
```

**Step 3: Delete old binary files**

Delete: `crates/kalla-ballista/src/bin/kalla-scheduler.rs`
Delete: `crates/kalla-ballista/src/bin/kalla-executor.rs`
Delete: `crates/kalla-ballista/src/bin/` directory (if empty)

**Step 4: Wire kallad subcommands**

Update `crates/kallad/src/main.rs` — replace the `todo!()` calls:

```rust
        Command::Scheduler {
            http_port,
            grpc_port,
            bind_host,
            partitions,
            staging_path,
        } => {
            tracing::info!("Starting scheduler (grpc={grpc_port}, http={http_port})");
            kalla_ballista::start_scheduler(kalla_ballista::SchedulerOpts {
                bind_host,
                grpc_port,
            })
            .await?;
        }
        Command::Executor {
            scheduler_host,
            scheduler_port,
            flight_port,
            grpc_port,
            bind_host,
            external_host,
        } => {
            kalla_ballista::start_executor(kalla_ballista::ExecutorOpts {
                bind_host,
                flight_port,
                grpc_port,
                scheduler_host,
                scheduler_port,
                external_host,
            })
            .await?;
        }
```

**Step 5: Verify builds and old binaries are gone**

Run: `cargo build --bin kallad`
Expected: Builds successfully.

Run: `cargo build --bin kalla-scheduler 2>&1 || echo "EXPECTED: old binary gone"`
Expected: Build fails — binary no longer exists.

**Step 6: Commit**

```bash
git add -A
git commit -m "refactor: extract scheduler/executor to library functions, wire kallad CLI"
```

---

### Task 3: Create HTTP runner module

This is the core new code. The runner accepts jobs via HTTP, executes them via Ballista (or local DataFusion), and reports progress via callbacks.

**Files:**
- Create: `crates/kalla-ballista/src/runner.rs`
- Modify: `crates/kalla-ballista/src/lib.rs` (add `pub mod runner;`)
- Modify: `crates/kalla-ballista/Cargo.toml` (add dependencies)

**Step 1: Add dependencies to kalla-ballista/Cargo.toml**

Add to `[dependencies]`:

```toml
axum.workspace = true
reqwest.workspace = true
uuid.workspace = true
kalla-recipe.workspace = true
kalla-evidence.workspace = true
prometheus-client.workspace = true
```

**Step 2: Create runner.rs**

Create `crates/kalla-ballista/src/runner.rs`:

```rust
//! HTTP runner — accepts reconciliation jobs and executes via Ballista or local DataFusion.
//!
//! Endpoints:
//! - `POST /api/jobs` — submit a job
//! - `GET /health`    — health check
//! - `GET /ready`     — readiness (always true for now)
//! - `GET /metrics`   — Prometheus metrics

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

use kalla_core::ReconciliationEngine;
use kalla_evidence::{EvidenceStore, MatchedRecord};

use crate::codec::KallaPhysicalCodec;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct JobRequest {
    pub run_id: Uuid,
    pub callback_url: String,
    pub match_sql: String,
    pub sources: Vec<ResolvedSource>,
    pub output_path: String,
    pub primary_keys: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResolvedSource {
    pub alias: String,
    pub uri: String,
}

#[derive(Debug, Serialize)]
struct JobAccepted {
    run_id: Uuid,
    status: String,
}

pub struct ExecResult {
    pub matched: u64,
    pub unmatched_left: u64,
    pub unmatched_right: u64,
}

// ---------------------------------------------------------------------------
// Callback client
// ---------------------------------------------------------------------------

pub struct CallbackClient {
    http: reqwest::Client,
}

impl CallbackClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }

    pub async fn report_progress(
        &self,
        callback_url: &str,
        progress: &serde_json::Value,
    ) -> Result<()> {
        self.http
            .post(format!("{}/progress", callback_url))
            .json(progress)
            .send()
            .await?;
        Ok(())
    }

    pub async fn report_complete(
        &self,
        callback_url: &str,
        result: &serde_json::Value,
    ) -> Result<()> {
        self.http
            .post(format!("{}/complete", callback_url))
            .json(result)
            .send()
            .await?;
        Ok(())
    }

    pub async fn report_error(
        &self,
        callback_url: &str,
        error: &serde_json::Value,
    ) -> Result<()> {
        self.http
            .post(format!("{}/error", callback_url))
            .json(error)
            .send()
            .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct RunnerMetrics {
    pub active_jobs: Gauge,
    pub jobs_completed: Counter,
    pub jobs_failed: Counter,
    registry: Arc<Registry>,
}

impl RunnerMetrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let active_jobs = Gauge::default();
        registry.register("kallad_active_jobs", "Jobs currently executing", active_jobs.clone());

        let jobs_completed = Counter::default();
        registry.register("kallad_jobs_completed_total", "Total jobs completed", jobs_completed.clone());

        let jobs_failed = Counter::default();
        registry.register("kallad_jobs_failed_total", "Total jobs failed", jobs_failed.clone());

        Self {
            active_jobs,
            jobs_completed,
            jobs_failed,
            registry: Arc::new(registry),
        }
    }

    pub fn encode(&self) -> String {
        let mut buf = String::new();
        encode(&mut buf, &self.registry).unwrap();
        buf
    }
}

// ---------------------------------------------------------------------------
// Runner configuration
// ---------------------------------------------------------------------------

pub struct RunnerConfig {
    /// gRPC port of the co-located Ballista scheduler (for SessionContext::remote).
    pub grpc_port: u16,
    /// Number of partitions per source.
    pub partitions: usize,
    /// Local path for evidence storage.
    pub staging_path: String,
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

struct RunnerState {
    job_tx: mpsc::Sender<JobRequest>,
    metrics: RunnerMetrics,
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

async fn submit_job(
    State(state): State<Arc<RunnerState>>,
    Json(req): Json<JobRequest>,
) -> Result<(StatusCode, Json<JobAccepted>), (StatusCode, String)> {
    let run_id = req.run_id;

    state.job_tx.send(req).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to enqueue job: {}", e),
        )
    })?;

    Ok((
        StatusCode::ACCEPTED,
        Json(JobAccepted {
            run_id,
            status: "accepted".to_string(),
        }),
    ))
}

async fn health() -> &'static str {
    "OK"
}

async fn ready() -> &'static str {
    "OK"
}

async fn metrics(State(state): State<Arc<RunnerState>>) -> String {
    state.metrics.encode()
}

// ---------------------------------------------------------------------------
// Source registration (with partitioning for cluster mode)
// ---------------------------------------------------------------------------

async fn register_source_partitioned(
    engine: &ReconciliationEngine,
    alias: &str,
    uri: &str,
    num_partitions: usize,
) -> Result<()> {
    if uri.starts_with("postgres://") || uri.starts_with("postgresql://") {
        let parsed = url::Url::parse(uri)?;
        let table_name = parsed
            .query_pairs()
            .find(|(k, _)| k == "table")
            .map(|(_, v)| v.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing 'table' query parameter in source URI"))?;
        let mut conn_url = parsed.clone();
        conn_url.set_query(None);

        kalla_connectors::postgres_partitioned::register(
            engine.context(),
            alias,
            conn_url.as_str(),
            &table_name,
            num_partitions,
            None,
        )
        .await?;
        info!("Registered partitioned Postgres table '{alias}' ({num_partitions} partitions)");
    } else if uri.starts_with("s3://") && uri.ends_with(".csv") {
        let s3_config = kalla_connectors::S3Config::from_env()?;
        kalla_connectors::csv_partitioned::register(
            engine.context(),
            alias,
            uri,
            num_partitions,
            s3_config,
        )
        .await?;
        info!("Registered byte-range CSV table '{alias}' ({num_partitions} partitions)");
    } else if uri.starts_with("s3://") && uri.ends_with(".parquet")
        || uri.contains("/staging/")
    {
        engine.register_parquet(alias, uri).await?;
        info!("Registered source '{alias}' from {uri}");
    } else if uri.ends_with(".csv") {
        engine.register_csv(alias, uri).await?;
        info!("Registered local CSV source '{alias}' from {uri}");
    } else if uri.starts_with("s3://") {
        let connector = kalla_connectors::S3Connector::from_env()?;
        connector
            .register_csv_listing_table(engine.context(), alias, uri)
            .await?;
        info!("Registered S3 CSV listing table '{alias}' from {uri}");
    } else {
        anyhow::bail!("Unsupported source URI format: {}", uri);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Unmatched counting
// ---------------------------------------------------------------------------

async fn count_unmatched(
    engine: &ReconciliationEngine,
    match_sql: &str,
    primary_keys: &HashMap<String, Vec<String>>,
    source_aliases: &[&str],
) -> Result<(u64, u64)> {
    if source_aliases.len() < 2 {
        return Ok((0, 0));
    }

    let left_alias = source_aliases[0];
    let right_alias = source_aliases[1];
    let left_pks = &primary_keys[left_alias];
    let right_pks = &primary_keys[right_alias];

    if left_pks.is_empty() || right_pks.is_empty() {
        return Ok((0, 0));
    }

    let left_pk = &left_pks[0];
    let unmatched_left_sql = format!(
        "SELECT COUNT(*) AS cnt FROM \"{left_alias}\" \
         WHERE \"{left_pk}\" NOT IN \
         (SELECT \"{left_pk}\" FROM ({match_sql}) AS _matched)"
    );

    let right_pk = &right_pks[0];
    let unmatched_right_sql = format!(
        "SELECT COUNT(*) AS cnt FROM \"{right_alias}\" \
         WHERE \"{right_pk}\" NOT IN \
         (SELECT \"{right_pk}\" FROM ({match_sql}) AS _matched)"
    );

    let unmatched_left = run_count_query(engine, &unmatched_left_sql)
        .await
        .unwrap_or_else(|e| {
            warn!("Unmatched left query failed: {e}");
            0
        });
    let unmatched_right = run_count_query(engine, &unmatched_right_sql)
        .await
        .unwrap_or_else(|e| {
            warn!("Unmatched right query failed: {e}");
            0
        });

    Ok((unmatched_left, unmatched_right))
}

async fn run_count_query(engine: &ReconciliationEngine, sql: &str) -> Result<u64> {
    let df = engine.sql(sql).await?;
    let batches = df.collect().await?;
    let count = batches
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
        })
        .map(|a| a.value(0) as u64)
        .unwrap_or(0);
    Ok(count)
}

// ---------------------------------------------------------------------------
// Key extraction helpers
// ---------------------------------------------------------------------------

fn extract_first_key(
    batch: &arrow::record_batch::RecordBatch,
    primary_keys: &HashMap<String, Vec<String>>,
    row_idx: usize,
    is_left: bool,
) -> Option<String> {
    let aliases: Vec<&String> = primary_keys.keys().collect();
    let alias = if is_left {
        aliases.first()?
    } else {
        aliases.get(1)?
    };
    let pks = &primary_keys[*alias];
    let pk = pks.first()?;

    let qualified = format!("{}.{}", alias, pk);
    extract_string_value(batch, &qualified, row_idx)
        .or_else(|| extract_string_value(batch, pk, row_idx))
}

fn extract_string_value(
    batch: &arrow::record_batch::RecordBatch,
    column_name: &str,
    row_idx: usize,
) -> Option<String> {
    let col_idx = batch.schema().index_of(column_name).ok()?;
    let col = batch.column(col_idx);
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        return Some(arr.value(row_idx).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return Some(arr.value(row_idx).to_string());
    }
    None
}

// ---------------------------------------------------------------------------
// Job execution
// ---------------------------------------------------------------------------

async fn execute_job(config: &RunnerConfig, job: JobRequest) -> Result<ExecResult> {
    let run_id = job.run_id;
    let callback_url = &job.callback_url;
    let callback = CallbackClient::new();

    // Report staging progress
    let _ = callback
        .report_progress(
            callback_url,
            &serde_json::json!({
                "run_id": run_id,
                "stage": "staging",
                "progress": 0.0
            }),
        )
        .await;

    // Create engine — try cluster mode first, fall back to local
    let scheduler_url = format!("df://localhost:{}", config.grpc_port);
    let engine = match ReconciliationEngine::new_cluster(
        &scheduler_url,
        Arc::new(KallaPhysicalCodec::new()),
    )
    .await
    {
        Ok(e) => {
            info!("Run {run_id}: using cluster engine (scheduler: {scheduler_url})");
            e
        }
        Err(_) => {
            info!("Run {run_id}: falling back to local DataFusion engine");
            ReconciliationEngine::new()
        }
    };

    // Register sources with partitioning
    let staging_start = Instant::now();
    for (i, source) in job.sources.iter().enumerate() {
        register_source_partitioned(
            &engine,
            &source.alias,
            &source.uri,
            config.partitions,
        )
        .await?;

        let progress = (i + 1) as f64 / job.sources.len() as f64;
        let _ = callback
            .report_progress(
                callback_url,
                &serde_json::json!({
                    "run_id": run_id,
                    "stage": "staging",
                    "source": source.alias,
                    "progress": progress
                }),
            )
            .await;
    }
    let staging_ms = staging_start.elapsed().as_millis();
    info!("Run {run_id}: staging completed in {staging_ms}ms");

    // Report matching started
    let _ = callback
        .report_progress(
            callback_url,
            &serde_json::json!({
                "run_id": run_id,
                "stage": "matching",
                "progress": 0.0
            }),
        )
        .await;

    // Execute match SQL with batch-counting progress
    let matching_start = Instant::now();
    let mut matched_count = 0u64;
    let mut matched_records: Vec<MatchedRecord> = Vec::new();

    match engine.sql_stream(&job.match_sql).await {
        Ok(mut stream) => {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                for row_idx in 0..batch.num_rows() {
                    let left_key =
                        extract_first_key(&batch, &job.primary_keys, row_idx, true)
                            .unwrap_or_else(|| format!("row_{}", matched_count + row_idx as u64));
                    let right_key =
                        extract_first_key(&batch, &job.primary_keys, row_idx, false)
                            .unwrap_or_else(|| format!("row_{}", matched_count + row_idx as u64));

                    matched_records.push(MatchedRecord::new(
                        left_key,
                        right_key,
                        "match_sql".to_string(),
                        1.0,
                    ));
                }
                matched_count += batch.num_rows() as u64;

                // Report batch-level progress
                let _ = callback
                    .report_progress(
                        callback_url,
                        &serde_json::json!({
                            "run_id": run_id,
                            "stage": "matching",
                            "matched_so_far": matched_count,
                        }),
                    )
                    .await;
            }
        }
        Err(e) => {
            let _ = callback
                .report_error(
                    callback_url,
                    &serde_json::json!({
                        "run_id": run_id,
                        "error": format!("Match SQL failed: {e}"),
                        "stage": "matching"
                    }),
                )
                .await;
            return Err(e.into());
        }
    }

    let matching_ms = matching_start.elapsed().as_millis();
    info!("Run {run_id}: {matched_count} matched records in {matching_ms}ms");

    // Count unmatched
    let source_aliases: Vec<&str> = job.sources.iter().map(|s| s.alias.as_str()).collect();
    let unmatched_start = Instant::now();
    let (unmatched_left, unmatched_right) =
        count_unmatched(&engine, &job.match_sql, &job.primary_keys, &source_aliases).await?;
    let unmatched_ms = unmatched_start.elapsed().as_millis();

    info!(
        "Run {run_id}: {unmatched_left} unmatched_left, {unmatched_right} unmatched_right in {unmatched_ms}ms"
    );

    // Write evidence
    let _ = callback
        .report_progress(
            callback_url,
            &serde_json::json!({
                "run_id": run_id,
                "stage": "writing_results",
                "matched_count": matched_count,
            }),
        )
        .await;

    let evidence_path = format!("{}/{}", config.staging_path, run_id);
    let evidence_store = EvidenceStore::new(&evidence_path)?;
    if !matched_records.is_empty() {
        let _ = evidence_store.write_matched(&run_id, &matched_records);
    }

    // Report completion
    let _ = callback
        .report_complete(
            callback_url,
            &serde_json::json!({
                "run_id": run_id,
                "matched_count": matched_count,
                "unmatched_left_count": unmatched_left,
                "unmatched_right_count": unmatched_right,
                "output_paths": {
                    "matched": format!("{}/matched.parquet", job.output_path),
                    "unmatched_left": format!("{}/unmatched_left.parquet", job.output_path),
                    "unmatched_right": format!("{}/unmatched_right.parquet", job.output_path),
                }
            }),
        )
        .await;

    Ok(ExecResult {
        matched: matched_count,
        unmatched_left,
        unmatched_right,
    })
}

// ---------------------------------------------------------------------------
// Public API: start the HTTP runner
// ---------------------------------------------------------------------------

/// Start the HTTP runner server. Spawns a background job processor.
/// Returns the Axum Router and a join handle for the job processor.
pub async fn start_runner(
    bind_addr: &str,
    config: RunnerConfig,
) -> Result<()> {
    let metrics = RunnerMetrics::new();
    let (job_tx, mut job_rx) = mpsc::channel::<JobRequest>(32);

    let state = Arc::new(RunnerState {
        job_tx,
        metrics: metrics.clone(),
    });

    let app = Router::new()
        .route("/api/jobs", post(submit_job))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    // Cannot use `metrics` as handler name — shadowed. Use a wrapper.
    async fn metrics_handler(State(state): State<Arc<RunnerState>>) -> String {
        state.metrics.encode()
    }

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!("HTTP runner listening on {bind_addr}");

    // Spawn HTTP server
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Process jobs from channel
    let config = Arc::new(config);
    while let Some(job) = job_rx.recv().await {
        let run_id = job.run_id;
        let cfg = config.clone();
        let m = metrics.clone();

        // Spawn each job as a separate task
        tokio::spawn(async move {
            m.active_jobs.inc();
            info!("Processing job for run {run_id}");

            match execute_job(&cfg, job).await {
                Ok(result) => {
                    info!(
                        "Run {run_id} completed: {} matched, {} unmatched_left, {} unmatched_right",
                        result.matched, result.unmatched_left, result.unmatched_right
                    );
                    m.jobs_completed.inc();
                }
                Err(e) => {
                    tracing::error!("Run {run_id} failed: {e}");
                    m.jobs_failed.inc();
                }
            }
            m.active_jobs.dec();
        });
    }

    Ok(())
}
```

**Step 3: Add `pub mod runner;` to lib.rs**

Add `pub mod runner;` to `crates/kalla-ballista/src/lib.rs`.

**Step 4: Verify it compiles**

Run: `cargo build --workspace`
Expected: Builds successfully.

**Step 5: Commit**

```bash
git add -A
git commit -m "feat: add HTTP runner module with job execution via Ballista"
```

---

### Task 4: Wire runner into scheduler startup

**Files:**
- Modify: `crates/kalla-ballista/src/lib.rs` (update `start_scheduler` to run gRPC + HTTP concurrently)
- Modify: `crates/kallad/src/main.rs` (pass runner config)

**Step 1: Update start_scheduler to run both servers**

Modify `start_scheduler` in `crates/kalla-ballista/src/lib.rs` to accept runner config and run both gRPC + HTTP:

```rust
/// Configuration for the scheduler with embedded HTTP runner.
pub struct SchedulerOpts {
    pub bind_host: String,
    pub grpc_port: u16,
    pub http_port: u16,
    pub partitions: usize,
    pub staging_path: String,
}

/// Start the Ballista scheduler gRPC server AND HTTP runner concurrently.
#[allow(clippy::field_reassign_with_default)]
pub async fn start_scheduler(opts: SchedulerOpts) -> anyhow::Result<()> {
    let mut config = SchedulerConfig::default();
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));
    config.bind_host = opts.bind_host.clone();
    config.bind_port = opts.grpc_port;

    let addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port).parse()?;
    let cluster = BallistaCluster::new_from_config(&config).await?;

    tracing::info!("Ballista scheduler listening on {addr}");

    // Start HTTP runner concurrently with gRPC scheduler
    let http_addr = format!("{}:{}", opts.bind_host, opts.http_port);
    let runner_config = runner::RunnerConfig {
        grpc_port: opts.grpc_port,
        partitions: opts.partitions,
        staging_path: opts.staging_path,
    };

    tokio::select! {
        result = start_server(cluster, addr, Arc::new(config)) => {
            result?;
        }
        result = runner::start_runner(&http_addr, runner_config) => {
            result?;
        }
    }

    Ok(())
}
```

**Step 2: Update kallad/main.rs scheduler command**

```rust
        Command::Scheduler {
            http_port,
            grpc_port,
            bind_host,
            partitions,
            staging_path,
        } => {
            tracing::info!("Starting scheduler (grpc={grpc_port}, http={http_port})");
            kalla_ballista::start_scheduler(kalla_ballista::SchedulerOpts {
                bind_host,
                grpc_port,
                http_port,
                partitions,
                staging_path,
            })
            .await?;
        }
```

**Step 3: Verify it builds**

Run: `cargo build --bin kallad`
Expected: Builds successfully.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: No warnings.

**Step 4: Commit**

```bash
git add -A
git commit -m "feat: wire HTTP runner into scheduler, both servers run concurrently"
```

---

### Task 5: Delete kalla-worker crate and NATS dependency

**Files:**
- Delete: `crates/kalla-worker/` (entire directory)
- Modify: `Cargo.toml` (root — remove from workspace members, remove async-nats, remove kalla-worker)
- Modify: `crates/kalla-ballista/Cargo.toml` (remove kalla-worker dependency if any)

**Step 1: Remove kalla-worker from workspace members**

In root `Cargo.toml`, remove `"crates/kalla-worker"` from `[workspace].members`.

Remove `kalla-worker = { path = "crates/kalla-worker" }` from `[workspace.dependencies]`.

Remove `async-nats = "0.38"` from `[workspace.dependencies]`.

**Step 2: Delete kalla-worker directory**

Delete entire `crates/kalla-worker/` directory.

**Step 3: Verify workspace builds**

Run: `cargo build --workspace`
Expected: Builds successfully.

Run: `cargo test --workspace`
Expected: All tests pass (kalla-worker tests are gone, remaining tests pass).

**Step 4: Commit**

```bash
git add -A
git commit -m "chore: remove kalla-worker crate and async-nats dependency"
```

---

### Task 6: Update Dockerfiles and docker-compose

**Files:**
- Create: `Dockerfile` (root — single Dockerfile for kallad)
- Delete: `crates/kalla-worker/Dockerfile`
- Delete: `crates/kalla-ballista/Dockerfile`
- Rewrite: `docker-compose.cluster.yml`
- Modify: `docker-compose.single.yml`

**Step 1: Create root Dockerfile for kallad**

Create `Dockerfile` at project root:

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
COPY crates/kalla-ballista/Cargo.toml crates/kalla-ballista/Cargo.toml
COPY crates/kallad/Cargo.toml crates/kallad/Cargo.toml

RUN mkdir -p crates/kalla-core/src && echo "pub fn _dummy() {}" > crates/kalla-core/src/lib.rs && \
    mkdir -p crates/kalla-connectors/src && echo "pub fn _dummy() {}" > crates/kalla-connectors/src/lib.rs && \
    mkdir -p crates/kalla-recipe/src && echo "pub fn _dummy() {}" > crates/kalla-recipe/src/lib.rs && \
    mkdir -p crates/kalla-evidence/src && echo "pub fn _dummy() {}" > crates/kalla-evidence/src/lib.rs && \
    mkdir -p crates/kalla-ballista/src && echo "pub fn _dummy() {}" > crates/kalla-ballista/src/lib.rs && \
    mkdir -p crates/kallad/src && echo "fn main() {}" > crates/kallad/src/main.rs

RUN cargo update home --precise 0.5.9 && \
    cargo update comfy-table --precise 7.1.4 && \
    cargo update time --precise 0.3.36 && \
    cargo update time-core --precise 0.1.2

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin kallad 2>&1 || true

COPY crates ./crates

RUN find /app/crates -name '*.rs' -exec touch {} +

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin kallad && \
    cp target/release/kallad /usr/local/bin/kallad

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/kallad /usr/local/bin/
EXPOSE 8080 50050 50051 50052
ENTRYPOINT ["kallad"]
```

**Step 2: Delete old Dockerfiles**

Delete: `crates/kalla-worker/Dockerfile` (already deleted with crate)
Delete: `crates/kalla-ballista/Dockerfile`

**Step 3: Rewrite docker-compose.cluster.yml**

```yaml
# Kalla Cluster Deployment
#
# Usage: docker compose -f docker-compose.cluster.yml up -d
#
# Architecture:
#   App --HTTP--> Scheduler (HTTP :8080 + gRPC :50050) --> Executors
#   Executors read from Postgres directly via PostgresScanExec

services:
  app:
    build:
      context: ./kalla-web
      dockerfile: Dockerfile
    ports:
      - "3000:3000"
    environment:
      DATABASE_URL: postgres://kalla:kalla_secret@postgres:5432/kalla
      WORKER_URL: http://scheduler:8080
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
    depends_on:
      postgres:
        condition: service_healthy
      scheduler:
        condition: service_started
    restart: unless-stopped

  scheduler:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["scheduler", "--http-port", "8080", "--grpc-port", "50050"]
    ports:
      - "8080:8080"
      - "50050:50050"
    environment:
      RUST_LOG: ${RUST_LOG:-info}
      BALLISTA_PARTITIONS: "8"
    restart: unless-stopped

  executor-1:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["executor", "--scheduler-host", "scheduler", "--scheduler-port", "50050", "--flight-port", "50051", "--grpc-port", "50052", "--external-host", "executor-1"]
    environment:
      RUST_LOG: ${RUST_LOG:-info}
    depends_on:
      scheduler:
        condition: service_started
    restart: unless-stopped

  executor-2:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["executor", "--scheduler-host", "scheduler", "--scheduler-port", "50050", "--flight-port", "50051", "--grpc-port", "50052", "--external-host", "executor-2"]
    environment:
      RUST_LOG: ${RUST_LOG:-info}
    depends_on:
      scheduler:
        condition: service_started
    restart: unless-stopped

  postgres:
    image: postgres:16-alpine
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: kalla
      POSTGRES_PASSWORD: kalla_secret
      POSTGRES_DB: kalla
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "kalla"]
      interval: 5s
      timeout: 5s
      retries: 5
    restart: unless-stopped

volumes:
  postgres_data:
```

**Step 4: Update docker-compose.single.yml**

Update the worker service to use kallad scheduler (local mode, no executors):

Replace the worker service with:

```yaml
  scheduler:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["scheduler", "--http-port", "9090", "--grpc-port", "50050"]
    ports:
      - "9090:9090"
    environment:
      RUST_LOG: ${RUST_LOG:-info}
    restart: unless-stopped
```

And update the app's `WORKER_URL` to `http://scheduler:9090`.

**Step 5: Commit**

```bash
git add -A
git commit -m "chore: unified Dockerfile for kallad, update docker-compose files"
```

---

### Task 7: Update benchmarks

**Files:**
- Modify: `benchmarks/run_cluster_benchmark.sh`
- Modify: `benchmarks/run_benchmark.sh`
- Rewrite: `benchmarks/inject_scaled_job.py` → `benchmarks/inject_cluster_job.py`

**Step 1: Rewrite inject script for direct HTTP**

The old `inject_scaled_job.py` used NATS. The new version POSTs directly to the scheduler HTTP API.

Rename `benchmarks/inject_scaled_job.py` to `benchmarks/inject_cluster_job.py` and rewrite:

```python
#!/usr/bin/env python3
"""Inject a benchmark job via HTTP POST to the scheduler."""

import argparse
import json
import sys
import time
import uuid
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

import psycopg2

# Callback state
result_data = {}
result_event = threading.Event()


class CallbackHandler(BaseHTTPRequestHandler):
    """Minimal HTTP server to receive worker callbacks."""

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}

        if self.path.endswith("/complete"):
            result_data["status"] = "complete"
            result_data.update(body)
            result_event.set()
        elif self.path.endswith("/error"):
            result_data["status"] = "error"
            result_data.update(body)
            result_event.set()
        elif self.path.endswith("/progress"):
            stage = body.get("stage", "")
            progress = body.get("progress", "")
            matched = body.get("matched_so_far", "")
            print(f"  Progress: stage={stage} progress={progress} matched={matched}", file=sys.stderr)

        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress default logging


def main():
    parser = argparse.ArgumentParser(description="Inject benchmark job via HTTP")
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--scheduler-url", default="http://localhost:8080")
    parser.add_argument("--match-sql", required=True)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--callback-port", type=int, default=9999)
    parser.add_argument("--json-output", action="store_true")
    args = parser.parse_args()

    # Seed benchmark data
    print(f"  Seeding {args.rows} rows to Postgres...", file=sys.stderr)
    seed_data(args.pg_url, args.rows)

    # Start callback server
    callback_server = HTTPServer(("0.0.0.0", args.callback_port), CallbackHandler)
    callback_thread = threading.Thread(target=callback_server.serve_forever, daemon=True)
    callback_thread.start()

    callback_url = f"http://localhost:{args.callback_port}/api/worker"
    run_id = str(uuid.uuid4())
    pg = args.pg_url.replace("postgresql://", "postgres://", 1)

    job = {
        "run_id": run_id,
        "callback_url": callback_url,
        "match_sql": args.match_sql,
        "sources": [
            {"alias": "left_src", "uri": f"{pg}?table=bench_invoices"},
            {"alias": "right_src", "uri": f"{pg}?table=bench_payments"},
        ],
        "output_path": f"/tmp/bench-output-{run_id}",
        "primary_keys": {
            "left_src": ["invoice_id"],
            "right_src": ["payment_id"],
        },
    }

    # POST job to scheduler
    import urllib.request
    req = urllib.request.Request(
        f"{args.scheduler_url}/api/jobs",
        data=json.dumps(job).encode(),
        headers={"Content-Type": "application/json"},
    )

    start = time.time()
    try:
        resp = urllib.request.urlopen(req)
        if resp.status not in (200, 202):
            print(json.dumps({"status": "error", "error": f"HTTP {resp.status}"}))
            sys.exit(1)
    except Exception as e:
        print(json.dumps({"status": "error", "error": str(e)}))
        sys.exit(1)

    print(f"  Job submitted (run_id={run_id}), waiting for callback...", file=sys.stderr)

    # Wait for callback
    if result_event.wait(timeout=args.timeout):
        elapsed = time.time() - start
        status = result_data.get("status", "unknown")
        matched = result_data.get("matched_count", 0)
        rows_per_sec = int(args.rows / elapsed) if elapsed > 0 else 0

        output = {
            "status": status,
            "elapsed_secs": f"{elapsed:.2f}",
            "rows_per_sec": rows_per_sec,
            "matched_count": matched,
            "unmatched_left_count": result_data.get("unmatched_left_count", 0),
            "unmatched_right_count": result_data.get("unmatched_right_count", 0),
        }
    else:
        elapsed = time.time() - start
        output = {"status": "timeout", "elapsed_secs": f"{elapsed:.2f}", "rows_per_sec": 0}

    callback_server.shutdown()

    if args.json_output:
        print(json.dumps(output))
    else:
        print(f"  Status: {output['status']} | Elapsed: {output['elapsed_secs']}s | Rows/sec: {output['rows_per_sec']}", file=sys.stderr)


def seed_data(pg_url, rows):
    """Seed benchmark tables using the existing seed_postgres.py logic."""
    import subprocess
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    subprocess.run(
        ["python3", os.path.join(script_dir, "seed_postgres.py"),
         "--rows", str(rows), "--pg-url", pg_url],
        check=True,
    )


if __name__ == "__main__":
    main()
```

**Step 2: Update run_cluster_benchmark.sh**

Key changes:
- Single binary `kallad` instead of 3 binaries
- No NATS — scheduler accepts jobs directly via HTTP
- No worker process — scheduler handles jobs
- Use `inject_cluster_job.py` instead of `inject_scaled_job.py`

Replace binary paths section and startup functions. The scheduler now serves HTTP on port 8080, and the inject script POSTs directly to it.

**Step 3: Update run_benchmark.sh**

Change binary from `kalla-worker` to `kallad scheduler`:
- Start: `./target/release/kallad scheduler --http-port 9090 &`
- Worker URL: `http://localhost:9090`
- Health check: `curl -sf http://localhost:9090/health`

**Step 4: Commit**

```bash
git add -A
git commit -m "chore: update benchmarks for unified kallad binary"
```

---

### Task 8: Update CI, init.sql, and docs

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `scripts/init.sql`
- Modify: `docs/deployment.md`

**Step 1: Update CI**

Key changes in `.github/workflows/ci.yml`:

**Rust job:**
- Build: `cargo build --release --bin kallad`
- Upload artifact: `target/release/kallad` (name: `kallad-binary`)

**Integration test:**
- Download `kallad-binary`
- Start: `./target/release/kallad scheduler --http-port 9090 &`
- Health check: `curl -sf http://localhost:9090/health`

**Benchmark job:**
- Download `kallad-binary`
- Start: `./target/release/kallad scheduler --http-port 9090 &`
- Health check: `curl -sf http://localhost:9090/health`
- Run: `bash benchmarks/run_benchmark.sh`

**Cluster benchmark job:**
- Build: `cargo build --release --bin kallad` (needs all source for codec)
- Remove NATS service — no longer needed
- Remove NATS-related env vars
- Start: `./target/release/kallad scheduler --http-port 8080 --grpc-port 50050 &`
- Start executors: `./target/release/kallad executor --scheduler-host localhost --scheduler-port 50050 --flight-port 50051 --grpc-port 50052 --external-host localhost &` (repeat for each executor with different ports)
- Run: `bash benchmarks/run_cluster_benchmark.sh`

**Step 2: Remove jobs table from init.sql**

Remove the `jobs` table definition and its indexes (lines 326-344 in current file):

```sql
-- DELETE: JOB QUEUE TRACKING section
-- The jobs table was used for NATS-based worker job tracking.
-- No longer needed — scheduler handles jobs in-process.
```

**Step 3: Update deployment.md**

Rewrite to reflect:
- Single `kallad` binary with subcommands
- No NATS in any deployment mode
- `docker-compose.cluster.yml` uses scheduler + executors
- `docker-compose.single.yml` uses scheduler in local mode
- Updated env var reference (remove NATS_URL, DATABASE_URL from worker, add HTTP_PORT/GRPC_PORT)
- Updated architecture diagrams

**Step 4: Commit**

```bash
git add -A
git commit -m "chore: update CI, remove jobs table, update deployment docs"
```

---

### Task 9: Final verification

**Step 1: Run all checks**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release --bin kallad
```

Expected: All pass. No warnings. Clean build.

**Step 2: Verify binary works**

```bash
./target/release/kallad --help
./target/release/kallad scheduler --help
./target/release/kallad executor --help
```

Expected: Help text shows subcommands and options.

**Step 3: Verify old binaries don't exist**

```bash
ls target/release/kalla-worker 2>&1 && echo "FAIL: old binary exists" || echo "OK: old binary gone"
ls target/release/kalla-scheduler 2>&1 && echo "FAIL: old binary exists" || echo "OK: old binary gone"
ls target/release/kalla-executor 2>&1 && echo "FAIL: old binary exists" || echo "OK: old binary gone"
```

**Step 4: Commit final state**

```bash
git add -A
git commit -m "chore: final verification — unified kallad binary complete"
```
