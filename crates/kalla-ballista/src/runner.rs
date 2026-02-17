//! HTTP runner — accepts reconciliation jobs via HTTP and executes them
//! using DataFusion (local) or Ballista (cluster mode).
//!
//! Embedded inside the scheduler process so that the scheduler can run
//! jobs directly when no external executors are connected.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

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

/// A job request received via `POST /api/jobs`.
#[derive(Debug, Clone, Deserialize)]
pub struct JobRequest {
    pub run_id: Uuid,
    pub callback_url: String,
    pub match_sql: String,
    pub sources: Vec<ResolvedSource>,
    pub output_path: String,
    pub primary_keys: HashMap<String, Vec<String>>,
}

/// A resolved data source with alias and URI.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResolvedSource {
    pub alias: String,
    pub uri: String,
}

/// Response returned when a job is accepted.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobAccepted {
    pub run_id: Uuid,
    pub status: String,
}

/// Result of executing a reconciliation job.
pub struct ExecResult {
    pub matched: u64,
    pub unmatched_left: u64,
    pub unmatched_right: u64,
}

// ---------------------------------------------------------------------------
// Callback client
// ---------------------------------------------------------------------------

/// HTTP client for reporting progress/completion/error back to the API.
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
    ) -> anyhow::Result<()> {
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
    ) -> anyhow::Result<()> {
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
    ) -> anyhow::Result<()> {
        self.http
            .post(format!("{}/error", callback_url))
            .json(error)
            .send()
            .await?;
        Ok(())
    }
}

impl Default for CallbackClient {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Prometheus metrics for the runner.
#[derive(Clone)]
pub struct RunnerMetrics {
    pub active_jobs: Gauge,
    pub jobs_completed: Counter,
    pub jobs_failed: Counter,
    pub registry: Arc<Registry>,
}

impl RunnerMetrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let active_jobs = Gauge::default();
        registry.register(
            "kalla_runner_active_jobs",
            "Number of jobs currently being processed",
            active_jobs.clone(),
        );

        let jobs_completed = Counter::default();
        registry.register(
            "kalla_runner_jobs_completed",
            "Total jobs completed successfully",
            jobs_completed.clone(),
        );

        let jobs_failed = Counter::default();
        registry.register(
            "kalla_runner_jobs_failed",
            "Total jobs that failed",
            jobs_failed.clone(),
        );

        Self {
            active_jobs,
            jobs_completed,
            jobs_failed,
            registry: Arc::new(registry),
        }
    }

    /// Encode all metrics as Prometheus text format.
    pub fn encode_metrics(&self) -> String {
        let mut buf = String::new();
        encode(&mut buf, &self.registry).unwrap();
        buf
    }
}

impl Default for RunnerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Runner configuration
// ---------------------------------------------------------------------------

/// Configuration for the HTTP runner.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// gRPC port of the co-located Ballista scheduler (for cluster-mode engine).
    pub grpc_port: u16,
    /// Number of partitions for distributed source reads.
    pub partitions: usize,
    /// Local directory for staging evidence files.
    pub staging_path: String,
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

struct RunnerState {
    job_tx: mpsc::Sender<JobRequest>,
    runner_metrics: RunnerMetrics,
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

/// POST /api/jobs — accept a job for processing.
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

/// GET /health — liveness probe.
async fn health() -> StatusCode {
    StatusCode::OK
}

/// GET /ready — readiness probe.
async fn ready() -> StatusCode {
    StatusCode::OK
}

/// GET /metrics — Prometheus metrics endpoint.
async fn metrics_handler(State(state): State<Arc<RunnerState>>) -> String {
    state.runner_metrics.encode_metrics()
}

// ---------------------------------------------------------------------------
// Source registration
// ---------------------------------------------------------------------------

/// Register a source with the engine, choosing partitioned registration when
/// a partition count > 1 is provided. Returns the total row count of the source.
async fn register_source_partitioned(
    engine: &ReconciliationEngine,
    alias: &str,
    uri: &str,
    num_partitions: usize,
) -> anyhow::Result<u64> {
    if uri.starts_with("postgres://") || uri.starts_with("postgresql://") {
        let parsed = url::Url::parse(uri)?;
        let table_name = parsed
            .query_pairs()
            .find(|(k, _)| k == "table")
            .map(|(_, v)| v.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing 'table' query parameter in source URI"))?;
        let mut conn_url = parsed.clone();
        conn_url.set_query(None);

        let table = kalla_connectors::postgres_partitioned::PostgresPartitionedTable::new(
            conn_url.as_str(),
            &table_name,
            num_partitions,
            Some("ctid".to_string()),
        )
        .await?;
        let total_rows = table.total_rows();
        engine.context().register_table(alias, Arc::new(table))?;
        info!(
            "Registered PostgresPartitionedTable '{}' -> '{}'",
            table_name, alias
        );
        return Ok(total_rows);
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
    } else if uri.starts_with("s3://") {
        // S3 listing table (parquet or other)
        let connector = kalla_connectors::S3Connector::from_env()?;
        connector
            .register_csv_listing_table(engine.context(), alias, uri)
            .await?;
    } else if uri.ends_with(".csv") {
        engine.register_csv(alias, uri).await?;
    } else if uri.ends_with(".parquet") || uri.contains("/staging/") {
        engine.register_parquet(alias, uri).await?;
    } else {
        anyhow::bail!("Unsupported source URI format: {}", uri);
    }

    // For non-Postgres sources, count rows after registration.
    let count = run_count_query(
        engine,
        &format!("SELECT COUNT(*) AS cnt FROM \"{}\"", alias),
    )
    .await
    .unwrap_or(0);
    Ok(count)
}

// ---------------------------------------------------------------------------
// Unmatched counting
// ---------------------------------------------------------------------------

/// Count unmatched records by subtracting distinct matched keys from source totals.
///
/// Uses the already-collected matched records and pre-computed source row counts,
/// avoiding any additional queries. This works reliably in both local and Ballista
/// cluster modes (the previous `NOT IN (subquery)` approach failed in Ballista's
/// distributed planner).
fn count_unmatched_from_matched(
    matched_records: &[MatchedRecord],
    primary_keys: &HashMap<String, Vec<String>>,
    source_aliases: &[&str],
    source_row_counts: &HashMap<String, u64>,
) -> (u64, u64) {
    if source_aliases.len() < 2 {
        return (0, 0);
    }

    let left_alias = source_aliases[0];
    let right_alias = source_aliases[1];
    let left_pks = &primary_keys[left_alias];
    let right_pks = &primary_keys[right_alias];

    if left_pks.is_empty() || right_pks.is_empty() {
        return (0, 0);
    }

    // Count distinct matched keys from the already-collected results.
    let distinct_left: std::collections::HashSet<&str> = matched_records
        .iter()
        .map(|r| r.left_key.as_str())
        .collect();
    let distinct_right: std::collections::HashSet<&str> = matched_records
        .iter()
        .map(|r| r.right_key.as_str())
        .collect();

    let left_total = source_row_counts.get(left_alias).copied().unwrap_or(0);
    let right_total = source_row_counts.get(right_alias).copied().unwrap_or(0);

    let unmatched_left = left_total.saturating_sub(distinct_left.len() as u64);
    let unmatched_right = right_total.saturating_sub(distinct_right.len() as u64);

    (unmatched_left, unmatched_right)
}

async fn run_count_query(engine: &ReconciliationEngine, sql: &str) -> anyhow::Result<u64> {
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

/// Extract the first primary key value from a record batch for a given source alias.
fn extract_first_key(
    batch: &arrow::record_batch::RecordBatch,
    primary_keys: &HashMap<String, Vec<String>>,
    row_idx: usize,
    alias: &str,
) -> Option<String> {
    let pks = primary_keys.get(alias)?;
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

/// Execute a single reconciliation job.
///
/// Creates a DataFusion engine (cluster mode via the co-located scheduler,
/// falling back to local mode), registers sources, runs the match SQL,
/// counts unmatched records, writes evidence, and reports results.
async fn execute_job(job: JobRequest, config: &RunnerConfig, metrics: &RunnerMetrics) {
    let run_id = job.run_id;
    let callback = CallbackClient::new();

    metrics.active_jobs.inc();

    let result = execute_job_inner(&job, config, &callback).await;

    metrics.active_jobs.dec();

    match result {
        Ok(exec_result) => {
            metrics.jobs_completed.inc();
            info!(
                "Run {} completed: {} matched, {} unmatched_left, {} unmatched_right",
                run_id,
                exec_result.matched,
                exec_result.unmatched_left,
                exec_result.unmatched_right
            );
        }
        Err(e) => {
            metrics.jobs_failed.inc();
            tracing::error!("Run {} failed: {}", run_id, e);
            let _ = callback
                .report_error(
                    &job.callback_url,
                    &serde_json::json!({
                        "run_id": run_id,
                        "error": format!("{}", e),
                    }),
                )
                .await;
        }
    }
}

/// Try to create a cluster engine and verify executors are present.
/// Returns Some(engine) if cluster mode works, None to fall back to local.
async fn create_engine(scheduler_url: &str, run_id: Uuid) -> Option<ReconciliationEngine> {
    let engine = match ReconciliationEngine::new_cluster(
        scheduler_url,
        Arc::new(KallaPhysicalCodec::new()),
        Arc::new(crate::codec::KallaLogicalCodec::new()),
    )
    .await
    {
        Ok(e) => e,
        Err(e) => {
            warn!(
                "Run {}: cluster connection failed ({}), falling back to local",
                run_id, e
            );
            return None;
        }
    };

    // Probe: run a trivial query to verify executors can handle work.
    // Without executors the job stays queued forever, so we add a timeout.
    let probe = async {
        let df = engine.sql("SELECT 1").await?;
        df.collect().await
    };

    match tokio::time::timeout(std::time::Duration::from_secs(10), probe).await {
        Ok(Ok(_)) => {
            info!(
                "Run {}: using cluster engine (scheduler={})",
                run_id, scheduler_url
            );
            Some(engine)
        }
        Ok(Err(e)) => {
            warn!(
                "Run {}: cluster probe failed ({}), falling back to local",
                run_id, e
            );
            None
        }
        Err(_) => {
            warn!(
                "Run {}: cluster probe timed out (no executors?), falling back to local",
                run_id
            );
            None
        }
    }
}

async fn execute_job_inner(
    job: &JobRequest,
    config: &RunnerConfig,
    callback: &CallbackClient,
) -> anyhow::Result<ExecResult> {
    let run_id = job.run_id;
    let callback_url = &job.callback_url;

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

    // Create engine: try cluster mode first, verify executors exist, fallback to local.
    // The co-located Ballista gRPC scheduler always accepts connections, so new_cluster()
    // succeeds even without executors. We probe with SELECT 1 to verify executors are present.
    let scheduler_url = format!("df://localhost:{}", config.grpc_port);
    let engine = match create_engine(&scheduler_url, run_id).await {
        Some(e) => e,
        None => {
            info!("Run {}: using local DataFusion engine", run_id);
            ReconciliationEngine::new()
        }
    };

    // Register all sources and collect row counts for unmatched calculation.
    let staging_start = Instant::now();
    let mut source_row_counts: HashMap<String, u64> = HashMap::new();
    for (i, source) in job.sources.iter().enumerate() {
        let row_count =
            register_source_partitioned(&engine, &source.alias, &source.uri, config.partitions)
                .await?;
        source_row_counts.insert(source.alias.clone(), row_count);
        info!(
            "Registered source '{}' from {} ({} rows)",
            source.alias, source.uri, row_count
        );

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
    info!("Run {}: staging completed in {}ms", run_id, staging_ms);

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

    // Execute match SQL streaming
    let matching_start = Instant::now();
    let mut matched_count = 0u64;
    let mut matched_records: Vec<MatchedRecord> = Vec::new();

    let left_alias = job
        .sources
        .first()
        .map(|s| s.alias.as_str())
        .unwrap_or("left_src");
    let right_alias = job
        .sources
        .get(1)
        .map(|s| s.alias.as_str())
        .unwrap_or("right_src");

    match engine.sql_stream(&job.match_sql).await {
        Ok(mut stream) => {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                for row_idx in 0..batch.num_rows() {
                    let left_key =
                        extract_first_key(&batch, &job.primary_keys, row_idx, left_alias)
                            .unwrap_or_else(|| format!("row_{}", matched_count + row_idx as u64));
                    let right_key =
                        extract_first_key(&batch, &job.primary_keys, row_idx, right_alias)
                            .unwrap_or_else(|| format!("row_{}", matched_count + row_idx as u64));

                    matched_records.push(MatchedRecord::new(
                        left_key,
                        right_key,
                        "match_sql".to_string(),
                        1.0,
                    ));
                }
                let batch_rows = batch.num_rows() as u64;
                matched_count += batch_rows;

                // Progress callback per batch
                let _ = callback
                    .report_progress(
                        callback_url,
                        &serde_json::json!({
                            "run_id": run_id,
                            "stage": "matching",
                            "matched_count": matched_count,
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
                        "error": format!("Match SQL failed: {}", e),
                        "stage": "matching"
                    }),
                )
                .await;
            return Err(e.into());
        }
    }

    let matching_ms = matching_start.elapsed().as_millis();
    info!(
        "Run {}: {} matched records in {}ms",
        run_id, matched_count, matching_ms
    );

    // Count unmatched records by subtracting distinct matched keys from total rows.
    // This avoids complex NOT IN subqueries that fail in Ballista's distributed planner.
    let source_aliases: Vec<&str> = job.sources.iter().map(|s| s.alias.as_str()).collect();
    let unmatched_start = Instant::now();
    let (unmatched_left, unmatched_right) = count_unmatched_from_matched(
        &matched_records,
        &job.primary_keys,
        &source_aliases,
        &source_row_counts,
    );
    let unmatched_ms = unmatched_start.elapsed().as_millis();

    info!(
        "Run {}: {} unmatched_left, {} unmatched_right in {}ms",
        run_id, unmatched_left, unmatched_right, unmatched_ms
    );

    // Write evidence
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
// Runner entry point
// ---------------------------------------------------------------------------

/// Start the HTTP runner.
///
/// Binds an Axum HTTP server on the given address and spawns a job processor
/// that executes jobs concurrently as separate tokio tasks.
pub async fn start_runner(bind_addr: &str, config: RunnerConfig) -> anyhow::Result<()> {
    let runner_metrics = RunnerMetrics::new();

    let (job_tx, mut job_rx) = mpsc::channel::<JobRequest>(64);

    let state = Arc::new(RunnerState {
        job_tx,
        runner_metrics: runner_metrics.clone(),
    });

    let app = Router::new()
        .route("/api/jobs", post(submit_job))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    info!("HTTP runner listening on {}", bind_addr);

    // Spawn the HTTP server
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Job processor loop — spawns each job as a separate tokio task for
    // concurrent execution.
    while let Some(job) = job_rx.recv().await {
        let cfg = config.clone();
        let m = runner_metrics.clone();
        tokio::spawn(async move {
            execute_job(job, &cfg, &m).await;
        });
    }

    Ok(())
}
