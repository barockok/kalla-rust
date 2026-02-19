//! HTTP runner — accepts reconciliation jobs via HTTP and executes them
//! using DataFusion (local) or Ballista (cluster mode).
//!
//! Embedded inside the scheduler process so that the scheduler can run
//! jobs directly when no external executors are connected.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    #[serde(default)]
    pub filters: Vec<kalla_connectors::FilterCondition>,
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
// Callback payloads (B1)
// ---------------------------------------------------------------------------

/// Progress callback payload.
#[derive(Debug, Serialize)]
#[serde(tag = "stage")]
pub enum ProgressCallback {
    /// Source staging progress.
    #[serde(rename = "staging")]
    Staging {
        run_id: Uuid,
        progress: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<String>,
    },
    /// Match SQL execution progress.
    #[serde(rename = "matching")]
    Matching {
        run_id: Uuid,
        progress: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        matched_count: Option<u64>,
    },
}

/// Output file paths included in completion callback.
#[derive(Debug, Serialize)]
pub struct OutputPaths {
    pub matched: String,
    pub unmatched_left: String,
    pub unmatched_right: String,
}

/// Completion callback payload.
#[derive(Debug, Serialize)]
pub struct CompletionCallback {
    pub run_id: Uuid,
    pub matched_count: u64,
    pub unmatched_left_count: u64,
    pub unmatched_right_count: u64,
    pub output_paths: OutputPaths,
}

/// Error callback payload.
#[derive(Debug, Serialize)]
pub struct ErrorCallback {
    pub run_id: Uuid,
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
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
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to build HTTP client");
        Self { http }
    }

    /// Best-effort progress report (single attempt).
    pub async fn report_progress(
        &self,
        callback_url: &str,
        progress: &ProgressCallback,
    ) -> anyhow::Result<()> {
        self.http
            .post(format!("{}/progress", callback_url))
            .json(progress)
            .send()
            .await?;
        Ok(())
    }

    /// Report completion with retry and exponential backoff.
    ///
    /// This is the most critical callback — if it fails, the caller never learns
    /// the job finished. Retries up to 3 times with exponential backoff.
    pub async fn report_complete(
        &self,
        callback_url: &str,
        result: &CompletionCallback,
    ) -> anyhow::Result<()> {
        let url = format!("{}/complete", callback_url);
        let mut last_err = None;
        for attempt in 0..3u32 {
            match self.http.post(&url).json(result).send().await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!("Completion callback attempt {} failed: {}", attempt + 1, e);
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(500 * 2u64.pow(attempt))).await;
                }
            }
        }
        Err(last_err.unwrap().into())
    }

    /// Best-effort error report (single attempt).
    pub async fn report_error(
        &self,
        callback_url: &str,
        error: &ErrorCallback,
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
    pub queued_jobs: Gauge,
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

        let queued_jobs = Gauge::default();
        registry.register(
            "kalla_runner_queued_jobs",
            "Number of jobs waiting for a concurrency slot",
            queued_jobs.clone(),
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
            queued_jobs,
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
    /// Maximum number of jobs that can execute concurrently.
    pub max_concurrent_jobs: usize,
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

/// GET /ready — readiness probe. Returns 503 when the job channel is full.
async fn ready(State(state): State<Arc<RunnerState>>) -> StatusCode {
    if state.job_tx.capacity() > 0 {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// GET /metrics — Prometheus metrics endpoint.
async fn metrics_handler(State(state): State<Arc<RunnerState>>) -> String {
    state.runner_metrics.encode_metrics()
}

// ---------------------------------------------------------------------------
// Source registration
// ---------------------------------------------------------------------------

/// Register a source and count rows if needed.
async fn register_source(
    engine: &ReconciliationEngine,
    alias: &str,
    uri: &str,
    partitions: usize,
    filters: &[kalla_connectors::FilterCondition],
) -> anyhow::Result<u64> {
    let row_count =
        kalla_connectors::register_source(engine.context(), alias, uri, partitions, filters)
            .await?;

    // Factories that return 0 don't know the row count at registration time.
    // Run a COUNT query to fill in the actual value.
    if row_count == 0 {
        let count = run_count_query(
            engine,
            &format!("SELECT COUNT(*) AS cnt FROM \"{}\"", alias),
        )
        .await
        .unwrap_or(0);
        return Ok(count);
    }

    Ok(row_count)
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
    use arrow::array::*;

    let col_idx = batch.schema().index_of(column_name).ok()?;
    let col = batch.column(col_idx);

    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Some(arr.value(row_idx).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
        return Some(arr.value(row_idx).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(arr.value(row_idx).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        return Some(arr.value(row_idx).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int16Array>() {
        return Some(arr.value(row_idx).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(arr.value(row_idx).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
        return Some(arr.value(row_idx).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
        return Some(arr.value(row_idx).to_string());
    }

    // Fallback: use ArrayFormatter for any unhandled type.
    arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &Default::default())
        .ok()
        .map(|fmt| fmt.value(row_idx).to_string())
}

// ---------------------------------------------------------------------------
// SQL rewriting for cluster mode
// ---------------------------------------------------------------------------

/// Replace the SELECT clause with `SELECT *` to work around Ballista's
/// distributed planner limitation where column projections in JOINs cause
/// "missing columns on join" errors.
///
/// Input:  `SELECT l.invoice_id, r.payment_id FROM left_src l JOIN ...`
/// Output: `SELECT * FROM left_src l JOIN ...`
fn rewrite_select_star(sql: &str) -> String {
    // Find the first ` FROM ` (case-insensitive). Everything before it is the
    // SELECT clause which we replace with `SELECT *`.
    let upper = sql.to_uppercase();
    if let Some(pos) = upper.find(" FROM ") {
        format!("SELECT *{}", &sql[pos..])
    } else {
        sql.to_string()
    }
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
                    &ErrorCallback {
                        run_id,
                        error: format!("{}", e),
                        stage: None,
                    },
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
            &ProgressCallback::Staging {
                run_id,
                progress: 0.0,
                source: None,
            },
        )
        .await;

    // Create engine: try cluster mode first, verify executors exist, fallback to local.
    // The co-located Ballista gRPC scheduler always accepts connections, so new_cluster()
    // succeeds even without executors. We probe with SELECT 1 to verify executors are present.
    let scheduler_url = format!("df://localhost:{}", config.grpc_port);
    let (engine, is_cluster) = match create_engine(&scheduler_url, run_id).await {
        Some(e) => (e, true),
        None => {
            info!("Run {}: using local DataFusion engine", run_id);
            (ReconciliationEngine::new(), false)
        }
    };

    // Register all sources and collect row counts for unmatched calculation.
    let staging_start = Instant::now();
    let mut source_row_counts: HashMap<String, u64> = HashMap::new();
    for (i, source) in job.sources.iter().enumerate() {
        let row_count = register_source(
            &engine,
            &source.alias,
            &source.uri,
            config.partitions,
            &source.filters,
        )
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
                &ProgressCallback::Staging {
                    run_id,
                    progress,
                    source: Some(source.alias.clone()),
                },
            )
            .await;
    }
    let staging_ms = staging_start.elapsed().as_millis();
    info!("Run {}: staging completed in {}ms", run_id, staging_ms);

    // Report matching started
    let _ = callback
        .report_progress(
            callback_url,
            &ProgressCallback::Matching {
                run_id,
                progress: 0.0,
                matched_count: None,
            },
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

    // Ballista's distributed planner cannot handle column projections in JOIN
    // queries — it loses track of join-key columns when they are not in the
    // SELECT list. Work around by replacing the SELECT clause with `SELECT *`
    // in cluster mode. The downstream key-extraction code finds columns by
    // name so extra columns are harmless.
    let match_sql = if is_cluster {
        rewrite_select_star(&job.match_sql)
    } else {
        job.match_sql.clone()
    };

    match engine.sql_stream(&match_sql).await {
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
                        &ProgressCallback::Matching {
                            run_id,
                            progress: 0.0, // progress unknown during streaming
                            matched_count: Some(matched_count),
                        },
                    )
                    .await;
            }
        }
        Err(e) => {
            let _ = callback
                .report_error(
                    callback_url,
                    &ErrorCallback {
                        run_id,
                        error: format!("Match SQL failed: {}", e),
                        stage: Some("matching".to_string()),
                    },
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

    // Report completion (with retry — most critical callback)
    let _ = callback
        .report_complete(
            callback_url,
            &CompletionCallback {
                run_id,
                matched_count,
                unmatched_left_count: unmatched_left,
                unmatched_right_count: unmatched_right,
                output_paths: OutputPaths {
                    matched: format!("{}/matched.parquet", job.output_path),
                    unmatched_left: format!("{}/unmatched_left.parquet", job.output_path),
                    unmatched_right: format!("{}/unmatched_right.parquet", job.output_path),
                },
            },
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
/// that executes jobs concurrently as separate tokio tasks, limited by
/// `config.max_concurrent_jobs`.
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

    // Concurrency limiter — at most max_concurrent_jobs run in parallel.
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_jobs));

    // Spawn the HTTP server
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Job processor loop — spawns each job as a separate tokio task,
    // gated by the semaphore for concurrency control.
    while let Some(job) = job_rx.recv().await {
        let cfg = config.clone();
        let m = runner_metrics.clone();
        let sem = semaphore.clone();
        m.queued_jobs.inc();
        tokio::spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
            m.queued_jobs.dec();
            execute_job(job, &cfg, &m).await;
        });
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_select_star_specific_columns() {
        let sql =
            "SELECT l.invoice_id, r.payment_id FROM left_src l JOIN right_src r ON l.id = r.id";
        let result = rewrite_select_star(sql);
        assert_eq!(
            result,
            "SELECT * FROM left_src l JOIN right_src r ON l.id = r.id"
        );
    }

    #[test]
    fn test_rewrite_select_star_already_star() {
        let sql = "SELECT * FROM left_src l JOIN right_src r ON l.id = r.id";
        let result = rewrite_select_star(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn test_rewrite_select_star_aliased_columns() {
        let sql = "SELECT l.amount AS invoice_amount, r.paid_amount AS payment_amount FROM left_src l JOIN right_src r ON l.batch_ref = r.reference_number";
        let result = rewrite_select_star(sql);
        assert_eq!(
            result,
            "SELECT * FROM left_src l JOIN right_src r ON l.batch_ref = r.reference_number"
        );
    }

    #[test]
    fn test_rewrite_select_star_case_insensitive() {
        let sql = "select l.id from left_src l join right_src r on l.id = r.id";
        let result = rewrite_select_star(sql);
        assert_eq!(
            result,
            "SELECT * from left_src l join right_src r on l.id = r.id"
        );
    }

    #[test]
    fn test_rewrite_select_star_no_from() {
        let sql = "SELECT 1";
        let result = rewrite_select_star(sql);
        assert_eq!(result, "SELECT 1");
    }
}
