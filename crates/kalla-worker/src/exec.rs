//! Exec handler — runs reconciliation directly from source URIs.
//!
//! Two execution paths:
//! - `handle_http_job`: Single mode — receives a self-contained JobRequest via HTTP,
//!   runs match_sql directly, reports progress via HTTP callbacks.
//! - `handle_exec`: Scaled mode — receives job via NATS, registers partitioned sources
//!   and executes match_sql via DataFusion.

use anyhow::Result;
use futures::StreamExt;
use kalla_core::ReconciliationEngine;
use kalla_evidence::{EvidenceStore, MatchedRecord};
use kalla_recipe::Recipe;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::{info, warn};
use uuid::Uuid;

use std::time::Instant;

use crate::config::WorkerConfig;
use crate::http_api::{CallbackClient, JobRequest};
use crate::queue::SourceUri;

pub struct ExecResult {
    pub matched: u64,
    pub unmatched_left: u64,
    pub unmatched_right: u64,
}

// ---------------------------------------------------------------------------
// Single mode: HTTP job execution with direct SQL
// ---------------------------------------------------------------------------

/// Execute a job received via HTTP (single mode).
/// Runs match_sql directly against registered sources and reports via callbacks.
pub async fn handle_http_job(
    config: &WorkerConfig,
    callback: &CallbackClient,
    job: JobRequest,
) -> Result<ExecResult> {
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

    let engine = ReconciliationEngine::new();

    // Register all sources
    let staging_start = Instant::now();
    for (i, source) in job.sources.iter().enumerate() {
        register_source(&engine, &source.alias, &source.uri, config).await?;
        info!("Registered source '{}' from {}", source.alias, source.uri);

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

    // Execute match SQL
    let matching_start = Instant::now();
    let mut matched_count = 0u64;
    let mut matched_records: Vec<MatchedRecord> = Vec::new();

    match engine.sql_stream(&job.match_sql).await {
        Ok(mut stream) => {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                for row_idx in 0..batch.num_rows() {
                    // Try to extract keys from the first primary key column of each source
                    let left_key = extract_first_key(&batch, &job.primary_keys, row_idx, true)
                        .unwrap_or_else(|| format!("row_{}", matched_count + row_idx as u64));
                    let right_key = extract_first_key(&batch, &job.primary_keys, row_idx, false)
                        .unwrap_or_else(|| format!("row_{}", matched_count + row_idx as u64));

                    matched_records.push(MatchedRecord::new(
                        left_key,
                        right_key,
                        "match_sql".to_string(),
                        1.0,
                    ));
                }
                matched_count += batch.num_rows() as u64;
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

    // Derive unmatched using LEFT ANTI JOIN on primary keys
    // Use source ordering from job.sources (Vec preserves order: left, right)
    let source_aliases: Vec<&str> = job.sources.iter().map(|s| s.alias.as_str()).collect();
    let unmatched_start = Instant::now();
    let (unmatched_left, unmatched_right) =
        count_unmatched(&engine, &job.match_sql, &job.primary_keys, &source_aliases).await?;
    let unmatched_ms = unmatched_start.elapsed().as_millis();

    info!(
        "Run {}: {} unmatched_left, {} unmatched_right in {}ms",
        run_id, unmatched_left, unmatched_right, unmatched_ms
    );

    // Report writing results
    let _ = callback
        .report_progress(
            callback_url,
            &serde_json::json!({
                "run_id": run_id,
                "stage": "writing_results",
                "matched_count": matched_count,
                "total_left": matched_count + unmatched_left,
                "total_right": matched_count + unmatched_right,
            }),
        )
        .await;

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

/// Register a source with the engine based on its URI.
async fn register_source(
    engine: &ReconciliationEngine,
    alias: &str,
    uri: &str,
    _config: &WorkerConfig,
) -> Result<()> {
    if uri.starts_with("s3://") && uri.ends_with(".csv") {
        // S3 CSV files (uploaded via presigned URL)
        let connector = kalla_connectors::S3Connector::from_env()?;
        connector
            .register_csv_listing_table(engine.context(), alias, uri)
            .await?;
    } else if uri.starts_with("s3://") || uri.ends_with(".parquet") || uri.contains("/staging/") {
        engine.register_parquet(alias, uri).await?;
    } else if uri.ends_with(".csv") {
        // Local CSV files
        engine.register_csv(alias, uri).await?;
    } else if uri.starts_with("postgres://") {
        // For Postgres data sources, use the connector to register as a table
        let parsed = url::Url::parse(uri)?;
        let table_name = parsed
            .query_pairs()
            .find(|(k, _)| k == "table")
            .map(|(_, v)| v.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing 'table' query parameter in source URI"))?;
        let mut conn_url = parsed;
        conn_url.set_query(None);

        let connector = kalla_connectors::PostgresConnector::new(conn_url.as_ref()).await?;
        connector
            .register_table(engine.context(), alias, &table_name, None)
            .await?;
    } else {
        anyhow::bail!("Unsupported source URI format: {}", uri);
    }
    Ok(())
}

/// Count unmatched records by running LEFT ANTI JOIN queries using primary keys.
///
/// `source_aliases` provides deterministic left/right ordering (from job.sources Vec).
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

    // Build NOT IN subquery for unmatched counts.
    // The match_sql result is aliased as _matched — column refs inside must be
    // unqualified since the original table aliases don't exist in the subquery result.
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

    let unmatched_left = match run_count_query(engine, &unmatched_left_sql).await {
        Ok(count) => count,
        Err(e) => {
            warn!("Unmatched left query failed: {}", e);
            0
        }
    };
    let unmatched_right = match run_count_query(engine, &unmatched_right_sql).await {
        Ok(count) => count,
        Err(e) => {
            warn!("Unmatched right query failed: {}", e);
            0
        }
    };

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

/// Try to extract a primary key value from a record batch.
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

    // Try qualified name first (alias.pk), then just pk
    let qualified = format!("{}.{}", alias, pk);
    extract_string_value(batch, &qualified, row_idx)
        .or_else(|| extract_string_value(batch, pk, row_idx))
}

// ---------------------------------------------------------------------------
// Scaled mode: NATS job execution — direct from source URIs
// ---------------------------------------------------------------------------

/// Execute the reconciliation run (scaled mode — NATS).
///
/// Registers partitioned sources directly and executes match_sql via DataFusion.
///
/// If `callback_url` is provided, POSTs completion results to `{callback_url}/complete`.
#[allow(clippy::too_many_arguments)]
pub async fn handle_exec(
    config: &WorkerConfig,
    pool: &PgPool,
    run_id: Uuid,
    job_id: Uuid,
    recipe_json: &str,
    source_uris: &[SourceUri],
    callback_url: Option<&str>,
) -> Result<ExecResult> {
    let recipe: Recipe = serde_json::from_str(recipe_json)?;

    // Choose engine: cluster mode (Ballista scheduler) or local DataFusion.
    let engine = if let Some(scheduler_url) = &config.ballista_scheduler_url {
        let codec = std::sync::Arc::new(kalla_ballista::codec::KallaPhysicalCodec::new());
        let e = ReconciliationEngine::new_cluster(scheduler_url, codec).await?;
        info!(
            "Run {}: engine created (cluster mode, scheduler={})",
            run_id, scheduler_url
        );
        e
    } else {
        let e = ReconciliationEngine::new();
        info!("Run {}: engine created (partitioned sources)", run_id);
        e
    };

    // Register sources directly from source URIs
    let num_partitions = config.ballista_partitions;

    for source_uri in source_uris {
        if source_uri.uri.starts_with("postgres://") || source_uri.uri.starts_with("postgresql://")
        {
            let parsed = url::Url::parse(&source_uri.uri)?;
            let table_name = parsed
                .query_pairs()
                .find(|(k, _)| k == "table")
                .map(|(_, v)| v.to_string())
                .ok_or_else(|| anyhow::anyhow!("Missing 'table' query parameter"))?;
            let mut conn_url = parsed.clone();
            conn_url.set_query(None);

            kalla_connectors::postgres_partitioned::register(
                engine.context(),
                &source_uri.alias,
                conn_url.as_str(),
                &table_name,
                num_partitions,
                None,
            )
            .await?;
            info!(
                "Run {}: registered partitioned Postgres table '{}' ({} partitions)",
                run_id, source_uri.alias, num_partitions
            );
        } else if source_uri.uri.starts_with("s3://") && source_uri.uri.ends_with(".csv") {
            let s3_config = kalla_connectors::S3Config::from_env()?;
            kalla_connectors::csv_partitioned::register(
                engine.context(),
                &source_uri.alias,
                &source_uri.uri,
                num_partitions,
                s3_config,
            )
            .await?;
            info!(
                "Run {}: registered byte-range CSV table '{}' ({} partitions)",
                run_id, source_uri.alias, num_partitions
            );
        } else if source_uri.uri.ends_with(".parquet") || source_uri.uri.contains("/staging/") {
            engine
                .register_parquet(&source_uri.alias, &source_uri.uri)
                .await?;
            info!(
                "Run {}: registered source '{}' from {}",
                run_id, source_uri.alias, source_uri.uri
            );
        } else if source_uri.uri.ends_with(".csv") {
            engine
                .register_csv(&source_uri.alias, &source_uri.uri)
                .await?;
            info!(
                "Run {}: registered source '{}' from {}",
                run_id, source_uri.alias, source_uri.uri
            );
        } else {
            anyhow::bail!(
                "Unsupported source URI format for direct exec: {}",
                source_uri.uri
            );
        }
    }

    // Execute match SQL
    let matching_start = Instant::now();
    let mut total_matched = 0u64;
    let mut matched_records: Vec<MatchedRecord> = Vec::new();

    let left_pk = recipe
        .sources
        .left
        .primary_key
        .first()
        .map(|s| s.as_str())
        .unwrap_or("id");
    let right_pk = recipe
        .sources
        .right
        .primary_key
        .first()
        .map(|s| s.as_str())
        .unwrap_or("id");

    match engine.sql_stream(&recipe.match_sql).await {
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
                        "match_sql".to_string(),
                        1.0,
                    ));
                }
                total_matched += batch.num_rows() as u64;
            }
        }
        Err(e) => warn!("Run {}: match SQL failed: {}", run_id, e),
    }

    let matching_ms = matching_start.elapsed().as_millis();
    info!(
        "Run {}: {} matched records in {}ms",
        run_id, total_matched, matching_ms
    );

    // Derive unmatched using NOT IN (match_sql) — same approach as single-mode
    let left_alias = &recipe.sources.left.alias;
    let right_alias = &recipe.sources.right.alias;

    let mut primary_keys = HashMap::new();
    primary_keys.insert(left_alias.clone(), recipe.sources.left.primary_key.clone());
    primary_keys.insert(
        right_alias.clone(),
        recipe.sources.right.primary_key.clone(),
    );
    let source_aliases: Vec<&str> = vec![left_alias.as_str(), right_alias.as_str()];

    let (unmatched_left, unmatched_right) =
        count_unmatched(&engine, &recipe.match_sql, &primary_keys, &source_aliases).await?;

    info!(
        "Run {}: {} unmatched_left, {} unmatched_right",
        run_id, unmatched_left, unmatched_right
    );

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

    // POST results to callback URL if provided
    if let Some(url) = callback_url {
        let callback = CallbackClient::new();
        let _ = callback
            .report_complete(
                url,
                &serde_json::json!({
                    "run_id": run_id,
                    "matched_count": total_matched,
                    "unmatched_left_count": unmatched_left,
                    "unmatched_right_count": unmatched_right,
                }),
            )
            .await;
    }

    Ok(ExecResult {
        matched: total_matched,
        unmatched_left,
        unmatched_right,
    })
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
