//! Exec handler — runs reconciliation after all sources are staged.
//!
//! Executes match_sql directly via DataFusion and derives unmatched records
//! using primary keys.

use anyhow::Result;
use futures::StreamExt;
use kalla_core::ReconciliationEngine;
use kalla_evidence::{EvidenceStore, MatchedRecord};
use kalla_recipe::Recipe;
use sqlx::PgPool;
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
    let recipe: Recipe = serde_json::from_str(recipe_json)?;

    let engine = ReconciliationEngine::new();

    // Register all sources (staged Parquet or native)
    for source in staged_sources {
        if source.s3_path.ends_with(".parquet") || source.s3_path.contains("/staging/") {
            engine
                .register_parquet(&source.alias, &source.s3_path)
                .await?;
        } else if source.s3_path.ends_with(".csv") {
            engine.register_csv(&source.alias, &source.s3_path).await?;
        }
        info!(
            "Registered source '{}' from {}",
            source.alias, source.s3_path
        );
    }

    // Execute match SQL directly
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
        Err(e) => warn!("Match SQL failed: {}", e),
    }

    // Derive unmatched using primary keys via LEFT ANTI JOIN
    let mut unmatched_left = 0u64;
    let mut unmatched_right = 0u64;

    let left_alias = &recipe.sources.left.alias;
    let right_alias = &recipe.sources.right.alias;

    if !recipe.sources.left.primary_key.is_empty()
        && !recipe.sources.right.primary_key.is_empty()
    {
        let lpk = &recipe.sources.left.primary_key[0];
        let rpk = &recipe.sources.right.primary_key[0];

        let left_orphan_sql = format!(
            "SELECT {l}.* FROM {l} LEFT JOIN {r} ON {l}.{lpk} = {r}.{rpk} WHERE {r}.{rpk} IS NULL",
            l = left_alias, r = right_alias, lpk = lpk, rpk = rpk
        );
        if let Ok(mut stream) = engine.sql_stream(&left_orphan_sql).await {
            while let Some(Ok(batch)) = stream.next().await {
                unmatched_left += batch.num_rows() as u64;
            }
        }

        let right_orphan_sql = format!(
            "SELECT {r}.* FROM {r} LEFT JOIN {l} ON {r}.{rpk} = {l}.{lpk} WHERE {l}.{lpk} IS NULL",
            l = left_alias, r = right_alias, lpk = lpk, rpk = rpk
        );
        if let Ok(mut stream) = engine.sql_stream(&right_orphan_sql).await {
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

#[allow(dead_code)]
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
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        return Some(arr.value(row_idx).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return Some(arr.value(row_idx).to_string());
    }
    None
}
