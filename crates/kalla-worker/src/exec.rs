//! Exec handler — runs reconciliation after all sources are staged.
//!
//! Transpiles the recipe to SQL, executes via DataFusion (local or Ballista),
//! and writes evidence.

use anyhow::Result;
use futures::StreamExt;
use kalla_core::ReconciliationEngine;
use kalla_evidence::{EvidenceStore, MatchedRecord};
use kalla_recipe::{MatchRecipe, Transpiler};
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
    let recipe: MatchRecipe = serde_json::from_str(recipe_json)?;

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
