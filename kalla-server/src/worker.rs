//! Worker process — reconciliation execution, DataFusion queries, evidence writing.
//!
//! Communicates with the API server via tokio channels (in-process).
//! Can be replaced with gRPC later without changing the command/result types.

use std::sync::Arc;

use arrow::array::Int64Array;
use futures::StreamExt;
use kalla_connectors::PostgresConnector;
use kalla_core::ReconciliationEngine;
use kalla_evidence::{EvidenceStore, MatchedRecord, RunMetadata, UnmatchedRecord};
use kalla_recipe::{MatchRecipe, Transpiler};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{extract_string_value, parse_postgres_uri};

// ---------------------------------------------------------------------------
// Command / result types
// ---------------------------------------------------------------------------

/// A command sent from the API server to the worker.
pub enum WorkerCommand {
    /// Execute a full reconciliation run.
    ExecuteRun {
        run_id: Uuid,
        recipe: MatchRecipe,
        /// Caller gets notified when the run finishes.
        reply: oneshot::Sender<Result<RunResult, String>>,
    },
}

/// Summary returned after a run completes.
#[derive(Debug, Clone)]
pub struct RunResult {
    pub run_id: Uuid,
    pub left_count: u64,
    pub right_count: u64,
    pub matched_count: u64,
    pub unmatched_left: u64,
    pub unmatched_right: u64,
}

// ---------------------------------------------------------------------------
// WorkerHandle — the sending half that lives in the API server
// ---------------------------------------------------------------------------

/// A cheaply-cloneable handle for sending commands to the worker.
#[derive(Clone)]
pub struct WorkerHandle {
    tx: mpsc::Sender<WorkerCommand>,
}

impl WorkerHandle {
    /// Submit a reconciliation run to the worker. Returns a oneshot receiver
    /// that will resolve when the run finishes.
    pub async fn submit_run(
        &self,
        run_id: Uuid,
        recipe: MatchRecipe,
    ) -> Result<oneshot::Receiver<Result<RunResult, String>>, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WorkerCommand::ExecuteRun {
                run_id,
                recipe,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "Worker channel closed".to_string())?;
        Ok(reply_rx)
    }
}

// ---------------------------------------------------------------------------
// Worker — the receiving half
// ---------------------------------------------------------------------------

/// The reconciliation worker. Owns an engine and the evidence store.
pub struct Worker {
    rx: mpsc::Receiver<WorkerCommand>,
    evidence_store: Arc<EvidenceStore>,
    runs: Arc<RwLock<Vec<RunMetadata>>>,
}

impl Worker {
    /// Spawn a new worker, returning its handle and the shared runs/evidence refs.
    pub fn spawn(
        evidence_store: Arc<EvidenceStore>,
        runs: Arc<RwLock<Vec<RunMetadata>>>,
        buffer: usize,
    ) -> WorkerHandle {
        let (tx, rx) = mpsc::channel(buffer);
        let worker = Worker {
            rx,
            evidence_store,
            runs,
        };
        tokio::spawn(worker.run());
        WorkerHandle { tx }
    }

    async fn run(mut self) {
        info!("Worker started, waiting for commands");
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                WorkerCommand::ExecuteRun {
                    run_id,
                    recipe,
                    reply,
                } => {
                    let result = self.execute_reconciliation(run_id, &recipe).await;

                    // Update run metadata based on result
                    {
                        let mut runs = self.runs.write().await;
                        if let Some(run) = runs.iter_mut().find(|r| r.run_id == run_id) {
                            match &result {
                                Ok(res) => {
                                    run.left_record_count = res.left_count;
                                    run.right_record_count = res.right_count;
                                    run.matched_count = res.matched_count;
                                    run.unmatched_left_count = res.unmatched_left;
                                    run.unmatched_right_count = res.unmatched_right;
                                    run.complete();
                                    let _ = self.evidence_store.update_metadata(run);
                                }
                                Err(_) => {
                                    run.fail();
                                    let _ = self.evidence_store.update_metadata(run);
                                }
                            }
                        }
                    }

                    let _ = reply.send(result);
                }
            }
        }
        info!("Worker shutting down — channel closed");
    }

    async fn execute_reconciliation(
        &self,
        run_id: Uuid,
        recipe: &MatchRecipe,
    ) -> Result<RunResult, String> {
        info!("Starting reconciliation execution for run {}", run_id);

        let (left_conn, left_table) = parse_postgres_uri(&recipe.sources.left.uri)?;
        let (_right_conn, right_table) = parse_postgres_uri(&recipe.sources.right.uri)?;

        let engine = ReconciliationEngine::new();

        let connector = PostgresConnector::new(&left_conn)
            .await
            .map_err(|e| format!("Failed to connect to database: {}", e))?;

        connector
            .register_table(
                engine.context(),
                &recipe.sources.left.alias,
                &left_table,
                None,
            )
            .await
            .map_err(|e| format!("Failed to register left table: {}", e))?;

        connector
            .register_table(
                engine.context(),
                &recipe.sources.right.alias,
                &right_table,
                None,
            )
            .await
            .map_err(|e| format!("Failed to register right table: {}", e))?;

        // Counts (streamed)
        let left_count = Self::stream_count(&engine, &recipe.sources.left.alias).await?;
        let right_count = Self::stream_count(&engine, &recipe.sources.right.alias).await?;

        info!(
            "Left source has {} records, right source has {} records",
            left_count, right_count
        );

        let transpiled =
            Transpiler::transpile(recipe).map_err(|e| format!("Failed to transpile: {}", e))?;

        // Matches — stream through batches instead of collecting everything
        let mut total_matched: u64 = 0;
        let mut matched_records: Vec<MatchedRecord> = Vec::new();

        for rule in &transpiled.match_queries {
            info!(
                "Executing match rule: {} with query: {}",
                rule.name, rule.query
            );
            match engine.sql_stream(&rule.query).await {
                Ok(mut stream) => {
                    while let Some(batch_result) = stream.next().await {
                        let batch = batch_result
                            .map_err(|e| format!("Stream error in rule '{}': {}", rule.name, e))?;
                        let count = batch.num_rows() as u64;

                        let left_pk_col = recipe
                            .sources
                            .left
                            .primary_key
                            .as_ref()
                            .and_then(|v| v.first())
                            .map(|s| s.as_str())
                            .unwrap_or("id");
                        let right_pk_col = recipe
                            .sources
                            .right
                            .primary_key
                            .as_ref()
                            .and_then(|v| v.first())
                            .map(|s| s.as_str())
                            .unwrap_or("id");

                        for row_idx in 0..batch.num_rows() {
                            let left_key = extract_string_value(&batch, left_pk_col, row_idx)
                                .unwrap_or_else(|| format!("row_{}", row_idx));
                            let right_key = extract_string_value(&batch, right_pk_col, row_idx)
                                .unwrap_or_else(|| format!("row_{}", row_idx));
                            matched_records.push(MatchedRecord::new(
                                left_key,
                                right_key,
                                rule.name.clone(),
                                1.0,
                            ));
                        }

                        total_matched += count;
                    }
                    info!("Rule '{}' matched {} records total", rule.name, total_matched);
                }
                Err(e) => {
                    warn!("Failed to execute match rule '{}': {}", rule.name, e);
                }
            }
        }

        // Orphans — streamed
        let mut unmatched_left: u64 = 0;
        let mut unmatched_right: u64 = 0;
        let mut left_orphan_records: Vec<UnmatchedRecord> = Vec::new();
        let mut right_orphan_records: Vec<UnmatchedRecord> = Vec::new();

        if let Some(ref query) = transpiled.left_orphan_query {
            Self::stream_orphans(
                &engine,
                query,
                &transpiled.match_queries.iter().map(|r| r.name.clone()).collect::<Vec<_>>(),
                "left",
                &mut unmatched_left,
                &mut left_orphan_records,
            )
            .await?;
        }

        if let Some(ref query) = transpiled.right_orphan_query {
            Self::stream_orphans(
                &engine,
                query,
                &transpiled.match_queries.iter().map(|r| r.name.clone()).collect::<Vec<_>>(),
                "right",
                &mut unmatched_right,
                &mut right_orphan_records,
            )
            .await?;
        }

        // Write evidence
        if !matched_records.is_empty() {
            let _ = self.evidence_store.write_matched(&run_id, &matched_records);
        }
        if !left_orphan_records.is_empty() {
            let _ = self
                .evidence_store
                .write_unmatched(&run_id, &left_orphan_records, "left");
        }
        if !right_orphan_records.is_empty() {
            let _ = self
                .evidence_store
                .write_unmatched(&run_id, &right_orphan_records, "right");
        }

        info!(
            "Reconciliation complete for run {}. Matched: {}, Left orphans: {}, Right orphans: {}",
            run_id, total_matched, unmatched_left, unmatched_right
        );

        Ok(RunResult {
            run_id,
            left_count,
            right_count,
            matched_count: total_matched,
            unmatched_left,
            unmatched_right,
        })
    }

    /// Get row count via streaming (constant memory).
    async fn stream_count(engine: &ReconciliationEngine, alias: &str) -> Result<u64, String> {
        let mut stream = engine
            .sql_stream(&format!("SELECT COUNT(*) as cnt FROM {}", alias))
            .await
            .map_err(|e| format!("Failed to count {}: {}", alias, e))?;

        let mut count: u64 = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| format!("Stream error counting {}: {}", alias, e))?;
            if let Some(arr) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                if arr.len() > 0 {
                    count = arr.value(0) as u64;
                }
            }
        }
        Ok(count)
    }

    /// Stream orphan query results and populate records.
    async fn stream_orphans(
        engine: &ReconciliationEngine,
        query: &str,
        rule_names: &[String],
        side: &str,
        unmatched_count: &mut u64,
        records: &mut Vec<UnmatchedRecord>,
    ) -> Result<(), String> {
        info!("Executing {} orphan query: {}", side, query);
        match engine.sql_stream(query).await {
            Ok(mut stream) => {
                let mut idx = 0u64;
                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result.map_err(|e| {
                        format!("Stream error in {} orphan query: {}", side, e)
                    })?;
                    let rows = batch.num_rows() as u64;
                    for _ in 0..batch.num_rows() {
                        records.push(UnmatchedRecord {
                            record_key: format!("{}_row_{}", side, idx),
                            attempted_rules: rule_names.to_vec(),
                            closest_candidate: None,
                            rejection_reason: "No matching record found".to_string(),
                        });
                        idx += 1;
                    }
                    *unmatched_count += rows;
                }
                info!("Found {} unmatched {} records", unmatched_count, side);
            }
            Err(e) => {
                warn!("Failed to execute {} orphan query: {}", side, e);
            }
        }
        Ok(())
    }
}
