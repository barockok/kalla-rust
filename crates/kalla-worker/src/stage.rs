//! Stage handler — extracts non-native sources to Parquet on S3.

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use kalla_connectors::{PostgresConnector, S3Config, S3Connector};
use kalla_core::ReconciliationEngine;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::WorkerConfig;
use crate::queue::{JobMessage, QueueClient};

/// Handle a StagePlan job — count rows, decide on chunking, fan out chunk jobs.
#[allow(clippy::too_many_arguments)]
pub async fn handle_stage_plan(
    pool: &PgPool,
    queue: &QueueClient,
    config: &WorkerConfig,
    run_id: Uuid,
    job_id: Uuid,
    source_uri: &str,
    source_alias: &str,
    _partition_key: Option<&str>,
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
        let num_chunks =
            (row_count / config.chunk_threshold_rows).min(config.max_parallel_chunks as u64) as u32;
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
#[allow(clippy::too_many_arguments)]
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

        if std::env::var("AWS_ACCESS_KEY_ID").is_ok() {
            let (bucket, key) = S3Connector::parse_s3_uri(output_path)?;
            let s3_config = S3Config::from_env()?;
            let mut builder = AmazonS3Builder::new()
                .with_region(&s3_config.region)
                .with_bucket_name(&bucket)
                .with_access_key_id(&s3_config.access_key_id)
                .with_secret_access_key(&s3_config.secret_access_key);
            if let Some(ref endpoint) = s3_config.endpoint_url {
                builder = builder.with_endpoint(endpoint);
            }
            if s3_config.allow_http {
                builder = builder.with_allow_http(true);
            }
            let store = builder.build()?;
            let path = ObjectPath::from(key.as_str());
            let buf_len = buf.len();
            store.put(&path, buf.into()).await?;
            info!("Uploaded {} bytes to {}", buf_len, output_path);
        } else {
            warn!(
                "AWS_ACCESS_KEY_ID not set, skipping S3 upload ({} bytes to {})",
                buf.len(),
                output_path
            );
        }
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
