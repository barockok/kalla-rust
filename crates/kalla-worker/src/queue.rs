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
    stage_stream: tokio::sync::Mutex<JsStream>,
    exec_stream: tokio::sync::Mutex<JsStream>,
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
            stage_stream: tokio::sync::Mutex::new(stage_stream),
            exec_stream: tokio::sync::Mutex::new(exec_stream),
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
        let stream = self.stage_stream.lock().await;
        let consumer = stream
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
        let stream = self.exec_stream.lock().await;
        let consumer = stream
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
        let mut stream = self.stage_stream.lock().await;
        let info = stream.info().await?;
        Ok(info.state.messages)
    }

    /// Get current pending message count for the exec stream.
    pub async fn exec_queue_depth(&self) -> Result<u64> {
        let mut stream = self.exec_stream.lock().await;
        let info = stream.info().await?;
        Ok(info.state.messages)
    }
}
