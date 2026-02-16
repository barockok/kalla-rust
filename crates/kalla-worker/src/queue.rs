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
        /// Optional HTTP callback URL — worker POSTs results to `{url}/complete` on finish.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        callback_url: Option<String>,
        /// Original source URIs for direct (non-staged) execution.
        /// When present with BALLISTA_ENABLED, workers read directly from sources.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_uris: Option<Vec<SourceUri>>,
    },
}

/// A source that has been staged to S3 Parquet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedSource {
    pub alias: String,
    pub s3_path: String,
    pub is_native: bool,
}

/// Original source URI for direct (non-staged) execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceUri {
    pub alias: String,
    pub uri: String,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_plan_roundtrip() {
        let msg = JobMessage::StagePlan {
            job_id: Uuid::nil(),
            run_id: Uuid::nil(),
            source_uri: "postgres://localhost/db?table=accounts".to_string(),
            source_alias: "accounts".to_string(),
            partition_key: Some("region".to_string()),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: JobMessage = serde_json::from_str(&json).unwrap();

        match deserialized {
            JobMessage::StagePlan {
                job_id,
                run_id,
                source_uri,
                source_alias,
                partition_key,
            } => {
                assert_eq!(job_id, Uuid::nil());
                assert_eq!(run_id, Uuid::nil());
                assert_eq!(source_uri, "postgres://localhost/db?table=accounts");
                assert_eq!(source_alias, "accounts");
                assert_eq!(partition_key, Some("region".to_string()));
            }
            other => panic!("Expected StagePlan, got {:?}", other),
        }
    }

    #[test]
    fn stage_plan_without_partition_key() {
        let msg = JobMessage::StagePlan {
            job_id: Uuid::nil(),
            run_id: Uuid::nil(),
            source_uri: "postgres://localhost/db?table=t".to_string(),
            source_alias: "t".to_string(),
            partition_key: None,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"partition_key\":null"));

        let deserialized: JobMessage = serde_json::from_str(&json).unwrap();
        match deserialized {
            JobMessage::StagePlan { partition_key, .. } => {
                assert_eq!(partition_key, None);
            }
            other => panic!("Expected StagePlan, got {:?}", other),
        }
    }

    #[test]
    fn stage_chunk_roundtrip() {
        let msg = JobMessage::StageChunk {
            job_id: Uuid::nil(),
            run_id: Uuid::nil(),
            source_uri: "postgres://localhost/db?table=txns".to_string(),
            source_alias: "txns".to_string(),
            chunk_index: 2,
            total_chunks: 5,
            offset: 2_000_000,
            limit: 1_000_000,
            output_path: "s3://kalla-staging/run-1/txns/part-02.parquet".to_string(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: JobMessage = serde_json::from_str(&json).unwrap();

        match deserialized {
            JobMessage::StageChunk {
                chunk_index,
                total_chunks,
                offset,
                limit,
                output_path,
                ..
            } => {
                assert_eq!(chunk_index, 2);
                assert_eq!(total_chunks, 5);
                assert_eq!(offset, 2_000_000);
                assert_eq!(limit, 1_000_000);
                assert_eq!(output_path, "s3://kalla-staging/run-1/txns/part-02.parquet");
            }
            other => panic!("Expected StageChunk, got {:?}", other),
        }
    }

    #[test]
    fn exec_roundtrip() {
        let msg = JobMessage::Exec {
            job_id: Uuid::nil(),
            run_id: Uuid::nil(),
            recipe_json: r#"{"match_rules":[]}"#.to_string(),
            staged_sources: vec![
                StagedSource {
                    alias: "left".to_string(),
                    s3_path: "s3://bucket/left.parquet".to_string(),
                    is_native: false,
                },
                StagedSource {
                    alias: "right".to_string(),
                    s3_path: "s3://bucket/right.parquet".to_string(),
                    is_native: true,
                },
            ],
            callback_url: None,
            source_uris: None,
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: JobMessage = serde_json::from_str(&json).unwrap();

        match deserialized {
            JobMessage::Exec {
                staged_sources,
                recipe_json,
                ..
            } => {
                assert_eq!(staged_sources.len(), 2);
                assert_eq!(staged_sources[0].alias, "left");
                assert!(!staged_sources[0].is_native);
                assert_eq!(staged_sources[1].alias, "right");
                assert!(staged_sources[1].is_native);
                assert_eq!(recipe_json, r#"{"match_rules":[]}"#);
            }
            other => panic!("Expected Exec, got {:?}", other),
        }
    }

    #[test]
    fn serde_tag_discriminator() {
        let plan = JobMessage::StagePlan {
            job_id: Uuid::nil(),
            run_id: Uuid::nil(),
            source_uri: "s".to_string(),
            source_alias: "a".to_string(),
            partition_key: None,
        };
        let chunk = JobMessage::StageChunk {
            job_id: Uuid::nil(),
            run_id: Uuid::nil(),
            source_uri: "s".to_string(),
            source_alias: "a".to_string(),
            chunk_index: 0,
            total_chunks: 1,
            offset: 0,
            limit: 100,
            output_path: "out".to_string(),
        };
        let exec = JobMessage::Exec {
            job_id: Uuid::nil(),
            run_id: Uuid::nil(),
            recipe_json: "{}".to_string(),
            staged_sources: vec![],
            callback_url: None,
            source_uris: None,
        };

        let plan_json = serde_json::to_string(&plan).unwrap();
        let chunk_json = serde_json::to_string(&chunk).unwrap();
        let exec_json = serde_json::to_string(&exec).unwrap();

        assert!(
            plan_json.contains(r#""type":"StagePlan""#),
            "Plan JSON: {plan_json}"
        );
        assert!(
            chunk_json.contains(r#""type":"StageChunk""#),
            "Chunk JSON: {chunk_json}"
        );
        assert!(
            exec_json.contains(r#""type":"Exec""#),
            "Exec JSON: {exec_json}"
        );
    }

    #[test]
    fn staged_source_roundtrip() {
        let source = StagedSource {
            alias: "transactions".to_string(),
            s3_path: "s3://bucket/staging/run-1/transactions/part-00.parquet".to_string(),
            is_native: false,
        };

        let json = serde_json::to_string(&source).unwrap();
        let deserialized: StagedSource = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.alias, "transactions");
        assert_eq!(
            deserialized.s3_path,
            "s3://bucket/staging/run-1/transactions/part-00.parquet"
        );
        assert!(!deserialized.is_native);
    }

    #[test]
    fn constants_are_correct() {
        assert_eq!(STAGE_STREAM, "KALLA_STAGE");
        assert_eq!(EXEC_STREAM, "KALLA_EXEC");
        assert_eq!(STAGE_SUBJECT, "kalla.stage");
        assert_eq!(EXEC_SUBJECT, "kalla.exec");
    }

    #[test]
    fn exec_with_source_uris_roundtrip() {
        let msg = JobMessage::Exec {
            job_id: Uuid::nil(),
            run_id: Uuid::nil(),
            recipe_json: "{}".to_string(),
            staged_sources: vec![],
            callback_url: None,
            source_uris: Some(vec![SourceUri {
                alias: "invoices".to_string(),
                uri: "postgres://localhost/db?table=invoices".to_string(),
            }]),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("source_uris"));
        let deserialized: JobMessage = serde_json::from_str(&json).unwrap();
        match deserialized {
            JobMessage::Exec { source_uris, .. } => {
                let uris = source_uris.unwrap();
                assert_eq!(uris.len(), 1);
                assert_eq!(uris[0].alias, "invoices");
                assert_eq!(uris[0].uri, "postgres://localhost/db?table=invoices");
            }
            _ => panic!("Expected Exec"),
        }
    }

    #[test]
    fn exec_without_source_uris_backward_compat() {
        // Old format without source_uris should deserialize fine
        let json = r#"{"type":"Exec","job_id":"00000000-0000-0000-0000-000000000000","run_id":"00000000-0000-0000-0000-000000000000","recipe_json":"{}","staged_sources":[]}"#;
        let msg: JobMessage = serde_json::from_str(json).unwrap();
        match msg {
            JobMessage::Exec { source_uris, .. } => {
                assert!(source_uris.is_none());
            }
            _ => panic!("Expected Exec"),
        }
    }
}
