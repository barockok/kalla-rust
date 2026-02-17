//! NATS JetStream queue client and message types.

use anyhow::Result;
use async_nats::jetstream::{self, consumer::PullConsumer, stream::Stream as JsStream};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const EXEC_STREAM: &str = "KALLA_EXEC";
pub const EXEC_SUBJECT: &str = "kalla.exec";

/// Job types that flow through the queues.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JobMessage {
    /// Execute reconciliation directly from source URIs.
    Exec {
        job_id: Uuid,
        run_id: Uuid,
        recipe_json: String,
        /// Original source URIs for direct execution.
        source_uris: Vec<SourceUri>,
        /// Optional HTTP callback URL — worker POSTs results to `{url}/complete` on finish.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        callback_url: Option<String>,
    },
}

/// Original source URI for direct execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceUri {
    pub alias: String,
    pub uri: String,
}

/// NATS JetStream queue client.
pub struct QueueClient {
    exec_stream: tokio::sync::Mutex<JsStream>,
}

impl QueueClient {
    /// Connect to NATS and ensure streams exist.
    pub async fn connect(nats_url: &str) -> Result<Self> {
        let client = async_nats::connect(nats_url).await?;
        let jetstream = jetstream::new(client);

        let exec_stream = jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: EXEC_STREAM.to_string(),
                subjects: vec![EXEC_SUBJECT.to_string()],
                retention: jetstream::stream::RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await?;

        Ok(Self {
            exec_stream: tokio::sync::Mutex::new(exec_stream),
        })
    }

    /// Create a pull consumer for the exec queue.
    ///
    /// Uses a shared consumer name so multiple workers pull from the same
    /// consumer on the WorkQueue stream.
    pub async fn exec_consumer(&self, _worker_id: &str) -> Result<PullConsumer> {
        let stream = self.exec_stream.lock().await;
        let consumer = stream
            .get_or_create_consumer(
                "kalla-exec-workers",
                jetstream::consumer::pull::Config {
                    durable_name: Some("kalla-exec-workers".to_string()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await?;
        Ok(consumer)
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
    fn exec_roundtrip() {
        let msg = JobMessage::Exec {
            job_id: Uuid::nil(),
            run_id: Uuid::nil(),
            recipe_json: r#"{"match_rules":[]}"#.to_string(),
            source_uris: vec![SourceUri {
                alias: "left".to_string(),
                uri: "postgres://localhost/db?table=left".to_string(),
            }],
            callback_url: None,
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: JobMessage = serde_json::from_str(&json).unwrap();

        match deserialized {
            JobMessage::Exec {
                source_uris,
                recipe_json,
                ..
            } => {
                assert_eq!(source_uris.len(), 1);
                assert_eq!(source_uris[0].alias, "left");
                assert_eq!(recipe_json, r#"{"match_rules":[]}"#);
            }
        }
    }

    #[test]
    fn serde_tag_discriminator() {
        let exec = JobMessage::Exec {
            job_id: Uuid::nil(),
            run_id: Uuid::nil(),
            recipe_json: "{}".to_string(),
            source_uris: vec![],
            callback_url: None,
        };

        let exec_json = serde_json::to_string(&exec).unwrap();
        assert!(
            exec_json.contains(r#""type":"Exec""#),
            "Exec JSON: {exec_json}"
        );
    }

    #[test]
    fn constants_are_correct() {
        assert_eq!(EXEC_STREAM, "KALLA_EXEC");
        assert_eq!(EXEC_SUBJECT, "kalla.exec");
    }

    #[test]
    fn exec_with_source_uris_roundtrip() {
        let msg = JobMessage::Exec {
            job_id: Uuid::nil(),
            run_id: Uuid::nil(),
            recipe_json: "{}".to_string(),
            source_uris: vec![SourceUri {
                alias: "invoices".to_string(),
                uri: "postgres://localhost/db?table=invoices".to_string(),
            }],
            callback_url: None,
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("source_uris"));
        let deserialized: JobMessage = serde_json::from_str(&json).unwrap();
        match deserialized {
            JobMessage::Exec { source_uris, .. } => {
                assert_eq!(source_uris.len(), 1);
                assert_eq!(source_uris[0].alias, "invoices");
                assert_eq!(source_uris[0].uri, "postgres://localhost/db?table=invoices");
            }
        }
    }
}
