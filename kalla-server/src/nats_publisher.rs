//! NATS publisher — pushes jobs to stage and exec queues.

use anyhow::Result;
use async_nats::jetstream;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const STAGE_STREAM: &str = "KALLA_STAGE";
const EXEC_STREAM: &str = "KALLA_EXEC";
const STAGE_SUBJECT: &str = "kalla.stage";
const EXEC_SUBJECT: &str = "kalla.exec";

/// Job message published to NATS.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JobMessage {
    StagePlan {
        job_id: Uuid,
        run_id: Uuid,
        source_uri: String,
        source_alias: String,
        partition_key: Option<String>,
    },
    Exec {
        job_id: Uuid,
        run_id: Uuid,
        recipe_json: String,
        staged_sources: Vec<StagedSource>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StagedSource {
    pub alias: String,
    pub s3_path: String,
    pub is_native: bool,
}

/// Publisher for pushing jobs to NATS queues.
#[derive(Clone)]
pub struct NatsPublisher {
    jetstream: jetstream::Context,
}

impl NatsPublisher {
    pub async fn connect(nats_url: &str) -> Result<Self> {
        let client = async_nats::connect(nats_url).await?;
        let jetstream = jetstream::new(client);

        // Ensure streams exist
        jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: STAGE_STREAM.to_string(),
                subjects: vec![STAGE_SUBJECT.to_string()],
                retention: jetstream::stream::RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await?;

        jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: EXEC_STREAM.to_string(),
                subjects: vec![EXEC_SUBJECT.to_string()],
                retention: jetstream::stream::RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await?;

        Ok(Self { jetstream })
    }

    pub async fn publish_stage(&self, msg: &JobMessage) -> Result<()> {
        let payload = serde_json::to_vec(msg)?;
        self.jetstream
            .publish(STAGE_SUBJECT, payload.into())
            .await?
            .await?;
        Ok(())
    }

    pub async fn publish_exec(&self, msg: &JobMessage) -> Result<()> {
        let payload = serde_json::to_vec(msg)?;
        self.jetstream
            .publish(EXEC_SUBJECT, payload.into())
            .await?
            .await?;
        Ok(())
    }
}
