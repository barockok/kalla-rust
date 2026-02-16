//! HTTP API for single-mode job submission and callback client for progress reporting.

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::WorkerState;

// ---------------------------------------------------------------------------
// Job submission types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct JobRequest {
    pub run_id: Uuid,
    pub callback_url: String,
    pub match_sql: String,
    pub sources: Vec<ResolvedSource>,
    pub output_path: String,
    pub primary_keys: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub stage_to_parquet: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResolvedSource {
    pub alias: String,
    pub uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobAccepted {
    pub run_id: Uuid,
    pub status: String,
}

/// POST /api/jobs — accept a job for processing.
async fn submit_job(
    State(state): State<Arc<WorkerState>>,
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

/// Build the job submission router.
pub fn job_router(state: Arc<WorkerState>) -> Router {
    Router::new()
        .route("/api/jobs", post(submit_job))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Callback client — reports progress/completion/error back to the API.
// ---------------------------------------------------------------------------

pub struct CallbackClient {
    http: reqwest::Client,
}

impl CallbackClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }

    pub async fn report_progress(
        &self,
        callback_url: &str,
        progress: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.http
            .post(format!("{}/progress", callback_url))
            .json(progress)
            .send()
            .await?;
        Ok(())
    }

    pub async fn report_complete(
        &self,
        callback_url: &str,
        result: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.http
            .post(format!("{}/complete", callback_url))
            .json(result)
            .send()
            .await?;
        Ok(())
    }

    pub async fn report_error(
        &self,
        callback_url: &str,
        error: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.http
            .post(format!("{}/error", callback_url))
            .json(error)
            .send()
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_state() -> (Arc<WorkerState>, tokio::sync::mpsc::Receiver<JobRequest>) {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        (Arc::new(WorkerState { job_tx: tx }), rx)
    }

    #[tokio::test]
    async fn submit_job_returns_202() {
        let (state, _rx) = test_state();
        let app = job_router(state);

        let body = serde_json::json!({
            "run_id": "00000000-0000-0000-0000-000000000001",
            "callback_url": "http://localhost:3000/api/worker",
            "match_sql": "SELECT * FROM left JOIN right ON left.id = right.id",
            "sources": [
                {"alias": "left", "uri": "postgres://localhost/db?table=left"},
                {"alias": "right", "uri": "s3://staging/right.parquet"}
            ],
            "output_path": "s3://results/run-1/",
            "primary_keys": {
                "left": ["id"],
                "right": ["id"]
            }
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let accepted: JobAccepted = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(accepted.status, "accepted");
    }

    #[tokio::test]
    async fn submit_job_rejects_invalid_json() {
        let (state, _rx) = test_state();
        let app = job_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(b"not json".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Axum returns 422 for deserialization failures
        assert!(resp.status().is_client_error());
    }

    #[test]
    fn job_request_roundtrip() {
        let json = serde_json::json!({
            "run_id": "00000000-0000-0000-0000-000000000001",
            "callback_url": "http://localhost:3000/api/worker",
            "match_sql": "SELECT * FROM a JOIN b ON a.id = b.id",
            "sources": [{"alias": "a", "uri": "file:///tmp/a.csv"}],
            "output_path": "/tmp/results/",
            "primary_keys": {"a": ["id"]}
        });

        let req: JobRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.sources.len(), 1);
        assert_eq!(req.sources[0].alias, "a");
        assert_eq!(req.primary_keys["a"], vec!["id"]);
    }

    #[test]
    fn callback_client_creates() {
        let _client = CallbackClient::new();
    }
}
