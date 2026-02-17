//! Health and metrics HTTP endpoints (Axum).

use axum::{extract::State, http::StatusCode, routing::get, Router};
use std::sync::Arc;

use crate::metrics::WorkerMetrics;

pub struct HealthState {
    pub metrics: WorkerMetrics,
    pub ready: Arc<std::sync::atomic::AtomicBool>,
}

pub fn health_router(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn health() -> &'static str {
    "OK"
}

async fn ready(State(state): State<Arc<HealthState>>) -> Result<&'static str, StatusCode> {
    if state.ready.load(std::sync::atomic::Ordering::Relaxed) {
        Ok("OK")
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

async fn metrics(State(state): State<Arc<HealthState>>) -> String {
    state.metrics.encode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::WorkerMetrics;
    use axum::body::Body;
    use http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_state(ready: bool) -> Arc<HealthState> {
        Arc::new(HealthState {
            metrics: WorkerMetrics::new(),
            ready: Arc::new(std::sync::atomic::AtomicBool::new(ready)),
        })
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let app = health_router(test_state(true));

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"OK");
    }

    #[tokio::test]
    async fn ready_returns_ok_when_ready() {
        let app = health_router(test_state(true));

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"OK");
    }

    #[tokio::test]
    async fn ready_returns_503_when_not_ready() {
        let app = health_router(test_state(false));

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn metrics_returns_prometheus_text() {
        let state = test_state(true);
        state.metrics.exec_queue_depth.set(10);
        let app = health_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(
            text.contains("kalla_exec_queue_depth"),
            "Expected metric name in output: {text}"
        );
        assert!(text.contains("# HELP"), "Expected HELP lines: {text}");
        assert!(text.contains("# TYPE"), "Expected TYPE lines: {text}");
    }
}
