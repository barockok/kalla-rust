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
