//! HTTP handler for `/api/sources/:alias/load-scoped`.
//!
//! Loads filtered rows from a registered data source (DB or CSV)
//! by delegating to a polymorphic `ScopedLoader` from kalla-connectors.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use tracing::info;

use crate::runner::RunnerState;

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct LoadScopedRequest {
    #[serde(default)]
    pub conditions: Vec<kalla_connectors::FilterCondition>,
    pub limit: Option<usize>,
    /// Optional S3 URI for ephemeral CSV sources (skips DB lookup).
    pub csv_uri: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Serialize)]
pub struct LoadScopedResponse {
    pub alias: String,
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<String>>,
    pub total_rows: usize,
    pub preview_rows: usize,
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

pub(crate) async fn load_scoped(
    State(state): State<Arc<RunnerState>>,
    Path(alias): Path<String>,
    Json(req): Json<LoadScopedRequest>,
) -> Result<Json<LoadScopedResponse>, (StatusCode, Json<serde_json::Value>)> {
    let limit = req.limit.unwrap_or(200).min(1000);

    // Resolve source type + URI: ephemeral CSV via csv_uri, or DB lookup
    let (source_type, uri) = if let Some(csv_uri) = &req.csv_uri {
        // Validate s3:// prefix to prevent SSRF
        if !csv_uri.starts_with("s3://") {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "csv_uri must start with s3://" })),
            ));
        }
        ("csv".to_string(), csv_uri.clone())
    } else {
        // Look up source from the `sources` table (metadata DB only)
        let source: sqlx::postgres::PgRow =
            sqlx::query("SELECT alias, uri, source_type FROM sources WHERE alias = $1")
                .bind(&alias)
                .fetch_optional(&state.db_pool)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": format!("DB error: {}", e) })),
                    )
                })?
                .ok_or_else(|| {
                    (
                        StatusCode::NOT_FOUND,
                        Json(
                            serde_json::json!({ "error": format!("Source '{}' not found", alias) }),
                        ),
                    )
                })?;
        (source.get("source_type"), source.get("uri"))
    };

    info!(
        "load_scoped: alias={}, type={}, conditions={}, limit={}",
        alias,
        source_type,
        req.conditions.len(),
        limit
    );

    // Build connector via trait-based factory — no match/branching here
    let loader = kalla_connectors::build_scoped_loader(&source_type, &uri, &state.s3_config)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("{}", e) })),
            )
        })?;

    let result = loader
        .load_scoped(&req.conditions, limit)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Load error: {}", e) })),
            )
        })?;

    let columns: Vec<ColumnInfo> = result
        .columns
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        })
        .collect();

    let preview_rows = result.rows.len();
    Ok(Json(LoadScopedResponse {
        alias,
        columns,
        rows: result.rows,
        total_rows: result.total_rows,
        preview_rows,
    }))
}
