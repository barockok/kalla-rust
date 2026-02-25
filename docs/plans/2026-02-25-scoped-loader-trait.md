# ScopedLoader Trait Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Introduce a `ScopedLoader` trait so connectors implement a shared interface and the handler uses polymorphic dispatch instead of manual branching.

**Architecture:** Define `ScopedLoader` trait + `ScopedResult` in a new `scoped.rs` module in kalla-connectors. `PostgresLoader` and `CsvLoader` implement the trait, delegating to existing `load_db_scoped()` and `load_csv_scoped()`. A `build_scoped_loader()` factory returns `Box<dyn ScopedLoader>`. The handler calls the factory then the trait method — no match, no connector knowledge.

**Tech Stack:** Rust, async-trait (already a dependency), kalla-connectors, kalla-ballista

---

## Task 1: Create `scoped.rs` with trait, types, and implementations

**Files:**
- Create: `crates/kalla-connectors/src/scoped.rs`
- Modify: `crates/kalla-connectors/src/lib.rs`

**Step 1: Create `scoped.rs`**

Create `crates/kalla-connectors/src/scoped.rs` with the following content:

```rust
//! ScopedLoader trait — polymorphic interface for loading filtered data
//! from any connector type.

use async_trait::async_trait;
use tracing::info;

use crate::filter::FilterCondition;
use crate::postgres_connector::ColumnMeta;
use crate::s3::S3Config;

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

/// Result of a scoped load operation.
#[derive(Debug)]
pub struct ScopedResult {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Vec<String>>,
    pub total_rows: usize,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Interface for loading filtered, limited rows from a data source.
///
/// Each connector type (Postgres, CSV, etc.) implements this trait.
/// Consumers obtain a `Box<dyn ScopedLoader>` from [`build_scoped_loader`]
/// and call [`load_scoped`] without knowing the underlying connector.
#[async_trait]
pub trait ScopedLoader: Send + Sync {
    async fn load_scoped(
        &self,
        conditions: &[FilterCondition],
        limit: usize,
    ) -> anyhow::Result<ScopedResult>;
}

// ---------------------------------------------------------------------------
// PostgresLoader
// ---------------------------------------------------------------------------

/// Loads filtered rows from a Postgres table via an ephemeral connection.
pub struct PostgresLoader {
    conn_string: String,
    table_name: String,
}

#[async_trait]
impl ScopedLoader for PostgresLoader {
    async fn load_scoped(
        &self,
        conditions: &[FilterCondition],
        limit: usize,
    ) -> anyhow::Result<ScopedResult> {
        let (columns, rows, total_rows) =
            crate::postgres_connector::load_db_scoped(
                &self.conn_string,
                &self.table_name,
                conditions,
                limit,
            )
            .await?;
        Ok(ScopedResult {
            columns,
            rows,
            total_rows,
        })
    }
}

// ---------------------------------------------------------------------------
// CsvLoader
// ---------------------------------------------------------------------------

/// Loads filtered rows from an S3 CSV file with in-memory filtering.
pub struct CsvLoader {
    s3_uri: String,
    s3_config: S3Config,
}

#[async_trait]
impl ScopedLoader for CsvLoader {
    async fn load_scoped(
        &self,
        conditions: &[FilterCondition],
        limit: usize,
    ) -> anyhow::Result<ScopedResult> {
        let (col_names, rows, total_rows) =
            crate::csv_connector::load_csv_scoped(
                &self.s3_uri,
                &self.s3_config,
                conditions,
                limit,
            )
            .await?;

        let columns = col_names
            .into_iter()
            .map(|name| ColumnMeta {
                name,
                data_type: "text".to_string(),
                nullable: true,
            })
            .collect();

        Ok(ScopedResult {
            columns,
            rows,
            total_rows,
        })
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Build a [`ScopedLoader`] for the given source type and URI.
///
/// For Postgres sources, parses the URI to extract the connection string
/// (base URL without query params) and table name (`?table=` param).
/// For CSV sources, passes the URI and S3 config directly.
pub fn build_scoped_loader(
    source_type: &str,
    uri: &str,
    s3_config: &S3Config,
) -> anyhow::Result<Box<dyn ScopedLoader>> {
    match source_type {
        "csv" => {
            info!("Building CsvLoader for URI: {}", uri);
            Ok(Box::new(CsvLoader {
                s3_uri: uri.to_string(),
                s3_config: s3_config.clone(),
            }))
        }
        _ => {
            let parsed = url::Url::parse(uri)
                .map_err(|e| anyhow::anyhow!("Invalid source URI: {}", e))?;
            let table_name = parsed
                .query_pairs()
                .find(|(k, _)| k == "table")
                .map(|(_, v)| v.to_string())
                .ok_or_else(|| {
                    anyhow::anyhow!("Missing ?table= in source URI: {}", uri)
                })?;
            let mut conn_url = parsed.clone();
            conn_url.set_query(None);

            info!(
                "Building PostgresLoader for table '{}' at {}",
                table_name,
                conn_url.as_str()
            );
            Ok(Box::new(PostgresLoader {
                conn_string: conn_url.to_string(),
                table_name,
            }))
        }
    }
}
```

**Step 2: Register module and add re-exports in `lib.rs`**

In `crates/kalla-connectors/src/lib.rs`:

1. Add `pub mod scoped;` after the existing module declarations
2. Add re-exports: `pub use scoped::{build_scoped_loader, ScopedLoader, ScopedResult};`
3. Remove `load_csv_scoped` from the csv_connector re-export line (it's now internal to the trait impl)
4. Remove `load_db_scoped` from the postgres_connector re-export line (same reason)
5. Keep `ColumnMeta` re-exported (still part of `ScopedResult`)

The new `lib.rs` should be:

```rust
//! Kalla Connectors - Data source adapters
//!
//! This crate provides connectors for various data sources:
//! - PostgreSQL
//! - S3 CSV (byte-range partitioned)
//! - Local CSV / Parquet files

pub mod csv_connector;
pub mod error;
pub mod factory;
pub mod filter;
pub mod postgres_connector;
pub mod s3;
pub mod scoped;
pub mod wire;

pub use csv_connector::{CsvByteRangeTable, CsvRangeScanExec};
pub use error::ConnectorError;
pub use factory::register_source;
pub use filter::{build_where_clause, FilterCondition, FilterOp, FilterValue};
pub use postgres_connector::{ColumnMeta, PostgresPartitionedTable, PostgresScanExec};
pub use s3::{parse_s3_uri, S3Config};
pub use scoped::{build_scoped_loader, ScopedLoader, ScopedResult};
pub use wire::{exec_codecs, table_codecs, ExecCodecEntry, TableCodecEntry};
```

**Step 3: Verify connector crate compiles**

Run: `cargo build -p kalla-connectors`

---

## Task 2: Simplify `sources.rs` handler to use the trait

**Files:**
- Modify: `crates/kalla-ballista/src/sources.rs`

**Step 1: Replace the entire handler file**

The new `sources.rs` should be:

```rust
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
                    Json(serde_json::json!({ "error": format!("Source '{}' not found", alias) })),
                )
            })?;

    let source_type: String = source.get("source_type");
    let uri: String = source.get("uri");

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
```

Key changes from previous version:
- Removed `use url::Url;` (URI parsing moved to factory)
- Removed `load_csv()` function entirely
- Removed `load_db()` function entirely
- Removed the `match source_type` block
- Added `build_scoped_loader()` call + `load_scoped()` call
- Handler is now ~60 lines total (was ~180)

**Step 2: Verify full workspace compiles**

Run: `cargo build`

---

## Task 3: Verify all tests pass

**Step 1:** Run `cargo test -p kalla-connectors` — expect all 85 existing tests pass

**Step 2:** Run `cargo test -p kalla-ballista` — expect all 12 existing tests pass

---

## Files Modified (Summary)

| File | Action |
|---|---|
| `crates/kalla-connectors/src/scoped.rs` | NEW — trait, types, implementations, factory |
| `crates/kalla-connectors/src/lib.rs` | Add module + re-exports, remove old free-function re-exports |
| `crates/kalla-ballista/src/sources.rs` | Replace match/load_csv/load_db with trait-based dispatch |
