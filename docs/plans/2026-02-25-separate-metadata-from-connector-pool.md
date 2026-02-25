# Separate Metadata DB from Connector DB Pool

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** The load-scoped handler should use `state.db_pool` only for Kalla's internal `sources` table lookup, and connect to the source's own Postgres using the connection string from `source.uri`.

**Architecture:** Move the DB data-loading logic (`information_schema` query + `SELECT` data) out of `sources.rs` into a new `load_db_scoped()` function in `postgres_connector.rs`. This function takes a connection string + table name (extracted from the source URI) and creates its own ephemeral pool — same pattern `PostgresPartitionedTable::new()` and `fetch_partition()` already use. The handler becomes a thin dispatcher.

**Tech Stack:** Rust, sqlx, kalla-connectors, kalla-ballista

---

## Task 1: Add `load_db_scoped()` to `postgres_connector.rs`

**Files:**
- Modify: `crates/kalla-connectors/src/postgres_connector.rs` (append new public function)
- Modify: `crates/kalla-connectors/src/lib.rs` (add re-export)

**Step 1: Add `load_db_scoped()` to `postgres_connector.rs`**

Append before the `// Tests` section:

```rust
/// Column metadata returned by `load_db_scoped`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Load filtered rows from a Postgres table using a dedicated connection.
///
/// Creates an ephemeral pool from `conn_string`, queries `information_schema`
/// for column metadata, builds a filtered SELECT, and returns structured data.
/// Returns `(columns, rows_as_strings, row_count)`.
pub async fn load_db_scoped(
    conn_string: &str,
    table_name: &str,
    conditions: &[crate::filter::FilterCondition],
    limit: usize,
) -> anyhow::Result<(Vec<ColumnMeta>, Vec<Vec<String>>, usize)> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(conn_string)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to source DB: {}", e))?;

    // Query column metadata
    let meta_rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT column_name, data_type, is_nullable \
         FROM information_schema.columns \
         WHERE table_name = $1 AND table_schema = 'public' \
         ORDER BY ordinal_position",
    )
    .bind(table_name)
    .fetch_all(&pool)
    .await?;

    if meta_rows.is_empty() {
        pool.close().await;
        anyhow::bail!("Table '{}' not found or has no columns", table_name);
    }

    let columns: Vec<ColumnMeta> = meta_rows
        .iter()
        .map(|(name, dt, nullable)| ColumnMeta {
            name: name.clone(),
            data_type: dt.clone(),
            nullable: nullable == "YES",
        })
        .collect();

    // Build SELECT with all columns cast to ::text
    let select_cols: String = columns
        .iter()
        .map(|c| format!("\"{}\"::text", c.name))
        .collect::<Vec<_>>()
        .join(", ");

    let where_clause = crate::filter::build_where_clause(conditions);

    let sql = format!(
        "SELECT {} FROM \"{}\" {} LIMIT {}",
        select_cols, table_name, where_clause, limit
    );

    debug!("load_db_scoped query: {}", sql);

    let data_rows: Vec<PgRow> = sqlx::query(&sql).fetch_all(&pool).await?;
    pool.close().await;

    let rows: Vec<Vec<String>> = data_rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    row.try_get::<Option<String>, _>(i)
                        .unwrap_or(None)
                        .unwrap_or_default()
                })
                .collect()
        })
        .collect();

    let count = rows.len();
    Ok((columns, rows, count))
}
```

**Step 2: Add re-export to `lib.rs`**

In `crates/kalla-connectors/src/lib.rs`, add:

```rust
pub use postgres_connector::{load_db_scoped, ColumnMeta, ...existing exports...};
```

**Step 3: Verify connector crate compiles**

Run: `cargo build -p kalla-connectors`

---

## Task 2: Update `sources.rs` to use connector functions

**Files:**
- Modify: `crates/kalla-ballista/src/sources.rs`

**Step 1: Replace `load_db()` with a call to `kalla_connectors::load_db_scoped()`**

The handler should:
1. Use `state.db_pool` ONLY for the `sources` table lookup (already correct)
2. Parse the URI to extract `conn_string` and `table_name` (reuse the same pattern as `factory.rs:39-46`)
3. Call `kalla_connectors::load_db_scoped(conn_string, table_name, conditions, limit)`
4. Map the result into `LoadScopedResponse`

The new `load_db()` in `sources.rs` becomes ~30 lines instead of ~80:

```rust
async fn load_db(
    alias: &str,
    uri: &str,
    conditions: &[kalla_connectors::FilterCondition],
    limit: usize,
) -> Result<Json<LoadScopedResponse>, (StatusCode, Json<serde_json::Value>)> {
    // Parse URI to extract connection string and table name
    let parsed = url::Url::parse(uri).map_err(|e| {
        (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": format!("Invalid URI: {}", e) })))
    })?;
    let table_name = parsed
        .query_pairs()
        .find(|(k, _)| k == "table")
        .map(|(_, v)| v.to_string())
        .ok_or_else(|| {
            (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": format!("Missing ?table= in URI: {}", uri) })))
        })?;
    let mut conn_url = parsed.clone();
    conn_url.set_query(None);

    let (col_meta, rows, total) =
        kalla_connectors::load_db_scoped(conn_url.as_str(), &table_name, conditions, limit)
            .await
            .map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": format!("DB load error: {}", e) })))
            })?;

    let columns: Vec<ColumnInfo> = col_meta
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        })
        .collect();

    Ok(Json(LoadScopedResponse {
        alias: alias.to_string(),
        columns,
        rows,
        total_rows: total,
        preview_rows: total,
    }))
}
```

Note: `load_db()` no longer takes `state: &RunnerState` since it doesn't need the metadata pool.

**Step 2: Update `load_csv()` signature to also not take `state`**

For symmetry, extract `s3_config` before the match and pass it directly:

```rust
match source_type.as_str() {
    "csv" => load_csv(&alias, &uri, &req.conditions, limit, &state.s3_config).await,
    _ => load_db(&alias, &uri, &req.conditions, limit).await,
}
```

Update `load_csv` to take `s3_config: &kalla_connectors::S3Config` instead of `state: &RunnerState`.

**Step 3: Remove `sqlx::Row` import from `sources.rs`**

The `use sqlx::Row;` import is no longer needed since all sqlx usage is now in the connectors crate. Remove it, and also the `use sqlx` in general if unused.

**Step 4: Verify full workspace compiles**

Run: `cargo build`

---

## Task 3: Verify tests pass

**Step 1:** Run `cargo test -p kalla-connectors` — expect all existing tests pass (no behavioral change)

**Step 2:** Run `cargo test -p kalla-ballista` — expect all existing tests pass

---

## Files Modified (Summary)

| File | Action |
|---|---|
| `crates/kalla-connectors/src/postgres_connector.rs` | Add `load_db_scoped()` + `ColumnMeta` |
| `crates/kalla-connectors/src/lib.rs` | Add re-exports |
| `crates/kalla-ballista/src/sources.rs` | Replace inline DB logic with connector call |
