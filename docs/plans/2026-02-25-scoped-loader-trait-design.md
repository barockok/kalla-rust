# ScopedLoader Trait Design

## Goal

Introduce a `ScopedLoader` trait so that connectors implement a shared interface for loading filtered data, and the handler uses polymorphic dispatch instead of manual match/branching.

## Architecture

```
sources.rs handler
  ├─ state.db_pool → lookup source metadata (unchanged)
  └─ kalla_connectors::build_scoped_loader(source_type, uri, s3_config)
       → Box<dyn ScopedLoader>
           ├─ PostgresLoader { conn_string, table_name }
           └─ CsvLoader { s3_uri, s3_config }
       → loader.load_scoped(conditions, limit)
           → ScopedResult { columns, rows, total }
```

## Components

### New types (in `kalla-connectors/src/scoped.rs`)

```rust
pub struct ScopedResult {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Vec<String>>,
    pub total_rows: usize,
}

#[async_trait]
pub trait ScopedLoader: Send + Sync {
    async fn load_scoped(
        &self,
        conditions: &[FilterCondition],
        limit: usize,
    ) -> anyhow::Result<ScopedResult>;
}

pub struct PostgresLoader { conn_string: String, table_name: String }
pub struct CsvLoader { s3_uri: String, s3_config: S3Config }

pub fn build_scoped_loader(
    source_type: &str,
    uri: &str,
    s3_config: &S3Config,
) -> anyhow::Result<Box<dyn ScopedLoader>>
```

### Trait implementations

- `PostgresLoader::load_scoped()` — delegates to existing `load_db_scoped()` logic
- `CsvLoader::load_scoped()` — delegates to existing `load_csv_scoped()` logic
- Both return `ScopedResult` using `ColumnMeta` (CSV sets `data_type: "text"` for all columns)

### Factory

`build_scoped_loader()` parses the URI, extracts connector-specific config (conn_string + table for Postgres, s3_uri for CSV), returns the appropriate `Box<dyn ScopedLoader>`.

### Handler simplification

`sources.rs` collapses to:
```rust
let loader = kalla_connectors::build_scoped_loader(&source_type, &uri, &state.s3_config)?;
let result = loader.load_scoped(&req.conditions, limit).await?;
// map ScopedResult into LoadScopedResponse
```

No `load_csv()`, no `load_db()`, no match block.

## Error handling

- Factory returns `anyhow::Result` (fails on unparseable URI)
- Trait impl returns `anyhow::Result<ScopedResult>`
- Handler maps errors to HTTP status codes

## What gets removed

- `load_db_scoped()` and `load_csv_scoped()` public re-exports from `lib.rs` (consumers use the trait)
- `load_csv()` and `load_db()` private functions from `sources.rs`
- The match block in the handler

## Extensibility

Adding a new connector = create struct + implement `ScopedLoader` + add branch to factory. Handler untouched.
