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
