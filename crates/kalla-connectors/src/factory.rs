//! Connector factory — pluggable source registration by URI scheme.

use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use tracing::info;

use crate::filter::{build_where_clause, FilterCondition};

/// A factory that can register a data source with a DataFusion `SessionContext`
/// based on URI pattern matching.
#[async_trait]
pub trait ConnectorFactory: Send + Sync {
    /// Returns `true` if this factory can handle the given URI.
    fn can_handle(&self, uri: &str) -> bool;

    /// Returns `true` if this factory supports pushing filters down at registration time.
    fn supports_filter_pushdown(&self) -> bool {
        false
    }

    /// Register the source under `alias` with the given context.
    /// Returns the total row count of the registered source.
    async fn register(
        &self,
        ctx: &SessionContext,
        alias: &str,
        uri: &str,
        partitions: usize,
        filters: &[FilterCondition],
    ) -> Result<u64>;
}

/// Registry of connector factories. Iterates factories in order and delegates
/// to the first one that can handle a URI.
pub struct ConnectorRegistry {
    factories: Vec<Arc<dyn ConnectorFactory>>,
}

impl ConnectorRegistry {
    pub fn new(factories: Vec<Arc<dyn ConnectorFactory>>) -> Self {
        Self { factories }
    }

    /// Register a source by finding the first factory that can handle the URI.
    ///
    /// If `filters` are provided and the factory does not support pushdown,
    /// the source is registered under a raw alias and a DataFusion view with
    /// the WHERE clause is created as `alias`.
    pub async fn register_source(
        &self,
        ctx: &SessionContext,
        alias: &str,
        uri: &str,
        partitions: usize,
        filters: &[FilterCondition],
    ) -> Result<u64> {
        for factory in &self.factories {
            if factory.can_handle(uri) {
                if !filters.is_empty() && !factory.supports_filter_pushdown() {
                    // Register under a raw alias, then create a view with WHERE clause.
                    let raw_alias = format!("__raw_{}", alias);
                    let row_count = factory
                        .register(ctx, &raw_alias, uri, partitions, filters)
                        .await?;
                    let where_clause = build_where_clause(filters);
                    let view_sql =
                        format!("CREATE VIEW \"{}\" AS SELECT * FROM \"{}\"{}", alias, raw_alias, where_clause);
                    ctx.sql(&view_sql).await?;
                    info!(
                        "Created filtered view '{}' over '{}' with{}",
                        alias, raw_alias, where_clause
                    );
                    return Ok(row_count);
                }
                return factory.register(ctx, alias, uri, partitions, filters).await;
            }
        }
        anyhow::bail!("Unsupported source URI format: {}", uri);
    }
}

// ---------------------------------------------------------------------------
// Built-in factory implementations
// ---------------------------------------------------------------------------

/// Factory for PostgreSQL sources (partitioned).
pub struct PostgresFactory;

#[async_trait]
impl ConnectorFactory for PostgresFactory {
    fn can_handle(&self, uri: &str) -> bool {
        uri.starts_with("postgres://") || uri.starts_with("postgresql://")
    }

    fn supports_filter_pushdown(&self) -> bool {
        true
    }

    async fn register(
        &self,
        ctx: &SessionContext,
        alias: &str,
        uri: &str,
        partitions: usize,
        filters: &[FilterCondition],
    ) -> Result<u64> {
        let parsed = url::Url::parse(uri)?;
        let table_name = parsed
            .query_pairs()
            .find(|(k, _)| k == "table")
            .map(|(_, v)| v.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing 'table' query parameter in source URI"))?;
        let mut conn_url = parsed.clone();
        conn_url.set_query(None);

        let where_clause = if filters.is_empty() {
            None
        } else {
            Some(build_where_clause(filters))
        };

        let table = crate::postgres_partitioned::PostgresPartitionedTable::new(
            conn_url.as_str(),
            &table_name,
            partitions,
            Some("ctid".to_string()),
            where_clause,
        )
        .await?;
        let total_rows = table.total_rows();
        ctx.register_table(alias, Arc::new(table))?;
        info!(
            "Registered PostgresPartitionedTable '{}' -> '{}'",
            table_name, alias
        );
        Ok(total_rows)
    }
}

/// Factory for S3 CSV sources (byte-range partitioned).
pub struct S3CsvFactory;

#[async_trait]
impl ConnectorFactory for S3CsvFactory {
    fn can_handle(&self, uri: &str) -> bool {
        uri.starts_with("s3://") && uri.ends_with(".csv")
    }

    async fn register(
        &self,
        ctx: &SessionContext,
        alias: &str,
        uri: &str,
        partitions: usize,
        _filters: &[FilterCondition],
    ) -> Result<u64> {
        let s3_config = crate::S3Config::from_env()?;
        crate::csv_partitioned::register(ctx, alias, uri, partitions, s3_config).await?;
        Ok(0) // row count determined after registration
    }
}

/// Factory for S3 Parquet/listing sources.
pub struct S3ParquetFactory;

#[async_trait]
impl ConnectorFactory for S3ParquetFactory {
    fn can_handle(&self, uri: &str) -> bool {
        uri.starts_with("s3://") && !uri.ends_with(".csv")
    }

    async fn register(
        &self,
        ctx: &SessionContext,
        alias: &str,
        uri: &str,
        _partitions: usize,
        _filters: &[FilterCondition],
    ) -> Result<u64> {
        let connector = crate::S3Connector::from_env()?;
        connector
            .register_csv_listing_table(ctx, alias, uri)
            .await?;
        Ok(0)
    }
}

/// Factory for local CSV files.
pub struct LocalCsvFactory;

#[async_trait]
impl ConnectorFactory for LocalCsvFactory {
    fn can_handle(&self, uri: &str) -> bool {
        !uri.starts_with("s3://")
            && !uri.starts_with("postgres://")
            && !uri.starts_with("postgresql://")
            && uri.ends_with(".csv")
    }

    async fn register(
        &self,
        ctx: &SessionContext,
        alias: &str,
        uri: &str,
        _partitions: usize,
        _filters: &[FilterCondition],
    ) -> Result<u64> {
        ctx.register_csv(alias, uri, Default::default()).await?;
        Ok(0)
    }
}

/// Factory for local Parquet files and staging directories.
pub struct LocalParquetFactory;

#[async_trait]
impl ConnectorFactory for LocalParquetFactory {
    fn can_handle(&self, uri: &str) -> bool {
        !uri.starts_with("s3://")
            && !uri.starts_with("postgres://")
            && !uri.starts_with("postgresql://")
            && (uri.ends_with(".parquet") || uri.contains("/staging/"))
    }

    async fn register(
        &self,
        ctx: &SessionContext,
        alias: &str,
        uri: &str,
        _partitions: usize,
        _filters: &[FilterCondition],
    ) -> Result<u64> {
        ctx.register_parquet(alias, uri, Default::default()).await?;
        Ok(0)
    }
}

/// Build the default registry with all built-in factories.
pub fn default_registry() -> ConnectorRegistry {
    ConnectorRegistry::new(vec![
        Arc::new(PostgresFactory),
        Arc::new(S3CsvFactory),
        Arc::new(S3ParquetFactory),
        Arc::new(LocalCsvFactory),
        Arc::new(LocalParquetFactory),
    ])
}
