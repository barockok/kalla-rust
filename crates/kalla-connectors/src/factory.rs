//! Source registration — routes URIs to the appropriate connector.

use anyhow::Result;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use tracing::info;

use crate::filter::{build_where_clause, FilterCondition};

/// Register a data source with a DataFusion `SessionContext` based on URI.
///
/// Routes to the appropriate connector by inspecting the URI scheme/extension.
/// Returns the total row count (or 0 if unknown at registration time).
pub async fn register_source(
    ctx: &SessionContext,
    alias: &str,
    uri: &str,
    partitions: usize,
    filters: &[FilterCondition],
) -> Result<u64> {
    if uri.starts_with("postgres://") || uri.starts_with("postgresql://") {
        register_postgres(ctx, alias, uri, partitions, filters).await
    } else if uri.starts_with("s3://") && uri.ends_with(".csv") {
        register_s3_csv(ctx, alias, uri, partitions).await
    } else if uri.ends_with(".csv") {
        register_local_csv(ctx, alias, uri).await
    } else {
        register_local_parquet(ctx, alias, uri).await
    }
}

async fn register_postgres(
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

    let table = crate::postgres_connector::PostgresPartitionedTable::new(
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

async fn register_s3_csv(
    ctx: &SessionContext,
    alias: &str,
    uri: &str,
    partitions: usize,
) -> Result<u64> {
    let s3_config = crate::S3Config::from_env()?;
    crate::csv_connector::register(ctx, alias, uri, partitions, s3_config).await?;
    Ok(0)
}

async fn register_local_csv(ctx: &SessionContext, alias: &str, uri: &str) -> Result<u64> {
    ctx.register_csv(alias, uri, Default::default()).await?;
    Ok(0)
}

async fn register_local_parquet(ctx: &SessionContext, alias: &str, uri: &str) -> Result<u64> {
    ctx.register_parquet(alias, uri, Default::default()).await?;
    Ok(0)
}
