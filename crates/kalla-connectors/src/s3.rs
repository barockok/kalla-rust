//! S3 connector for DataFusion
//!
//! Reads Parquet files from S3 (or S3-compatible stores like MinIO) and
//! registers them as DataFusion tables.  Predicate pushdown is handled by
//! DataFusion's built-in Parquet pruning — no extra work needed once the
//! table is registered with `ListingTable`.

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use tracing::{debug, info};
use url::Url;

use crate::filter::{build_where_clause, FilterCondition};
use crate::SourceConnector;

/// Configuration for connecting to S3-compatible storage.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// AWS region (e.g. "us-east-1")
    pub region: String,
    /// Access key id
    pub access_key_id: String,
    /// Secret access key
    pub secret_access_key: String,
    /// Optional custom endpoint URL (for MinIO / LocalStack)
    pub endpoint_url: Option<String>,
    /// Allow HTTP (non-TLS) connections — useful for local MinIO
    pub allow_http: bool,
}

impl S3Config {
    /// Build an S3Config from environment variables:
    ///   AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID")
                .context("AWS_ACCESS_KEY_ID not set")?,
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")
                .context("AWS_SECRET_ACCESS_KEY not set")?,
            endpoint_url: std::env::var("AWS_ENDPOINT_URL").ok(),
            allow_http: std::env::var("AWS_ALLOW_HTTP")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
        })
    }
}

/// S3 connector that registers Parquet files from S3 as DataFusion tables.
pub struct S3Connector {
    config: S3Config,
}

impl S3Connector {
    /// Create a new S3Connector with the given configuration.
    pub fn new(config: S3Config) -> Self {
        Self { config }
    }

    /// Create an S3Connector from environment variables.
    pub fn from_env() -> Result<Self> {
        Ok(Self::new(S3Config::from_env()?))
    }

    /// Parse an `s3://bucket/key` URI into (bucket, key).
    pub fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
        let url = Url::parse(uri).context("invalid S3 URI")?;
        anyhow::ensure!(url.scheme() == "s3", "URI scheme must be s3://");
        let bucket = url
            .host_str()
            .context("missing bucket in S3 URI")?
            .to_string();
        // path() starts with '/', strip the leading slash
        let key = url.path().trim_start_matches('/').to_string();
        anyhow::ensure!(!key.is_empty(), "missing object key in S3 URI");
        Ok((bucket, key))
    }

    /// Build an `object_store::aws::AmazonS3` instance for the given bucket.
    fn build_store(&self, bucket: &str) -> Result<object_store::aws::AmazonS3> {
        let mut builder = AmazonS3Builder::new()
            .with_region(&self.config.region)
            .with_bucket_name(bucket)
            .with_access_key_id(&self.config.access_key_id)
            .with_secret_access_key(&self.config.secret_access_key);

        if let Some(ref endpoint) = self.config.endpoint_url {
            builder = builder.with_endpoint(endpoint);
        }
        if self.config.allow_http {
            builder = builder.with_allow_http(true);
        }

        builder.build().context("failed to build S3 object store")
    }

    /// Register the object store for a bucket with the given SessionContext.
    fn register_store(&self, ctx: &SessionContext, bucket: &str) -> Result<()> {
        let store = self.build_store(bucket)?;
        let s3_url = Url::parse(&format!("s3://{}", bucket))
            .context("failed to construct S3 URL for store registration")?;
        ctx.register_object_store(&s3_url, Arc::new(store));
        Ok(())
    }

    /// Register an S3 Parquet file as a ListingTable.
    async fn register_listing_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        s3_uri: &str,
    ) -> Result<()> {
        let (bucket, _key) = Self::parse_s3_uri(s3_uri)?;

        // Make sure the object store is registered for this bucket
        self.register_store(ctx, &bucket)?;

        let table_url = ListingTableUrl::parse(s3_uri)
            .context("failed to parse S3 URI as ListingTableUrl")?;

        let format = ParquetFormat::default();
        let options = ListingOptions::new(Arc::new(format))
            .with_file_extension(".parquet");

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await
            .context("failed to infer schema from S3 Parquet file")?;

        let table = ListingTable::try_new(config)
            .context("failed to create ListingTable")?;

        ctx.register_table(table_name, Arc::new(table))
            .context("failed to register table with SessionContext")?;

        info!("Registered S3 table '{}' from '{}'", table_name, s3_uri);
        Ok(())
    }
}

#[async_trait]
impl SourceConnector for S3Connector {
    async fn register_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        source_table: &str, // s3://bucket/path/to/file.parquet
        where_clause: Option<&str>,
    ) -> Result<()> {
        self.register_listing_table(ctx, table_name, source_table)
            .await?;

        // If a where clause was provided, create a filtered view on top
        if let Some(clause) = where_clause {
            let view_name = format!("{}_filtered", table_name);
            let sql = format!(
                "CREATE VIEW \"{}\" AS SELECT * FROM \"{}\" WHERE {}",
                view_name, table_name, clause,
            );
            ctx.sql(&sql).await.context("failed to create filtered view")?;
            debug!("Created filtered view '{}' with WHERE {}", view_name, clause);
        }

        Ok(())
    }

    async fn register_scoped(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        source_table: &str,
        conditions: &[FilterCondition],
        limit: Option<usize>,
    ) -> Result<usize> {
        // Deregister existing table to allow re-registration
        let _ = ctx.deregister_table(table_name);

        // First register the raw Parquet file under a temporary name
        let raw_name = format!("_raw_{}", table_name);
        self.register_listing_table(ctx, &raw_name, source_table)
            .await?;

        // Build a scoped query with filter conditions
        let where_clause = build_where_clause(conditions);
        let mut sql = format!("SELECT * FROM \"{}\"{}",
            raw_name.replace('"', "\"\""),
            where_clause,
        );
        if let Some(lim) = limit {
            sql.push_str(&format!(" LIMIT {}", lim));
        }

        debug!("S3 scoped query: {}", sql);

        // Execute and materialize into a MemTable for accurate row count
        let df = ctx.sql(&sql).await.context("failed scoped query on S3 table")?;
        let batches = df.collect().await.context("failed to collect S3 scoped data")?;
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        if row_count > 0 {
            let schema = batches[0].schema();
            let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
            ctx.register_table(table_name, Arc::new(mem_table))
                .context("failed to register scoped S3 table")?;
        }

        info!("Registered scoped S3 table '{}' with {} rows", table_name, row_count);
        // Clean up temporary raw table
        let _ = ctx.deregister_table(&raw_name);
        Ok(row_count)
    }

    async fn stream_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
    ) -> Result<SendableRecordBatchStream> {
        let df = ctx
            .sql(&format!("SELECT * FROM \"{}\"", table_name))
            .await
            .context("failed to query S3 table for streaming")?;
        Ok(df.execute_stream().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use crate::filter::{FilterOp, FilterValue};
    use std::fs;

    fn test_config() -> S3Config {
        S3Config {
            region: "us-east-1".to_string(),
            access_key_id: "test-key".to_string(),
            secret_access_key: "test-secret".to_string(),
            endpoint_url: Some("http://localhost:9000".to_string()),
            allow_http: true,
        }
    }

    #[test]
    fn test_s3_connector_creation() {
        let config = test_config();
        let connector = S3Connector::new(config.clone());
        assert_eq!(connector.config.region, "us-east-1");
        assert_eq!(connector.config.access_key_id, "test-key");
        assert_eq!(connector.config.endpoint_url, Some("http://localhost:9000".to_string()));
        assert!(connector.config.allow_http);
    }

    #[test]
    fn test_parse_s3_uri_valid() {
        let (bucket, key) = S3Connector::parse_s3_uri("s3://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.parquet");
    }

    #[test]
    fn test_parse_s3_uri_root_key() {
        let (bucket, key) = S3Connector::parse_s3_uri("s3://bucket/file.parquet").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "file.parquet");
    }

    #[test]
    fn test_parse_s3_uri_nested() {
        let (bucket, key) = S3Connector::parse_s3_uri("s3://data/year=2024/month=01/data.parquet").unwrap();
        assert_eq!(bucket, "data");
        assert_eq!(key, "year=2024/month=01/data.parquet");
    }

    #[test]
    fn test_parse_s3_uri_no_key() {
        let result = S3Connector::parse_s3_uri("s3://bucket/");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_s3_uri_no_bucket() {
        let result = S3Connector::parse_s3_uri("s3:///key");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_s3_uri_wrong_scheme() {
        let result = S3Connector::parse_s3_uri("http://bucket/key");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_s3_uri_invalid() {
        let result = S3Connector::parse_s3_uri("not a uri");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_store() {
        let connector = S3Connector::new(test_config());
        let store = connector.build_store("my-bucket");
        assert!(store.is_ok());
    }

    /// Helper: write a small Parquet file and return its path.
    fn write_test_parquet(dir: &std::path::Path) -> String {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
                Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0])),
            ],
        )
        .unwrap();

        let path = dir.join("test.parquet");
        let file = fs::File::create(&path).unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        path.to_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_register_local_parquet_as_listing_table() {
        // Use a local Parquet file to verify the ListingTable registration path
        // (no actual S3 needed).
        let tmp = tempfile::tempdir().unwrap();
        let parquet_path = write_test_parquet(tmp.path());

        let ctx = SessionContext::new();
        ctx.register_parquet("test_t", &parquet_path, Default::default())
            .await
            .unwrap();

        let df = ctx.sql("SELECT COUNT(*) AS cnt FROM test_t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let cnt = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 3);
    }

    #[tokio::test]
    async fn test_filter_conditions_applied_via_sql() {
        // Verify that filter conditions translate correctly when applied as SQL
        // over a registered table (mimics the register_scoped path).
        let tmp = tempfile::tempdir().unwrap();
        let parquet_path = write_test_parquet(tmp.path());

        let ctx = SessionContext::new();
        ctx.register_parquet("raw", &parquet_path, Default::default())
            .await
            .unwrap();

        let conditions = vec![
            FilterCondition {
                column: "amount".to_string(),
                op: FilterOp::Gte,
                value: FilterValue::Number(200.0),
            },
        ];
        let where_clause = build_where_clause(&conditions);
        let sql = format!("SELECT * FROM raw{}", where_clause);

        let df = ctx.sql(&sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Only Bob (200) and Carol (300) match >= 200
        assert_eq!(row_count, 2);
    }

    #[tokio::test]
    async fn test_filter_conditions_with_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let parquet_path = write_test_parquet(tmp.path());

        let ctx = SessionContext::new();
        ctx.register_parquet("raw2", &parquet_path, Default::default())
            .await
            .unwrap();

        let conditions = vec![
            FilterCondition {
                column: "amount".to_string(),
                op: FilterOp::Gt,
                value: FilterValue::Number(0.0),
            },
        ];
        let where_clause = build_where_clause(&conditions);
        let sql = format!("SELECT * FROM raw2{} LIMIT 1", where_clause);

        let df = ctx.sql(&sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(row_count, 1);
    }

    #[tokio::test]
    async fn test_stream_from_registered_table() {
        use futures::StreamExt;

        let tmp = tempfile::tempdir().unwrap();
        let parquet_path = write_test_parquet(tmp.path());

        let ctx = SessionContext::new();
        ctx.register_parquet("stream_t", &parquet_path, Default::default())
            .await
            .unwrap();

        let df = ctx.sql("SELECT * FROM stream_t").await.unwrap();
        let mut stream = df.execute_stream().await.unwrap();
        let mut total = 0usize;
        while let Some(batch) = stream.next().await {
            total += batch.unwrap().num_rows();
        }
        assert_eq!(total, 3);
    }

    #[test]
    fn test_s3_config_defaults() {
        let config = S3Config {
            region: "eu-west-1".to_string(),
            access_key_id: "ak".to_string(),
            secret_access_key: "sk".to_string(),
            endpoint_url: None,
            allow_http: false,
        };
        let connector = S3Connector::new(config);
        assert_eq!(connector.config.region, "eu-west-1");
        assert!(connector.config.endpoint_url.is_none());
        assert!(!connector.config.allow_http);
    }
}
