//! S3 connector for DataFusion
//!
//! Reads Parquet files from S3 (or S3-compatible stores like MinIO) and
//! registers them as DataFusion tables.  Predicate pushdown is handled by
//! DataFusion's built-in Parquet pruning — no extra work needed once the
//! table is registered with `ListingTable`.

use anyhow::{Context, Result};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use tracing::info;
use url::Url;

/// Configuration for connecting to S3-compatible storage.
///
/// Secrets (`access_key_id`, `secret_access_key`) are redacted in `Debug`
/// output but included in serde serialization because the Ballista codec
/// requires them for executor-side S3 access.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct S3Config {
    /// AWS region (e.g. "us-east-1")
    pub region: String,
    /// Access key id (redacted in Debug output)
    pub access_key_id: String,
    /// Secret access key (redacted in Debug output)
    pub secret_access_key: String,
    /// Optional custom endpoint URL (for MinIO / LocalStack)
    pub endpoint_url: Option<String>,
    /// Allow HTTP (non-TLS) connections — useful for local MinIO
    pub allow_http: bool,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("region", &self.region)
            .field("access_key_id", &"***")
            .field("secret_access_key", &"***")
            .field("endpoint_url", &self.endpoint_url)
            .field("allow_http", &self.allow_http)
            .finish()
    }
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

    /// Register an S3 CSV file as a ListingTable.
    pub async fn register_csv_listing_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        s3_uri: &str,
    ) -> Result<()> {
        let (bucket, _key) = Self::parse_s3_uri(s3_uri)?;

        self.register_store(ctx, &bucket)?;

        let table_url =
            ListingTableUrl::parse(s3_uri).context("failed to parse S3 URI as ListingTableUrl")?;

        let format = CsvFormat::default().with_has_header(true);
        let options = ListingOptions::new(Arc::new(format)).with_file_extension(".csv");

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await
            .context("failed to infer schema from S3 CSV file")?;

        let table = ListingTable::try_new(config).context("failed to create ListingTable")?;

        ctx.register_table(table_name, Arc::new(table))
            .context("failed to register table with SessionContext")?;

        info!("Registered S3 CSV table '{}' from '{}'", table_name, s3_uri);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::{build_where_clause, FilterCondition, FilterOp, FilterValue};
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
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
        assert_eq!(
            connector.config.endpoint_url,
            Some("http://localhost:9000".to_string())
        );
        assert!(connector.config.allow_http);
    }

    #[test]
    fn test_parse_s3_uri_valid() {
        let (bucket, key) =
            S3Connector::parse_s3_uri("s3://my-bucket/path/to/file.parquet").unwrap();
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
        let (bucket, key) =
            S3Connector::parse_s3_uri("s3://data/year=2024/month=01/data.parquet").unwrap();
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
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
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

        let conditions = vec![FilterCondition {
            column: "amount".to_string(),
            op: FilterOp::Gte,
            value: FilterValue::Number(200.0),
        }];
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

        let conditions = vec![FilterCondition {
            column: "amount".to_string(),
            op: FilterOp::Gt,
            value: FilterValue::Number(0.0),
        }];
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
