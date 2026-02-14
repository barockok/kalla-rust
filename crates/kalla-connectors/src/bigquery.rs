//! BigQuery connector stub
//!
//! Placeholder implementation of the `SourceConnector` trait for Google BigQuery.
//! All methods currently return an error indicating the connector is not yet
//! implemented.

use anyhow::Result;
use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;

use crate::filter::FilterCondition;
use crate::SourceConnector;

/// BigQuery connector (stub — not yet implemented).
pub struct BigQueryConnector {
    /// GCP project id
    pub project_id: String,
    /// BigQuery dataset name
    pub dataset: String,
}

impl BigQueryConnector {
    /// Create a new BigQueryConnector stub.
    pub fn new(project_id: impl Into<String>, dataset: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            dataset: dataset.into(),
        }
    }
}

#[async_trait]
impl SourceConnector for BigQueryConnector {
    async fn register_table(
        &self,
        _ctx: &SessionContext,
        _table_name: &str,
        _source_table: &str,
        _where_clause: Option<&str>,
    ) -> Result<()> {
        anyhow::bail!("BigQuery connector not yet implemented")
    }

    async fn register_scoped(
        &self,
        _ctx: &SessionContext,
        _table_name: &str,
        _source_table: &str,
        _conditions: &[FilterCondition],
        _limit: Option<usize>,
    ) -> Result<usize> {
        anyhow::bail!("BigQuery connector not yet implemented")
    }

    async fn stream_table(
        &self,
        _ctx: &SessionContext,
        _table_name: &str,
    ) -> Result<SendableRecordBatchStream> {
        anyhow::bail!("BigQuery connector not yet implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bigquery_connector_creation() {
        let bq = BigQueryConnector::new("my-project", "my_dataset");
        assert_eq!(bq.project_id, "my-project");
        assert_eq!(bq.dataset, "my_dataset");
    }

    #[tokio::test]
    async fn test_register_table_returns_error() {
        let bq = BigQueryConnector::new("proj", "ds");
        let ctx = SessionContext::new();
        let result = bq.register_table(&ctx, "t", "src", None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not yet implemented"));
    }

    #[tokio::test]
    async fn test_register_scoped_returns_error() {
        let bq = BigQueryConnector::new("proj", "ds");
        let ctx = SessionContext::new();
        let result = bq.register_scoped(&ctx, "t", "src", &[], None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not yet implemented"));
    }

    #[tokio::test]
    async fn test_stream_table_returns_error() {
        let bq = BigQueryConnector::new("proj", "ds");
        let ctx = SessionContext::new();
        let result = bq.stream_table(&ctx, "t").await;
        match result {
            Err(e) => assert!(e.to_string().contains("not yet implemented")),
            Ok(_) => panic!("expected error from BigQuery stub"),
        }
    }
}
