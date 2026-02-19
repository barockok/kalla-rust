//! BigQuery connector stub
//!
//! Placeholder for a future Google BigQuery connector.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bigquery_connector_creation() {
        let bq = BigQueryConnector::new("my-project", "my_dataset");
        assert_eq!(bq.project_id, "my-project");
        assert_eq!(bq.dataset, "my_dataset");
    }
}
