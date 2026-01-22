//! Reconciliation engine built on DataFusion

use datafusion::prelude::*;
use datafusion::error::Result as DFResult;
use tracing::info;

use crate::udf;

/// The main reconciliation engine wrapping DataFusion's SessionContext
pub struct ReconciliationEngine {
    ctx: SessionContext,
}

impl ReconciliationEngine {
    /// Create a new reconciliation engine with financial UDFs registered
    pub fn new() -> Self {
        let ctx = SessionContext::new();

        // Register financial UDFs
        udf::register_financial_udfs(&ctx);

        info!("ReconciliationEngine initialized with financial UDFs");

        Self { ctx }
    }

    /// Get a reference to the underlying SessionContext
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get a mutable reference to the underlying SessionContext
    pub fn context_mut(&mut self) -> &mut SessionContext {
        &mut self.ctx
    }

    /// Register a CSV file as a table
    pub async fn register_csv(
        &self,
        table_name: &str,
        path: &str,
    ) -> DFResult<()> {
        self.ctx.register_csv(table_name, path, CsvReadOptions::default()).await
    }

    /// Register a Parquet file as a table
    pub async fn register_parquet(
        &self,
        table_name: &str,
        path: &str,
    ) -> DFResult<()> {
        self.ctx.register_parquet(table_name, path, ParquetReadOptions::default()).await
    }

    /// Execute a SQL query and return a DataFrame
    pub async fn sql(&self, query: &str) -> DFResult<DataFrame> {
        self.ctx.sql(query).await
    }

    /// Execute a reconciliation join between two tables
    /// Returns matched records based on join conditions
    pub async fn execute_join(
        &self,
        left_table: &str,
        right_table: &str,
        join_conditions: &str,
    ) -> DFResult<DataFrame> {
        let query = format!(
            "SELECT * FROM {} AS l INNER JOIN {} AS r ON {}",
            left_table, right_table, join_conditions
        );
        self.sql(&query).await
    }

    /// Find unmatched records from the left table (orphans)
    pub async fn find_left_orphans(
        &self,
        left_table: &str,
        right_table: &str,
        left_key: &str,
        right_key: &str,
    ) -> DFResult<DataFrame> {
        let query = format!(
            "SELECT l.* FROM {} AS l \
             LEFT JOIN {} AS r ON l.{} = r.{} \
             WHERE r.{} IS NULL",
            left_table, right_table, left_key, right_key, right_key
        );
        self.sql(&query).await
    }

    /// Find unmatched records from the right table (orphans)
    pub async fn find_right_orphans(
        &self,
        left_table: &str,
        right_table: &str,
        left_key: &str,
        right_key: &str,
    ) -> DFResult<DataFrame> {
        let query = format!(
            "SELECT r.* FROM {} AS r \
             LEFT JOIN {} AS l ON r.{} = l.{} \
             WHERE l.{} IS NULL",
            right_table, left_table, right_key, left_key, left_key
        );
        self.sql(&query).await
    }
}

impl Default for ReconciliationEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_creation() {
        let engine = ReconciliationEngine::new();
        // Engine should be created successfully with UDFs registered
        let result = engine.sql("SELECT tolerance_match(1.0, 1.005, 0.01)").await;
        assert!(result.is_ok());
    }
}
