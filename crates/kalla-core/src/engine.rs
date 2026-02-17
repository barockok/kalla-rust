//! Reconciliation engine built on DataFusion

use datafusion::common::Result as DFResult;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{CsvReadOptions, DataFrame, ParquetReadOptions, SessionContext};
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

    /// Create a distributed reconciliation engine using Ballista standalone mode.
    /// Embeds scheduler + executor in-process for distributed query execution.
    pub async fn new_distributed() -> anyhow::Result<Self> {
        use ballista::prelude::SessionContextExt as _;

        let ctx: SessionContext = SessionContext::standalone().await?;
        udf::register_financial_udfs(&ctx);

        info!("ReconciliationEngine (distributed/Ballista standalone) initialized");

        Ok(Self { ctx })
    }

    /// Create a cluster-mode engine that connects to an external Ballista scheduler.
    ///
    /// Queries submitted to this engine are distributed across Ballista executors.
    /// The scheduler URL should be in the form `df://host:port`.
    ///
    /// The `codec` parameter should be a [`PhysicalExtensionCodec`] that knows how
    /// to serialize/deserialize Kalla's custom execution plan nodes (e.g.
    /// `KallaPhysicalCodec` from the `kalla-ballista` crate).
    ///
    /// # Example
    ///
    /// ```ignore
    /// use kalla_ballista::codec::KallaPhysicalCodec;
    /// use std::sync::Arc;
    ///
    /// let engine = kalla_core::engine::ReconciliationEngine::new_cluster(
    ///     "df://scheduler-host:50050",
    ///     Arc::new(KallaPhysicalCodec::new()),
    /// ).await?;
    /// ```
    pub async fn new_cluster(
        scheduler_url: &str,
        codec: std::sync::Arc<dyn datafusion_proto::physical_plan::PhysicalExtensionCodec>,
    ) -> anyhow::Result<Self> {
        use ballista::prelude::{SessionConfigExt as _, SessionContextExt as _};
        use datafusion::execution::session_state::SessionStateBuilder;

        let config = datafusion::prelude::SessionConfig::new()
            .with_information_schema(true)
            .with_ballista_physical_extension_codec(codec);

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();

        let ctx: SessionContext = SessionContext::remote_with_state(scheduler_url, state).await?;
        udf::register_financial_udfs(&ctx);

        info!(
            "ReconciliationEngine (cluster mode, scheduler={}) initialized",
            scheduler_url
        );

        Ok(Self { ctx })
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
    pub async fn register_csv(&self, table_name: &str, path: &str) -> DFResult<()> {
        self.ctx
            .register_csv(table_name, path, CsvReadOptions::default())
            .await
    }

    /// Register a Parquet file as a table
    pub async fn register_parquet(&self, table_name: &str, path: &str) -> DFResult<()> {
        self.ctx
            .register_parquet(table_name, path, ParquetReadOptions::default())
            .await
    }

    /// Execute a SQL query and return a DataFrame
    pub async fn sql(&self, query: &str) -> DFResult<DataFrame> {
        self.ctx.sql(query).await
    }

    /// Execute a SQL query and return a streaming RecordBatch result.
    /// Memory stays constant regardless of result size.
    pub async fn sql_stream(&self, query: &str) -> DFResult<SendableRecordBatchStream> {
        let df = self.ctx.sql(query).await?;
        df.execute_stream().await
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

    /// Streaming version of execute_join — returns a RecordBatch stream
    pub async fn execute_join_stream(
        &self,
        left_table: &str,
        right_table: &str,
        join_conditions: &str,
    ) -> DFResult<SendableRecordBatchStream> {
        let query = format!(
            "SELECT * FROM {} AS l INNER JOIN {} AS r ON {}",
            left_table, right_table, join_conditions
        );
        self.sql_stream(&query).await
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

    /// Streaming version of find_left_orphans
    pub async fn find_left_orphans_stream(
        &self,
        left_table: &str,
        right_table: &str,
        left_key: &str,
        right_key: &str,
    ) -> DFResult<SendableRecordBatchStream> {
        let query = format!(
            "SELECT l.* FROM {} AS l \
             LEFT JOIN {} AS r ON l.{} = r.{} \
             WHERE r.{} IS NULL",
            left_table, right_table, left_key, right_key, right_key
        );
        self.sql_stream(&query).await
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

    /// Streaming version of find_right_orphans
    pub async fn find_right_orphans_stream(
        &self,
        left_table: &str,
        right_table: &str,
        left_key: &str,
        right_key: &str,
    ) -> DFResult<SendableRecordBatchStream> {
        let query = format!(
            "SELECT r.* FROM {} AS r \
             LEFT JOIN {} AS l ON r.{} = l.{} \
             WHERE l.{} IS NULL",
            right_table, left_table, right_key, left_key, left_key
        );
        self.sql_stream(&query).await
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
    use arrow::array::Int64Array;
    use std::io::Write;

    /// Helper: write a CSV string to a temp file with .csv extension and return the path
    fn write_temp_csv(content: &str) -> (tempfile::NamedTempFile, String) {
        let mut f = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f.as_file().sync_all().unwrap();
        let path = f.path().to_str().unwrap().to_string();
        (f, path)
    }

    #[tokio::test]
    async fn test_engine_creation() {
        let engine = ReconciliationEngine::new();
        let result = engine.sql("SELECT tolerance_match(1.0, 1.005, 0.01)").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_engine_default() {
        let engine = ReconciliationEngine::default();
        let result = engine.sql("SELECT 1").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_context_accessors() {
        let mut engine = ReconciliationEngine::new();
        let _ctx = engine.context();
        let _ctx_mut = engine.context_mut();
    }

    #[tokio::test]
    async fn test_register_csv_and_query() {
        let csv_data = "id,name,amount\n1,Alice,100.0\n2,Bob,200.0\n3,Carol,300.0\n";
        let (_f, path) = write_temp_csv(csv_data);

        let engine = ReconciliationEngine::new();
        engine.register_csv("test_table", &path).await.unwrap();

        let df = engine
            .sql("SELECT COUNT(*) AS cnt FROM test_table")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let cnt = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 3);
    }

    #[tokio::test]
    async fn test_register_csv_nonexistent_file() {
        let engine = ReconciliationEngine::new();
        let result = engine.register_csv("bad", "/nonexistent/file.csv").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sql_invalid_query() {
        let engine = ReconciliationEngine::new();
        let result = engine.sql("SELECT * FROM nonexistent_table").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_join() {
        let left = "id,name,amount\n1,Alice,100.0\n2,Bob,200.0\n3,Carol,300.0\n";
        let right = "ref_id,payment\n1,100.0\n2,200.0\n4,400.0\n";
        let (_fl, lpath) = write_temp_csv(left);
        let (_fr, rpath) = write_temp_csv(right);

        let engine = ReconciliationEngine::new();
        engine.register_csv("left_t", &lpath).await.unwrap();
        engine.register_csv("right_t", &rpath).await.unwrap();

        let df = engine
            .execute_join("left_t", "right_t", "l.id = r.ref_id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // IDs 1 and 2 match
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_execute_join_stream() {
        use futures::StreamExt;

        let left = "id,name\n1,Alice\n2,Bob\n";
        let right = "ref_id,val\n1,X\n2,Y\n";
        let (_fl, lpath) = write_temp_csv(left);
        let (_fr, rpath) = write_temp_csv(right);

        let engine = ReconciliationEngine::new();
        engine.register_csv("sl", &lpath).await.unwrap();
        engine.register_csv("sr", &rpath).await.unwrap();

        let mut stream = engine
            .execute_join_stream("sl", "sr", "l.id = r.ref_id")
            .await
            .unwrap();

        let mut total = 0usize;
        while let Some(batch) = stream.next().await {
            total += batch.unwrap().num_rows();
        }
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_find_left_orphans() {
        let left = "id,name\n1,Alice\n2,Bob\n3,Carol\n";
        let right = "ref_id,val\n1,X\n2,Y\n";
        let (_fl, lpath) = write_temp_csv(left);
        let (_fr, rpath) = write_temp_csv(right);

        let engine = ReconciliationEngine::new();
        engine.register_csv("lo_left", &lpath).await.unwrap();
        engine.register_csv("lo_right", &rpath).await.unwrap();

        let df = engine
            .find_left_orphans("lo_left", "lo_right", "id", "ref_id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Only id=3 is an orphan
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn test_find_right_orphans() {
        let left = "id,name\n1,Alice\n";
        let right = "ref_id,val\n1,X\n2,Y\n3,Z\n";
        let (_fl, lpath) = write_temp_csv(left);
        let (_fr, rpath) = write_temp_csv(right);

        let engine = ReconciliationEngine::new();
        engine.register_csv("ro_left", &lpath).await.unwrap();
        engine.register_csv("ro_right", &rpath).await.unwrap();

        let df = engine
            .find_right_orphans("ro_left", "ro_right", "id", "ref_id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // ref_id 2 and 3 are orphans
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_find_left_orphans_stream() {
        use futures::StreamExt;

        let left = "id,name\n1,A\n2,B\n3,C\n";
        let right = "ref_id,val\n1,X\n";
        let (_fl, lpath) = write_temp_csv(left);
        let (_fr, rpath) = write_temp_csv(right);

        let engine = ReconciliationEngine::new();
        engine.register_csv("los_l", &lpath).await.unwrap();
        engine.register_csv("los_r", &rpath).await.unwrap();

        let mut stream = engine
            .find_left_orphans_stream("los_l", "los_r", "id", "ref_id")
            .await
            .unwrap();

        let mut total = 0usize;
        while let Some(batch) = stream.next().await {
            total += batch.unwrap().num_rows();
        }
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_find_right_orphans_stream() {
        use futures::StreamExt;

        let left = "id,name\n1,A\n";
        let right = "ref_id,val\n1,X\n2,Y\n";
        let (_fl, lpath) = write_temp_csv(left);
        let (_fr, rpath) = write_temp_csv(right);

        let engine = ReconciliationEngine::new();
        engine.register_csv("ros_l", &lpath).await.unwrap();
        engine.register_csv("ros_r", &rpath).await.unwrap();

        let mut stream = engine
            .find_right_orphans_stream("ros_l", "ros_r", "id", "ref_id")
            .await
            .unwrap();

        let mut total = 0usize;
        while let Some(batch) = stream.next().await {
            total += batch.unwrap().num_rows();
        }
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn test_sql_stream() {
        use futures::StreamExt;

        let csv = "id,val\n1,10\n2,20\n";
        let (_f, path) = write_temp_csv(csv);

        let engine = ReconciliationEngine::new();
        engine.register_csv("ss_t", &path).await.unwrap();

        let mut stream = engine.sql_stream("SELECT * FROM ss_t").await.unwrap();
        let mut total = 0usize;
        while let Some(batch) = stream.next().await {
            total += batch.unwrap().num_rows();
        }
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_empty_dataset_join() {
        let left = "id,name\n";
        let right = "ref_id,val\n1,X\n";
        let (_fl, lpath) = write_temp_csv(left);
        let (_fr, rpath) = write_temp_csv(right);

        let engine = ReconciliationEngine::new();
        engine.register_csv("empty_left", &lpath).await.unwrap();
        engine.register_csv("empty_right", &rpath).await.unwrap();

        let df = engine
            .execute_join("empty_left", "empty_right", "l.id = r.ref_id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn test_single_row_join() {
        let left = "id,name\n1,Alice\n";
        let right = "ref_id,val\n1,X\n";
        let (_fl, lpath) = write_temp_csv(left);
        let (_fr, rpath) = write_temp_csv(right);

        let engine = ReconciliationEngine::new();
        engine.register_csv("sr_left", &lpath).await.unwrap();
        engine.register_csv("sr_right", &rpath).await.unwrap();

        let df = engine
            .execute_join("sr_left", "sr_right", "l.id = r.ref_id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn test_no_orphans_when_fully_matched() {
        let left = "id,name\n1,Alice\n2,Bob\n";
        let right = "ref_id,val\n1,X\n2,Y\n";
        let (_fl, lpath) = write_temp_csv(left);
        let (_fr, rpath) = write_temp_csv(right);

        let engine = ReconciliationEngine::new();
        engine.register_csv("nm_left", &lpath).await.unwrap();
        engine.register_csv("nm_right", &rpath).await.unwrap();

        let left_orphans = engine
            .find_left_orphans("nm_left", "nm_right", "id", "ref_id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let right_orphans = engine
            .find_right_orphans("nm_left", "nm_right", "id", "ref_id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let lo_count: usize = left_orphans.iter().map(|b| b.num_rows()).sum();
        let ro_count: usize = right_orphans.iter().map(|b| b.num_rows()).sum();
        assert_eq!(lo_count, 0);
        assert_eq!(ro_count, 0);
    }

    #[tokio::test]
    async fn test_distributed_engine_creation() {
        let engine = ReconciliationEngine::new_distributed().await.unwrap();
        // Verify UDFs are registered
        let result = engine.sql("SELECT tolerance_match(1.0, 1.005, 0.01)").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_distributed_engine_csv_query() {
        let csv = "id,val\n1,10\n2,20\n3,30\n";
        let (_f, path) = write_temp_csv(csv);

        let engine = ReconciliationEngine::new_distributed().await.unwrap();
        engine.register_csv("dist_t", &path).await.unwrap();

        let df = engine
            .sql("SELECT COUNT(*) AS cnt FROM dist_t")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let cnt = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 3);
    }

    #[tokio::test]
    async fn test_tolerance_match_in_join() {
        let left = "id,amount\n1,100.0\n2,200.0\n";
        let right = "ref_id,paid\n1,100.005\n2,200.02\n";
        let (_fl, lpath) = write_temp_csv(left);
        let (_fr, rpath) = write_temp_csv(right);

        let engine = ReconciliationEngine::new();
        engine.register_csv("tm_left", &lpath).await.unwrap();
        engine.register_csv("tm_right", &rpath).await.unwrap();

        // Join with tolerance on amount
        let df = engine
            .execute_join(
                "tm_left",
                "tm_right",
                "l.id = r.ref_id AND tolerance_match(l.amount, r.paid, 0.01)",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Only id=1 matches within 0.01 tolerance; id=2 difference is 0.02
        assert_eq!(total, 1);
    }

    #[tokio::test]
    #[ignore] // Requires running Ballista scheduler
    async fn test_cluster_engine_creation() {
        use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
        use std::sync::Arc;

        let codec = Arc::new(DefaultPhysicalExtensionCodec {});
        let result = ReconciliationEngine::new_cluster("df://localhost:50050", codec).await;
        // Will fail without a scheduler, just verify it compiles
        assert!(result.is_err());
    }
}
