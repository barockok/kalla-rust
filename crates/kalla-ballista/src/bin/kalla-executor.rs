//! Custom Ballista executor binary with Kalla's physical codec registered.
//!
//! This binary wraps the standard Ballista executor and injects
//! [`KallaPhysicalCodec`] so that custom execution plan nodes
//! (`PostgresScanExec`, `CsvRangeScanExec`) received from the scheduler
//! can be deserialized and executed on remote workers.

use std::sync::Arc;

use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};

use kalla_ballista::codec::KallaPhysicalCodec;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Build executor config with our custom physical codec.
    let mut config = ExecutorProcessConfig::default();
    config.override_physical_codec = Some(Arc::new(KallaPhysicalCodec::new()));

    // Start the executor process (blocks until shutdown).
    start_executor_process(Arc::new(config)).await?;

    Ok(())
}
