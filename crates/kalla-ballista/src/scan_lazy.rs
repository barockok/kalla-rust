//! Lazy scan extensions for connector table types.
//!
//! Provides `scan_lazy()` methods that produce lazy `ExecutionPlan` trees
//! suitable for distribution across Ballista cluster executors. Each
//! partition becomes an independent `PostgresScanExec` (or similar) node
//! that fetches data only when polled on a remote executor.

use std::sync::Arc;

use datafusion::error::Result as DFResult;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;

use kalla_connectors::postgres_partitioned::{
    compute_partition_ranges, PostgresPartitionedTable,
};

use crate::postgres_scan_exec::PostgresScanExec;

/// Extension trait that adds `scan_lazy()` to connector table types.
///
/// This lives in `kalla-ballista` (not `kalla-connectors`) because the lazy
/// execution nodes (`PostgresScanExec`, etc.) are defined here, and placing
/// the method in `kalla-connectors` would create a cyclic dependency.
pub trait ScanLazy {
    /// Create a lazy execution plan for Ballista cluster mode.
    ///
    /// Returns N scan nodes (one per partition) combined with `UnionExec`.
    /// Each node fetches its partition data lazily when executed on a remote
    /// Ballista executor.
    fn scan_lazy(&self) -> DFResult<Arc<dyn ExecutionPlan>>;
}

impl ScanLazy for PostgresPartitionedTable {
    fn scan_lazy(&self) -> DFResult<Arc<dyn ExecutionPlan>> {
        let ranges = compute_partition_ranges(self.total_rows(), self.num_partitions());
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(ranges.len());

        for (offset, limit) in &ranges {
            plans.push(Arc::new(PostgresScanExec::new(
                self.conn_string().to_string(),
                self.pg_table().to_string(),
                Arc::clone(self.arrow_schema()),
                *offset,
                *limit,
                self.order_column().map(|s| s.to_string()),
            )));
        }

        if plans.len() == 1 {
            Ok(plans.into_iter().next().unwrap())
        } else {
            Ok(Arc::new(UnionExec::new(plans)))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_lazy_types_accessible() {
        // Verify that the extension trait and underlying types compile.
        let _ = std::any::type_name::<PostgresScanExec>();
        let _ = std::any::type_name::<UnionExec>();
        // Full execution testing requires a live Postgres connection;
        // covered by integration tests.
    }
}
