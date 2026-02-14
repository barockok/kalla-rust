//! Partitioned execution — split large reconciliation jobs by key ranges
//! so they can be processed in parallel chunks.

use datafusion::common::Result as DFResult;
use datafusion::physical_plan::SendableRecordBatchStream;

use crate::engine::ReconciliationEngine;

/// A single key-range partition.
#[derive(Debug, Clone)]
pub struct KeyPartition {
    /// Inclusive lower bound (SQL literal). `None` = unbounded.
    pub lower: Option<String>,
    /// Exclusive upper bound (SQL literal). `None` = unbounded.
    pub upper: Option<String>,
}

/// Specification for how to partition a reconciliation job.
#[derive(Debug, Clone)]
pub struct PartitionSpec {
    /// The column to partition on (must exist in both left and right tables).
    pub partition_key: String,
    /// Ordered, non-overlapping partitions.
    pub partitions: Vec<KeyPartition>,
}

impl PartitionSpec {
    /// Build a `PartitionSpec` by splitting a numeric range into `n` equal-sized chunks.
    pub fn numeric_ranges(partition_key: &str, min: i64, max: i64, n: usize) -> Self {
        assert!(n > 0);
        let step = ((max - min) as f64 / n as f64).ceil() as i64;
        let mut partitions = Vec::with_capacity(n);
        let mut lo = min;
        for _ in 0..n {
            let hi = (lo + step).min(max);
            partitions.push(KeyPartition {
                lower: Some(lo.to_string()),
                upper: if hi >= max {
                    None
                } else {
                    Some(hi.to_string())
                },
            });
            lo = hi;
            if lo >= max {
                break;
            }
        }
        Self {
            partition_key: partition_key.to_string(),
            partitions,
        }
    }

    /// Return the WHERE clause fragment for a given partition index.
    pub fn where_clause(&self, idx: usize) -> String {
        let p = &self.partitions[idx];
        let key = &self.partition_key;
        match (&p.lower, &p.upper) {
            (Some(lo), Some(hi)) => format!("{key} >= {lo} AND {key} < {hi}"),
            (Some(lo), None) => format!("{key} >= {lo}"),
            (None, Some(hi)) => format!("{key} < {hi}"),
            (None, None) => "1=1".to_string(),
        }
    }
}

/// Execute a join query for a single partition, returning a stream.
pub async fn execute_partitioned_join_stream(
    engine: &ReconciliationEngine,
    left_table: &str,
    right_table: &str,
    join_conditions: &str,
    spec: &PartitionSpec,
    partition_idx: usize,
) -> DFResult<SendableRecordBatchStream> {
    let where_clause = spec.where_clause(partition_idx);
    let query = format!(
        "SELECT * FROM {left_table} AS l INNER JOIN {right_table} AS r \
         ON {join_conditions} WHERE {where_clause}"
    );
    engine.sql_stream(&query).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_ranges() {
        let spec = PartitionSpec::numeric_ranges("id", 0, 100, 4);
        assert_eq!(spec.partitions.len(), 4);
        assert_eq!(spec.where_clause(0), "id >= 0 AND id < 25");
        assert_eq!(spec.where_clause(1), "id >= 25 AND id < 50");
        assert_eq!(spec.where_clause(2), "id >= 50 AND id < 75");
        assert_eq!(spec.where_clause(3), "id >= 75");
    }

    #[test]
    fn test_single_partition() {
        let spec = PartitionSpec::numeric_ranges("id", 0, 10, 1);
        assert_eq!(spec.partitions.len(), 1);
        assert_eq!(spec.where_clause(0), "id >= 0");
    }

    #[test]
    fn test_where_clause_unbounded() {
        let spec = PartitionSpec {
            partition_key: "amount".to_string(),
            partitions: vec![KeyPartition {
                lower: None,
                upper: None,
            }],
        };
        assert_eq!(spec.where_clause(0), "1=1");
    }
}
