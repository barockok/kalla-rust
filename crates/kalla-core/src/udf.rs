//! Financial User-Defined Functions for reconciliation

use arrow::array::{ArrayRef, BooleanArray, Float64Array};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Register all financial UDFs with the session context
pub fn register_financial_udfs(ctx: &SessionContext) {
    ctx.register_udf(tolerance_match_udf());
}

/// Create the tolerance_match UDF
///
/// tolerance_match(a, b, threshold) -> bool
/// Returns true if abs(a - b) <= threshold
pub fn tolerance_match_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ToleranceMatch::new())
}

#[derive(Debug)]
struct ToleranceMatch {
    signature: datafusion::logical_expr::Signature,
}

impl ToleranceMatch {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::exact(
                vec![DataType::Float64, DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for ToleranceMatch {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "tolerance_match"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> datafusion::error::Result<ColumnarValue> {
        // Extract arrays from arguments
        let a = match &args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
        };
        let b = match &args[1] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
        };
        let threshold = match &args[2] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
        };

        let a_arr = a.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Expected Float64Array for arg 0".into())
        })?;
        let b_arr = b.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Expected Float64Array for arg 1".into())
        })?;
        let threshold_arr = threshold.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Expected Float64Array for arg 2".into())
        })?;

        // Compute tolerance match: abs(a - b) <= threshold
        let result: BooleanArray = a_arr
            .iter()
            .zip(b_arr.iter())
            .zip(threshold_arr.iter())
            .map(|((a_opt, b_opt), t_opt)| {
                match (a_opt, b_opt, t_opt) {
                    (Some(a), Some(b), Some(t)) => Some((a - b).abs() <= t),
                    _ => None,
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_tolerance_match() {
        let ctx = SessionContext::new();
        ctx.register_udf(tolerance_match_udf());

        // Test exact match
        let result = ctx
            .sql("SELECT tolerance_match(100.0, 100.0, 0.01)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &result[0];
        let col = batch.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(col.value(0));

        // Test within tolerance
        let result = ctx
            .sql("SELECT tolerance_match(100.0, 100.005, 0.01)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &result[0];
        let col = batch.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(col.value(0));

        // Test outside tolerance
        let result = ctx
            .sql("SELECT tolerance_match(100.0, 100.02, 0.01)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &result[0];
        let col = batch.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!col.value(0));
    }
}
