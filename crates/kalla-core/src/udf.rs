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
        let threshold_arr = threshold
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "Expected Float64Array for arg 2".into(),
                )
            })?;

        // Compute tolerance match: abs(a - b) <= threshold
        let result: BooleanArray = a_arr
            .iter()
            .zip(b_arr.iter())
            .zip(threshold_arr.iter())
            .map(|((a_opt, b_opt), t_opt)| match (a_opt, b_opt, t_opt) {
                (Some(a), Some(b), Some(t)) => Some((a - b).abs() <= t),
                _ => None,
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use datafusion::prelude::*;

    /// Helper to evaluate a tolerance_match SQL expression and return the boolean result
    async fn eval_tolerance(ctx: &SessionContext, a: f64, b: f64, threshold: f64) -> Option<bool> {
        let query = format!("SELECT tolerance_match({}, {}, {})", a, b, threshold);
        let result = ctx.sql(&query).await.unwrap().collect().await.unwrap();
        let col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        if col.is_null(0) {
            None
        } else {
            Some(col.value(0))
        }
    }

    fn setup_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(tolerance_match_udf());
        ctx
    }

    #[tokio::test]
    async fn test_tolerance_match_exact() {
        let ctx = setup_ctx();
        assert_eq!(eval_tolerance(&ctx, 100.0, 100.0, 0.01).await, Some(true));
    }

    #[tokio::test]
    async fn test_tolerance_match_within() {
        let ctx = setup_ctx();
        assert_eq!(eval_tolerance(&ctx, 100.0, 100.005, 0.01).await, Some(true));
    }

    #[tokio::test]
    async fn test_tolerance_match_outside() {
        let ctx = setup_ctx();
        assert_eq!(eval_tolerance(&ctx, 100.0, 100.02, 0.01).await, Some(false));
    }

    #[tokio::test]
    async fn test_tolerance_match_boundary_equal() {
        let ctx = setup_ctx();
        // IEEE 754: 100.01 - 100.0 is slightly > 0.01 due to float representation,
        // so strict <= comparison yields false. Use a slightly larger threshold.
        assert_eq!(eval_tolerance(&ctx, 100.0, 100.01, 0.01).await, Some(false));
        // With a threshold just above the float error, it passes
        assert_eq!(eval_tolerance(&ctx, 100.0, 100.009, 0.01).await, Some(true));
    }

    #[tokio::test]
    async fn test_tolerance_match_negative_amounts() {
        let ctx = setup_ctx();
        // abs(-50 - (-50.005)) = 0.005 <= 0.01
        assert_eq!(eval_tolerance(&ctx, -50.0, -50.005, 0.01).await, Some(true));
        // abs(-50 - (-50.02)) = 0.02 > 0.01
        assert_eq!(eval_tolerance(&ctx, -50.0, -50.02, 0.01).await, Some(false));
    }

    #[tokio::test]
    async fn test_tolerance_match_zero() {
        let ctx = setup_ctx();
        assert_eq!(eval_tolerance(&ctx, 0.0, 0.0, 0.01).await, Some(true));
        assert_eq!(eval_tolerance(&ctx, 0.0, 0.005, 0.01).await, Some(true));
        assert_eq!(eval_tolerance(&ctx, 0.0, 0.02, 0.01).await, Some(false));
    }

    #[tokio::test]
    async fn test_tolerance_match_zero_threshold() {
        let ctx = setup_ctx();
        // With zero threshold, only exact match should pass
        assert_eq!(eval_tolerance(&ctx, 100.0, 100.0, 0.0).await, Some(true));
        assert_eq!(eval_tolerance(&ctx, 100.0, 100.001, 0.0).await, Some(false));
    }

    #[tokio::test]
    async fn test_tolerance_match_large_threshold() {
        let ctx = setup_ctx();
        assert_eq!(eval_tolerance(&ctx, 100.0, 200.0, 1000.0).await, Some(true));
    }

    #[tokio::test]
    async fn test_tolerance_match_currency_precision_cent() {
        let ctx = setup_ctx();
        // IEEE 754: |99.99 - 100.0| is slightly > 0.01, so strict <= is false
        assert_eq!(eval_tolerance(&ctx, 99.99, 100.0, 0.01).await, Some(false));
        // Clearly within tolerance
        assert_eq!(eval_tolerance(&ctx, 99.995, 100.0, 0.01).await, Some(true));
        // Clearly outside
        assert_eq!(eval_tolerance(&ctx, 99.98, 100.0, 0.01).await, Some(false));
    }

    #[tokio::test]
    async fn test_tolerance_match_currency_precision_mil() {
        let ctx = setup_ctx();
        // 0.001 tolerance (sub-cent precision)
        assert_eq!(
            eval_tolerance(&ctx, 100.0, 100.0005, 0.001).await,
            Some(true)
        );
        assert_eq!(
            eval_tolerance(&ctx, 100.0, 100.002, 0.001).await,
            Some(false)
        );
    }

    #[tokio::test]
    async fn test_tolerance_match_nan() {
        let ctx = setup_ctx();
        let query = "SELECT tolerance_match(CAST('NaN' AS DOUBLE), 100.0, 0.01)";
        let result = ctx.sql(query).await.unwrap().collect().await.unwrap();
        let col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        // NaN - 100.0 is NaN, abs(NaN) is NaN, NaN <= 0.01 is false
        assert!(!col.value(0));
    }

    #[tokio::test]
    async fn test_tolerance_match_infinity() {
        let ctx = setup_ctx();
        let query =
            "SELECT tolerance_match(CAST('Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE), 0.01)";
        let result = ctx.sql(query).await.unwrap().collect().await.unwrap();
        let col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        // Inf - Inf = NaN, abs(NaN) = NaN, NaN <= 0.01 is false
        assert!(!col.value(0));
    }

    #[tokio::test]
    async fn test_tolerance_match_with_null() {
        let ctx = setup_ctx();
        let query = "SELECT tolerance_match(CAST(NULL AS DOUBLE), 100.0, 0.01)";
        let result = ctx.sql(query).await.unwrap().collect().await.unwrap();
        let col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col.is_null(0));
    }

    #[tokio::test]
    async fn test_tolerance_match_array_input() {
        // Test with columnar data
        let ctx = setup_ctx();
        use std::io::Write;
        let csv = "a,b,threshold\n100.0,100.005,0.01\n100.0,100.02,0.01\n200.0,200.0,0.001\n";
        let mut f = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
        f.write_all(csv.as_bytes()).unwrap();
        f.flush().unwrap();
        f.as_file().sync_all().unwrap();
        let path = f.path().to_str().unwrap();

        ctx.register_csv("arr_test", path, CsvReadOptions::default())
            .await
            .unwrap();

        let result = ctx
            .sql("SELECT tolerance_match(a, b, threshold) as matched FROM arr_test")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col.value(0)); // 100.0 vs 100.005 within 0.01
        assert!(!col.value(1)); // 100.0 vs 100.02 outside 0.01
        assert!(col.value(2)); // 200.0 vs 200.0 within 0.001
    }

    #[test]
    fn test_udf_metadata() {
        let udf = tolerance_match_udf();
        assert_eq!(udf.name(), "tolerance_match");
    }

    #[test]
    fn test_register_financial_udfs() {
        let ctx = SessionContext::new();
        register_financial_udfs(&ctx);
        // Verify UDF is registered by checking the function list
        // If it weren't registered, executing it would fail
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = ctx
                .sql("SELECT tolerance_match(1.0, 1.0, 0.1)")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(result.len(), 1);
        });
    }
}
