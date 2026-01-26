//! Schema extraction for LLM prompts (PII-safe)

use arrow::array::Int64Array;
use datafusion::common::DataFusionError;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};

/// A sanitized schema that contains NO actual data, only metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SanitizedSchema {
    /// Table name or alias
    pub table_name: String,

    /// Column metadata (names and types only)
    pub columns: Vec<ColumnMeta>,

    /// Total row count (no data values)
    pub row_count: usize,
}

/// Column metadata (safe to send to LLM)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    /// Column name
    pub name: String,

    /// Data type as string
    pub data_type: String,

    /// Whether the column is nullable
    pub nullable: bool,
}

/// Extract sanitized schema from a registered DataFusion table
///
/// This function extracts ONLY schema information - no data values
/// are ever read or sent to the LLM.
pub async fn extract_schema(
    ctx: &SessionContext,
    table_name: &str,
) -> anyhow::Result<SanitizedSchema> {
    // Get the table provider
    let table = ctx
        .table(table_name)
        .await?;

    let schema = table.schema();

    let columns: Vec<ColumnMeta> = schema
        .fields()
        .iter()
        .map(|field| ColumnMeta {
            name: field.name().clone(),
            data_type: format!("{:?}", field.data_type()),
            nullable: field.is_nullable(),
        })
        .collect();

    // Get row count without fetching actual data
    let count_df = ctx
        .sql(&format!("SELECT COUNT(*) FROM {}", table_name))
        .await?;
    let batches = count_df.collect().await?;
    let row_count = if !batches.is_empty() && batches[0].num_rows() > 0 {
        use arrow::array::Int64Array;
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr: &Int64Array| arr.value(0) as usize)
            .unwrap_or(0)
    } else {
        0
    };

    Ok(SanitizedSchema {
        table_name: table_name.to_string(),
        columns,
        row_count,
    })
}

/// Detects likely primary key columns using heuristics:
/// 1. Column name ends with "_id" or "id"
/// 2. Column name is exactly "id"
/// 3. Column has unique values (checked via COUNT DISTINCT)
pub async fn detect_primary_key(
    ctx: &SessionContext,
    table_name: &str,
) -> Result<Vec<String>, DataFusionError> {
    let table = ctx.table(table_name).await?;
    let schema = table.schema();

    let mut candidates: Vec<String> = Vec::new();

    // Heuristic 1: Column names suggesting primary key
    for field in schema.fields() {
        let name = field.name().to_lowercase();
        if name == "id" || name.ends_with("_id") || name.ends_with("id") {
            candidates.push(field.name().to_string());
        }
    }

    // If no heuristic matches, check first column
    if candidates.is_empty() {
        if let Some(first_field) = schema.fields().first() {
            candidates.push(first_field.name().to_string());
        }
    }

    // Verify uniqueness for top candidate
    if let Some(candidate) = candidates.first() {
        let count_query = format!(
            "SELECT COUNT(*) as total, COUNT(DISTINCT \"{}\") as distinct_count FROM {}",
            candidate, table_name
        );
        let df = ctx.sql(&count_query).await?;
        let batches = df.collect().await?;

        if let Some(batch) = batches.first() {
            let total = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(0))
                .unwrap_or(0);
            let distinct = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(0))
                .unwrap_or(0);

            // If not unique, return empty (needs user confirmation)
            if total != distinct {
                return Ok(vec![]);
            }
        }
    }

    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::CsvReadOptions;

    // Helper to get workspace root testdata path
    fn testdata_path(filename: &str) -> String {
        // CARGO_MANIFEST_DIR points to crates/kalla-ai, so we need to go up two levels
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        format!("{}/../../testdata/{}", manifest_dir, filename)
    }

    #[tokio::test]
    async fn test_detect_primary_key_single_column() {
        let ctx = SessionContext::new();
        ctx.register_csv(
            "test_table",
            &testdata_path("invoices.csv"),
            CsvReadOptions::new(),
        )
        .await
        .unwrap();

        let detected = detect_primary_key(&ctx, "test_table").await.unwrap();

        // invoice_id is unique and ends with "_id"
        assert!(detected.contains(&"invoice_id".to_string()));
    }

    #[tokio::test]
    async fn test_detect_primary_key_heuristics() {
        let ctx = SessionContext::new();
        ctx.register_csv(
            "test_table",
            &testdata_path("payments.csv"),
            CsvReadOptions::new(),
        )
        .await
        .unwrap();

        let detected = detect_primary_key(&ctx, "test_table").await.unwrap();

        // payment_id matches heuristics
        assert!(detected.contains(&"payment_id".to_string()));
    }

    #[test]
    fn test_column_meta_serialization() {
        let meta = ColumnMeta {
            name: "invoice_id".to_string(),
            data_type: "Utf8".to_string(),
            nullable: false,
        };

        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("invoice_id"));
        assert!(json.contains("Utf8"));
    }

    #[test]
    fn test_sanitized_schema_has_no_values() {
        let schema = SanitizedSchema {
            table_name: "invoices".to_string(),
            columns: vec![
                ColumnMeta {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "amount".to_string(),
                    data_type: "Float64".to_string(),
                    nullable: false,
                },
            ],
            row_count: 1000,
        };

        let json = serde_json::to_string(&schema).unwrap();

        // Verify NO actual data values could be in this
        assert!(!json.contains("$"));
        assert!(!json.contains("123.45"));
        // Only metadata is present
        assert!(json.contains("row_count"));
        assert!(json.contains("1000"));
    }
}
