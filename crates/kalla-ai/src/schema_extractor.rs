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

    fn testdata_path(filename: &str) -> String {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        format!("{}/../../testdata/{}", manifest_dir, filename)
    }

    #[tokio::test]
    async fn test_extract_schema_invoices() {
        let ctx = SessionContext::new();
        ctx.register_csv("invoices", &testdata_path("invoices.csv"), CsvReadOptions::new())
            .await
            .unwrap();

        let schema = extract_schema(&ctx, "invoices").await.unwrap();
        assert_eq!(schema.table_name, "invoices");
        assert!(!schema.columns.is_empty());
        assert!(schema.row_count > 0);
        // Verify column names include expected fields
        let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"invoice_id"));
        assert!(col_names.contains(&"amount"));
    }

    #[tokio::test]
    async fn test_extract_schema_payments() {
        let ctx = SessionContext::new();
        ctx.register_csv("payments", &testdata_path("payments.csv"), CsvReadOptions::new())
            .await
            .unwrap();

        let schema = extract_schema(&ctx, "payments").await.unwrap();
        assert_eq!(schema.table_name, "payments");
        let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"payment_id"));
        assert!(col_names.contains(&"paid_amount"));
    }

    #[tokio::test]
    async fn test_extract_schema_nonexistent_table() {
        let ctx = SessionContext::new();
        let result = extract_schema(&ctx, "nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_extract_schema_no_data_values() {
        let ctx = SessionContext::new();
        ctx.register_csv("invoices", &testdata_path("invoices.csv"), CsvReadOptions::new())
            .await
            .unwrap();

        let schema = extract_schema(&ctx, "invoices").await.unwrap();
        let json = serde_json::to_string(&schema).unwrap();

        // Should not contain any actual data values from the CSV
        assert!(!json.contains("Acme"));
        assert!(!json.contains("15000"));
        assert!(!json.contains("INV-2024"));
    }

    #[tokio::test]
    async fn test_detect_primary_key_single_column() {
        let ctx = SessionContext::new();
        ctx.register_csv("test_table", &testdata_path("invoices.csv"), CsvReadOptions::new())
            .await
            .unwrap();

        let detected = detect_primary_key(&ctx, "test_table").await.unwrap();
        assert!(detected.contains(&"invoice_id".to_string()));
    }

    #[tokio::test]
    async fn test_detect_primary_key_heuristics() {
        let ctx = SessionContext::new();
        ctx.register_csv("test_table", &testdata_path("payments.csv"), CsvReadOptions::new())
            .await
            .unwrap();

        let detected = detect_primary_key(&ctx, "test_table").await.unwrap();
        assert!(detected.contains(&"payment_id".to_string()));
    }

    #[tokio::test]
    async fn test_detect_primary_key_non_unique() {
        use std::io::Write;
        // Create a CSV with duplicate values in the id-like column
        let csv = "user_id,name\n1,Alice\n1,Bob\n2,Carol\n";
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(csv.as_bytes()).unwrap();
        f.flush().unwrap();
        f.as_file().sync_all().unwrap();
        let path = f.path().to_str().unwrap();

        let ctx = SessionContext::new();
        ctx.register_csv("dup_table", path, CsvReadOptions::new())
            .await
            .unwrap();

        let detected = detect_primary_key(&ctx, "dup_table").await.unwrap();
        // user_id has duplicates, so no confirmed primary key
        assert!(detected.is_empty());
    }

    #[tokio::test]
    async fn test_extract_schema_column_types() {
        let ctx = SessionContext::new();
        ctx.register_csv("inv", &testdata_path("invoices.csv"), CsvReadOptions::new())
            .await
            .unwrap();

        let schema = extract_schema(&ctx, "inv").await.unwrap();
        // Verify we get reasonable types and multiple columns
        assert!(schema.columns.len() >= 5);
        // All columns should have non-empty data type strings
        for col in &schema.columns {
            assert!(!col.data_type.is_empty());
            assert!(!col.name.is_empty());
        }
    }

    #[tokio::test]
    async fn test_detect_primary_key_no_id_column_non_unique_fallback() {
        use std::io::Write;
        // first column has duplicates
        let csv = "first_name,last_name\nAlice,Smith\nAlice,Jones\nBob,Brown\n";
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(csv.as_bytes()).unwrap();
        f.flush().unwrap();
        f.as_file().sync_all().unwrap();
        let path = f.path().to_str().unwrap();

        let ctx = SessionContext::new();
        ctx.register_csv("dup_fb_table", path, CsvReadOptions::new())
            .await
            .unwrap();

        let detected = detect_primary_key(&ctx, "dup_fb_table").await.unwrap();
        // Fallback column has duplicates, returns empty
        assert!(detected.is_empty());
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
        assert!(json.contains("\"nullable\":false"));
    }

    #[test]
    fn test_column_meta_deserialization() {
        let json = r#"{"name":"col","data_type":"Int64","nullable":true}"#;
        let meta: ColumnMeta = serde_json::from_str(json).unwrap();
        assert_eq!(meta.name, "col");
        assert_eq!(meta.data_type, "Int64");
        assert!(meta.nullable);
    }

    #[test]
    fn test_sanitized_schema_has_no_values() {
        let schema = SanitizedSchema {
            table_name: "invoices".to_string(),
            columns: vec![
                ColumnMeta { name: "id".to_string(), data_type: "Int64".to_string(), nullable: false },
                ColumnMeta { name: "amount".to_string(), data_type: "Float64".to_string(), nullable: false },
            ],
            row_count: 1000,
        };
        let json = serde_json::to_string(&schema).unwrap();
        assert!(!json.contains("$"));
        assert!(!json.contains("123.45"));
        assert!(json.contains("row_count"));
        assert!(json.contains("1000"));
    }

    #[test]
    fn test_sanitized_schema_deserialization() {
        let json = r#"{"table_name":"t","columns":[{"name":"c","data_type":"Utf8","nullable":false}],"row_count":42}"#;
        let schema: SanitizedSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.table_name, "t");
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.row_count, 42);
    }
}
