//! Schema extraction for LLM prompts (PII-safe)

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

#[cfg(test)]
mod tests {
    use super::*;

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
