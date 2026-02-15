//! Prompt building for LLM recipe generation

use crate::schema_extractor::SanitizedSchema;
use kalla_recipe::schema::Recipe;

/// System prompt for recipe generation
pub const SYSTEM_PROMPT: &str = r#"You are a reconciliation expert. Your task is to generate Recipe JSON configurations for data reconciliation.

Given table schemas (column names and types only - you will NOT see any actual data values), generate a valid Recipe with a DataFusion SQL query that matches records between the left and right data sources.

The Recipe JSON schema is:
{
  "recipe_id": "<descriptive-id>",
  "name": "<human-readable name>",
  "description": "<what this recipe does>",
  "sources": {
    "left": { "alias": "<left_alias>", "type": "postgres"|"file", "uri": "<uri>", "primary_key": ["<col>"] },
    "right": { "alias": "<right_alias>", "type": "postgres"|"file", "schema": ["<col1>", "<col2>"], "primary_key": ["<col>"] }
  },
  "match_sql": "<DataFusion SQL query that JOINs left and right sources>",
  "match_description": "<human-readable explanation of the match logic>"
}

Guidelines:
1. Identify likely join keys by column names (e.g., "id", "ref", "key" suffixes)
2. For financial amounts, use tolerance matching: ABS(l.amount - r.amount) / NULLIF(l.amount, 0) < 0.01
3. Use descriptive aliases in the SQL
4. Output ONLY valid JSON, no explanation

IMPORTANT: You will NEVER see actual data values - only column names and types. This is intentional for PII protection."#;

/// Build a user prompt from schemas and natural language request
pub fn build_user_prompt(
    left_schema: &SanitizedSchema,
    right_schema: &SanitizedSchema,
    user_request: &str,
    left_uri: &str,
    right_uri: &str,
) -> String {
    format!(
        r#"## Left Data Source: {left_name}
URI: {left_uri}
Columns:
{left_columns}
Row count: {left_rows}

## Right Data Source: {right_name}
URI: {right_uri}
Columns:
{right_columns}
Row count: {right_rows}

## User Request
{request}

Generate a Recipe JSON for this reconciliation task."#,
        left_name = left_schema.table_name,
        left_uri = left_uri,
        left_columns = format_columns(&left_schema.columns),
        left_rows = left_schema.row_count,
        right_name = right_schema.table_name,
        right_uri = right_uri,
        right_columns = format_columns(&right_schema.columns),
        right_rows = right_schema.row_count,
        request = user_request
    )
}

fn format_columns(columns: &[crate::schema_extractor::ColumnMeta]) -> String {
    columns
        .iter()
        .map(|c| {
            format!(
                "- {}: {} {}",
                c.name,
                c.data_type,
                if c.nullable { "(nullable)" } else { "" }
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Parse LLM response into a Recipe
pub fn parse_recipe_response(response: &str) -> anyhow::Result<Recipe> {
    // Try to extract JSON from the response (in case of markdown code blocks)
    let json_str = if response.contains("```json") {
        response
            .split("```json")
            .nth(1)
            .and_then(|s| s.split("```").next())
            .map(|s| s.trim())
            .unwrap_or(response)
    } else if response.contains("```") {
        response
            .split("```")
            .nth(1)
            .map(|s| s.trim())
            .unwrap_or(response)
    } else {
        response.trim()
    };

    let recipe: Recipe = serde_json::from_str(json_str)?;
    Ok(recipe)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_extractor::ColumnMeta;

    fn make_schema(name: &str, columns: Vec<(&str, &str, bool)>, rows: usize) -> SanitizedSchema {
        SanitizedSchema {
            table_name: name.to_string(),
            columns: columns
                .into_iter()
                .map(|(n, t, nullable)| ColumnMeta {
                    name: n.to_string(),
                    data_type: t.to_string(),
                    nullable,
                })
                .collect(),
            row_count: rows,
        }
    }

    #[test]
    fn test_build_user_prompt_contains_all_fields() {
        let left = make_schema(
            "invoices",
            vec![("invoice_id", "Utf8", false), ("amount", "Float64", false)],
            1000,
        );
        let right = make_schema(
            "payments",
            vec![
                ("payment_ref", "Utf8", false),
                ("paid_amount", "Float64", false),
            ],
            950,
        );

        let prompt = build_user_prompt(
            &left,
            &right,
            "Match invoices to payments by ID with 1 cent tolerance",
            "file://invoices.csv",
            "file://payments.csv",
        );

        assert!(prompt.contains("invoices"));
        assert!(prompt.contains("payments"));
        assert!(prompt.contains("invoice_id"));
        assert!(prompt.contains("payment_ref"));
        assert!(prompt.contains("1000"));
        assert!(prompt.contains("950"));
        assert!(prompt.contains("file://invoices.csv"));
        assert!(prompt.contains("file://payments.csv"));
        assert!(prompt.contains("1 cent tolerance"));
        assert!(!prompt.contains("$"));
    }

    #[test]
    fn test_build_user_prompt_nullable_annotation() {
        let left = make_schema("t", vec![("col", "Int64", true)], 10);
        let right = make_schema("t2", vec![("col2", "Int64", false)], 10);
        let prompt = build_user_prompt(&left, &right, "test", "u1", "u2");
        assert!(prompt.contains("(nullable)"));
    }

    #[test]
    fn test_build_user_prompt_empty_columns() {
        let left = make_schema("empty", vec![], 0);
        let right = make_schema("empty2", vec![], 0);
        let prompt = build_user_prompt(&left, &right, "test", "u1", "u2");
        assert!(prompt.contains("empty"));
        assert!(prompt.contains("0"));
    }

    #[test]
    fn test_system_prompt_not_empty() {
        assert!(!SYSTEM_PROMPT.is_empty());
        assert!(SYSTEM_PROMPT.contains("tolerance"));
        assert!(SYSTEM_PROMPT.contains("PII"));
    }

    #[test]
    fn test_parse_recipe_response_json_code_block() {
        let response = r#"```json
{
  "recipe_id": "test",
  "name": "Test Recipe",
  "description": "Test",
  "sources": {
    "left": { "alias": "left", "type": "file", "schema": ["id"], "primary_key": ["id"] },
    "right": { "alias": "right", "type": "file", "schema": ["ref"], "primary_key": ["ref"] }
  },
  "match_sql": "SELECT l.id, r.ref FROM left l JOIN right r ON l.id = r.ref",
  "match_description": "Match by ID"
}
```"#;
        let recipe = parse_recipe_response(response).unwrap();
        assert_eq!(recipe.recipe_id, "test");
    }

    #[test]
    fn test_parse_recipe_response_raw_json() {
        let response = r#"{
  "recipe_id": "raw",
  "name": "Raw Recipe",
  "description": "Test",
  "sources": {
    "left": { "alias": "l", "type": "file", "schema": ["a"], "primary_key": ["a"] },
    "right": { "alias": "r", "type": "file", "schema": ["b"], "primary_key": ["b"] }
  },
  "match_sql": "SELECT * FROM l JOIN r ON l.a = r.b",
  "match_description": "Match a to b"
}"#;
        let recipe = parse_recipe_response(response).unwrap();
        assert_eq!(recipe.recipe_id, "raw");
    }

    #[test]
    fn test_parse_recipe_response_invalid_json() {
        let response = "This is not valid JSON at all";
        let result = parse_recipe_response(response);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_recipe_response_incomplete_json() {
        let response = r#"```json
{ "recipe_id": "1.0" }
```"#;
        let result = parse_recipe_response(response);
        assert!(result.is_err());
    }

    #[test]
    fn test_format_columns() {
        let cols = vec![
            ColumnMeta {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
            },
            ColumnMeta {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
            },
        ];
        let result = format_columns(&cols);
        assert!(result.contains("- id: Int64"));
        assert!(result.contains("- name: Utf8 (nullable)"));
    }

    #[test]
    fn test_format_columns_empty() {
        let result = format_columns(&[]);
        assert_eq!(result, "");
    }
}
