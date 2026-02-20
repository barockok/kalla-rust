//! Simplified recipe schema — raw DataFusion SQL match rules

use serde::{Deserialize, Serialize};

/// Source type determines how data is provided at execution time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    /// Persistent remote source (Postgres, Elasticsearch)
    Postgres,
    Elasticsearch,
    /// Disposable file source — schema stored, file uploaded each execution
    File,
}

/// A data source in a recipe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipeSource {
    pub alias: String,
    #[serde(rename = "type")]
    pub source_type: SourceType,
    /// Connection URI for persistent sources (postgres://)
    /// None for file sources (file is uploaded at execution time)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// Expected column names — required for file sources, optional for persistent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Vec<String>>,
    /// Primary key columns for deriving unmatched records
    pub primary_key: Vec<String>,
}

/// Recipe sources (left and right).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipeSources {
    pub left: RecipeSource,
    pub right: RecipeSource,
}

/// A reconciliation recipe — the core configuration unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recipe {
    pub recipe_id: String,
    pub name: String,
    pub description: String,
    /// The SQL query that DataFusion executes to produce matched records.
    /// References source aliases as table names.
    pub match_sql: String,
    /// Human-readable explanation of what the SQL does.
    pub match_description: String,
    /// Left and right data sources.
    pub sources: RecipeSources,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recipe_serialization_roundtrip() {
        let recipe = Recipe {
            recipe_id: "monthly-invoice-payment".to_string(),
            name: "Invoice-Payment Reconciliation".to_string(),
            description: "Match invoices to payments by reference number and amount".to_string(),
            match_sql: "SELECT i.invoice_id, p.payment_id FROM invoices i JOIN payments p ON i.invoice_id = p.reference_number".to_string(),
            match_description: "Matches invoices to payments where reference numbers are identical".to_string(),
            sources: RecipeSources {
                left: RecipeSource {
                    alias: "invoices".to_string(),
                    source_type: SourceType::Postgres,
                    uri: Some("postgres://host/db?table=invoices".to_string()),
                    schema: None,
                    primary_key: vec!["invoice_id".to_string()],
                },
                right: RecipeSource {
                    alias: "payments".to_string(),
                    source_type: SourceType::File,
                    uri: None,
                    schema: Some(vec!["payment_id".to_string(), "reference_number".to_string(), "amount".to_string()]),
                    primary_key: vec!["payment_id".to_string()],
                },
            },
        };

        let json = serde_json::to_string_pretty(&recipe).unwrap();
        let parsed: Recipe = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.recipe_id, recipe.recipe_id);
        assert_eq!(parsed.name, recipe.name);
        assert_eq!(parsed.match_sql, recipe.match_sql);
        assert_eq!(parsed.sources.left.source_type, SourceType::Postgres);
        assert_eq!(parsed.sources.right.source_type, SourceType::File);
        assert!(parsed.sources.left.uri.is_some());
        assert!(parsed.sources.right.uri.is_none());
        assert!(parsed.sources.right.schema.is_some());
    }

    #[test]
    fn test_source_type_serialization() {
        let types = vec![
            (SourceType::Postgres, "\"postgres\""),
            (SourceType::Elasticsearch, "\"elasticsearch\""),
            (SourceType::File, "\"file\""),
        ];
        for (st, expected) in types {
            let json = serde_json::to_string(&st).unwrap();
            assert_eq!(json, expected);
            let parsed: SourceType = serde_json::from_str(expected).unwrap();
            assert_eq!(parsed, st);
        }
    }

    #[test]
    fn test_recipe_deserialization_from_json() {
        let json = r#"{
            "recipe_id": "monthly-invoice-payment",
            "name": "Invoice-Payment Reconciliation",
            "description": "Match invoices to payments",
            "sources": {
                "left": {
                    "alias": "invoices",
                    "type": "postgres",
                    "uri": "postgres://host/db?table=invoices",
                    "primary_key": ["invoice_id"]
                },
                "right": {
                    "alias": "payments",
                    "type": "file",
                    "schema": ["payment_id", "reference_number", "amount", "date"],
                    "primary_key": ["payment_id"]
                }
            },
            "match_sql": "SELECT i.invoice_id, p.payment_id FROM invoices i JOIN payments p ON i.invoice_id = p.reference_number AND ABS(i.amount - p.amount) / NULLIF(i.amount, 0) < 0.01",
            "match_description": "Matches invoices to payments where reference numbers are identical and amounts are within 1%."
        }"#;
        let recipe: Recipe = serde_json::from_str(json).unwrap();
        assert_eq!(recipe.recipe_id, "monthly-invoice-payment");
        assert_eq!(recipe.sources.left.source_type, SourceType::Postgres);
        assert_eq!(recipe.sources.right.source_type, SourceType::File);
        assert_eq!(recipe.sources.right.schema.as_ref().unwrap().len(), 4);
        assert_eq!(recipe.sources.right.primary_key, vec!["payment_id"]);
        assert!(recipe.match_sql.contains("JOIN"));
    }

    #[test]
    fn test_file_source_uri_skipped_in_serialization() {
        let source = RecipeSource {
            alias: "data".to_string(),
            source_type: SourceType::File,
            uri: None,
            schema: Some(vec!["col1".to_string()]),
            primary_key: vec!["col1".to_string()],
        };
        let json = serde_json::to_string(&source).unwrap();
        assert!(!json.contains("uri"));
    }

    #[test]
    fn test_persistent_source_schema_skipped_in_serialization() {
        let source = RecipeSource {
            alias: "data".to_string(),
            source_type: SourceType::Postgres,
            uri: Some("postgres://host/db".to_string()),
            schema: None,
            primary_key: vec!["id".to_string()],
        };
        let json = serde_json::to_string(&source).unwrap();
        assert!(!json.contains("schema"));
    }

    #[test]
    fn test_malformed_json_missing_required_field() {
        let json = r#"{"recipe_id":"test"}"#;
        let result = serde_json::from_str::<Recipe>(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_source_type() {
        let json = r#"{"alias":"test","type":"mysql","primary_key":["id"]}"#;
        let result = serde_json::from_str::<RecipeSource>(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_composite_primary_key() {
        let source = RecipeSource {
            alias: "data".to_string(),
            source_type: SourceType::Postgres,
            uri: Some("postgres://host/db".to_string()),
            schema: None,
            primary_key: vec!["org_id".to_string(), "invoice_id".to_string()],
        };
        let json = serde_json::to_string(&source).unwrap();
        let parsed: RecipeSource = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.primary_key.len(), 2);
    }
}
