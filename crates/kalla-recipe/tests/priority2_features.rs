//! Integration tests for the simplified recipe schema

use kalla_recipe::schema::{Recipe, RecipeSource, RecipeSources, SourceType};
use kalla_recipe::validate_recipe;

fn sample_recipe() -> Recipe {
    Recipe {
        recipe_id: "invoice-payment-match".to_string(),
        name: "Invoice-Payment Reconciliation".to_string(),
        description: "Match invoices to payments by reference and amount".to_string(),
        match_sql: "SELECT i.invoice_id, p.payment_id, i.amount AS left_amount, p.amount AS right_amount \
                     FROM invoices i JOIN payments p \
                     ON i.invoice_id = p.reference_number \
                     AND ABS(i.amount - p.amount) / NULLIF(i.amount, 0) < 0.01"
            .to_string(),
        match_description:
            "Matches invoices to payments where reference numbers match and amounts are within 1%"
                .to_string(),
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
                schema: Some(vec![
                    "payment_id".to_string(),
                    "reference_number".to_string(),
                    "amount".to_string(),
                    "date".to_string(),
                ]),
                primary_key: vec!["payment_id".to_string()],
            },
        },
    }
}

#[test]
fn test_recipe_validation_valid() {
    let recipe = sample_recipe();
    assert!(validate_recipe(&recipe).is_ok());
}

#[test]
fn test_recipe_serialization_roundtrip() {
    let recipe = sample_recipe();
    let json = serde_json::to_string_pretty(&recipe).unwrap();
    let parsed: Recipe = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.recipe_id, recipe.recipe_id);
    assert_eq!(parsed.match_sql, recipe.match_sql);
    assert_eq!(parsed.sources.left.source_type, SourceType::Postgres);
    assert_eq!(parsed.sources.right.source_type, SourceType::File);
}

#[test]
fn test_recipe_validation_empty_sql() {
    let mut recipe = sample_recipe();
    recipe.match_sql = String::new();
    assert!(validate_recipe(&recipe).is_err());
}

#[test]
fn test_recipe_validation_file_source_needs_schema() {
    let mut recipe = sample_recipe();
    recipe.sources.right.schema = None;
    assert!(validate_recipe(&recipe).is_err());
}

#[test]
fn test_recipe_validation_persistent_source_needs_uri() {
    let mut recipe = sample_recipe();
    recipe.sources.left.uri = None;
    assert!(validate_recipe(&recipe).is_err());
}

#[test]
fn test_recipe_validation_sources_need_primary_keys() {
    let mut recipe = sample_recipe();
    recipe.sources.left.primary_key = vec![];
    assert!(validate_recipe(&recipe).is_err());
}

#[test]
fn test_all_source_types_deserialize() {
    for (json_val, expected) in [
        ("\"postgres\"", SourceType::Postgres),
        ("\"bigquery\"", SourceType::Bigquery),
        ("\"elasticsearch\"", SourceType::Elasticsearch),
        ("\"file\"", SourceType::File),
    ] {
        let parsed: SourceType = serde_json::from_str(json_val).unwrap();
        assert_eq!(parsed, expected);
    }
}

#[test]
fn test_recipe_from_design_doc_json() {
    let json = r#"{
        "recipe_id": "monthly-invoice-payment",
        "name": "Invoice-Payment Reconciliation",
        "description": "Match invoices to payments by reference number and amount within 1% tolerance",
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
        "match_sql": "SELECT i.invoice_id, p.payment_id, i.amount AS left_amount, p.amount AS right_amount FROM invoices i JOIN payments p ON i.invoice_id = p.reference_number AND ABS(i.amount - p.amount) / NULLIF(i.amount, 0) < 0.01",
        "match_description": "Matches invoices to payments where the reference numbers are identical and the amounts are within 1% of each other."
    }"#;
    let recipe: Recipe = serde_json::from_str(json).unwrap();
    assert!(validate_recipe(&recipe).is_ok());
    assert_eq!(recipe.sources.right.schema.as_ref().unwrap().len(), 4);
}
