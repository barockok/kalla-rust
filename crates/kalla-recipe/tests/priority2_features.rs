//! Integration tests for Priority 2 adoption features

use kalla_recipe::field_resolver::{
    normalize_field_name, resolve_field_name, resolve_recipe_fields,
};
use kalla_recipe::schema::{
    ComparisonOp, DataSource, MatchCondition, MatchPattern, MatchRecipe, MatchRule, OutputConfig,
    Sources,
};
use kalla_recipe::schema_validation::validate_recipe_against_schema;

#[test]
fn test_field_resolution_end_to_end() {
    // Simulating recipe with various naming conventions
    let conditions = vec![
        ("Invoice-ID".to_string(), "payment_ref".to_string()),
        ("AMOUNT".to_string(), "Paid_Amount".to_string()),
    ];

    let left_fields = vec![
        "invoice_id".to_string(),
        "amount".to_string(),
        "customer_name".to_string(),
    ];

    let right_fields = vec![
        "payment_ref".to_string(),
        "paid_amount".to_string(),
        "transaction_date".to_string(),
    ];

    let result = resolve_recipe_fields(&conditions, &left_fields, &right_fields);
    assert!(result.is_ok());

    let resolved = result.unwrap();
    assert_eq!(resolved.len(), 2);
    assert_eq!(resolved[0].1, "invoice_id"); // Invoice-ID -> invoice_id
    assert_eq!(resolved[1].1, "amount"); // AMOUNT -> amount
}

#[test]
fn test_schema_validation_with_suggestions() {
    // Test that validation provides helpful suggestions
    let recipe = MatchRecipe {
        version: "1.0".to_string(),
        recipe_id: "test".to_string(),
        sources: Sources {
            left: DataSource {
                alias: "invoices".to_string(),
                uri: "file://test.csv".to_string(),
                primary_key: None,
            },
            right: DataSource {
                alias: "payments".to_string(),
                uri: "file://test2.csv".to_string(),
                primary_key: None,
            },
        },
        match_rules: vec![MatchRule {
            name: "test_rule".to_string(),
            pattern: MatchPattern::OneToOne,
            conditions: vec![MatchCondition {
                left: "invoice".to_string(), // Typo - missing "_id"
                op: ComparisonOp::Eq,
                right: "payment_ref".to_string(),
                threshold: None,
            }],
            priority: Some(1),
        }],
        output: OutputConfig {
            matched: "matched.parquet".to_string(),
            unmatched_left: "unmatched_left.parquet".to_string(),
            unmatched_right: "unmatched_right.parquet".to_string(),
        },
    };

    let left_fields = vec!["invoice_id".to_string(), "amount".to_string()];
    let right_fields = vec!["payment_ref".to_string()];

    let result = validate_recipe_against_schema(&recipe, &left_fields, &right_fields);

    assert!(!result.valid);
    assert_eq!(result.errors.len(), 1);
    // Should suggest "invoice_id" since "invoice" is a substring
    assert_eq!(result.errors[0].suggestion, Some("invoice_id".to_string()));
}

#[test]
fn test_normalization_variations() {
    // Test all normalization scenarios work correctly
    assert_eq!(normalize_field_name("CustomerName"), "customername");
    assert_eq!(normalize_field_name("customer-name"), "customer_name");
    assert_eq!(normalize_field_name("customer name"), "customer_name");
    assert_eq!(normalize_field_name("CUSTOMER_NAME"), "customer_name");
    assert_eq!(normalize_field_name("  trimmed  "), "trimmed");
}

#[test]
fn test_resolve_with_mixed_conventions() {
    // Test resolving fields with different naming conventions
    let fields = vec![
        "CustomerID".to_string(),
        "order_total".to_string(),
        "ship-date".to_string(),
    ];

    // All these should resolve to CustomerID
    assert_eq!(
        resolve_field_name("customerid", &fields),
        Some("CustomerID".to_string())
    );
    assert_eq!(
        resolve_field_name("CUSTOMERID", &fields),
        Some("CustomerID".to_string())
    );

    // These should resolve to order_total
    assert_eq!(
        resolve_field_name("order-total", &fields),
        Some("order_total".to_string())
    );
    assert_eq!(
        resolve_field_name("ORDER_TOTAL", &fields),
        Some("order_total".to_string())
    );

    // This should resolve to ship-date
    assert_eq!(
        resolve_field_name("ship_date", &fields),
        Some("ship-date".to_string())
    );
}

#[test]
fn test_validation_with_warnings_for_resolved_fields() {
    let recipe = MatchRecipe {
        version: "1.0".to_string(),
        recipe_id: "test".to_string(),
        sources: Sources {
            left: DataSource {
                alias: "orders".to_string(),
                uri: "file://orders.csv".to_string(),
                primary_key: None,
            },
            right: DataSource {
                alias: "shipments".to_string(),
                uri: "file://shipments.csv".to_string(),
                primary_key: None,
            },
        },
        match_rules: vec![MatchRule {
            name: "order_match".to_string(),
            pattern: MatchPattern::OneToOne,
            conditions: vec![MatchCondition {
                left: "Order-ID".to_string(), // Will be resolved to order_id
                op: ComparisonOp::Eq,
                right: "order_ref".to_string(),
                threshold: None,
            }],
            priority: Some(1),
        }],
        output: OutputConfig {
            matched: "matched.parquet".to_string(),
            unmatched_left: "unmatched_left.parquet".to_string(),
            unmatched_right: "unmatched_right.parquet".to_string(),
        },
    };

    let left_fields = vec!["order_id".to_string(), "total".to_string()];
    let right_fields = vec!["order_ref".to_string(), "ship_date".to_string()];

    let result = validate_recipe_against_schema(&recipe, &left_fields, &right_fields);

    // Should be valid because Order-ID resolves to order_id
    assert!(result.valid);
    // Should have a warning about the resolution
    assert!(!result.warnings.is_empty());
    assert!(result.warnings[0].contains("resolved"));
}
