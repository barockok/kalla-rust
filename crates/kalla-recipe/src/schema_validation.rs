//! Recipe schema validation - validates recipe fields against actual source schemas

use crate::field_resolver::{normalize_field_name, resolve_field_name};
use crate::schema::MatchRecipe;

#[derive(Debug, Clone)]
pub struct SchemaValidationError {
    pub rule_name: String,
    pub field: String,
    pub source: String, // "left" or "right"
    pub message: String,
    pub suggestion: Option<String>,
}

#[derive(Debug)]
pub struct SchemaValidationResult {
    pub valid: bool,
    pub errors: Vec<SchemaValidationError>,
    pub warnings: Vec<String>,
    pub resolved_fields: Vec<(String, String)>, // (original, resolved)
}

/// Validates a recipe's match conditions against actual source schemas
pub fn validate_recipe_against_schema(
    recipe: &MatchRecipe,
    left_fields: &[String],
    right_fields: &[String],
) -> SchemaValidationResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut resolved_fields = Vec::new();

    for rule in &recipe.match_rules {
        for condition in &rule.conditions {
            // Validate left field
            match resolve_field_name(&condition.left, left_fields) {
                Some(resolved) => {
                    if resolved != condition.left {
                        warnings.push(format!(
                            "Field '{}' resolved to '{}' in left source",
                            condition.left, resolved
                        ));
                        resolved_fields.push((condition.left.clone(), resolved));
                    }
                }
                None => {
                    let suggestion = find_closest_match(&condition.left, left_fields);
                    errors.push(SchemaValidationError {
                        rule_name: rule.name.clone(),
                        field: condition.left.clone(),
                        source: "left".to_string(),
                        message: format!(
                            "Field '{}' not found in left source '{}'",
                            condition.left, recipe.sources.left.alias
                        ),
                        suggestion,
                    });
                }
            }

            // Validate right field
            match resolve_field_name(&condition.right, right_fields) {
                Some(resolved) => {
                    if resolved != condition.right {
                        warnings.push(format!(
                            "Field '{}' resolved to '{}' in right source",
                            condition.right, resolved
                        ));
                        resolved_fields.push((condition.right.clone(), resolved));
                    }
                }
                None => {
                    let suggestion = find_closest_match(&condition.right, right_fields);
                    errors.push(SchemaValidationError {
                        rule_name: rule.name.clone(),
                        field: condition.right.clone(),
                        source: "right".to_string(),
                        message: format!(
                            "Field '{}' not found in right source '{}'",
                            condition.right, recipe.sources.right.alias
                        ),
                        suggestion,
                    });
                }
            }
        }
    }

    SchemaValidationResult {
        valid: errors.is_empty(),
        errors,
        warnings,
        resolved_fields,
    }
}

/// Finds the closest matching field name using simple heuristics
fn find_closest_match(field: &str, available: &[String]) -> Option<String> {
    let normalized = normalize_field_name(field);

    // Check if any field contains the normalized name or vice versa
    for available_field in available {
        let norm_available = normalize_field_name(available_field);
        if norm_available.contains(&normalized) || normalized.contains(&norm_available) {
            return Some(available_field.clone());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        ComparisonOp, DataSource, MatchCondition, MatchPattern, MatchRecipe, MatchRule,
        OutputConfig, Sources,
    };

    fn test_recipe() -> MatchRecipe {
        MatchRecipe {
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
                    left: "invoice_id".to_string(),
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
        }
    }

    #[test]
    fn test_validate_valid_recipe() {
        let recipe = test_recipe();
        let left_fields = vec!["invoice_id".to_string(), "amount".to_string()];
        let right_fields = vec!["payment_ref".to_string(), "paid_amount".to_string()];

        let result = validate_recipe_against_schema(&recipe, &left_fields, &right_fields);

        assert!(result.valid);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_validate_missing_left_field() {
        let recipe = test_recipe();
        let left_fields = vec!["wrong_field".to_string()];
        let right_fields = vec!["payment_ref".to_string()];

        let result = validate_recipe_against_schema(&recipe, &left_fields, &right_fields);

        assert!(!result.valid);
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].source, "left");
        assert!(result.errors[0].message.contains("invoice_id"));
    }

    #[test]
    fn test_validate_with_case_difference() {
        let mut recipe = test_recipe();
        recipe.match_rules[0].conditions[0].left = "Invoice_ID".to_string();

        let left_fields = vec!["invoice_id".to_string()];
        let right_fields = vec!["payment_ref".to_string()];

        let result = validate_recipe_against_schema(&recipe, &left_fields, &right_fields);

        assert!(result.valid);
        assert!(!result.warnings.is_empty());
        assert!(result.warnings[0].contains("resolved"));
    }

    #[test]
    fn test_find_closest_match() {
        let fields = vec!["invoice_id".to_string(), "customer_name".to_string()];

        assert_eq!(
            find_closest_match("invoice", &fields),
            Some("invoice_id".to_string())
        );
        assert_eq!(find_closest_match("xyz", &fields), None);
    }
}
