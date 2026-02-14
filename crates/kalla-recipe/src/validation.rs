//! Recipe validation

use crate::schema::{ComparisonOp, MatchRecipe};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Invalid version: expected '1.0', got '{0}'")]
    InvalidVersion(String),

    #[error("Recipe ID cannot be empty")]
    EmptyRecipeId,

    #[error("Source URI cannot be empty for '{0}'")]
    EmptySourceUri(String),

    #[error("No match rules defined")]
    NoMatchRules,

    #[error("Rule '{0}' has no conditions")]
    NoConditions(String),

    #[error("Rule '{0}': tolerance operation on '{1}' requires a threshold")]
    MissingThreshold(String, String),

    #[error("Rule '{0}': column name cannot be empty")]
    EmptyColumnName(String),
}

/// Validate a match recipe
pub fn validate_recipe(recipe: &MatchRecipe) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    // Validate version
    if recipe.version != "1.0" {
        errors.push(ValidationError::InvalidVersion(recipe.version.clone()));
    }

    // Validate recipe ID
    if recipe.recipe_id.trim().is_empty() {
        errors.push(ValidationError::EmptyRecipeId);
    }

    // Validate sources
    if recipe.sources.left.uri.trim().is_empty() {
        errors.push(ValidationError::EmptySourceUri(
            recipe.sources.left.alias.clone(),
        ));
    }
    if recipe.sources.right.uri.trim().is_empty() {
        errors.push(ValidationError::EmptySourceUri(
            recipe.sources.right.alias.clone(),
        ));
    }

    // Validate match rules
    if recipe.match_rules.is_empty() {
        errors.push(ValidationError::NoMatchRules);
    }

    for rule in &recipe.match_rules {
        if rule.conditions.is_empty() {
            errors.push(ValidationError::NoConditions(rule.name.clone()));
        }

        for condition in &rule.conditions {
            if condition.left.trim().is_empty() || condition.right.trim().is_empty() {
                errors.push(ValidationError::EmptyColumnName(rule.name.clone()));
            }

            if condition.op == ComparisonOp::Tolerance && condition.threshold.is_none() {
                errors.push(ValidationError::MissingThreshold(
                    rule.name.clone(),
                    condition.left.clone(),
                ));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;

    fn valid_recipe() -> MatchRecipe {
        MatchRecipe {
            version: "1.0".to_string(),
            recipe_id: "test".to_string(),
            sources: Sources {
                left: DataSource {
                    alias: "left".to_string(),
                    uri: "file://test.csv".to_string(),
                    primary_key: None,
                },
                right: DataSource {
                    alias: "right".to_string(),
                    uri: "file://test2.csv".to_string(),
                    primary_key: None,
                },
            },
            match_rules: vec![MatchRule {
                name: "test_rule".to_string(),
                pattern: MatchPattern::OneToOne,
                conditions: vec![MatchCondition {
                    left: "id".to_string(),
                    op: ComparisonOp::Eq,
                    right: "ref".to_string(),
                    threshold: None,
                }],
                priority: None,
            }],
            output: OutputConfig {
                matched: "matched.parquet".to_string(),
                unmatched_left: "left.parquet".to_string(),
                unmatched_right: "right.parquet".to_string(),
            },
        }
    }

    #[test]
    fn test_valid_recipe() {
        assert!(validate_recipe(&valid_recipe()).is_ok());
    }

    #[test]
    fn test_invalid_version() {
        let mut recipe = valid_recipe();
        recipe.version = "2.0".to_string();
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidVersion(_))));
    }

    #[test]
    fn test_missing_threshold() {
        let mut recipe = valid_recipe();
        recipe.match_rules[0].conditions[0].op = ComparisonOp::Tolerance;
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::MissingThreshold(_, _))));
    }

    #[test]
    fn test_tolerance_with_threshold_is_valid() {
        let mut recipe = valid_recipe();
        recipe.match_rules[0].conditions[0].op = ComparisonOp::Tolerance;
        recipe.match_rules[0].conditions[0].threshold = Some(0.01);
        assert!(validate_recipe(&recipe).is_ok());
    }

    #[test]
    fn test_empty_recipe_id() {
        let mut recipe = valid_recipe();
        recipe.recipe_id = "".to_string();
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::EmptyRecipeId)));
    }

    #[test]
    fn test_whitespace_recipe_id() {
        let mut recipe = valid_recipe();
        recipe.recipe_id = "   ".to_string();
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::EmptyRecipeId)));
    }

    #[test]
    fn test_empty_left_source_uri() {
        let mut recipe = valid_recipe();
        recipe.sources.left.uri = "".to_string();
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::EmptySourceUri(_))));
    }

    #[test]
    fn test_empty_right_source_uri() {
        let mut recipe = valid_recipe();
        recipe.sources.right.uri = "".to_string();
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::EmptySourceUri(_))));
    }

    #[test]
    fn test_no_match_rules() {
        let mut recipe = valid_recipe();
        recipe.match_rules = vec![];
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::NoMatchRules)));
    }

    #[test]
    fn test_rule_with_no_conditions() {
        let mut recipe = valid_recipe();
        recipe.match_rules[0].conditions = vec![];
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::NoConditions(_))));
    }

    #[test]
    fn test_empty_column_name_left() {
        let mut recipe = valid_recipe();
        recipe.match_rules[0].conditions[0].left = "".to_string();
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::EmptyColumnName(_))));
    }

    #[test]
    fn test_empty_column_name_right() {
        let mut recipe = valid_recipe();
        recipe.match_rules[0].conditions[0].right = "  ".to_string();
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::EmptyColumnName(_))));
    }

    #[test]
    fn test_multiple_errors_accumulated() {
        let mut recipe = valid_recipe();
        recipe.version = "3.0".to_string();
        recipe.recipe_id = "".to_string();
        recipe.sources.left.uri = "".to_string();
        let errors = validate_recipe(&recipe).unwrap_err();
        // Should have at least 3 errors
        assert!(errors.len() >= 3);
    }

    #[test]
    fn test_validation_error_display() {
        let err = ValidationError::InvalidVersion("2.0".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("2.0"));

        let err = ValidationError::MissingThreshold("rule1".to_string(), "amount".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("rule1"));
        assert!(msg.contains("amount"));
    }
}
