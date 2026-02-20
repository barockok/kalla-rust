//! Recipe validation for the simplified SQL-based schema

use crate::schema::{Recipe, SourceType};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Recipe ID cannot be empty")]
    EmptyRecipeId,

    #[error("Recipe name cannot be empty")]
    EmptyName,

    #[error("match_sql cannot be empty")]
    EmptyMatchSql,

    #[error("Source '{0}' must have at least one primary key column")]
    EmptyPrimaryKey(String),

    #[error("File source '{0}' must have a schema (expected column names)")]
    FileSourceMissingSchema(String),

    #[error("Persistent source '{0}' must have a URI")]
    PersistentSourceMissingUri(String),
}

/// Validate a recipe against the new schema rules.
pub fn validate_recipe(recipe: &Recipe) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    if recipe.recipe_id.trim().is_empty() {
        errors.push(ValidationError::EmptyRecipeId);
    }

    if recipe.name.trim().is_empty() {
        errors.push(ValidationError::EmptyName);
    }

    if recipe.match_sql.trim().is_empty() {
        errors.push(ValidationError::EmptyMatchSql);
    }

    validate_source(&recipe.sources.left, &mut errors);
    validate_source(&recipe.sources.right, &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn validate_source(source: &crate::schema::RecipeSource, errors: &mut Vec<ValidationError>) {
    if source.primary_key.is_empty() {
        errors.push(ValidationError::EmptyPrimaryKey(source.alias.clone()));
    }

    match source.source_type {
        SourceType::File => {
            if source.schema.as_ref().is_none_or(|s| s.is_empty()) {
                errors.push(ValidationError::FileSourceMissingSchema(
                    source.alias.clone(),
                ));
            }
        }
        SourceType::Postgres | SourceType::Elasticsearch => {
            if source.uri.as_ref().is_none_or(|u| u.trim().is_empty()) {
                errors.push(ValidationError::PersistentSourceMissingUri(
                    source.alias.clone(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;

    fn valid_recipe() -> Recipe {
        Recipe {
            recipe_id: "test".to_string(),
            name: "Test Recipe".to_string(),
            description: "A test recipe".to_string(),
            match_sql: "SELECT l.id, r.id FROM left_src l JOIN right_src r ON l.id = r.ref"
                .to_string(),
            match_description: "Match by ID".to_string(),
            sources: RecipeSources {
                left: RecipeSource {
                    alias: "left_src".to_string(),
                    source_type: SourceType::Postgres,
                    uri: Some("postgres://host/db".to_string()),
                    schema: None,
                    primary_key: vec!["id".to_string()],
                },
                right: RecipeSource {
                    alias: "right_src".to_string(),
                    source_type: SourceType::File,
                    uri: None,
                    schema: Some(vec!["id".to_string(), "ref".to_string()]),
                    primary_key: vec!["id".to_string()],
                },
            },
        }
    }

    #[test]
    fn test_valid_recipe() {
        assert!(validate_recipe(&valid_recipe()).is_ok());
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
    fn test_empty_name() {
        let mut recipe = valid_recipe();
        recipe.name = "".to_string();
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::EmptyName)));
    }

    #[test]
    fn test_empty_match_sql() {
        let mut recipe = valid_recipe();
        recipe.match_sql = "".to_string();
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::EmptyMatchSql)));
    }

    #[test]
    fn test_whitespace_match_sql() {
        let mut recipe = valid_recipe();
        recipe.match_sql = "   ".to_string();
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::EmptyMatchSql)));
    }

    #[test]
    fn test_empty_primary_key() {
        let mut recipe = valid_recipe();
        recipe.sources.left.primary_key = vec![];
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::EmptyPrimaryKey(_))));
    }

    #[test]
    fn test_file_source_missing_schema() {
        let mut recipe = valid_recipe();
        recipe.sources.right.schema = None;
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::FileSourceMissingSchema(_))));
    }

    #[test]
    fn test_file_source_empty_schema() {
        let mut recipe = valid_recipe();
        recipe.sources.right.schema = Some(vec![]);
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::FileSourceMissingSchema(_))));
    }

    #[test]
    fn test_persistent_source_missing_uri() {
        let mut recipe = valid_recipe();
        recipe.sources.left.uri = None;
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::PersistentSourceMissingUri(_))));
    }

    #[test]
    fn test_persistent_source_empty_uri() {
        let mut recipe = valid_recipe();
        recipe.sources.left.uri = Some("  ".to_string());
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::PersistentSourceMissingUri(_))));
    }

    #[test]
    fn test_multiple_errors_accumulated() {
        let mut recipe = valid_recipe();
        recipe.recipe_id = "".to_string();
        recipe.match_sql = "".to_string();
        recipe.sources.left.primary_key = vec![];
        let errors = validate_recipe(&recipe).unwrap_err();
        assert!(errors.len() >= 3);
    }

    #[test]
    fn test_validation_error_display() {
        let err = ValidationError::EmptyMatchSql;
        let msg = format!("{}", err);
        assert!(msg.contains("match_sql"));

        let err = ValidationError::FileSourceMissingSchema("payments".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("payments"));
    }

    #[test]
    fn test_both_sources_as_persistent() {
        let recipe = Recipe {
            recipe_id: "test".to_string(),
            name: "Test".to_string(),
            description: "Test".to_string(),
            match_sql: "SELECT * FROM a JOIN b ON a.id = b.id".to_string(),
            match_description: "Match by ID".to_string(),
            sources: RecipeSources {
                left: RecipeSource {
                    alias: "a".to_string(),
                    source_type: SourceType::Postgres,
                    uri: Some("postgres://host/db".to_string()),
                    schema: None,
                    primary_key: vec!["id".to_string()],
                },
                right: RecipeSource {
                    alias: "b".to_string(),
                    source_type: SourceType::Elasticsearch,
                    uri: Some("http://localhost:9200/index".to_string()),
                    schema: None,
                    primary_key: vec!["id".to_string()],
                },
            },
        };
        assert!(validate_recipe(&recipe).is_ok());
    }
}
