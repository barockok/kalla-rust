//! Recipe resolution — maps a Recipe to the components needed by the runner.

use std::collections::HashMap;

use crate::schema::{Recipe, RecipeSource};

/// A resolved data source with alias and URI, ready for the runner.
#[derive(Debug, Clone)]
pub struct ResolvedSource {
    pub alias: String,
    pub uri: String,
}

/// The resolved components of a recipe, ready for the runner.
#[derive(Debug, Clone)]
pub struct RecipeResolution {
    /// The match SQL query to execute.
    pub match_sql: String,
    /// Resolved data sources with their URIs.
    pub sources: Vec<ResolvedSource>,
    /// Primary key columns per source alias.
    pub primary_keys: HashMap<String, Vec<String>>,
}

/// Resolve a recipe into runner-ready components.
///
/// `file_uris` maps source aliases to uploaded file URIs for file-type sources
/// that don't have a persistent URI in the recipe.
pub fn resolve_recipe(recipe: &Recipe, file_uris: &HashMap<String, String>) -> RecipeResolution {
    let mut sources = Vec::new();
    let mut primary_keys = HashMap::new();

    resolve_source(
        &recipe.sources.left,
        file_uris,
        &mut sources,
        &mut primary_keys,
    );
    resolve_source(
        &recipe.sources.right,
        file_uris,
        &mut sources,
        &mut primary_keys,
    );

    RecipeResolution {
        match_sql: recipe.match_sql.clone(),
        sources,
        primary_keys,
    }
}

fn resolve_source(
    source: &RecipeSource,
    file_uris: &HashMap<String, String>,
    sources: &mut Vec<ResolvedSource>,
    primary_keys: &mut HashMap<String, Vec<String>>,
) {
    let uri = source
        .uri
        .clone()
        .or_else(|| file_uris.get(&source.alias).cloned())
        .unwrap_or_default();

    sources.push(ResolvedSource {
        alias: source.alias.clone(),
        uri,
    });

    primary_keys.insert(source.alias.clone(), source.primary_key.clone());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{RecipeSources, SourceType};

    fn test_recipe() -> Recipe {
        Recipe {
            recipe_id: "test".to_string(),
            name: "Test".to_string(),
            description: "Test recipe".to_string(),
            match_sql: "SELECT * FROM invoices i JOIN payments p ON i.id = p.ref_id".to_string(),
            match_description: "Test match".to_string(),
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
                    schema: Some(vec!["payment_id".to_string(), "amount".to_string()]),
                    primary_key: vec!["payment_id".to_string()],
                },
            },
        }
    }

    #[test]
    fn test_resolve_recipe_with_file_uri() {
        let recipe = test_recipe();
        let mut file_uris = HashMap::new();
        file_uris.insert("payments".to_string(), "/tmp/payments.csv".to_string());

        let resolution = resolve_recipe(&recipe, &file_uris);

        assert_eq!(resolution.match_sql, recipe.match_sql);
        assert_eq!(resolution.sources.len(), 2);
        assert_eq!(resolution.sources[0].alias, "invoices");
        assert_eq!(
            resolution.sources[0].uri,
            "postgres://host/db?table=invoices"
        );
        assert_eq!(resolution.sources[1].alias, "payments");
        assert_eq!(resolution.sources[1].uri, "/tmp/payments.csv");
    }

    #[test]
    fn test_resolve_recipe_primary_keys() {
        let recipe = test_recipe();
        let resolution = resolve_recipe(&recipe, &HashMap::new());

        assert_eq!(
            resolution.primary_keys["invoices"],
            vec!["invoice_id".to_string()]
        );
        assert_eq!(
            resolution.primary_keys["payments"],
            vec!["payment_id".to_string()]
        );
    }

    #[test]
    fn test_resolve_recipe_missing_file_uri_defaults_empty() {
        let recipe = test_recipe();
        let resolution = resolve_recipe(&recipe, &HashMap::new());

        // File source without a provided URI defaults to empty string
        assert_eq!(resolution.sources[1].uri, "");
    }

    #[test]
    fn test_resolve_recipe_persistent_source_ignores_file_uris() {
        let recipe = test_recipe();
        let mut file_uris = HashMap::new();
        file_uris.insert(
            "invoices".to_string(),
            "/tmp/should-be-ignored.csv".to_string(),
        );

        let resolution = resolve_recipe(&recipe, &file_uris);

        // Persistent source uses its own URI, not the file_uris override
        assert_eq!(
            resolution.sources[0].uri,
            "postgres://host/db?table=invoices"
        );
    }
}
