//! Kalla Recipe — simplified SQL-based recipe schema
//!
//! This crate defines the Recipe schema with raw DataFusion SQL match rules.

pub mod schema;
pub mod validation;

pub use schema::{Recipe, RecipeSource, RecipeSources, SourceType};
pub use validation::validate_recipe;
