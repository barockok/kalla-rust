//! Kalla Recipe - Match Recipe schema and transpiler
//!
//! This crate defines the Match Recipe JSON schema and provides
//! a transpiler to convert recipes into DataFusion logical plans.

pub mod schema;
pub mod transpiler;
pub mod validation;

pub use schema::{MatchRecipe, MatchRule, MatchCondition, MatchPattern, DataSource, OutputConfig};
pub use transpiler::Transpiler;
pub use validation::validate_recipe;
