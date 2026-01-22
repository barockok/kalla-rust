//! Match Recipe schema definitions

use serde::{Deserialize, Serialize};

/// The main Match Recipe structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchRecipe {
    /// Schema version
    pub version: String,

    /// Unique identifier for this recipe
    pub recipe_id: String,

    /// Data sources configuration
    pub sources: Sources,

    /// Match rules to apply
    pub match_rules: Vec<MatchRule>,

    /// Output configuration
    pub output: OutputConfig,
}

/// Data sources configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sources {
    /// Left data source
    pub left: DataSource,

    /// Right data source
    pub right: DataSource,
}

/// A data source definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSource {
    /// Alias for the source in queries
    pub alias: String,

    /// URI to the data source
    /// Examples:
    /// - file://path/to/file.csv
    /// - file://path/to/file.parquet
    /// - postgres://user:pass@host:port/db?table=tablename
    pub uri: String,

    /// Optional primary key column(s)
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
}

/// A match rule definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchRule {
    /// Name of the rule
    pub name: String,

    /// Match pattern (1:1, 1:N, M:1)
    pub pattern: MatchPattern,

    /// Conditions that must be satisfied for a match
    pub conditions: Vec<MatchCondition>,

    /// Priority of this rule (lower = higher priority)
    #[serde(default)]
    pub priority: Option<i32>,
}

/// Match pattern types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchPattern {
    /// One-to-one matching
    #[serde(rename = "1:1")]
    OneToOne,

    /// One left to many right
    #[serde(rename = "1:N")]
    OneToMany,

    /// Many left to one right
    #[serde(rename = "M:1")]
    ManyToOne,
}

/// A match condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchCondition {
    /// Left column name
    pub left: String,

    /// Comparison operator
    pub op: ComparisonOp,

    /// Right column name
    pub right: String,

    /// Threshold for tolerance operations
    #[serde(default)]
    pub threshold: Option<f64>,
}

/// Comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ComparisonOp {
    /// Exact equality
    Eq,

    /// Tolerance-based matching (for numeric values)
    Tolerance,

    /// Greater than
    Gt,

    /// Less than
    Lt,

    /// Greater than or equal
    Gte,

    /// Less than or equal
    Lte,

    /// String contains
    Contains,

    /// String starts with
    StartsWith,

    /// String ends with
    EndsWith,
}

/// Output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    /// Path for matched records
    pub matched: String,

    /// Path for unmatched left records
    pub unmatched_left: String,

    /// Path for unmatched right records
    pub unmatched_right: String,
}

impl Default for MatchRecipe {
    fn default() -> Self {
        Self {
            version: "1.0".to_string(),
            recipe_id: "default".to_string(),
            sources: Sources {
                left: DataSource {
                    alias: "left".to_string(),
                    uri: String::new(),
                    primary_key: None,
                },
                right: DataSource {
                    alias: "right".to_string(),
                    uri: String::new(),
                    primary_key: None,
                },
            },
            match_rules: Vec::new(),
            output: OutputConfig {
                matched: "matched.parquet".to_string(),
                unmatched_left: "unmatched_left.parquet".to_string(),
                unmatched_right: "unmatched_right.parquet".to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recipe_serialization() {
        let recipe = MatchRecipe {
            version: "1.0".to_string(),
            recipe_id: "invoice-payment-match".to_string(),
            sources: Sources {
                left: DataSource {
                    alias: "invoices".to_string(),
                    uri: "file://invoices.csv".to_string(),
                    primary_key: Some(vec!["invoice_id".to_string()]),
                },
                right: DataSource {
                    alias: "payments".to_string(),
                    uri: "postgres://localhost/db?table=payments".to_string(),
                    primary_key: Some(vec!["payment_id".to_string()]),
                },
            },
            match_rules: vec![
                MatchRule {
                    name: "exact_id_match".to_string(),
                    pattern: MatchPattern::OneToOne,
                    conditions: vec![MatchCondition {
                        left: "invoice_id".to_string(),
                        op: ComparisonOp::Eq,
                        right: "payment_ref".to_string(),
                        threshold: None,
                    }],
                    priority: Some(1),
                },
                MatchRule {
                    name: "amount_tolerance".to_string(),
                    pattern: MatchPattern::OneToOne,
                    conditions: vec![MatchCondition {
                        left: "amount".to_string(),
                        op: ComparisonOp::Tolerance,
                        right: "paid_amount".to_string(),
                        threshold: Some(0.01),
                    }],
                    priority: Some(2),
                },
            ],
            output: OutputConfig {
                matched: "matched_results.parquet".to_string(),
                unmatched_left: "orphan_invoices.parquet".to_string(),
                unmatched_right: "orphan_payments.parquet".to_string(),
            },
        };

        let json = serde_json::to_string_pretty(&recipe).unwrap();
        println!("{}", json);

        let parsed: MatchRecipe = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.recipe_id, recipe.recipe_id);
        assert_eq!(parsed.match_rules.len(), 2);
    }
}
