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
        let parsed: MatchRecipe = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.recipe_id, recipe.recipe_id);
        assert_eq!(parsed.match_rules.len(), 2);
    }

    #[test]
    fn test_recipe_default() {
        let recipe = MatchRecipe::default();
        assert_eq!(recipe.version, "1.0");
        assert_eq!(recipe.recipe_id, "default");
        assert_eq!(recipe.sources.left.alias, "left");
        assert_eq!(recipe.sources.right.alias, "right");
        assert!(recipe.sources.left.uri.is_empty());
        assert!(recipe.match_rules.is_empty());
    }

    #[test]
    fn test_match_pattern_serialization() {
        // 1:1
        let json = serde_json::to_string(&MatchPattern::OneToOne).unwrap();
        assert_eq!(json, "\"1:1\"");
        let parsed: MatchPattern = serde_json::from_str("\"1:1\"").unwrap();
        assert_eq!(parsed, MatchPattern::OneToOne);

        // 1:N
        let json = serde_json::to_string(&MatchPattern::OneToMany).unwrap();
        assert_eq!(json, "\"1:N\"");
        let parsed: MatchPattern = serde_json::from_str("\"1:N\"").unwrap();
        assert_eq!(parsed, MatchPattern::OneToMany);

        // M:1
        let json = serde_json::to_string(&MatchPattern::ManyToOne).unwrap();
        assert_eq!(json, "\"M:1\"");
        let parsed: MatchPattern = serde_json::from_str("\"M:1\"").unwrap();
        assert_eq!(parsed, MatchPattern::ManyToOne);
    }

    #[test]
    fn test_comparison_op_serialization() {
        let ops = vec![
            (ComparisonOp::Eq, "\"eq\""),
            (ComparisonOp::Tolerance, "\"tolerance\""),
            (ComparisonOp::Gt, "\"gt\""),
            (ComparisonOp::Lt, "\"lt\""),
            (ComparisonOp::Gte, "\"gte\""),
            (ComparisonOp::Lte, "\"lte\""),
            (ComparisonOp::Contains, "\"contains\""),
            (ComparisonOp::StartsWith, "\"startswith\""),
            (ComparisonOp::EndsWith, "\"endswith\""),
        ];
        for (op, expected_json) in ops {
            let json = serde_json::to_string(&op).unwrap();
            assert_eq!(json, expected_json);
            let parsed: ComparisonOp = serde_json::from_str(expected_json).unwrap();
            assert_eq!(parsed, op);
        }
    }

    #[test]
    fn test_data_source_optional_primary_key() {
        let json = r#"{"alias":"test","uri":"file://test.csv"}"#;
        let ds: DataSource = serde_json::from_str(json).unwrap();
        assert!(ds.primary_key.is_none());

        let json_with_pk =
            r#"{"alias":"test","uri":"file://test.csv","primary_key":["id","sub_id"]}"#;
        let ds: DataSource = serde_json::from_str(json_with_pk).unwrap();
        assert_eq!(ds.primary_key.unwrap().len(), 2);
    }

    #[test]
    fn test_match_condition_with_threshold() {
        let json = r#"{"left":"amount","op":"tolerance","right":"paid","threshold":0.05}"#;
        let cond: MatchCondition = serde_json::from_str(json).unwrap();
        assert_eq!(cond.threshold, Some(0.05));
    }

    #[test]
    fn test_match_condition_without_threshold() {
        let json = r#"{"left":"id","op":"eq","right":"ref"}"#;
        let cond: MatchCondition = serde_json::from_str(json).unwrap();
        assert_eq!(cond.threshold, None);
    }

    #[test]
    fn test_match_rule_optional_priority() {
        let json = r#"{
            "name":"test_rule",
            "pattern":"1:1",
            "conditions":[{"left":"id","op":"eq","right":"ref"}]
        }"#;
        let rule: MatchRule = serde_json::from_str(json).unwrap();
        assert!(rule.priority.is_none());
    }

    #[test]
    fn test_recipe_deserialization_from_json() {
        let json = r#"{
            "version": "1.0",
            "recipe_id": "test",
            "sources": {
                "left": {"alias": "l", "uri": "file://l.csv"},
                "right": {"alias": "r", "uri": "file://r.csv"}
            },
            "match_rules": [
                {
                    "name": "rule1",
                    "pattern": "1:1",
                    "conditions": [
                        {"left": "id", "op": "eq", "right": "ref"},
                        {"left": "amt", "op": "tolerance", "right": "paid", "threshold": 0.01}
                    ]
                },
                {
                    "name": "rule2",
                    "pattern": "1:N",
                    "conditions": [
                        {"left": "name", "op": "contains", "right": "payer"}
                    ],
                    "priority": 2
                }
            ],
            "output": {
                "matched": "m.parquet",
                "unmatched_left": "ul.parquet",
                "unmatched_right": "ur.parquet"
            }
        }"#;
        let recipe: MatchRecipe = serde_json::from_str(json).unwrap();
        assert_eq!(recipe.match_rules.len(), 2);
        assert_eq!(recipe.match_rules[0].conditions.len(), 2);
        assert_eq!(recipe.match_rules[1].pattern, MatchPattern::OneToMany);
        assert_eq!(recipe.match_rules[1].priority, Some(2));
    }

    #[test]
    fn test_malformed_json_missing_required_field() {
        let json = r#"{"version":"1.0"}"#;
        let result = serde_json::from_str::<MatchRecipe>(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_operator_in_json() {
        let json = r#"{"left":"id","op":"invalid_op","right":"ref"}"#;
        let result = serde_json::from_str::<MatchCondition>(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_pattern_in_json() {
        let result = serde_json::from_str::<MatchPattern>("\"2:2\"");
        assert!(result.is_err());
    }
}
