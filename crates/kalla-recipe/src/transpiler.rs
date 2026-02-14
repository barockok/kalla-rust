//! Recipe transpiler - converts Match Recipes to DataFusion queries

use crate::schema::{ComparisonOp, MatchCondition, MatchPattern, MatchRecipe, MatchRule};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TranspileError {
    #[error("Invalid recipe: {0}")]
    InvalidRecipe(String),

    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("Missing threshold for tolerance operation")]
    MissingThreshold,
}

/// Transpiler converts Match Recipes into SQL queries for DataFusion
pub struct Transpiler;

impl Transpiler {
    /// Transpile a match rule into a SQL join condition
    pub fn transpile_condition(condition: &MatchCondition, left_alias: &str, right_alias: &str) -> Result<String, TranspileError> {
        let left_col = format!("{}.{}", left_alias, condition.left);
        let right_col = format!("{}.{}", right_alias, condition.right);

        match condition.op {
            ComparisonOp::Eq => Ok(format!("{} = {}", left_col, right_col)),

            ComparisonOp::Tolerance => {
                let threshold = condition
                    .threshold
                    .ok_or(TranspileError::MissingThreshold)?;
                Ok(format!(
                    "tolerance_match({}, {}, {})",
                    left_col, right_col, threshold
                ))
            }

            ComparisonOp::Gt => Ok(format!("{} > {}", left_col, right_col)),
            ComparisonOp::Lt => Ok(format!("{} < {}", left_col, right_col)),
            ComparisonOp::Gte => Ok(format!("{} >= {}", left_col, right_col)),
            ComparisonOp::Lte => Ok(format!("{} <= {}", left_col, right_col)),

            ComparisonOp::Contains => Ok(format!(
                "{} LIKE '%' || {} || '%'",
                left_col, right_col
            )),
            ComparisonOp::StartsWith => Ok(format!(
                "{} LIKE {} || '%'",
                left_col, right_col
            )),
            ComparisonOp::EndsWith => Ok(format!(
                "{} LIKE '%' || {}",
                left_col, right_col
            )),
        }
    }

    /// Transpile a match rule into a full SQL query
    pub fn transpile_rule(
        rule: &MatchRule,
        left_alias: &str,
        right_alias: &str,
    ) -> Result<String, TranspileError> {
        if rule.conditions.is_empty() {
            return Err(TranspileError::InvalidRecipe(
                "Match rule must have at least one condition".to_string(),
            ));
        }

        // Build ON clause from conditions
        let conditions: Result<Vec<String>, _> = rule
            .conditions
            .iter()
            .map(|c| Self::transpile_condition(c, left_alias, right_alias))
            .collect();
        let on_clause = conditions?.join(" AND ");

        // Build the SELECT and JOIN based on pattern
        let query = match rule.pattern {
            MatchPattern::OneToOne => {
                format!(
                    "SELECT {l}.*, {r}.* \
                     FROM {l} \
                     INNER JOIN {r} ON {on}",
                    l = left_alias,
                    r = right_alias,
                    on = on_clause
                )
            }
            MatchPattern::OneToMany => {
                // 1:N - one left to many right, group by left
                format!(
                    "SELECT {l}.*, {r}.* \
                     FROM {l} \
                     LEFT JOIN {r} ON {on}",
                    l = left_alias,
                    r = right_alias,
                    on = on_clause
                )
            }
            MatchPattern::ManyToOne => {
                // M:1 - many left to one right, group by right
                format!(
                    "SELECT {l}.*, {r}.* \
                     FROM {l} \
                     RIGHT JOIN {r} ON {on}",
                    l = left_alias,
                    r = right_alias,
                    on = on_clause
                )
            }
        };

        Ok(query)
    }

    /// Transpile a complete recipe into executable queries
    pub fn transpile(recipe: &MatchRecipe) -> Result<TranspiledRecipe, TranspileError> {
        let left_alias = &recipe.sources.left.alias;
        let right_alias = &recipe.sources.right.alias;

        let mut match_queries = Vec::new();

        for rule in &recipe.match_rules {
            let query = Self::transpile_rule(rule, left_alias, right_alias)?;
            match_queries.push(TranspiledRule {
                name: rule.name.clone(),
                query,
                pattern: rule.pattern,
            });
        }

        // Build orphan detection queries
        // Find first equality condition for orphan detection
        let first_eq_condition = recipe
            .match_rules
            .iter()
            .flat_map(|r| r.conditions.iter())
            .find(|c| c.op == ComparisonOp::Eq);

        let (left_orphan_query, right_orphan_query) = if let Some(cond) = first_eq_condition {
            let left_key = &cond.left;
            let right_key = &cond.right;

            let left_orphan = format!(
                "SELECT {l}.* FROM {l} \
                 LEFT JOIN {r} ON {l}.{lk} = {r}.{rk} \
                 WHERE {r}.{rk} IS NULL",
                l = left_alias,
                r = right_alias,
                lk = left_key,
                rk = right_key
            );

            let right_orphan = format!(
                "SELECT {r}.* FROM {r} \
                 LEFT JOIN {l} ON {r}.{rk} = {l}.{lk} \
                 WHERE {l}.{lk} IS NULL",
                l = left_alias,
                r = right_alias,
                lk = left_key,
                rk = right_key
            );

            (Some(left_orphan), Some(right_orphan))
        } else {
            (None, None)
        };

        Ok(TranspiledRecipe {
            left_source: recipe.sources.left.uri.clone(),
            right_source: recipe.sources.right.uri.clone(),
            left_alias: left_alias.clone(),
            right_alias: right_alias.clone(),
            match_queries,
            left_orphan_query,
            right_orphan_query,
            output: recipe.output.clone(),
        })
    }
}

/// A transpiled recipe ready for execution
#[derive(Debug)]
pub struct TranspiledRecipe {
    pub left_source: String,
    pub right_source: String,
    pub left_alias: String,
    pub right_alias: String,
    pub match_queries: Vec<TranspiledRule>,
    pub left_orphan_query: Option<String>,
    pub right_orphan_query: Option<String>,
    pub output: crate::schema::OutputConfig,
}

/// A transpiled match rule
#[derive(Debug)]
pub struct TranspiledRule {
    pub name: String,
    pub query: String,
    pub pattern: MatchPattern,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;

    fn make_condition(left: &str, op: ComparisonOp, right: &str, threshold: Option<f64>) -> MatchCondition {
        MatchCondition {
            left: left.to_string(),
            op,
            right: right.to_string(),
            threshold,
        }
    }

    fn make_rule(name: &str, pattern: MatchPattern, conditions: Vec<MatchCondition>) -> MatchRule {
        MatchRule {
            name: name.to_string(),
            pattern,
            conditions,
            priority: None,
        }
    }

    fn make_recipe(rules: Vec<MatchRule>) -> MatchRecipe {
        MatchRecipe {
            version: "1.0".to_string(),
            recipe_id: "test-recipe".to_string(),
            sources: Sources {
                left: DataSource {
                    alias: "inv".to_string(),
                    uri: "file://invoices.csv".to_string(),
                    primary_key: None,
                },
                right: DataSource {
                    alias: "pay".to_string(),
                    uri: "file://payments.csv".to_string(),
                    primary_key: None,
                },
            },
            match_rules: rules,
            output: OutputConfig {
                matched: "matched.parquet".to_string(),
                unmatched_left: "unmatched_left.parquet".to_string(),
                unmatched_right: "unmatched_right.parquet".to_string(),
            },
        }
    }

    // --- Condition transpilation tests ---

    #[test]
    fn test_transpile_eq_condition() {
        let condition = make_condition("invoice_id", ComparisonOp::Eq, "payment_ref", None);
        let result = Transpiler::transpile_condition(&condition, "inv", "pay").unwrap();
        assert_eq!(result, "inv.invoice_id = pay.payment_ref");
    }

    #[test]
    fn test_transpile_tolerance_condition() {
        let condition = make_condition("amount", ComparisonOp::Tolerance, "paid_amount", Some(0.01));
        let result = Transpiler::transpile_condition(&condition, "inv", "pay").unwrap();
        assert_eq!(result, "tolerance_match(inv.amount, pay.paid_amount, 0.01)");
    }

    #[test]
    fn test_transpile_tolerance_missing_threshold() {
        let condition = make_condition("amount", ComparisonOp::Tolerance, "paid_amount", None);
        let result = Transpiler::transpile_condition(&condition, "inv", "pay");
        assert!(matches!(result, Err(TranspileError::MissingThreshold)));
    }

    #[test]
    fn test_transpile_gt_condition() {
        let condition = make_condition("amount", ComparisonOp::Gt, "paid", None);
        let result = Transpiler::transpile_condition(&condition, "l", "r").unwrap();
        assert_eq!(result, "l.amount > r.paid");
    }

    #[test]
    fn test_transpile_lt_condition() {
        let condition = make_condition("amount", ComparisonOp::Lt, "paid", None);
        let result = Transpiler::transpile_condition(&condition, "l", "r").unwrap();
        assert_eq!(result, "l.amount < r.paid");
    }

    #[test]
    fn test_transpile_gte_condition() {
        let condition = make_condition("amount", ComparisonOp::Gte, "paid", None);
        let result = Transpiler::transpile_condition(&condition, "l", "r").unwrap();
        assert_eq!(result, "l.amount >= r.paid");
    }

    #[test]
    fn test_transpile_lte_condition() {
        let condition = make_condition("amount", ComparisonOp::Lte, "paid", None);
        let result = Transpiler::transpile_condition(&condition, "l", "r").unwrap();
        assert_eq!(result, "l.amount <= r.paid");
    }

    #[test]
    fn test_transpile_contains_condition() {
        let condition = make_condition("name", ComparisonOp::Contains, "payer", None);
        let result = Transpiler::transpile_condition(&condition, "l", "r").unwrap();
        assert_eq!(result, "l.name LIKE '%' || r.payer || '%'");
    }

    #[test]
    fn test_transpile_startswith_condition() {
        let condition = make_condition("name", ComparisonOp::StartsWith, "prefix", None);
        let result = Transpiler::transpile_condition(&condition, "l", "r").unwrap();
        assert_eq!(result, "l.name LIKE r.prefix || '%'");
    }

    #[test]
    fn test_transpile_endswith_condition() {
        let condition = make_condition("name", ComparisonOp::EndsWith, "suffix", None);
        let result = Transpiler::transpile_condition(&condition, "l", "r").unwrap();
        assert_eq!(result, "l.name LIKE '%' || r.suffix");
    }

    // --- Rule transpilation tests ---

    #[test]
    fn test_transpile_one_to_one_rule() {
        let rule = make_rule(
            "test",
            MatchPattern::OneToOne,
            vec![make_condition("id", ComparisonOp::Eq, "ref", None)],
        );
        let result = Transpiler::transpile_rule(&rule, "left", "right").unwrap();
        assert!(result.contains("INNER JOIN"));
        assert!(result.contains("left.id = right.ref"));
    }

    #[test]
    fn test_transpile_one_to_many_rule() {
        let rule = make_rule(
            "one_to_many",
            MatchPattern::OneToMany,
            vec![make_condition("id", ComparisonOp::Eq, "ref", None)],
        );
        let result = Transpiler::transpile_rule(&rule, "left", "right").unwrap();
        assert!(result.contains("LEFT JOIN"));
    }

    #[test]
    fn test_transpile_many_to_one_rule() {
        let rule = make_rule(
            "many_to_one",
            MatchPattern::ManyToOne,
            vec![make_condition("id", ComparisonOp::Eq, "ref", None)],
        );
        let result = Transpiler::transpile_rule(&rule, "left", "right").unwrap();
        assert!(result.contains("RIGHT JOIN"));
    }

    #[test]
    fn test_transpile_rule_empty_conditions() {
        let rule = make_rule("empty", MatchPattern::OneToOne, vec![]);
        let result = Transpiler::transpile_rule(&rule, "l", "r");
        assert!(matches!(result, Err(TranspileError::InvalidRecipe(_))));
    }

    #[test]
    fn test_transpile_rule_multiple_conditions() {
        let rule = make_rule(
            "multi",
            MatchPattern::OneToOne,
            vec![
                make_condition("id", ComparisonOp::Eq, "ref", None),
                make_condition("amount", ComparisonOp::Tolerance, "paid", Some(0.01)),
            ],
        );
        let result = Transpiler::transpile_rule(&rule, "l", "r").unwrap();
        assert!(result.contains("l.id = r.ref"));
        assert!(result.contains("AND"));
        assert!(result.contains("tolerance_match(l.amount, r.paid, 0.01)"));
    }

    // --- Full recipe transpilation tests ---

    #[test]
    fn test_transpile_recipe() {
        let recipe = make_recipe(vec![make_rule(
            "id_match",
            MatchPattern::OneToOne,
            vec![make_condition("invoice_id", ComparisonOp::Eq, "payment_ref", None)],
        )]);

        let result = Transpiler::transpile(&recipe).unwrap();
        assert_eq!(result.left_source, "file://invoices.csv");
        assert_eq!(result.right_source, "file://payments.csv");
        assert_eq!(result.left_alias, "inv");
        assert_eq!(result.right_alias, "pay");
        assert_eq!(result.match_queries.len(), 1);
        assert_eq!(result.match_queries[0].name, "id_match");
        assert!(result.left_orphan_query.is_some());
        assert!(result.right_orphan_query.is_some());
    }

    #[test]
    fn test_transpile_recipe_orphan_queries() {
        let recipe = make_recipe(vec![make_rule(
            "eq_match",
            MatchPattern::OneToOne,
            vec![make_condition("id", ComparisonOp::Eq, "ref", None)],
        )]);

        let result = Transpiler::transpile(&recipe).unwrap();
        let left_orphan = result.left_orphan_query.unwrap();
        let right_orphan = result.right_orphan_query.unwrap();

        assert!(left_orphan.contains("LEFT JOIN"));
        assert!(left_orphan.contains("IS NULL"));
        assert!(right_orphan.contains("LEFT JOIN"));
        assert!(right_orphan.contains("IS NULL"));
    }

    #[test]
    fn test_transpile_recipe_no_eq_condition_no_orphan_queries() {
        // When there are no Eq conditions, orphan queries should be None
        let recipe = make_recipe(vec![make_rule(
            "tolerance_only",
            MatchPattern::OneToOne,
            vec![make_condition("amount", ComparisonOp::Tolerance, "paid", Some(0.01))],
        )]);

        let result = Transpiler::transpile(&recipe).unwrap();
        assert!(result.left_orphan_query.is_none());
        assert!(result.right_orphan_query.is_none());
    }

    #[test]
    fn test_transpile_recipe_multiple_rules() {
        let recipe = make_recipe(vec![
            make_rule(
                "exact_match",
                MatchPattern::OneToOne,
                vec![make_condition("id", ComparisonOp::Eq, "ref", None)],
            ),
            make_rule(
                "fuzzy_match",
                MatchPattern::OneToOne,
                vec![
                    make_condition("name", ComparisonOp::Contains, "payer", None),
                    make_condition("amount", ComparisonOp::Tolerance, "paid", Some(0.05)),
                ],
            ),
        ]);

        let result = Transpiler::transpile(&recipe).unwrap();
        assert_eq!(result.match_queries.len(), 2);
        assert_eq!(result.match_queries[0].name, "exact_match");
        assert_eq!(result.match_queries[1].name, "fuzzy_match");
    }

    #[test]
    fn test_transpile_recipe_output_config_preserved() {
        let recipe = make_recipe(vec![make_rule(
            "r",
            MatchPattern::OneToOne,
            vec![make_condition("id", ComparisonOp::Eq, "ref", None)],
        )]);
        let result = Transpiler::transpile(&recipe).unwrap();
        assert_eq!(result.output.matched, "matched.parquet");
        assert_eq!(result.output.unmatched_left, "unmatched_left.parquet");
        assert_eq!(result.output.unmatched_right, "unmatched_right.parquet");
    }

    #[test]
    fn test_transpile_recipe_pattern_preserved() {
        let recipe = make_recipe(vec![
            make_rule("1to1", MatchPattern::OneToOne, vec![make_condition("id", ComparisonOp::Eq, "ref", None)]),
            make_rule("1toN", MatchPattern::OneToMany, vec![make_condition("id", ComparisonOp::Eq, "ref", None)]),
            make_rule("Mto1", MatchPattern::ManyToOne, vec![make_condition("id", ComparisonOp::Eq, "ref", None)]),
        ]);
        let result = Transpiler::transpile(&recipe).unwrap();
        assert_eq!(result.match_queries[0].pattern, MatchPattern::OneToOne);
        assert_eq!(result.match_queries[1].pattern, MatchPattern::OneToMany);
        assert_eq!(result.match_queries[2].pattern, MatchPattern::ManyToOne);
    }
}
