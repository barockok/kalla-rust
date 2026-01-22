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

    #[test]
    fn test_transpile_eq_condition() {
        let condition = MatchCondition {
            left: "invoice_id".to_string(),
            op: ComparisonOp::Eq,
            right: "payment_ref".to_string(),
            threshold: None,
        };

        let result = Transpiler::transpile_condition(&condition, "inv", "pay").unwrap();
        assert_eq!(result, "inv.invoice_id = pay.payment_ref");
    }

    #[test]
    fn test_transpile_tolerance_condition() {
        let condition = MatchCondition {
            left: "amount".to_string(),
            op: ComparisonOp::Tolerance,
            right: "paid_amount".to_string(),
            threshold: Some(0.01),
        };

        let result = Transpiler::transpile_condition(&condition, "inv", "pay").unwrap();
        assert_eq!(result, "tolerance_match(inv.amount, pay.paid_amount, 0.01)");
    }

    #[test]
    fn test_transpile_one_to_one_rule() {
        let rule = MatchRule {
            name: "test".to_string(),
            pattern: MatchPattern::OneToOne,
            conditions: vec![
                MatchCondition {
                    left: "id".to_string(),
                    op: ComparisonOp::Eq,
                    right: "ref".to_string(),
                    threshold: None,
                },
            ],
            priority: None,
        };

        let result = Transpiler::transpile_rule(&rule, "left", "right").unwrap();
        assert!(result.contains("INNER JOIN"));
        assert!(result.contains("left.id = right.ref"));
    }
}
