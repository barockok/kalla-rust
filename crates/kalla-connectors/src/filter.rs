use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct FilterCondition {
    pub column: String,
    pub op: FilterOp,
    pub value: FilterValue,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Between,
    In,
    Like,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FilterValue {
    String(String),
    Number(f64),
    StringArray(Vec<String>),
    Range([String; 2]),
}

impl FilterCondition {
    pub fn to_sql_where(&self) -> String {
        let col = format!("\"{}\"", self.column);
        match (&self.op, &self.value) {
            (FilterOp::Eq, FilterValue::String(v)) => format!("{} = '{}'", col, v),
            (FilterOp::Eq, FilterValue::Number(v)) => format!("{} = {}", col, format_number(*v)),
            (FilterOp::Neq, FilterValue::String(v)) => format!("{} != '{}'", col, v),
            (FilterOp::Neq, FilterValue::Number(v)) => {
                format!("{} != {}", col, format_number(*v))
            }
            (FilterOp::Gt, FilterValue::Number(v)) => format!("{} > {}", col, format_number(*v)),
            (FilterOp::Gt, FilterValue::String(v)) => format!("{} > '{}'", col, v),
            (FilterOp::Gte, FilterValue::Number(v)) => {
                format!("{} >= {}", col, format_number(*v))
            }
            (FilterOp::Gte, FilterValue::String(v)) => format!("{} >= '{}'", col, v),
            (FilterOp::Lt, FilterValue::Number(v)) => format!("{} < {}", col, format_number(*v)),
            (FilterOp::Lt, FilterValue::String(v)) => format!("{} < '{}'", col, v),
            (FilterOp::Lte, FilterValue::Number(v)) => {
                format!("{} <= {}", col, format_number(*v))
            }
            (FilterOp::Lte, FilterValue::String(v)) => format!("{} <= '{}'", col, v),
            (FilterOp::Between, FilterValue::Range([from, to])) => {
                format!("{} BETWEEN '{}' AND '{}'", col, from, to)
            }
            (FilterOp::In, FilterValue::StringArray(vals)) => {
                let list = vals
                    .iter()
                    .map(|v| format!("'{}'", v))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} IN ({})", col, list)
            }
            (FilterOp::Like, FilterValue::String(v)) => format!("{} LIKE '{}'", col, v),
            _ => format!("{} IS NOT NULL", col), // fallback for mismatched op/value
        }
    }
}

fn format_number(n: f64) -> String {
    if n == n.floor() {
        format!("{}", n as i64)
    } else {
        format!("{}", n)
    }
}

pub fn build_where_clause(conditions: &[FilterCondition]) -> String {
    if conditions.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = conditions.iter().map(|c| c.to_sql_where()).collect();
    format!(" WHERE {}", parts.join(" AND "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eq_to_sql() {
        let condition = FilterCondition {
            column: "status".to_string(),
            op: FilterOp::Eq,
            value: FilterValue::String("active".to_string()),
        };
        let sql = condition.to_sql_where();
        assert_eq!(sql, "\"status\" = 'active'");
    }

    #[test]
    fn test_between_to_sql() {
        let condition = FilterCondition {
            column: "invoice_date".to_string(),
            op: FilterOp::Between,
            value: FilterValue::Range(["2024-01-01".to_string(), "2024-01-31".to_string()]),
        };
        let sql = condition.to_sql_where();
        assert_eq!(
            sql,
            "\"invoice_date\" BETWEEN '2024-01-01' AND '2024-01-31'"
        );
    }

    #[test]
    fn test_in_to_sql() {
        let condition = FilterCondition {
            column: "category".to_string(),
            op: FilterOp::In,
            value: FilterValue::StringArray(vec!["food".to_string(), "drink".to_string()]),
        };
        let sql = condition.to_sql_where();
        assert_eq!(sql, "\"category\" IN ('food', 'drink')");
    }

    #[test]
    fn test_gt_number_to_sql() {
        let condition = FilterCondition {
            column: "amount".to_string(),
            op: FilterOp::Gt,
            value: FilterValue::Number(100.0),
        };
        let sql = condition.to_sql_where();
        assert_eq!(sql, "\"amount\" > 100");
    }

    #[test]
    fn test_like_to_sql() {
        let condition = FilterCondition {
            column: "name".to_string(),
            op: FilterOp::Like,
            value: FilterValue::String("%acme%".to_string()),
        };
        let sql = condition.to_sql_where();
        assert_eq!(sql, "\"name\" LIKE '%acme%'");
    }

    #[test]
    fn test_build_where_clause_empty() {
        let clause = build_where_clause(&[]);
        assert_eq!(clause, "");
    }

    #[test]
    fn test_build_where_clause_multiple() {
        let conditions = vec![
            FilterCondition {
                column: "status".to_string(),
                op: FilterOp::Eq,
                value: FilterValue::String("active".to_string()),
            },
            FilterCondition {
                column: "amount".to_string(),
                op: FilterOp::Gte,
                value: FilterValue::Number(50.0),
            },
        ];
        let clause = build_where_clause(&conditions);
        assert_eq!(
            clause,
            " WHERE \"status\" = 'active' AND \"amount\" >= 50"
        );
    }
}
