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
    Range([String; 2]),
    StringArray(Vec<String>),
}

fn sanitize_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

fn sanitize_sql_identifier(s: &str) -> String {
    s.replace('"', "\"\"")
}

impl FilterCondition {
    pub fn to_sql_where(&self) -> String {
        let col = format!("\"{}\"", sanitize_sql_identifier(&self.column));
        match (&self.op, &self.value) {
            (FilterOp::Eq, FilterValue::String(v)) => format!("{} = '{}'", col, sanitize_sql_string(v)),
            (FilterOp::Eq, FilterValue::Number(v)) => format!("{} = {}", col, format_number(*v)),
            (FilterOp::Neq, FilterValue::String(v)) => format!("{} != '{}'", col, sanitize_sql_string(v)),
            (FilterOp::Neq, FilterValue::Number(v)) => {
                format!("{} != {}", col, format_number(*v))
            }
            (FilterOp::Gt, FilterValue::Number(v)) => format!("{} > {}", col, format_number(*v)),
            (FilterOp::Gt, FilterValue::String(v)) => format!("{} > '{}'", col, sanitize_sql_string(v)),
            (FilterOp::Gte, FilterValue::Number(v)) => {
                format!("{} >= {}", col, format_number(*v))
            }
            (FilterOp::Gte, FilterValue::String(v)) => format!("{} >= '{}'", col, sanitize_sql_string(v)),
            (FilterOp::Lt, FilterValue::Number(v)) => format!("{} < {}", col, format_number(*v)),
            (FilterOp::Lt, FilterValue::String(v)) => format!("{} < '{}'", col, sanitize_sql_string(v)),
            (FilterOp::Lte, FilterValue::Number(v)) => {
                format!("{} <= {}", col, format_number(*v))
            }
            (FilterOp::Lte, FilterValue::String(v)) => format!("{} <= '{}'", col, sanitize_sql_string(v)),
            (FilterOp::Between, FilterValue::Range([from, to])) => {
                format!("{} BETWEEN '{}' AND '{}'", col, sanitize_sql_string(from), sanitize_sql_string(to))
            }
            (FilterOp::In, FilterValue::StringArray(vals)) => {
                let list = vals
                    .iter()
                    .map(|v| format!("'{}'", sanitize_sql_string(v)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} IN ({})", col, list)
            }
            (FilterOp::Like, FilterValue::String(v)) => format!("{} LIKE '{}'", col, sanitize_sql_string(v)),
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

    fn fc(column: &str, op: FilterOp, value: FilterValue) -> FilterCondition {
        FilterCondition {
            column: column.to_string(),
            op,
            value,
        }
    }

    // --- Eq tests ---
    #[test]
    fn test_eq_string_to_sql() {
        let sql = fc("status", FilterOp::Eq, FilterValue::String("active".to_string())).to_sql_where();
        assert_eq!(sql, "\"status\" = 'active'");
    }

    #[test]
    fn test_eq_number_to_sql() {
        let sql = fc("count", FilterOp::Eq, FilterValue::Number(42.0)).to_sql_where();
        assert_eq!(sql, "\"count\" = 42");
    }

    #[test]
    fn test_eq_float_number_to_sql() {
        let sql = fc("price", FilterOp::Eq, FilterValue::Number(19.99)).to_sql_where();
        assert_eq!(sql, "\"price\" = 19.99");
    }

    // --- Neq tests ---
    #[test]
    fn test_neq_string_to_sql() {
        let sql = fc("status", FilterOp::Neq, FilterValue::String("closed".to_string())).to_sql_where();
        assert_eq!(sql, "\"status\" != 'closed'");
    }

    #[test]
    fn test_neq_number_to_sql() {
        let sql = fc("id", FilterOp::Neq, FilterValue::Number(0.0)).to_sql_where();
        assert_eq!(sql, "\"id\" != 0");
    }

    // --- Gt tests ---
    #[test]
    fn test_gt_number_to_sql() {
        let sql = fc("amount", FilterOp::Gt, FilterValue::Number(100.0)).to_sql_where();
        assert_eq!(sql, "\"amount\" > 100");
    }

    #[test]
    fn test_gt_string_to_sql() {
        let sql = fc("date", FilterOp::Gt, FilterValue::String("2024-01-01".to_string())).to_sql_where();
        assert_eq!(sql, "\"date\" > '2024-01-01'");
    }

    // --- Gte tests ---
    #[test]
    fn test_gte_number_to_sql() {
        let sql = fc("amount", FilterOp::Gte, FilterValue::Number(50.0)).to_sql_where();
        assert_eq!(sql, "\"amount\" >= 50");
    }

    #[test]
    fn test_gte_string_to_sql() {
        let sql = fc("date", FilterOp::Gte, FilterValue::String("2024-06-01".to_string())).to_sql_where();
        assert_eq!(sql, "\"date\" >= '2024-06-01'");
    }

    // --- Lt tests ---
    #[test]
    fn test_lt_number_to_sql() {
        let sql = fc("amount", FilterOp::Lt, FilterValue::Number(500.0)).to_sql_where();
        assert_eq!(sql, "\"amount\" < 500");
    }

    #[test]
    fn test_lt_string_to_sql() {
        let sql = fc("date", FilterOp::Lt, FilterValue::String("2024-12-31".to_string())).to_sql_where();
        assert_eq!(sql, "\"date\" < '2024-12-31'");
    }

    // --- Lte tests ---
    #[test]
    fn test_lte_number_to_sql() {
        let sql = fc("amount", FilterOp::Lte, FilterValue::Number(999.99)).to_sql_where();
        assert_eq!(sql, "\"amount\" <= 999.99");
    }

    #[test]
    fn test_lte_string_to_sql() {
        let sql = fc("name", FilterOp::Lte, FilterValue::String("Z".to_string())).to_sql_where();
        assert_eq!(sql, "\"name\" <= 'Z'");
    }

    // --- Between tests ---
    #[test]
    fn test_between_to_sql() {
        let sql = fc(
            "invoice_date",
            FilterOp::Between,
            FilterValue::Range(["2024-01-01".to_string(), "2024-01-31".to_string()]),
        ).to_sql_where();
        assert_eq!(sql, "\"invoice_date\" BETWEEN '2024-01-01' AND '2024-01-31'");
    }

    // --- In tests ---
    #[test]
    fn test_in_to_sql() {
        let sql = fc(
            "category",
            FilterOp::In,
            FilterValue::StringArray(vec!["food".to_string(), "drink".to_string()]),
        ).to_sql_where();
        assert_eq!(sql, "\"category\" IN ('food', 'drink')");
    }

    #[test]
    fn test_in_single_value() {
        let sql = fc(
            "type",
            FilterOp::In,
            FilterValue::StringArray(vec!["a".to_string()]),
        ).to_sql_where();
        assert_eq!(sql, "\"type\" IN ('a')");
    }

    // --- Like tests ---
    #[test]
    fn test_like_to_sql() {
        let sql = fc("name", FilterOp::Like, FilterValue::String("%acme%".to_string())).to_sql_where();
        assert_eq!(sql, "\"name\" LIKE '%acme%'");
    }

    // --- Fallback (mismatched op/value) ---
    #[test]
    fn test_mismatched_op_value_fallback() {
        // Between with a String value (not Range) => fallback
        let sql = fc("col", FilterOp::Between, FilterValue::String("bad".to_string())).to_sql_where();
        assert_eq!(sql, "\"col\" IS NOT NULL");
    }

    #[test]
    fn test_in_with_number_fallback() {
        // In with a Number value => fallback
        let sql = fc("col", FilterOp::In, FilterValue::Number(42.0)).to_sql_where();
        assert_eq!(sql, "\"col\" IS NOT NULL");
    }

    #[test]
    fn test_like_with_number_fallback() {
        let sql = fc("col", FilterOp::Like, FilterValue::Number(42.0)).to_sql_where();
        assert_eq!(sql, "\"col\" IS NOT NULL");
    }

    // --- build_where_clause tests ---
    #[test]
    fn test_build_where_clause_empty() {
        assert_eq!(build_where_clause(&[]), "");
    }

    #[test]
    fn test_build_where_clause_single() {
        let clause = build_where_clause(&[
            fc("status", FilterOp::Eq, FilterValue::String("active".to_string())),
        ]);
        assert_eq!(clause, " WHERE \"status\" = 'active'");
    }

    #[test]
    fn test_build_where_clause_multiple() {
        let clause = build_where_clause(&[
            fc("status", FilterOp::Eq, FilterValue::String("active".to_string())),
            fc("amount", FilterOp::Gte, FilterValue::Number(50.0)),
        ]);
        assert_eq!(clause, " WHERE \"status\" = 'active' AND \"amount\" >= 50");
    }

    #[test]
    fn test_build_where_clause_three_conditions() {
        let clause = build_where_clause(&[
            fc("a", FilterOp::Eq, FilterValue::Number(1.0)),
            fc("b", FilterOp::Gt, FilterValue::Number(2.0)),
            fc("c", FilterOp::Lt, FilterValue::Number(3.0)),
        ]);
        assert_eq!(clause, " WHERE \"a\" = 1 AND \"b\" > 2 AND \"c\" < 3");
    }

    // --- format_number tests ---
    #[test]
    fn test_format_number_integer() {
        assert_eq!(format_number(42.0), "42");
        assert_eq!(format_number(0.0), "0");
        assert_eq!(format_number(-10.0), "-10");
    }

    #[test]
    fn test_format_number_float() {
        assert_eq!(format_number(19.99), "19.99");
        assert_eq!(format_number(0.001), "0.001");
    }

    // --- Deserialization tests ---
    #[test]
    fn test_between_deserializes_as_range() {
        let json = r#"{"column":"date","op":"between","value":["2024-01-01","2024-01-31"]}"#;
        let condition: FilterCondition = serde_json::from_str(json).unwrap();
        match &condition.value {
            FilterValue::Range([from, to]) => {
                assert_eq!(from, "2024-01-01");
                assert_eq!(to, "2024-01-31");
            }
            other => panic!("Expected Range, got {:?}", other),
        }
    }

    #[test]
    fn test_deserialize_eq_string() {
        let json = r#"{"column":"status","op":"eq","value":"active"}"#;
        let cond: FilterCondition = serde_json::from_str(json).unwrap();
        assert!(matches!(cond.value, FilterValue::String(ref s) if s == "active"));
    }

    #[test]
    fn test_deserialize_eq_number() {
        let json = r#"{"column":"amount","op":"eq","value":42.5}"#;
        let cond: FilterCondition = serde_json::from_str(json).unwrap();
        assert!(matches!(cond.value, FilterValue::Number(n) if (n - 42.5).abs() < f64::EPSILON));
    }

    #[test]
    fn test_deserialize_in_array() {
        let json = r#"{"column":"cat","op":"in","value":["a","b","c"]}"#;
        let cond: FilterCondition = serde_json::from_str(json).unwrap();
        match &cond.value {
            FilterValue::StringArray(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("Expected StringArray"),
        }
    }

    // --- SQL injection tests ---
    #[test]
    fn test_sql_injection_string_value() {
        let sql = fc("status", FilterOp::Eq, FilterValue::String("'; DROP TABLE users; --".to_string())).to_sql_where();
        assert_eq!(sql, "\"status\" = '''; DROP TABLE users; --'");
        // The single quote is escaped (doubled), so the injection becomes a string literal
        assert!(sql.contains("'''"));
    }

    #[test]
    fn test_sql_injection_column_name() {
        let sql = fc("col\"; DROP TABLE users; --", FilterOp::Eq, FilterValue::String("test".to_string())).to_sql_where();
        assert!(sql.starts_with("\"col\"\"; DROP TABLE users; --\""));
    }

    #[test]
    fn test_sql_injection_in_between_values() {
        let sql = fc(
            "date",
            FilterOp::Between,
            FilterValue::Range(["2024'; DROP TABLE x; --".to_string(), "2024-12-31".to_string()]),
        ).to_sql_where();
        // Quotes should be escaped
        assert!(sql.contains("''"));
    }

    #[test]
    fn test_sql_injection_in_array() {
        let sql = fc(
            "cat",
            FilterOp::In,
            FilterValue::StringArray(vec!["val'; DROP TABLE x; --".to_string()]),
        ).to_sql_where();
        assert!(sql.contains("''"));
    }

    // --- Edge cases ---
    #[test]
    fn test_empty_string_value() {
        let sql = fc("col", FilterOp::Eq, FilterValue::String("".to_string())).to_sql_where();
        assert_eq!(sql, "\"col\" = ''");
    }

    #[test]
    fn test_empty_column_name() {
        let sql = fc("", FilterOp::Eq, FilterValue::String("val".to_string())).to_sql_where();
        assert_eq!(sql, "\"\" = 'val'");
    }

    #[test]
    fn test_negative_number() {
        let sql = fc("temp", FilterOp::Lt, FilterValue::Number(-10.0)).to_sql_where();
        assert_eq!(sql, "\"temp\" < -10");
    }

    #[test]
    fn test_very_large_number() {
        let sql = fc("big", FilterOp::Gt, FilterValue::Number(1_000_000.0)).to_sql_where();
        assert_eq!(sql, "\"big\" > 1000000");
    }
}
