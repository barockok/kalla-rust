//! Smart field name resolution - auto-resolves common variations

/// (orig_left, resolved_left, orig_right, resolved_right)
pub type ResolvedFieldPair = (String, String, String, String);

/// Normalizes a field name by:
/// - Converting to lowercase
/// - Replacing dashes with underscores
/// - Removing leading/trailing whitespace
pub fn normalize_field_name(name: &str) -> String {
    name.trim()
        .to_lowercase()
        .replace(['-', ' '], "_")
}

/// Attempts to find a matching field in the schema using fuzzy matching
pub fn resolve_field_name(field: &str, available_fields: &[String]) -> Option<String> {
    let normalized = normalize_field_name(field);

    // Exact match after normalization
    for available in available_fields {
        if normalize_field_name(available) == normalized {
            return Some(available.clone());
        }
    }

    None
}

/// Resolves all condition fields against available schema fields
/// Returns a map of original -> resolved field names
pub fn resolve_recipe_fields(
    conditions: &[(String, String)], // (left_field, right_field)
    left_fields: &[String],
    right_fields: &[String],
) -> Result<Vec<ResolvedFieldPair>, Vec<String>> {
    // Returns: (orig_left, resolved_left, orig_right, resolved_right)
    let mut resolved = Vec::new();
    let mut errors = Vec::new();

    for (left, right) in conditions {
        let resolved_left = resolve_field_name(left, left_fields);
        let resolved_right = resolve_field_name(right, right_fields);

        match (resolved_left, resolved_right) {
            (Some(l), Some(r)) => {
                resolved.push((left.clone(), l, right.clone(), r));
            }
            (None, Some(_)) => {
                errors.push(format!("Left field '{}' not found in source", left));
            }
            (Some(_), None) => {
                errors.push(format!("Right field '{}' not found in source", right));
            }
            (None, None) => {
                errors.push(format!("Fields '{}' and '{}' not found", left, right));
            }
        }
    }

    if errors.is_empty() {
        Ok(resolved)
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_field_name_lowercase() {
        assert_eq!(normalize_field_name("InvoiceID"), "invoiceid");
        assert_eq!(normalize_field_name("AMOUNT"), "amount");
    }

    #[test]
    fn test_normalize_field_name_dashes() {
        assert_eq!(normalize_field_name("invoice-id"), "invoice_id");
        assert_eq!(normalize_field_name("customer-name"), "customer_name");
    }

    #[test]
    fn test_normalize_field_name_spaces() {
        assert_eq!(normalize_field_name("invoice id"), "invoice_id");
        assert_eq!(normalize_field_name(" amount "), "amount");
    }

    #[test]
    fn test_resolve_field_name_exact() {
        let fields = vec!["invoice_id".to_string(), "amount".to_string()];
        assert_eq!(
            resolve_field_name("invoice_id", &fields),
            Some("invoice_id".to_string())
        );
    }

    #[test]
    fn test_resolve_field_name_case_insensitive() {
        let fields = vec!["InvoiceID".to_string(), "Amount".to_string()];
        assert_eq!(
            resolve_field_name("invoiceid", &fields),
            Some("InvoiceID".to_string())
        );
    }

    #[test]
    fn test_resolve_field_name_dash_underscore() {
        let fields = vec!["invoice_id".to_string()];
        assert_eq!(
            resolve_field_name("invoice-id", &fields),
            Some("invoice_id".to_string())
        );
    }

    #[test]
    fn test_resolve_recipe_fields_success() {
        let conditions = vec![("invoice-id".to_string(), "Payment_Ref".to_string())];
        let left = vec!["invoice_id".to_string(), "amount".to_string()];
        let right = vec!["payment_ref".to_string(), "paid_amount".to_string()];

        let result = resolve_recipe_fields(&conditions, &left, &right);
        assert!(result.is_ok());

        let resolved = result.unwrap();
        assert_eq!(resolved[0].1, "invoice_id");
        assert_eq!(resolved[0].3, "payment_ref");
    }

    #[test]
    fn test_resolve_recipe_fields_missing() {
        let conditions = vec![("nonexistent".to_string(), "amount".to_string())];
        let left = vec!["invoice_id".to_string()];
        let right = vec!["amount".to_string()];

        let result = resolve_recipe_fields(&conditions, &left, &right);
        assert!(result.is_err());
        assert!(result.unwrap_err()[0].contains("not found"));
    }
}
