# Priority 2: Adoption Improvements Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve user adoption by adding guided setup features including primary key confirmation, smart field name resolution, recipe schema validation, and row preview capabilities.

**Architecture:** Four independent features that extend existing source/recipe validation infrastructure. Primary key detection uses schema analysis. Field resolution uses normalization functions. Schema validation compares recipe fields against DataFusion table schemas. Row preview executes LIMIT queries on registered sources.

**Tech Stack:** Rust (backend), TypeScript/React (frontend), DataFusion (SQL engine), PostgreSQL (metadata)

---

## Task 1: Add Primary Key Detection to Schema Extractor

**Files:**
- Modify: `crates/kalla-ai/src/schema_extractor.rs`
- Test: `crates/kalla-ai/src/schema_extractor.rs` (inline tests)

**Step 1: Write the failing test for primary key detection**

Add to the `#[cfg(test)]` module:

```rust
#[tokio::test]
async fn test_detect_primary_key_single_column() {
    let ctx = SessionContext::new();
    ctx.register_csv("test_table", "testdata/invoices.csv", CsvReadOptions::new())
        .await
        .unwrap();

    let detected = detect_primary_key(&ctx, "test_table").await.unwrap();

    // invoice_id is unique and ends with "_id"
    assert!(detected.contains(&"invoice_id".to_string()));
}

#[tokio::test]
async fn test_detect_primary_key_heuristics() {
    let ctx = SessionContext::new();
    ctx.register_csv("test_table", "testdata/payments.csv", CsvReadOptions::new())
        .await
        .unwrap();

    let detected = detect_primary_key(&ctx, "test_table").await.unwrap();

    // payment_id matches heuristics
    assert!(detected.contains(&"payment_id".to_string()));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p kalla-ai test_detect_primary_key`
Expected: FAIL with "cannot find function `detect_primary_key`"

**Step 3: Write minimal implementation**

Add to `schema_extractor.rs`:

```rust
/// Detects likely primary key columns using heuristics:
/// 1. Column name ends with "_id" or "id"
/// 2. Column name is exactly "id"
/// 3. Column has unique values (checked via COUNT DISTINCT)
pub async fn detect_primary_key(
    ctx: &SessionContext,
    table_name: &str,
) -> Result<Vec<String>, DataFusionError> {
    let table = ctx.table(table_name).await?;
    let schema = table.schema();

    let mut candidates: Vec<String> = Vec::new();

    // Heuristic 1: Column names suggesting primary key
    for field in schema.fields() {
        let name = field.name().to_lowercase();
        if name == "id" || name.ends_with("_id") || name.ends_with("id") {
            candidates.push(field.name().to_string());
        }
    }

    // If no heuristic matches, check first column
    if candidates.is_empty() {
        if let Some(first_field) = schema.fields().first() {
            candidates.push(first_field.name().to_string());
        }
    }

    // Verify uniqueness for top candidate
    if let Some(candidate) = candidates.first() {
        let count_query = format!(
            "SELECT COUNT(*) as total, COUNT(DISTINCT \"{}\") as distinct_count FROM {}",
            candidate, table_name
        );
        let df = ctx.sql(&count_query).await?;
        let batches = df.collect().await?;

        if let Some(batch) = batches.first() {
            let total = batch.column(0).as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(0))
                .unwrap_or(0);
            let distinct = batch.column(1).as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(0))
                .unwrap_or(0);

            // If not unique, return empty (needs user confirmation)
            if total != distinct {
                return Ok(vec![]);
            }
        }
    }

    Ok(candidates)
}
```

Add import at top:
```rust
use datafusion::arrow::array::Int64Array;
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p kalla-ai test_detect_primary_key`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/kalla-ai/src/schema_extractor.rs
git commit -m "feat: add primary key detection with heuristics"
```

---

## Task 2: Add Primary Key Detection API Endpoint

**Files:**
- Modify: `kalla-server/src/main.rs`

**Step 1: Write the endpoint handler**

Add to `main.rs` after the `/api/sources` endpoint:

```rust
// GET /api/sources/:alias/primary-key
async fn get_source_primary_key(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
) -> Result<Json<PrimaryKeyResponse>, (StatusCode, String)> {
    let ctx = state.ctx.read().await;

    let detected = kalla_ai::schema_extractor::detect_primary_key(&ctx, &alias)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("Source not found or error: {}", e)))?;

    Ok(Json(PrimaryKeyResponse {
        alias: alias.clone(),
        detected_keys: detected,
        confidence: if detected.is_empty() { "low" } else { "high" }.to_string(),
    }))
}

#[derive(Serialize)]
struct PrimaryKeyResponse {
    alias: String,
    detected_keys: Vec<String>,
    confidence: String,
}
```

**Step 2: Add route to router**

Find the router definition and add:

```rust
.route("/api/sources/:alias/primary-key", get(get_source_primary_key))
```

**Step 3: Run server and test endpoint**

Run: `cargo run -p kalla-server`
Test: `curl http://localhost:3001/api/sources/invoices/primary-key`
Expected: `{"alias":"invoices","detected_keys":["invoice_id"],"confidence":"high"}`

**Step 4: Commit**

```bash
git add kalla-server/src/main.rs
git commit -m "feat: add API endpoint for primary key detection"
```

---

## Task 3: Add Primary Key Confirmation UI Component

**Files:**
- Create: `kalla-web/src/components/PrimaryKeyConfirmation.tsx`
- Modify: `kalla-web/src/app/reconcile/page.tsx`

**Step 1: Create the confirmation component**

```typescript
'use client';

import { useState, useEffect } from 'react';

interface PrimaryKeyConfirmationProps {
  sourceAlias: string;
  onConfirm: (keys: string[]) => void;
  onCancel: () => void;
}

interface PrimaryKeyResponse {
  alias: string;
  detected_keys: string[];
  confidence: string;
}

export function PrimaryKeyConfirmation({
  sourceAlias,
  onConfirm,
  onCancel,
}: PrimaryKeyConfirmationProps) {
  const [detectedKeys, setDetectedKeys] = useState<string[]>([]);
  const [selectedKeys, setSelectedKeys] = useState<string[]>([]);
  const [customKey, setCustomKey] = useState('');
  const [loading, setLoading] = useState(true);
  const [confidence, setConfidence] = useState('');

  useEffect(() => {
    async function fetchPrimaryKey() {
      try {
        const res = await fetch(`/api/sources/${sourceAlias}/primary-key`);
        if (res.ok) {
          const data: PrimaryKeyResponse = await res.json();
          setDetectedKeys(data.detected_keys);
          setSelectedKeys(data.detected_keys);
          setConfidence(data.confidence);
        }
      } catch (error) {
        console.error('Failed to detect primary key:', error);
      } finally {
        setLoading(false);
      }
    }
    fetchPrimaryKey();
  }, [sourceAlias]);

  const handleConfirm = () => {
    const keys = customKey ? [customKey] : selectedKeys;
    onConfirm(keys);
  };

  const toggleKey = (key: string) => {
    setSelectedKeys((prev) =>
      prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]
    );
    setCustomKey('');
  };

  if (loading) {
    return <div className="p-4">Detecting primary key for {sourceAlias}...</div>;
  }

  return (
    <div className="border rounded-lg p-4 bg-gray-50">
      <h3 className="font-semibold mb-2">
        Confirm Primary Key for "{sourceAlias}"
      </h3>

      {confidence === 'high' ? (
        <p className="text-sm text-gray-600 mb-3">
          Detected primary key with high confidence:
        </p>
      ) : (
        <p className="text-sm text-yellow-600 mb-3">
          Could not auto-detect primary key. Please specify:
        </p>
      )}

      <div className="space-y-2 mb-4">
        {detectedKeys.map((key) => (
          <label key={key} className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={selectedKeys.includes(key)}
              onChange={() => toggleKey(key)}
              className="rounded"
            />
            <span className="font-mono text-sm">{key}</span>
          </label>
        ))}
      </div>

      <div className="mb-4">
        <label className="block text-sm text-gray-600 mb-1">
          Or enter custom column name:
        </label>
        <input
          type="text"
          value={customKey}
          onChange={(e) => {
            setCustomKey(e.target.value);
            setSelectedKeys([]);
          }}
          placeholder="column_name"
          className="border rounded px-2 py-1 w-full font-mono text-sm"
        />
      </div>

      <div className="flex gap-2">
        <button
          onClick={handleConfirm}
          disabled={selectedKeys.length === 0 && !customKey}
          className="px-4 py-2 bg-blue-600 text-white rounded disabled:opacity-50"
        >
          Confirm
        </button>
        <button
          onClick={onCancel}
          className="px-4 py-2 border rounded"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}
```

**Step 2: Run to verify component compiles**

Run: `cd kalla-web && npm run build`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add kalla-web/src/components/PrimaryKeyConfirmation.tsx
git commit -m "feat: add primary key confirmation UI component"
```

---

## Task 4: Add Field Name Normalization Module

**Files:**
- Create: `crates/kalla-recipe/src/field_resolver.rs`
- Modify: `crates/kalla-recipe/src/lib.rs`

**Step 1: Write the failing tests**

Create new file with tests first:

```rust
//! Smart field name resolution - auto-resolves common variations

/// Normalizes a field name by:
/// - Converting to lowercase
/// - Replacing dashes with underscores
/// - Removing leading/trailing whitespace
pub fn normalize_field_name(name: &str) -> String {
    name.trim()
        .to_lowercase()
        .replace('-', "_")
        .replace(' ', "_")
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
    conditions: &[(String, String)],  // (left_field, right_field)
    left_fields: &[String],
    right_fields: &[String],
) -> Result<Vec<(String, String, String, String)>, Vec<String>> {
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
        let conditions = vec![
            ("invoice-id".to_string(), "Payment_Ref".to_string()),
        ];
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
        let conditions = vec![
            ("nonexistent".to_string(), "amount".to_string()),
        ];
        let left = vec!["invoice_id".to_string()];
        let right = vec!["amount".to_string()];

        let result = resolve_recipe_fields(&conditions, &left, &right);
        assert!(result.is_err());
        assert!(result.unwrap_err()[0].contains("not found"));
    }
}
```

**Step 2: Run tests to verify they pass**

Run: `cargo test -p kalla-recipe field_resolver`
Expected: PASS (tests are with implementation)

**Step 3: Add module to lib.rs**

Add to `crates/kalla-recipe/src/lib.rs`:

```rust
pub mod field_resolver;
```

**Step 4: Commit**

```bash
git add crates/kalla-recipe/src/field_resolver.rs crates/kalla-recipe/src/lib.rs
git commit -m "feat: add smart field name resolution with normalization"
```

---

## Task 5: Add Schema Validation Against Source

**Files:**
- Create: `crates/kalla-recipe/src/schema_validation.rs`
- Modify: `crates/kalla-recipe/src/lib.rs`

**Step 1: Write the failing test**

```rust
//! Recipe schema validation - validates recipe fields against actual source schemas

use crate::field_resolver::{normalize_field_name, resolve_field_name};
use crate::schema::MatchRecipe;

#[derive(Debug, Clone)]
pub struct SchemaValidationError {
    pub rule_name: String,
    pub field: String,
    pub source: String,  // "left" or "right"
    pub message: String,
    pub suggestion: Option<String>,
}

#[derive(Debug)]
pub struct SchemaValidationResult {
    pub valid: bool,
    pub errors: Vec<SchemaValidationError>,
    pub warnings: Vec<String>,
    pub resolved_fields: Vec<(String, String)>,  // (original, resolved)
}

/// Validates a recipe's match conditions against actual source schemas
pub fn validate_recipe_against_schema(
    recipe: &MatchRecipe,
    left_fields: &[String],
    right_fields: &[String],
) -> SchemaValidationResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut resolved_fields = Vec::new();

    for rule in &recipe.match_rules {
        for condition in &rule.conditions {
            // Validate left field
            match resolve_field_name(&condition.left, left_fields) {
                Some(resolved) => {
                    if resolved != condition.left {
                        warnings.push(format!(
                            "Field '{}' resolved to '{}' in left source",
                            condition.left, resolved
                        ));
                        resolved_fields.push((condition.left.clone(), resolved));
                    }
                }
                None => {
                    let suggestion = find_closest_match(&condition.left, left_fields);
                    errors.push(SchemaValidationError {
                        rule_name: rule.name.clone(),
                        field: condition.left.clone(),
                        source: "left".to_string(),
                        message: format!(
                            "Field '{}' not found in left source '{}'",
                            condition.left,
                            recipe.sources.left.alias
                        ),
                        suggestion,
                    });
                }
            }

            // Validate right field
            match resolve_field_name(&condition.right, right_fields) {
                Some(resolved) => {
                    if resolved != condition.right {
                        warnings.push(format!(
                            "Field '{}' resolved to '{}' in right source",
                            condition.right, resolved
                        ));
                        resolved_fields.push((condition.right.clone(), resolved));
                    }
                }
                None => {
                    let suggestion = find_closest_match(&condition.right, right_fields);
                    errors.push(SchemaValidationError {
                        rule_name: rule.name.clone(),
                        field: condition.right.clone(),
                        source: "right".to_string(),
                        message: format!(
                            "Field '{}' not found in right source '{}'",
                            condition.right,
                            recipe.sources.right.alias
                        ),
                        suggestion,
                    });
                }
            }
        }
    }

    SchemaValidationResult {
        valid: errors.is_empty(),
        errors,
        warnings,
        resolved_fields,
    }
}

/// Finds the closest matching field name using simple heuristics
fn find_closest_match(field: &str, available: &[String]) -> Option<String> {
    let normalized = normalize_field_name(field);

    // Check if any field contains the normalized name or vice versa
    for available_field in available {
        let norm_available = normalize_field_name(available_field);
        if norm_available.contains(&normalized) || normalized.contains(&norm_available) {
            return Some(available_field.clone());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        DataSource, MatchCondition, MatchRule, MatchRecipe, OutputConfig, Sources,
        ComparisonOp,
    };

    fn test_recipe() -> MatchRecipe {
        MatchRecipe {
            version: "1.0".to_string(),
            recipe_id: "test".to_string(),
            sources: Sources {
                left: DataSource {
                    alias: "invoices".to_string(),
                    uri: "file://test.csv".to_string(),
                    primary_key: None,
                },
                right: DataSource {
                    alias: "payments".to_string(),
                    uri: "file://test2.csv".to_string(),
                    primary_key: None,
                },
            },
            match_rules: vec![MatchRule {
                name: "test_rule".to_string(),
                pattern: "1:1".to_string(),
                conditions: vec![MatchCondition {
                    left: "invoice_id".to_string(),
                    op: ComparisonOp::Eq,
                    right: "payment_ref".to_string(),
                    threshold: None,
                }],
                priority: Some(1),
            }],
            output: OutputConfig {
                matched: "matched.parquet".to_string(),
                unmatched_left: "unmatched_left.parquet".to_string(),
                unmatched_right: "unmatched_right.parquet".to_string(),
            },
        }
    }

    #[test]
    fn test_validate_valid_recipe() {
        let recipe = test_recipe();
        let left_fields = vec!["invoice_id".to_string(), "amount".to_string()];
        let right_fields = vec!["payment_ref".to_string(), "paid_amount".to_string()];

        let result = validate_recipe_against_schema(&recipe, &left_fields, &right_fields);

        assert!(result.valid);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_validate_missing_left_field() {
        let recipe = test_recipe();
        let left_fields = vec!["wrong_field".to_string()];
        let right_fields = vec!["payment_ref".to_string()];

        let result = validate_recipe_against_schema(&recipe, &left_fields, &right_fields);

        assert!(!result.valid);
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].source, "left");
        assert!(result.errors[0].message.contains("invoice_id"));
    }

    #[test]
    fn test_validate_with_case_difference() {
        let mut recipe = test_recipe();
        recipe.match_rules[0].conditions[0].left = "Invoice_ID".to_string();

        let left_fields = vec!["invoice_id".to_string()];
        let right_fields = vec!["payment_ref".to_string()];

        let result = validate_recipe_against_schema(&recipe, &left_fields, &right_fields);

        assert!(result.valid);
        assert!(!result.warnings.is_empty());
        assert!(result.warnings[0].contains("resolved"));
    }

    #[test]
    fn test_find_closest_match() {
        let fields = vec!["invoice_id".to_string(), "customer_name".to_string()];

        assert_eq!(
            find_closest_match("invoice", &fields),
            Some("invoice_id".to_string())
        );
        assert_eq!(find_closest_match("xyz", &fields), None);
    }
}
```

**Step 2: Run tests**

Run: `cargo test -p kalla-recipe schema_validation`
Expected: PASS

**Step 3: Add module to lib.rs**

```rust
pub mod schema_validation;
```

**Step 4: Commit**

```bash
git add crates/kalla-recipe/src/schema_validation.rs crates/kalla-recipe/src/lib.rs
git commit -m "feat: add recipe schema validation against source schemas"
```

---

## Task 6: Add Schema Validation API Endpoint

**Files:**
- Modify: `kalla-server/src/main.rs`

**Step 1: Add the endpoint handler**

```rust
// POST /api/recipes/validate-schema
async fn validate_recipe_schema(
    State(state): State<Arc<AppState>>,
    Json(recipe): Json<MatchRecipe>,
) -> Result<Json<SchemaValidationResponse>, (StatusCode, String)> {
    let ctx = state.ctx.read().await;

    // Get left source schema
    let left_table = ctx.table(&recipe.sources.left.alias).await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Left source not found: {}", e)))?;
    let left_fields: Vec<String> = left_table.schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    // Get right source schema
    let right_table = ctx.table(&recipe.sources.right.alias).await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Right source not found: {}", e)))?;
    let right_fields: Vec<String> = right_table.schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    let result = kalla_recipe::schema_validation::validate_recipe_against_schema(
        &recipe,
        &left_fields,
        &right_fields,
    );

    Ok(Json(SchemaValidationResponse {
        valid: result.valid,
        errors: result.errors.iter().map(|e| SchemaError {
            rule_name: e.rule_name.clone(),
            field: e.field.clone(),
            source: e.source.clone(),
            message: e.message.clone(),
            suggestion: e.suggestion.clone(),
        }).collect(),
        warnings: result.warnings,
        resolved_fields: result.resolved_fields,
    }))
}

#[derive(Serialize)]
struct SchemaValidationResponse {
    valid: bool,
    errors: Vec<SchemaError>,
    warnings: Vec<String>,
    resolved_fields: Vec<(String, String)>,
}

#[derive(Serialize)]
struct SchemaError {
    rule_name: String,
    field: String,
    source: String,
    message: String,
    suggestion: Option<String>,
}
```

**Step 2: Add route**

```rust
.route("/api/recipes/validate-schema", post(validate_recipe_schema))
```

**Step 3: Test endpoint**

Run: `cargo run -p kalla-server`
Test with curl:
```bash
curl -X POST http://localhost:3001/api/recipes/validate-schema \
  -H "Content-Type: application/json" \
  -d @testdata/recipe.json
```
Expected: `{"valid":true,"errors":[],"warnings":[],"resolved_fields":[]}`

**Step 4: Commit**

```bash
git add kalla-server/src/main.rs
git commit -m "feat: add schema validation API endpoint"
```

---

## Task 7: Add Row Preview Endpoint

**Files:**
- Modify: `kalla-server/src/main.rs`

**Step 1: Add the endpoint handler**

```rust
// GET /api/sources/:alias/preview?limit=10
async fn get_source_preview(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
    Query(params): Query<PreviewParams>,
) -> Result<Json<SourcePreviewResponse>, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(10).min(100);  // Max 100 rows
    let ctx = state.ctx.read().await;

    // Get schema
    let table = ctx.table(&alias).await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("Source not found: {}", e)))?;
    let schema = table.schema();

    let columns: Vec<ColumnInfo> = schema.fields()
        .iter()
        .map(|f| ColumnInfo {
            name: f.name().to_string(),
            data_type: format!("{:?}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect();

    // Get sample rows
    let query = format!("SELECT * FROM \"{}\" LIMIT {}", alias, limit);
    let df = ctx.sql(&query).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Query failed: {}", e)))?;

    let batches = df.collect().await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Collect failed: {}", e)))?;

    // Convert to JSON-friendly format
    let mut rows: Vec<Vec<String>> = Vec::new();
    for batch in &batches {
        for row_idx in 0..batch.num_rows() {
            let mut row: Vec<String> = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let value = arrow_value_to_string(col, row_idx);
                row.push(value);
            }
            rows.push(row);
        }
    }

    // Get total count
    let count_query = format!("SELECT COUNT(*) FROM \"{}\"", alias);
    let count_df = ctx.sql(&count_query).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let count_batches = count_df.collect().await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let total_rows = count_batches.first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(0) as u64)
        .unwrap_or(0);

    Ok(Json(SourcePreviewResponse {
        alias,
        columns,
        rows,
        total_rows,
        preview_rows: rows.len(),
    }))
}

fn arrow_value_to_string(array: &ArrayRef, idx: usize) -> String {
    use datafusion::arrow::array::*;

    if array.is_null(idx) {
        return "null".to_string();
    }

    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        return arr.value(idx).to_string();
    }

    // Fallback for other types
    format!("{:?}", array.slice(idx, 1))
}

#[derive(Deserialize)]
struct PreviewParams {
    limit: Option<usize>,
}

#[derive(Serialize)]
struct SourcePreviewResponse {
    alias: String,
    columns: Vec<ColumnInfo>,
    rows: Vec<Vec<String>>,
    total_rows: u64,
    preview_rows: usize,
}

#[derive(Serialize)]
struct ColumnInfo {
    name: String,
    data_type: String,
    nullable: bool,
}
```

**Step 2: Add route**

```rust
.route("/api/sources/:alias/preview", get(get_source_preview))
```

**Step 3: Add imports**

```rust
use datafusion::arrow::array::{ArrayRef, StringArray, Int32Array, Float64Array, BooleanArray};
```

**Step 4: Test endpoint**

Run: `cargo run -p kalla-server`
Test: `curl "http://localhost:3001/api/sources/invoices/preview?limit=5"`
Expected: JSON with columns array and 5 sample rows

**Step 5: Commit**

```bash
git add kalla-server/src/main.rs
git commit -m "feat: add row preview API endpoint for data sources"
```

---

## Task 8: Add Row Preview UI Component

**Files:**
- Create: `kalla-web/src/components/SourcePreview.tsx`

**Step 1: Create the component**

```typescript
'use client';

import { useState, useEffect } from 'react';

interface ColumnInfo {
  name: string;
  data_type: string;
  nullable: boolean;
}

interface SourcePreviewResponse {
  alias: string;
  columns: ColumnInfo[];
  rows: string[][];
  total_rows: number;
  preview_rows: number;
}

interface SourcePreviewProps {
  sourceAlias: string;
  limit?: number;
}

export function SourcePreview({ sourceAlias, limit = 10 }: SourcePreviewProps) {
  const [preview, setPreview] = useState<SourcePreviewResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchPreview() {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(
          `/api/sources/${sourceAlias}/preview?limit=${limit}`
        );
        if (!res.ok) {
          throw new Error(await res.text());
        }
        const data: SourcePreviewResponse = await res.json();
        setPreview(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load preview');
      } finally {
        setLoading(false);
      }
    }
    fetchPreview();
  }, [sourceAlias, limit]);

  if (loading) {
    return (
      <div className="p-4 text-gray-500">Loading preview for {sourceAlias}...</div>
    );
  }

  if (error) {
    return (
      <div className="p-4 text-red-600">Error: {error}</div>
    );
  }

  if (!preview) {
    return null;
  }

  return (
    <div className="border rounded-lg overflow-hidden">
      <div className="bg-gray-100 px-4 py-2 flex justify-between items-center">
        <h3 className="font-semibold">{preview.alias}</h3>
        <span className="text-sm text-gray-600">
          Showing {preview.preview_rows} of {preview.total_rows.toLocaleString()} rows
        </span>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              {preview.columns.map((col) => (
                <th
                  key={col.name}
                  className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                  title={`Type: ${col.data_type}${col.nullable ? ' (nullable)' : ''}`}
                >
                  {col.name}
                  <span className="block font-normal normal-case text-gray-400">
                    {col.data_type}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {preview.rows.map((row, rowIdx) => (
              <tr key={rowIdx} className="hover:bg-gray-50">
                {row.map((cell, cellIdx) => (
                  <td
                    key={cellIdx}
                    className="px-4 py-2 text-sm text-gray-900 font-mono whitespace-nowrap"
                  >
                    {cell === 'null' ? (
                      <span className="text-gray-400 italic">null</span>
                    ) : (
                      cell
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
```

**Step 2: Verify component compiles**

Run: `cd kalla-web && npm run build`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add kalla-web/src/components/SourcePreview.tsx
git commit -m "feat: add row preview UI component"
```

---

## Task 9: Integrate Components into Sources Page

**Files:**
- Modify: `kalla-web/src/app/sources/page.tsx`

**Step 1: Read current sources page**

Read the file to understand current structure before modifying.

**Step 2: Add imports and state**

Add at top of file:
```typescript
import { SourcePreview } from '@/components/SourcePreview';
import { PrimaryKeyConfirmation } from '@/components/PrimaryKeyConfirmation';
```

Add state:
```typescript
const [previewSource, setPreviewSource] = useState<string | null>(null);
const [pkConfirmSource, setPkConfirmSource] = useState<string | null>(null);
```

**Step 3: Add preview toggle button to each source row**

In the source list mapping, add buttons:
```typescript
<button
  onClick={() => setPreviewSource(previewSource === source.alias ? null : source.alias)}
  className="text-blue-600 hover:underline text-sm"
>
  {previewSource === source.alias ? 'Hide Preview' : 'Preview'}
</button>
<button
  onClick={() => setPkConfirmSource(source.alias)}
  className="text-blue-600 hover:underline text-sm ml-2"
>
  Check PK
</button>
```

**Step 4: Add conditional preview and PK components**

After each source row:
```typescript
{previewSource === source.alias && (
  <div className="mt-2">
    <SourcePreview sourceAlias={source.alias} limit={10} />
  </div>
)}

{pkConfirmSource === source.alias && (
  <div className="mt-2">
    <PrimaryKeyConfirmation
      sourceAlias={source.alias}
      onConfirm={(keys) => {
        console.log('Confirmed PK:', keys);
        setPkConfirmSource(null);
      }}
      onCancel={() => setPkConfirmSource(null)}
    />
  </div>
)}
```

**Step 5: Test the integration**

Run: `cd kalla-web && npm run dev`
Navigate to: `http://localhost:3000/sources`
Expected: Preview and Check PK buttons visible, clicking shows components

**Step 6: Commit**

```bash
git add kalla-web/src/app/sources/page.tsx
git commit -m "feat: integrate preview and primary key components into sources page"
```

---

## Task 10: Integrate Schema Validation into Reconcile Flow

**Files:**
- Modify: `kalla-web/src/app/reconcile/page.tsx`
- Modify: `kalla-web/src/lib/api.ts`

**Step 1: Add API function**

Add to `api.ts`:
```typescript
export interface SchemaValidationResult {
  valid: boolean;
  errors: Array<{
    rule_name: string;
    field: string;
    source: string;
    message: string;
    suggestion: string | null;
  }>;
  warnings: string[];
  resolved_fields: Array<[string, string]>;
}

export async function validateRecipeSchema(
  recipe: RecipeConfig
): Promise<SchemaValidationResult> {
  const res = await fetch('/api/recipes/validate-schema', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(recipe),
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}
```

**Step 2: Add schema validation call in reconcile page**

In the Review step, before allowing to proceed to Run:
```typescript
const [schemaValidation, setSchemaValidation] = useState<SchemaValidationResult | null>(null);
const [validatingSchema, setValidatingSchema] = useState(false);

async function validateSchema() {
  setValidatingSchema(true);
  try {
    const result = await validateRecipeSchema(recipe);
    setSchemaValidation(result);
  } catch (error) {
    setSchemaValidation({
      valid: false,
      errors: [{ rule_name: '', field: '', source: '', message: String(error), suggestion: null }],
      warnings: [],
      resolved_fields: [],
    });
  } finally {
    setValidatingSchema(false);
  }
}
```

**Step 3: Display validation results**

```typescript
{schemaValidation && !schemaValidation.valid && (
  <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded">
    <h4 className="font-semibold text-red-800">Schema Validation Errors</h4>
    <ul className="mt-2 space-y-1">
      {schemaValidation.errors.map((err, idx) => (
        <li key={idx} className="text-sm text-red-700">
          <strong>{err.field}</strong> in {err.source} source: {err.message}
          {err.suggestion && (
            <span className="text-gray-600"> (Did you mean: {err.suggestion}?)</span>
          )}
        </li>
      ))}
    </ul>
  </div>
)}

{schemaValidation && schemaValidation.warnings.length > 0 && (
  <div className="mt-4 p-4 bg-yellow-50 border border-yellow-200 rounded">
    <h4 className="font-semibold text-yellow-800">Warnings</h4>
    <ul className="mt-2 space-y-1">
      {schemaValidation.warnings.map((warn, idx) => (
        <li key={idx} className="text-sm text-yellow-700">{warn}</li>
      ))}
    </ul>
  </div>
)}
```

**Step 4: Block proceed if validation fails**

```typescript
<button
  onClick={runReconciliation}
  disabled={!schemaValidation?.valid}
  className="px-4 py-2 bg-blue-600 text-white rounded disabled:opacity-50"
>
  Run Reconciliation
</button>
```

**Step 5: Verify build**

Run: `cd kalla-web && npm run build`
Expected: Build succeeds

**Step 6: Commit**

```bash
git add kalla-web/src/app/reconcile/page.tsx kalla-web/src/lib/api.ts
git commit -m "feat: integrate schema validation into reconciliation flow"
```

---

## Task 11: Add Integration Tests

**Files:**
- Create: `tests/integration/priority2_features.rs`

**Step 1: Create integration test file**

```rust
//! Integration tests for Priority 2 adoption features

use kalla_recipe::field_resolver::{normalize_field_name, resolve_field_name, resolve_recipe_fields};
use kalla_recipe::schema_validation::validate_recipe_against_schema;

#[test]
fn test_field_resolution_end_to_end() {
    // Simulating recipe with various naming conventions
    let conditions = vec![
        ("Invoice-ID".to_string(), "payment_ref".to_string()),
        ("AMOUNT".to_string(), "Paid_Amount".to_string()),
    ];

    let left_fields = vec![
        "invoice_id".to_string(),
        "amount".to_string(),
        "customer_name".to_string(),
    ];

    let right_fields = vec![
        "payment_ref".to_string(),
        "paid_amount".to_string(),
        "transaction_date".to_string(),
    ];

    let result = resolve_recipe_fields(&conditions, &left_fields, &right_fields);
    assert!(result.is_ok());

    let resolved = result.unwrap();
    assert_eq!(resolved.len(), 2);
    assert_eq!(resolved[0].1, "invoice_id");  // Invoice-ID -> invoice_id
    assert_eq!(resolved[1].1, "amount");      // AMOUNT -> amount
}

#[test]
fn test_schema_validation_with_suggestions() {
    // Test that validation provides helpful suggestions
    use kalla_recipe::schema::{
        MatchRecipe, Sources, DataSource, MatchRule, MatchCondition,
        OutputConfig, ComparisonOp,
    };

    let recipe = MatchRecipe {
        version: "1.0".to_string(),
        recipe_id: "test".to_string(),
        sources: Sources {
            left: DataSource {
                alias: "invoices".to_string(),
                uri: "file://test.csv".to_string(),
                primary_key: None,
            },
            right: DataSource {
                alias: "payments".to_string(),
                uri: "file://test2.csv".to_string(),
                primary_key: None,
            },
        },
        match_rules: vec![MatchRule {
            name: "test_rule".to_string(),
            pattern: "1:1".to_string(),
            conditions: vec![MatchCondition {
                left: "invoice".to_string(),  // Typo - missing "_id"
                op: ComparisonOp::Eq,
                right: "payment_ref".to_string(),
                threshold: None,
            }],
            priority: Some(1),
        }],
        output: OutputConfig {
            matched: "matched.parquet".to_string(),
            unmatched_left: "unmatched_left.parquet".to_string(),
            unmatched_right: "unmatched_right.parquet".to_string(),
        },
    };

    let left_fields = vec!["invoice_id".to_string(), "amount".to_string()];
    let right_fields = vec!["payment_ref".to_string()];

    let result = validate_recipe_against_schema(&recipe, &left_fields, &right_fields);

    assert!(!result.valid);
    assert_eq!(result.errors.len(), 1);
    // Should suggest "invoice_id" since "invoice" is a substring
    assert_eq!(result.errors[0].suggestion, Some("invoice_id".to_string()));
}
```

**Step 2: Run integration tests**

Run: `cargo test --test priority2_features`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/integration/priority2_features.rs
git commit -m "test: add integration tests for priority 2 features"
```

---

## Task 12: Update TODO.md

**Files:**
- Modify: `TODO.md`

**Step 1: Mark items as complete**

Change Priority 2 section:
```markdown
## Priority 2: Improves Adoption

### Guided Setup
- [x] **Primary key confirmation** - Prompt user to confirm detected primary key before matching
- [x] **Smart field name resolution** - Auto-resolve common variations (underscores, dashes, casing)
- [x] **Recipe schema validation** - Validate match rules against source schema before running

### Source Preview
- [x] **Row preview from source** - Show sample rows when exploring a data source
```

**Step 2: Commit**

```bash
git add TODO.md
git commit -m "docs: mark Priority 2 features as complete"
```

---

## Summary

| Task | Description | Est. Complexity |
|------|-------------|-----------------|
| 1 | Primary key detection in schema extractor | Medium |
| 2 | Primary key detection API endpoint | Low |
| 3 | Primary key confirmation UI component | Medium |
| 4 | Field name normalization module | Low |
| 5 | Schema validation against source | Medium |
| 6 | Schema validation API endpoint | Low |
| 7 | Row preview API endpoint | Medium |
| 8 | Row preview UI component | Medium |
| 9 | Integrate into sources page | Low |
| 10 | Integrate schema validation into reconcile | Medium |
| 11 | Integration tests | Low |
| 12 | Update TODO.md | Trivial |

**Total: 12 tasks across 4 features**
