//! Integration tests for API edge cases and failure modes.
//!
//! These tests require a running server + database.
//! Run `docker compose up -d` before running these tests.

use serde::{Deserialize, Serialize};
use std::time::Duration;

const API_URL: &str = "http://localhost:3001";

fn postgres_host() -> String {
    std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string())
}

// --- Request / Response types ---

#[derive(Debug, Serialize, Deserialize)]
struct RecipeConfig {
    version: String,
    recipe_id: String,
    sources: Sources,
    match_rules: Vec<MatchRule>,
    output: OutputConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct Sources {
    left: DataSource,
    right: DataSource,
}

#[derive(Debug, Serialize, Deserialize)]
struct DataSource {
    alias: String,
    uri: String,
    primary_key: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MatchRule {
    name: String,
    pattern: String,
    conditions: Vec<MatchCondition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MatchCondition {
    left: String,
    op: String,
    right: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    threshold: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OutputConfig {
    matched: String,
    unmatched_left: String,
    unmatched_right: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateRunRequest {
    recipe: RecipeConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateRunResponse {
    run_id: String,
    status: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RegisterSourceRequest {
    alias: String,
    uri: String,
}

async fn ensure_server() -> Option<reqwest::Client> {
    let client = reqwest::Client::new();
    match client.get(format!("{}/health", API_URL)).send().await {
        Ok(_) => Some(client),
        Err(_) => {
            println!("Skipping test: Server not running at {}", API_URL);
            None
        }
    }
}

// ===========================================================================
// Recipe validation edge cases
// ===========================================================================

#[tokio::test]
async fn test_create_run_invalid_recipe_empty_rules() {
    let Some(client) = ensure_server().await else { return };

    let pg_host = postgres_host();
    let recipe = RecipeConfig {
        version: "1.0".to_string(),
        recipe_id: "test-empty-rules".to_string(),
        sources: Sources {
            left: DataSource {
                alias: "invoices".to_string(),
                uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=invoices", pg_host),
                primary_key: vec!["invoice_id".to_string()],
            },
            right: DataSource {
                alias: "payments".to_string(),
                uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=payments", pg_host),
                primary_key: vec!["payment_id".to_string()],
            },
        },
        match_rules: vec![], // No rules — should be rejected
        output: OutputConfig {
            matched: "evidence/matched.parquet".to_string(),
            unmatched_left: "evidence/unmatched_left.parquet".to_string(),
            unmatched_right: "evidence/unmatched_right.parquet".to_string(),
        },
    };

    let response = client
        .post(format!("{}/api/runs", API_URL))
        .json(&CreateRunRequest { recipe })
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status(), 400, "Empty match rules should return 400");
}

#[tokio::test]
async fn test_create_run_invalid_version() {
    let Some(client) = ensure_server().await else { return };

    let pg_host = postgres_host();
    let recipe = RecipeConfig {
        version: "99.0".to_string(), // Invalid version
        recipe_id: "test-bad-version".to_string(),
        sources: Sources {
            left: DataSource {
                alias: "invoices".to_string(),
                uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=invoices", pg_host),
                primary_key: vec!["invoice_id".to_string()],
            },
            right: DataSource {
                alias: "payments".to_string(),
                uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=payments", pg_host),
                primary_key: vec!["payment_id".to_string()],
            },
        },
        match_rules: vec![MatchRule {
            name: "rule1".to_string(),
            pattern: "1:1".to_string(),
            conditions: vec![MatchCondition {
                left: "invoice_id".to_string(),
                op: "eq".to_string(),
                right: "reference_number".to_string(),
                threshold: None,
            }],
            priority: Some(1),
        }],
        output: OutputConfig {
            matched: "evidence/matched.parquet".to_string(),
            unmatched_left: "evidence/unmatched_left.parquet".to_string(),
            unmatched_right: "evidence/unmatched_right.parquet".to_string(),
        },
    };

    let response = client
        .post(format!("{}/api/runs", API_URL))
        .json(&CreateRunRequest { recipe })
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status(), 400, "Invalid version should return 400");
}

#[tokio::test]
async fn test_create_run_tolerance_without_threshold() {
    let Some(client) = ensure_server().await else { return };

    let pg_host = postgres_host();
    let recipe = RecipeConfig {
        version: "1.0".to_string(),
        recipe_id: "test-no-threshold".to_string(),
        sources: Sources {
            left: DataSource {
                alias: "invoices".to_string(),
                uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=invoices", pg_host),
                primary_key: vec!["invoice_id".to_string()],
            },
            right: DataSource {
                alias: "payments".to_string(),
                uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=payments", pg_host),
                primary_key: vec!["payment_id".to_string()],
            },
        },
        match_rules: vec![MatchRule {
            name: "missing-threshold".to_string(),
            pattern: "1:1".to_string(),
            conditions: vec![MatchCondition {
                left: "amount".to_string(),
                op: "tolerance".to_string(),
                right: "paid_amount".to_string(),
                threshold: None, // Missing required threshold
            }],
            priority: Some(1),
        }],
        output: OutputConfig {
            matched: "evidence/matched.parquet".to_string(),
            unmatched_left: "evidence/unmatched_left.parquet".to_string(),
            unmatched_right: "evidence/unmatched_right.parquet".to_string(),
        },
    };

    let response = client
        .post(format!("{}/api/runs", API_URL))
        .json(&CreateRunRequest { recipe })
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status(), 400, "Tolerance without threshold should return 400");
}

// ===========================================================================
// Source endpoint edge cases
// ===========================================================================

#[tokio::test]
async fn test_get_nonexistent_run() {
    let Some(client) = ensure_server().await else { return };

    let fake_id = "00000000-0000-0000-0000-000000000000";
    let response = client
        .get(format!("{}/api/runs/{}", API_URL, fake_id))
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status(), 404, "Nonexistent run should return 404");
}

#[tokio::test]
async fn test_preview_nonexistent_source() {
    let Some(client) = ensure_server().await else { return };

    let response = client
        .get(format!("{}/api/sources/nonexistent_source_xyz/preview?limit=5", API_URL))
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status(), 404, "Nonexistent source preview should return 404");
}

#[tokio::test]
async fn test_register_unsupported_uri_scheme() {
    let Some(client) = ensure_server().await else { return };

    let response = client
        .post(format!("{}/api/sources", API_URL))
        .json(&RegisterSourceRequest {
            alias: "bad_source".to_string(),
            uri: "ftp://example.com/data.csv".to_string(),
        })
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status(), 400, "Unsupported URI scheme should return 400");
}

// ===========================================================================
// Recipe validation endpoint
// ===========================================================================

#[tokio::test]
async fn test_validate_recipe_endpoint_valid() {
    let Some(client) = ensure_server().await else { return };

    let pg_host = postgres_host();
    let recipe = RecipeConfig {
        version: "1.0".to_string(),
        recipe_id: "test-validate-valid".to_string(),
        sources: Sources {
            left: DataSource {
                alias: "invoices".to_string(),
                uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=invoices", pg_host),
                primary_key: vec!["invoice_id".to_string()],
            },
            right: DataSource {
                alias: "payments".to_string(),
                uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=payments", pg_host),
                primary_key: vec!["payment_id".to_string()],
            },
        },
        match_rules: vec![MatchRule {
            name: "exact_match".to_string(),
            pattern: "1:1".to_string(),
            conditions: vec![MatchCondition {
                left: "invoice_id".to_string(),
                op: "eq".to_string(),
                right: "reference_number".to_string(),
                threshold: None,
            }],
            priority: Some(1),
        }],
        output: OutputConfig {
            matched: "evidence/matched.parquet".to_string(),
            unmatched_left: "evidence/unmatched_left.parquet".to_string(),
            unmatched_right: "evidence/unmatched_right.parquet".to_string(),
        },
    };

    let response = client
        .post(format!("{}/api/recipes/validate", API_URL))
        .json(&recipe)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["valid"], true);
    assert!(body["errors"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_validate_recipe_endpoint_invalid() {
    let Some(client) = ensure_server().await else { return };

    let recipe = RecipeConfig {
        version: "1.0".to_string(),
        recipe_id: "".to_string(), // Empty ID
        sources: Sources {
            left: DataSource {
                alias: "left".to_string(),
                uri: "".to_string(), // Empty URI
                primary_key: vec![],
            },
            right: DataSource {
                alias: "right".to_string(),
                uri: "file://test.csv".to_string(),
                primary_key: vec![],
            },
        },
        match_rules: vec![],
        output: OutputConfig {
            matched: "m.parquet".to_string(),
            unmatched_left: "l.parquet".to_string(),
            unmatched_right: "r.parquet".to_string(),
        },
    };

    let response = client
        .post(format!("{}/api/recipes/validate", API_URL))
        .json(&recipe)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["valid"], false);
    assert!(!body["errors"].as_array().unwrap().is_empty());
}

// ===========================================================================
// Health check
// ===========================================================================

#[tokio::test]
async fn test_health_check() {
    let Some(client) = ensure_server().await else { return };

    let response = client
        .get(format!("{}/health", API_URL))
        .send()
        .await
        .expect("Health check failed");

    assert!(response.status().is_success());
    let body = response.text().await.unwrap();
    assert_eq!(body, "OK");
}

// ===========================================================================
// Full reconciliation with all-match scenario
// ===========================================================================

#[tokio::test]
async fn test_reconciliation_exact_matches_only() {
    let Some(client) = ensure_server().await else { return };

    let pg_host = postgres_host();
    let recipe = RecipeConfig {
        version: "1.0".to_string(),
        recipe_id: "test-exact-only".to_string(),
        sources: Sources {
            left: DataSource {
                alias: "invoices".to_string(),
                uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=invoices", pg_host),
                primary_key: vec!["invoice_id".to_string()],
            },
            right: DataSource {
                alias: "payments".to_string(),
                uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=payments", pg_host),
                primary_key: vec!["payment_id".to_string()],
            },
        },
        match_rules: vec![MatchRule {
            name: "exact_reference_match".to_string(),
            pattern: "1:1".to_string(),
            conditions: vec![MatchCondition {
                left: "invoice_id".to_string(),
                op: "eq".to_string(),
                right: "reference_number".to_string(),
                threshold: None,
            }],
            priority: Some(1),
        }],
        output: OutputConfig {
            matched: "evidence/matched.parquet".to_string(),
            unmatched_left: "evidence/unmatched_left.parquet".to_string(),
            unmatched_right: "evidence/unmatched_right.parquet".to_string(),
        },
    };

    let response = client
        .post(format!("{}/api/runs", API_URL))
        .json(&CreateRunRequest { recipe })
        .send()
        .await
        .expect("Failed to create run");

    assert!(response.status().is_success());

    let create_response: CreateRunResponse = response.json().await.unwrap();
    let run_id = &create_response.run_id;

    // Poll for completion
    let mut final_status = String::new();
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let run = client
            .get(format!("{}/api/runs/{}", API_URL, run_id))
            .send()
            .await
            .expect("Failed to get run");

        let data: serde_json::Value = run.json().await.unwrap();
        let status = data["status"].as_str().unwrap_or("unknown").to_string();
        if status != "Running" {
            final_status = status;
            break;
        }
    }

    assert!(
        final_status.eq_ignore_ascii_case("Completed"),
        "Expected Completed, got: {}",
        final_status
    );

    // Get final counts
    let run = client
        .get(format!("{}/api/runs/{}", API_URL, run_id))
        .send()
        .await
        .unwrap();
    let data: serde_json::Value = run.json().await.unwrap();

    let matched = data["matched_count"].as_u64().unwrap_or(0);
    // Based on seed data, 7 invoices have exact reference matches (INV-2024-001 through 007 and 015)
    assert!(matched > 0, "Should have some exact matches from seed data");
}

// ===========================================================================
// Concurrent runs
// ===========================================================================

#[tokio::test]
async fn test_multiple_concurrent_runs() {
    let Some(client) = ensure_server().await else { return };

    let pg_host = postgres_host();

    let mut run_ids = Vec::new();
    for i in 0..3 {
        let recipe = RecipeConfig {
            version: "1.0".to_string(),
            recipe_id: format!("concurrent-test-{}", i),
            sources: Sources {
                left: DataSource {
                    alias: "invoices".to_string(),
                    uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=invoices", pg_host),
                    primary_key: vec!["invoice_id".to_string()],
                },
                right: DataSource {
                    alias: "payments".to_string(),
                    uri: format!("postgres://kalla:kalla_secret@{}:5432/kalla?table=payments", pg_host),
                    primary_key: vec!["payment_id".to_string()],
                },
            },
            match_rules: vec![MatchRule {
                name: "exact_match".to_string(),
                pattern: "1:1".to_string(),
                conditions: vec![MatchCondition {
                    left: "invoice_id".to_string(),
                    op: "eq".to_string(),
                    right: "reference_number".to_string(),
                    threshold: None,
                }],
                priority: Some(1),
            }],
            output: OutputConfig {
                matched: "evidence/matched.parquet".to_string(),
                unmatched_left: "evidence/unmatched_left.parquet".to_string(),
                unmatched_right: "evidence/unmatched_right.parquet".to_string(),
            },
        };

        let response = client
            .post(format!("{}/api/runs", API_URL))
            .json(&CreateRunRequest { recipe })
            .send()
            .await
            .expect("Failed to create run");

        assert!(response.status().is_success());
        let body: CreateRunResponse = response.json().await.unwrap();
        run_ids.push(body.run_id);
    }

    // Wait for all runs to complete
    for run_id in &run_ids {
        let mut completed = false;
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let run = client
                .get(format!("{}/api/runs/{}", API_URL, run_id))
                .send()
                .await
                .expect("Failed to get run");
            let data: serde_json::Value = run.json().await.unwrap();
            let status = data["status"].as_str().unwrap_or("unknown");
            if status != "Running" {
                completed = true;
                assert!(
                    status.eq_ignore_ascii_case("Completed"),
                    "Run {} should complete successfully, got: {}",
                    run_id,
                    status
                );
                break;
            }
        }
        assert!(completed, "Run {} did not complete within timeout", run_id);
    }
}

// ===========================================================================
// Source listing and preview
// ===========================================================================

#[tokio::test]
async fn test_list_sources_returns_seed_data() {
    let Some(client) = ensure_server().await else { return };

    let response = client
        .get(format!("{}/api/sources", API_URL))
        .send()
        .await
        .expect("Failed to list sources");

    assert!(response.status().is_success());
    let sources: Vec<serde_json::Value> = response.json().await.unwrap();

    // Seed data should include at least invoices and payments
    let aliases: Vec<&str> = sources
        .iter()
        .filter_map(|s| s["alias"].as_str())
        .collect();

    assert!(aliases.contains(&"invoices"), "Should have invoices source");
    assert!(aliases.contains(&"payments"), "Should have payments source");
}

#[tokio::test]
async fn test_source_preview_limit() {
    let Some(client) = ensure_server().await else { return };

    let response = client
        .get(format!("{}/api/sources/invoices_csv/preview?limit=3", API_URL))
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.unwrap();
    let rows = body["rows"].as_array().unwrap();
    assert!(rows.len() <= 3, "Should respect limit parameter");
}

// ===========================================================================
// Get recipe by ID
// ===========================================================================

#[tokio::test]
async fn test_get_recipe_by_id() {
    let Some(client) = ensure_server().await else { return };

    let response = client
        .get(format!("{}/api/recipes/invoice-payment-match", API_URL))
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["recipe_id"].as_str().unwrap(), "invoice-payment-match");
}

#[tokio::test]
async fn test_get_nonexistent_recipe() {
    let Some(client) = ensure_server().await else { return };

    let response = client
        .get(format!("{}/api/recipes/does-not-exist-xyz", API_URL))
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status(), 404);
}
