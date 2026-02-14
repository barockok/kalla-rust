//! Integration tests for the reconciliation process
//!
//! These tests require a running PostgreSQL database with seed data.
//! Run `docker compose up -d postgres` before running these tests.

use serde::{Deserialize, Serialize};
use std::time::Duration;

const API_URL: &str = "http://localhost:3001";

/// Get the postgres host to use in connection strings.
/// Use POSTGRES_HOST env var to override (e.g., "postgres" for Docker networking)
fn postgres_host() -> String {
    std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string())
}

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
struct RunSummary {
    run_id: String,
    recipe_id: String,
    status: String,
    started_at: String,
    matched_count: u64,
    unmatched_left_count: u64,
    unmatched_right_count: u64,
}

fn create_test_recipe() -> RecipeConfig {
    let pg_host = postgres_host();
    RecipeConfig {
        version: "1.0".to_string(),
        recipe_id: "invoice-payment-match-test".to_string(),
        sources: Sources {
            left: DataSource {
                alias: "invoices".to_string(),
                uri: format!(
                    "postgres://kalla:kalla_secret@{}:5432/kalla?table=invoices",
                    pg_host
                ),
                primary_key: vec!["invoice_id".to_string()],
            },
            right: DataSource {
                alias: "payments".to_string(),
                uri: format!(
                    "postgres://kalla:kalla_secret@{}:5432/kalla?table=payments",
                    pg_host
                ),
                primary_key: vec!["payment_id".to_string()],
            },
        },
        match_rules: vec![
            MatchRule {
                name: "exact_reference_match".to_string(),
                pattern: "1:1".to_string(),
                conditions: vec![MatchCondition {
                    left: "invoice_id".to_string(),
                    op: "eq".to_string(),
                    right: "reference_number".to_string(),
                    threshold: None,
                }],
                priority: Some(1),
            },
            MatchRule {
                name: "amount_and_customer_match".to_string(),
                pattern: "1:1".to_string(),
                conditions: vec![
                    MatchCondition {
                        left: "customer_id".to_string(),
                        op: "eq".to_string(),
                        right: "payer_id".to_string(),
                        threshold: None,
                    },
                    MatchCondition {
                        left: "amount".to_string(),
                        op: "tolerance".to_string(),
                        right: "paid_amount".to_string(),
                        threshold: Some(0.02),
                    },
                ],
                priority: Some(2),
            },
        ],
        output: OutputConfig {
            matched: "evidence/matched.parquet".to_string(),
            unmatched_left: "evidence/unmatched_invoices.parquet".to_string(),
            unmatched_right: "evidence/unmatched_payments.parquet".to_string(),
        },
    }
}

#[tokio::test]
async fn test_reconciliation_completes() {
    // Check if the server is running
    let client = reqwest::Client::new();
    let health_check = client.get(format!("{}/health", API_URL)).send().await;

    if health_check.is_err() {
        println!("Skipping test: Server not running at {}", API_URL);
        println!("Start the server with: cargo run -p kalla-server");
        println!("Or run: docker compose up -d");
        return;
    }

    // Create a run with the test recipe
    let recipe = create_test_recipe();
    let request = CreateRunRequest { recipe };

    let response = client
        .post(format!("{}/api/runs", API_URL))
        .json(&request)
        .send()
        .await
        .expect("Failed to create run");

    assert!(
        response.status().is_success(),
        "Create run failed: {:?}",
        response.status()
    );

    let create_response: CreateRunResponse =
        response.json().await.expect("Failed to parse response");
    let run_id = &create_response.run_id;
    println!("Created run: {}", run_id);

    // Poll for completion (max 30 seconds)
    let mut attempts = 0;
    let max_attempts = 30;
    let mut final_status = String::new();

    while attempts < max_attempts {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let run_response = client
            .get(format!("{}/api/runs/{}", API_URL, run_id))
            .send()
            .await
            .expect("Failed to get run");

        let run_data: serde_json::Value = run_response.json().await.expect("Failed to parse run");
        let status = run_data["status"].as_str().unwrap_or("unknown");
        println!("Run status: {}", status);

        if status != "Running" {
            final_status = status.to_string();
            break;
        }

        attempts += 1;
    }

    // Verify the run completed successfully (case-insensitive)
    assert!(
        !final_status.eq_ignore_ascii_case("Running"),
        "Run did not complete within timeout"
    );
    assert!(
        final_status.eq_ignore_ascii_case("Completed"),
        "Run did not complete successfully: {}",
        final_status
    );

    // Get final run details
    let run_response = client
        .get(format!("{}/api/runs/{}", API_URL, run_id))
        .send()
        .await
        .expect("Failed to get run");

    let run_data: serde_json::Value = run_response.json().await.expect("Failed to parse run");
    println!("Final run data: {:#?}", run_data);

    // Verify counts are populated
    let matched_count = run_data["matched_count"].as_u64().unwrap_or(0);
    let left_count = run_data["left_record_count"].as_u64().unwrap_or(0);
    let right_count = run_data["right_record_count"].as_u64().unwrap_or(0);

    println!(
        "Results: left={}, right={}, matched={}",
        left_count, right_count, matched_count
    );

    // Verify we got some data
    assert!(left_count > 0, "Left record count should be > 0");
    assert!(right_count > 0, "Right record count should be > 0");
    // Some records should match based on our seed data
    assert!(
        matched_count > 0,
        "Matched count should be > 0 (seed data has matching records)"
    );
}

#[tokio::test]
async fn test_list_runs() {
    let client = reqwest::Client::new();
    let health_check = client.get(format!("{}/health", API_URL)).send().await;

    if health_check.is_err() {
        println!("Skipping test: Server not running at {}", API_URL);
        return;
    }

    let response = client
        .get(format!("{}/api/runs", API_URL))
        .send()
        .await
        .expect("Failed to list runs");

    assert!(response.status().is_success());

    let runs: Vec<RunSummary> = response.json().await.expect("Failed to parse runs");
    println!("Found {} runs", runs.len());
}

#[tokio::test]
async fn test_list_recipes() {
    let client = reqwest::Client::new();
    let health_check = client.get(format!("{}/health", API_URL)).send().await;

    if health_check.is_err() {
        println!("Skipping test: Server not running at {}", API_URL);
        return;
    }

    let response = client
        .get(format!("{}/api/recipes", API_URL))
        .send()
        .await
        .expect("Failed to list recipes");

    assert!(response.status().is_success());

    let recipes: Vec<serde_json::Value> = response.json().await.expect("Failed to parse recipes");
    println!("Found {} recipes", recipes.len());

    // Should have at least the seed recipe
    assert!(!recipes.is_empty(), "Should have at least one seed recipe");
}

#[tokio::test]
async fn test_csv_source_preview() {
    let client = reqwest::Client::new();
    let health_check = client.get(format!("{}/health", API_URL)).send().await;

    if health_check.is_err() {
        println!("Skipping test: Server not running at {}", API_URL);
        return;
    }

    // The init.sql seeds invoices_csv and payments_csv.
    // After a server restart they should still be queryable.
    let res = client
        .get(format!(
            "{}/api/sources/invoices_csv/preview?limit=5",
            API_URL
        ))
        .send()
        .await
        .expect("request failed");

    assert_eq!(res.status(), 200, "CSV source preview should succeed");

    let body: serde_json::Value = res.json().await.unwrap();
    assert!(
        !body["rows"].as_array().unwrap().is_empty(),
        "Should return rows"
    );
    assert!(
        !body["columns"].as_array().unwrap().is_empty(),
        "Should return columns"
    );
}
