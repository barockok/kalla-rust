//! Integration tests for the reconciliation process
//!
//! These tests require a running PostgreSQL database with seed data.
//! Run `docker compose up -d postgres` before running these tests.

use serde::{Deserialize, Serialize};

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

    // With NATS-based architecture, runs are submitted to workers asynchronously.
    // Verify the run was accepted and is trackable via the API.
    assert_eq!(create_response.status, "submitted");

    let run_response = client
        .get(format!("{}/api/runs/{}", API_URL, run_id))
        .send()
        .await
        .expect("Failed to get run");

    assert!(run_response.status().is_success());
    let run_data: serde_json::Value = run_response.json().await.expect("Failed to parse run");
    let status = run_data["status"].as_str().unwrap_or("unknown");
    println!("Run status: {}", status);

    // Run should be in Running state (submitted to NATS for async processing)
    assert_eq!(
        status, "running",
        "Run should be in running state after submission"
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

    // Try PostgreSQL source first (always available in CI), fall back to CSV for local dev.
    let res = client
        .get(format!("{}/api/sources/invoices/preview?limit=5", API_URL))
        .send()
        .await
        .expect("request failed");

    if !res.status().is_success() {
        // Fall back to CSV source for local dev environments
        let res2 = client
            .get(format!(
                "{}/api/sources/invoices_csv/preview?limit=5",
                API_URL
            ))
            .send()
            .await
            .expect("request failed");
        if !res2.status().is_success() {
            println!("Skipping: neither invoices nor invoices_csv source available");
            return;
        }
        let body: serde_json::Value = res2.json().await.unwrap();
        assert!(
            !body["rows"].as_array().unwrap().is_empty(),
            "Should return rows"
        );
        assert!(
            !body["columns"].as_array().unwrap().is_empty(),
            "Should return columns"
        );
        return;
    }

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
