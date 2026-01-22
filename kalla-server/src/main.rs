//! Kalla Server - REST API for the reconciliation engine

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

use kalla_core::ReconciliationEngine;
use kalla_evidence::{EvidenceStore, RunMetadata};
use kalla_recipe::MatchRecipe;

/// Application state shared across handlers
struct AppState {
    engine: RwLock<ReconciliationEngine>,
    evidence_store: EvidenceStore,
    runs: RwLock<Vec<RunMetadata>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set up logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Initialize state
    let evidence_store = EvidenceStore::new("./evidence")?;
    let state = Arc::new(AppState {
        engine: RwLock::new(ReconciliationEngine::new()),
        evidence_store,
        runs: RwLock::new(Vec::new()),
    });

    // Build router
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/sources", post(register_source))
        .route("/api/recipes/validate", post(validate_recipe))
        .route("/api/recipes/generate", post(generate_recipe))
        .route("/api/runs", post(create_run))
        .route("/api/runs", get(list_runs))
        .route("/api/runs/:id", get(get_run))
        .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = "0.0.0.0:3001";
    info!("Starting Kalla server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> &'static str {
    "OK"
}

// === Data Source Endpoints ===

#[derive(Deserialize)]
struct RegisterSourceRequest {
    alias: String,
    uri: String,
}

#[derive(Serialize)]
struct RegisterSourceResponse {
    success: bool,
    message: String,
}

async fn register_source(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterSourceRequest>,
) -> Result<Json<RegisterSourceResponse>, (StatusCode, String)> {
    let engine = state.engine.read().await;

    // Handle file:// URIs
    if req.uri.starts_with("file://") {
        let path = req.uri.strip_prefix("file://").unwrap();
        if path.ends_with(".csv") {
            engine
                .register_csv(&req.alias, path)
                .await
                .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        } else if path.ends_with(".parquet") {
            engine
                .register_parquet(&req.alias, path)
                .await
                .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        } else {
            return Err((StatusCode::BAD_REQUEST, "Unsupported file format".to_string()));
        }
    } else {
        return Err((StatusCode::BAD_REQUEST, "Unsupported URI scheme".to_string()));
    }

    Ok(Json(RegisterSourceResponse {
        success: true,
        message: format!("Registered '{}' as '{}'", req.uri, req.alias),
    }))
}

// === Recipe Endpoints ===

#[derive(Serialize)]
struct ValidateRecipeResponse {
    valid: bool,
    errors: Vec<String>,
}

async fn validate_recipe(
    Json(recipe): Json<MatchRecipe>,
) -> Json<ValidateRecipeResponse> {
    match kalla_recipe::validate_recipe(&recipe) {
        Ok(()) => Json(ValidateRecipeResponse {
            valid: true,
            errors: vec![],
        }),
        Err(errors) => Json(ValidateRecipeResponse {
            valid: false,
            errors: errors.iter().map(|e| e.to_string()).collect(),
        }),
    }
}

#[derive(Deserialize)]
struct GenerateRecipeRequest {
    left_source: String,
    right_source: String,
    prompt: String,
}

#[derive(Serialize)]
struct GenerateRecipeResponse {
    recipe: Option<MatchRecipe>,
    error: Option<String>,
}

async fn generate_recipe(
    State(state): State<Arc<AppState>>,
    Json(req): Json<GenerateRecipeRequest>,
) -> Result<Json<GenerateRecipeResponse>, (StatusCode, String)> {
    use kalla_ai::{extract_schema, LlmClient};
    use kalla_ai::prompt::{build_user_prompt, parse_recipe_response, SYSTEM_PROMPT};

    let engine = state.engine.read().await;

    // Extract schemas
    let left_schema = extract_schema(engine.context(), &req.left_source)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Failed to extract left schema: {}", e)))?;

    let right_schema = extract_schema(engine.context(), &req.right_source)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Failed to extract right schema: {}", e)))?;

    // Build prompt
    let user_prompt = build_user_prompt(
        &left_schema,
        &right_schema,
        &req.prompt,
        &format!("registered://{}", req.left_source),
        &format!("registered://{}", req.right_source),
    );

    // Call LLM
    let client = LlmClient::from_env()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let response = client
        .generate(SYSTEM_PROMPT, &user_prompt)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("LLM error: {}", e)))?;

    // Parse response
    match parse_recipe_response(&response) {
        Ok(recipe) => Ok(Json(GenerateRecipeResponse {
            recipe: Some(recipe),
            error: None,
        })),
        Err(e) => Ok(Json(GenerateRecipeResponse {
            recipe: None,
            error: Some(format!("Failed to parse recipe: {}", e)),
        })),
    }
}

// === Run Endpoints ===

#[derive(Deserialize)]
struct CreateRunRequest {
    recipe: MatchRecipe,
}

#[derive(Serialize)]
struct CreateRunResponse {
    run_id: Uuid,
    status: String,
}

async fn create_run(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateRunRequest>,
) -> Result<Json<CreateRunResponse>, (StatusCode, String)> {
    // Validate recipe first
    if let Err(errors) = kalla_recipe::validate_recipe(&req.recipe) {
        let error_msg = errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ");
        return Err((StatusCode::BAD_REQUEST, format!("Invalid recipe: {}", error_msg)));
    }

    // Create run metadata
    let metadata = RunMetadata::new(
        req.recipe.recipe_id.clone(),
        req.recipe.sources.left.uri.clone(),
        req.recipe.sources.right.uri.clone(),
    );

    let run_id = metadata.run_id;

    // Initialize run in evidence store
    state.evidence_store.init_run(&metadata)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Store run metadata
    state.runs.write().await.push(metadata);

    // In a real implementation, we'd spawn a background task to run the reconciliation
    // For now, just return the run ID
    info!("Created run {}", run_id);

    Ok(Json(CreateRunResponse {
        run_id,
        status: "created".to_string(),
    }))
}

#[derive(Serialize)]
struct RunSummary {
    run_id: Uuid,
    recipe_id: String,
    status: String,
    started_at: String,
    matched_count: u64,
    unmatched_left_count: u64,
    unmatched_right_count: u64,
}

async fn list_runs(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<RunSummary>> {
    let runs = state.runs.read().await;
    let summaries: Vec<RunSummary> = runs
        .iter()
        .map(|r| RunSummary {
            run_id: r.run_id,
            recipe_id: r.recipe_id.clone(),
            status: format!("{:?}", r.status),
            started_at: r.started_at.to_rfc3339(),
            matched_count: r.matched_count,
            unmatched_left_count: r.unmatched_left_count,
            unmatched_right_count: r.unmatched_right_count,
        })
        .collect();

    Json(summaries)
}

async fn get_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<RunMetadata>, (StatusCode, String)> {
    let runs = state.runs.read().await;

    runs.iter()
        .find(|r| r.run_id == id)
        .cloned()
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, "Run not found".to_string()))
}
