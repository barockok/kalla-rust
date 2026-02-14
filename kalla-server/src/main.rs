//! Kalla Server - REST API for the reconciliation engine

pub mod nats_publisher;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

use kalla_connectors::{build_where_clause, FilterCondition, PostgresConnector};
use kalla_core::ReconciliationEngine;
use kalla_evidence::{EvidenceStore, RunMetadata};
use kalla_recipe::MatchRecipe;

use nats_publisher::{JobMessage, NatsPublisher, StagedSource};

/// Registered data source info
#[derive(Clone, Serialize, FromRow)]
struct RegisteredSource {
    alias: String,
    uri: String,
    source_type: String,
    status: String,
}

/// Saved recipe from database
#[derive(Clone, Serialize, FromRow)]
struct SavedRecipe {
    recipe_id: String,
    name: String,
    description: Option<String>,
    config: serde_json::Value,
}

/// Application state shared across handlers
struct AppState {
    engine: RwLock<ReconciliationEngine>,
    evidence_store: Arc<EvidenceStore>,
    runs: Arc<RwLock<Vec<RunMetadata>>>,
    sources: RwLock<Vec<RegisteredSource>>,
    recipes: RwLock<Vec<SavedRecipe>>,
    db_pool: Option<PgPool>,
    nats: Option<NatsPublisher>,
}

/// Parse a PostgreSQL URI to extract connection string and table name
/// Example: `postgres://user:pass@host:port/db?table=tablename`
/// Returns: (connection_string, table_name)
pub(crate) fn parse_postgres_uri(uri: &str) -> Result<(String, String), String> {
    let url = url::Url::parse(uri).map_err(|e| format!("Invalid URI: {}", e))?;

    let table_name = url
        .query_pairs()
        .find(|(k, _)| k == "table")
        .map(|(_, v)| v.to_string())
        .ok_or("Missing 'table' query parameter in URI")?;

    let mut connection_url = url.clone();
    connection_url.set_query(None);

    Ok((connection_url.to_string(), table_name))
}

/// Convert an Arrow array value at the given index to a string representation
fn arrow_value_to_string(array: &arrow::array::ArrayRef, idx: usize) -> String {
    use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};

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

    format!("{:?}", array.slice(idx, 1))
}

/// Register a source with the engine's SessionContext by looking up its URI
async fn register_source_with_engine(
    state: &Arc<AppState>,
    source_alias: &str,
) -> Result<(), String> {
    {
        let engine = state.engine.read().await;
        if engine
            .context()
            .table_exist(source_alias)
            .map_err(|e| e.to_string())?
        {
            return Ok(());
        }
    }

    let sources = state.sources.read().await;
    let source = sources
        .iter()
        .find(|s| s.alias == source_alias)
        .ok_or_else(|| format!("Source '{}' not found", source_alias))?;

    let uri = source.uri.clone();
    drop(sources);

    if uri.starts_with("postgres://") {
        let (conn_string, table_name) = parse_postgres_uri(&uri)?;
        let engine = state.engine.write().await;
        if engine
            .context()
            .table_exist(source_alias)
            .map_err(|e| e.to_string())?
        {
            return Ok(());
        }
        let connector = PostgresConnector::new(&conn_string)
            .await
            .map_err(|e| format!("Failed to connect to database: {}", e))?;
        connector
            .register_table(engine.context(), source_alias, &table_name, None)
            .await
            .map_err(|e| format!("Failed to register table: {}", e))?;
    } else if uri.starts_with("file://") {
        let path = uri.strip_prefix("file://").unwrap();
        let engine = state.engine.write().await;
        if engine
            .context()
            .table_exist(source_alias)
            .map_err(|e| e.to_string())?
        {
            return Ok(());
        }
        if path.ends_with(".csv") {
            engine
                .register_csv(source_alias, path)
                .await
                .map_err(|e| format!("Failed to register CSV: {}", e))?;
        } else if path.ends_with(".parquet") {
            engine
                .register_parquet(source_alias, path)
                .await
                .map_err(|e| format!("Failed to register parquet: {}", e))?;
        } else {
            return Err(format!("Unsupported file format for '{}'", source_alias));
        }
    } else {
        return Err(format!("Unsupported URI scheme for '{}'", source_alias));
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Connect to database and load sources/recipes
    let database_url = std::env::var("DATABASE_URL").ok();
    let (db_pool, initial_sources, initial_recipes) = if let Some(url) = database_url {
        match PgPoolOptions::new().max_connections(5).connect(&url).await {
            Ok(pool) => {
                info!("Connected to database");
                let sources: Vec<RegisteredSource> =
                    sqlx::query_as("SELECT alias, uri, source_type, status FROM sources")
                        .fetch_all(&pool)
                        .await
                        .unwrap_or_else(|e| {
                            warn!("Failed to load sources from database: {}", e);
                            Vec::new()
                        });
                info!("Loaded {} sources from database", sources.len());

                let recipes: Vec<SavedRecipe> =
                    sqlx::query_as("SELECT recipe_id, name, description, config FROM recipes")
                        .fetch_all(&pool)
                        .await
                        .unwrap_or_else(|e| {
                            warn!("Failed to load recipes from database: {}", e);
                            Vec::new()
                        });
                info!("Loaded {} recipes from database", recipes.len());

                (Some(pool), sources, recipes)
            }
            Err(e) => {
                warn!(
                    "Failed to connect to database: {}. Running without persistence.",
                    e
                );
                (None, Vec::new(), Vec::new())
            }
        }
    } else {
        info!("DATABASE_URL not set. Running without persistence.");
        (None, Vec::new(), Vec::new())
    };

    // Shared state
    let evidence_store = Arc::new(EvidenceStore::new("./evidence")?);
    let runs = Arc::new(RwLock::new(Vec::new()));

    // Connect to NATS if NATS_URL is set
    let nats = match std::env::var("NATS_URL") {
        Ok(nats_url) => match NatsPublisher::connect(&nats_url).await {
            Ok(publisher) => {
                info!("Connected to NATS at {}", nats_url);
                Some(publisher)
            }
            Err(e) => {
                warn!(
                    "Failed to connect to NATS at {}: {}. Runs will fail.",
                    nats_url, e
                );
                None
            }
        },
        Err(_) => {
            warn!("NATS_URL not set. Reconciliation runs will fail until NATS is configured.");
            None
        }
    };

    let state = Arc::new(AppState {
        engine: RwLock::new(ReconciliationEngine::new()),
        evidence_store,
        runs,
        sources: RwLock::new(initial_sources),
        recipes: RwLock::new(initial_recipes),
        db_pool,
        nats,
    });

    // Build router
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/sources", get(list_sources).post(register_source))
        .route(
            "/api/sources/:alias/primary-key",
            get(get_source_primary_key),
        )
        .route("/api/sources/:alias/preview", get(get_source_preview))
        .route("/api/sources/:alias/load-scoped", post(load_scoped))
        .route("/api/recipes", get(list_recipes).post(save_recipe))
        .route("/api/recipes/validate", post(validate_recipe))
        .route("/api/recipes/validate-schema", post(validate_recipe_schema))
        .route("/api/recipes/generate", post(generate_recipe))
        .route("/api/recipes/:id", get(get_recipe))
        .route("/api/runs", post(create_run))
        .route("/api/runs", get(list_runs))
        .route("/api/runs/:id", get(get_run))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
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

async fn list_sources(State(state): State<Arc<AppState>>) -> Json<Vec<RegisteredSource>> {
    let sources = state.sources.read().await;
    Json(sources.clone())
}

// GET /api/sources/:alias/primary-key
async fn get_source_primary_key(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
) -> Result<Json<PrimaryKeyResponse>, (StatusCode, String)> {
    let engine = state.engine.read().await;

    let detected = kalla_ai::schema_extractor::detect_primary_key(engine.context(), &alias)
        .await
        .map_err(|e| {
            (
                StatusCode::NOT_FOUND,
                format!("Source not found or error: {}", e),
            )
        })?;

    let confidence = if detected.is_empty() { "low" } else { "high" }.to_string();

    Ok(Json(PrimaryKeyResponse {
        alias: alias.clone(),
        detected_keys: detected,
        confidence,
    }))
}

#[derive(Serialize)]
struct PrimaryKeyResponse {
    alias: String,
    detected_keys: Vec<String>,
    confidence: String,
}

// GET /api/sources/:alias/preview?limit=10
async fn get_source_preview(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
    Query(params): Query<PreviewParams>,
) -> Result<Json<SourcePreviewResponse>, (StatusCode, String)> {
    use arrow::array::Int64Array;

    let limit = params.limit.unwrap_or(10).min(100);

    register_source_with_engine(&state, &alias)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("Source not found: {}", e)))?;

    let engine = state.engine.read().await;

    let table = engine
        .context()
        .table(&alias)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("Source not found: {}", e)))?;
    let schema = table.schema();

    let columns: Vec<ColumnInfo> = schema
        .fields()
        .iter()
        .map(|f| ColumnInfo {
            name: f.name().to_string(),
            data_type: format!("{:?}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect();

    let query = format!("SELECT * FROM \"{}\" LIMIT {}", alias, limit);
    let df = engine.context().sql(&query).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Query failed: {}", e),
        )
    })?;

    let batches = df.collect().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Collect failed: {}", e),
        )
    })?;

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

    let count_query = format!("SELECT COUNT(*) FROM \"{}\"", alias);
    let count_df = engine
        .context()
        .sql(&count_query)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let count_batches = count_df
        .collect()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let total_rows = count_batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(0) as u64)
        .unwrap_or(0);

    let preview_rows = rows.len();

    Ok(Json(SourcePreviewResponse {
        alias,
        columns,
        rows,
        total_rows,
        preview_rows,
    }))
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

#[derive(Deserialize)]
struct LoadScopedRequest {
    conditions: Vec<FilterCondition>,
    limit: Option<usize>,
}

// POST /api/sources/:alias/load-scoped
async fn load_scoped(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
    Json(req): Json<LoadScopedRequest>,
) -> Result<Json<SourcePreviewResponse>, (StatusCode, String)> {
    use arrow::array::Int64Array;

    let limit = req.limit.unwrap_or(200).min(1000);

    let source_uri = {
        let sources = state.sources.read().await;
        let source = sources.iter().find(|s| s.alias == alias).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("Source '{}' not found", alias),
            )
        })?;
        source.uri.clone()
    };

    if source_uri.starts_with("postgres://") {
        let (conn_string, table_name) =
            parse_postgres_uri(&source_uri).map_err(|e| (StatusCode::BAD_REQUEST, e))?;

        let connector = PostgresConnector::new(&conn_string).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to connect to database: {}", e),
            )
        })?;

        let engine = state.engine.write().await;
        connector
            .register_scoped(
                engine.context(),
                &alias,
                &table_name,
                &req.conditions,
                Some(limit),
            )
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to register scoped table: {}", e),
                )
            })?;

        let table = engine.context().table(&alias).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Table not found after register_scoped: {}", e),
            )
        })?;
        let schema = table.schema();

        let columns: Vec<ColumnInfo> = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().to_string(),
                data_type: format!("{:?}", f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect();

        let query = format!("SELECT * FROM \"{}\" LIMIT {}", alias, limit);
        let df = engine.context().sql(&query).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Query failed: {}", e),
            )
        })?;

        let batches = df.collect().await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Collect failed: {}", e),
            )
        })?;

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

        let count_query = format!("SELECT COUNT(*) FROM \"{}\"", alias);
        let count_df = engine
            .context()
            .sql(&count_query)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let count_batches = count_df
            .collect()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let total_rows = count_batches
            .first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(0) as u64)
            .unwrap_or(0);

        let preview_rows = rows.len();

        Ok(Json(SourcePreviewResponse {
            alias,
            columns,
            rows,
            total_rows,
            preview_rows,
        }))
    } else {
        register_source_with_engine(&state, &alias)
            .await
            .map_err(|e| (StatusCode::NOT_FOUND, format!("Source not found: {}", e)))?;

        let engine = state.engine.read().await;

        let table = engine
            .context()
            .table(&alias)
            .await
            .map_err(|e| (StatusCode::NOT_FOUND, format!("Source not found: {}", e)))?;
        let schema = table.schema();

        let columns: Vec<ColumnInfo> = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().to_string(),
                data_type: format!("{:?}", f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect();

        let where_clause = build_where_clause(&req.conditions);
        let query = format!(
            "SELECT * FROM \"{}\"{} LIMIT {}",
            alias, where_clause, limit
        );
        let df = engine.context().sql(&query).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Query failed: {}", e),
            )
        })?;

        let batches = df.collect().await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Collect failed: {}", e),
            )
        })?;

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

        let count_query = format!("SELECT COUNT(*) FROM \"{}\"{}", alias, where_clause);
        let count_df = engine
            .context()
            .sql(&count_query)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let count_batches = count_df
            .collect()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let total_rows = count_batches
            .first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(0) as u64)
            .unwrap_or(0);

        let preview_rows = rows.len();

        Ok(Json(SourcePreviewResponse {
            alias,
            columns,
            rows,
            total_rows,
            preview_rows,
        }))
    }
}

async fn register_source(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterSourceRequest>,
) -> Result<Json<RegisterSourceResponse>, (StatusCode, String)> {
    let engine = state.engine.read().await;

    let source_type: String;

    if req.uri.starts_with("file://") {
        let path = req.uri.strip_prefix("file://").unwrap();
        if path.ends_with(".csv") {
            engine
                .register_csv(&req.alias, path)
                .await
                .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
            source_type = "csv".to_string();
        } else if path.ends_with(".parquet") {
            engine
                .register_parquet(&req.alias, path)
                .await
                .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
            source_type = "parquet".to_string();
        } else {
            return Err((
                StatusCode::BAD_REQUEST,
                "Unsupported file format".to_string(),
            ));
        }
    } else if req.uri.starts_with("postgres://") {
        source_type = "postgres".to_string();
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Unsupported URI scheme".to_string(),
        ));
    }

    let registered = RegisteredSource {
        alias: req.alias.clone(),
        uri: req.uri.clone(),
        source_type: source_type.clone(),
        status: "connected".to_string(),
    };

    if let Some(pool) = &state.db_pool {
        let _ = sqlx::query(
            "INSERT INTO sources (alias, uri, source_type, status) VALUES ($1, $2, $3, $4)
             ON CONFLICT (alias) DO UPDATE SET uri = $2, source_type = $3, status = $4, updated_at = NOW()",
        )
        .bind(&req.alias)
        .bind(&req.uri)
        .bind(&source_type)
        .bind("connected")
        .execute(pool)
        .await;
    }

    state.sources.write().await.push(registered);

    Ok(Json(RegisterSourceResponse {
        success: true,
        message: format!("Registered '{}' as '{}'", req.uri, req.alias),
    }))
}

// === Recipe Endpoints ===

async fn list_recipes(State(state): State<Arc<AppState>>) -> Json<Vec<SavedRecipe>> {
    let recipes = state.recipes.read().await;
    Json(recipes.clone())
}

async fn get_recipe(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<SavedRecipe>, (StatusCode, String)> {
    let recipes = state.recipes.read().await;

    recipes
        .iter()
        .find(|r| r.recipe_id == id)
        .cloned()
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, "Recipe not found".to_string()))
}

#[derive(Deserialize)]
struct SaveRecipeRequest {
    recipe_id: String,
    name: String,
    description: Option<String>,
    config: MatchRecipe,
}

#[derive(Serialize)]
struct SaveRecipeResponse {
    success: bool,
    message: String,
}

async fn save_recipe(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SaveRecipeRequest>,
) -> Result<Json<SaveRecipeResponse>, (StatusCode, String)> {
    if let Err(errors) = kalla_recipe::validate_recipe(&req.config) {
        let error_msg = errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Invalid recipe: {}", error_msg),
        ));
    }

    let config_json = serde_json::to_value(&req.config)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let saved = SavedRecipe {
        recipe_id: req.recipe_id.clone(),
        name: req.name.clone(),
        description: req.description.clone(),
        config: config_json.clone(),
    };

    if let Some(pool) = &state.db_pool {
        sqlx::query(
            "INSERT INTO recipes (recipe_id, name, description, config) VALUES ($1, $2, $3, $4)
             ON CONFLICT (recipe_id) DO UPDATE SET name = $2, description = $3, config = $4, updated_at = NOW()",
        )
        .bind(&req.recipe_id)
        .bind(&req.name)
        .bind(&req.description)
        .bind(&config_json)
        .execute(pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    let mut recipes = state.recipes.write().await;
    if let Some(existing) = recipes.iter_mut().find(|r| r.recipe_id == req.recipe_id) {
        *existing = saved;
    } else {
        recipes.push(saved);
    }

    Ok(Json(SaveRecipeResponse {
        success: true,
        message: format!("Recipe '{}' saved successfully", req.recipe_id),
    }))
}

#[derive(Serialize)]
struct ValidateRecipeResponse {
    valid: bool,
    errors: Vec<String>,
}

async fn validate_recipe(Json(recipe): Json<MatchRecipe>) -> Json<ValidateRecipeResponse> {
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

// POST /api/recipes/validate-schema
async fn validate_recipe_schema(
    State(state): State<Arc<AppState>>,
    Json(recipe): Json<MatchRecipe>,
) -> Result<Json<SchemaValidationResponse>, (StatusCode, String)> {
    let engine = state.engine.read().await;

    let left_table = engine
        .context()
        .table(&recipe.sources.left.alias)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Left source not found: {}", e),
            )
        })?;
    let left_fields: Vec<String> = left_table
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    let right_table = engine
        .context()
        .table(&recipe.sources.right.alias)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Right source not found: {}", e),
            )
        })?;
    let right_fields: Vec<String> = right_table
        .schema()
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
        errors: result
            .errors
            .iter()
            .map(|e| SchemaError {
                rule_name: e.rule_name.clone(),
                field: e.field.clone(),
                source: e.source.clone(),
                message: e.message.clone(),
                suggestion: e.suggestion.clone(),
            })
            .collect(),
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
    use kalla_ai::prompt::{build_user_prompt, parse_recipe_response, SYSTEM_PROMPT};
    use kalla_ai::{extract_schema, LlmClient};

    register_source_with_engine(&state, &req.left_source)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to register left source: {}", e),
            )
        })?;

    register_source_with_engine(&state, &req.right_source)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to register right source: {}", e),
            )
        })?;

    let engine = state.engine.read().await;

    let left_schema = extract_schema(engine.context(), &req.left_source)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to extract left schema: {}", e),
            )
        })?;

    let right_schema = extract_schema(engine.context(), &req.right_source)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to extract right schema: {}", e),
            )
        })?;

    let user_prompt = build_user_prompt(
        &left_schema,
        &right_schema,
        &req.prompt,
        &format!("registered://{}", req.left_source),
        &format!("registered://{}", req.right_source),
    );

    let client =
        LlmClient::from_env().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let response = client
        .generate(SYSTEM_PROMPT, &user_prompt)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("LLM error: {}", e),
            )
        })?;

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

/// Classify whether a source URI is native (can be read directly by DataFusion)
/// or non-native (needs staging to Parquet on S3).
fn is_native_source(uri: &str) -> bool {
    if uri.starts_with("file://") {
        let path = uri.strip_prefix("file://").unwrap_or(uri);
        path.ends_with(".parquet") || path.ends_with(".csv")
    } else if uri.starts_with("s3://") {
        true
    } else {
        // postgres://, bigquery://, mysql://, etc. need staging
        false
    }
}

async fn create_run(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateRunRequest>,
) -> Result<Json<CreateRunResponse>, (StatusCode, String)> {
    if let Err(errors) = kalla_recipe::validate_recipe(&req.recipe) {
        let error_msg = errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Invalid recipe: {}", error_msg),
        ));
    }

    let nats = state.nats.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "NATS not configured — cannot dispatch reconciliation jobs".to_string(),
        )
    })?;

    let metadata = RunMetadata::new(
        req.recipe.recipe_id.clone(),
        req.recipe.sources.left.uri.clone(),
        req.recipe.sources.right.uri.clone(),
    );

    let run_id = metadata.run_id;

    state
        .evidence_store
        .init_run(&metadata)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    state.runs.write().await.push(metadata);

    info!("Created run {}", run_id);

    // Classify sources
    let left_uri = &req.recipe.sources.left.uri;
    let right_uri = &req.recipe.sources.right.uri;
    let left_alias = &req.recipe.sources.left.alias;
    let right_alias = &req.recipe.sources.right.alias;
    let left_native = is_native_source(left_uri);
    let right_native = is_native_source(right_uri);

    let recipe_json = serde_json::to_string(&req.recipe)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let needs_staging = !left_native || !right_native;

    if needs_staging {
        // Count non-native sources that need staging
        let mut staging_sources = Vec::new();
        if !left_native {
            staging_sources.push((left_uri.clone(), left_alias.clone()));
        }
        if !right_native {
            staging_sources.push((right_uri.clone(), right_alias.clone()));
        }

        let total_chunks = staging_sources.len() as i32;

        // Create run_staging_tracker in Postgres
        if let Some(pool) = &state.db_pool {
            sqlx::query("INSERT INTO run_staging_tracker (run_id, total_chunks) VALUES ($1, $2)")
                .bind(run_id)
                .bind(total_chunks)
                .execute(pool)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }

        // Build staged_sources list for the eventual exec job
        let staged_sources: Vec<StagedSource> = vec![
            StagedSource {
                alias: left_alias.clone(),
                s3_path: left_uri.clone(),
                is_native: left_native,
            },
            StagedSource {
                alias: right_alias.clone(),
                s3_path: right_uri.clone(),
                is_native: right_native,
            },
        ];

        // Create a pending exec job in Postgres (workers will publish it after staging)
        let exec_job_id = Uuid::new_v4();
        let exec_msg = JobMessage::Exec {
            job_id: exec_job_id,
            run_id,
            recipe_json: recipe_json.clone(),
            staged_sources,
        };
        let exec_payload = serde_json::to_value(&exec_msg)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        if let Some(pool) = &state.db_pool {
            sqlx::query(
                "INSERT INTO jobs (job_id, run_id, job_type, status, payload) VALUES ($1, $2, 'exec', 'pending', $3)",
            )
            .bind(exec_job_id)
            .bind(run_id)
            .bind(&exec_payload)
            .execute(pool)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }

        // Publish StagePlan jobs for non-native sources
        for (source_uri, source_alias) in &staging_sources {
            let stage_job_id = Uuid::new_v4();
            let stage_msg = JobMessage::StagePlan {
                job_id: stage_job_id,
                run_id,
                source_uri: source_uri.clone(),
                source_alias: source_alias.clone(),
                partition_key: None,
            };

            // Insert job row in Postgres
            if let Some(pool) = &state.db_pool {
                let stage_payload = serde_json::to_value(&stage_msg)
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                sqlx::query(
                    "INSERT INTO jobs (job_id, run_id, job_type, status, payload) VALUES ($1, $2, 'stage_plan', 'pending', $3)",
                )
                .bind(stage_job_id)
                .bind(run_id)
                .bind(&stage_payload)
                .execute(pool)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            }

            nats.publish_stage(&stage_msg)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

            info!(
                "Published StagePlan job {} for source '{}' (run {})",
                stage_job_id, source_alias, run_id
            );
        }
    } else {
        // All sources are native — push Exec job directly
        let exec_job_id = Uuid::new_v4();
        let staged_sources = vec![
            StagedSource {
                alias: left_alias.clone(),
                s3_path: left_uri.clone(),
                is_native: true,
            },
            StagedSource {
                alias: right_alias.clone(),
                s3_path: right_uri.clone(),
                is_native: true,
            },
        ];

        let exec_msg = JobMessage::Exec {
            job_id: exec_job_id,
            run_id,
            recipe_json,
            staged_sources,
        };

        if let Some(pool) = &state.db_pool {
            let exec_payload = serde_json::to_value(&exec_msg)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            sqlx::query(
                "INSERT INTO jobs (job_id, run_id, job_type, status, payload) VALUES ($1, $2, 'exec', 'pending', $3)",
            )
            .bind(exec_job_id)
            .bind(run_id)
            .bind(&exec_payload)
            .execute(pool)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }

        nats.publish_exec(&exec_msg)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        info!(
            "Published Exec job {} directly (all native sources, run {})",
            exec_job_id, run_id
        );
    }

    Ok(Json(CreateRunResponse {
        run_id,
        status: "submitted".to_string(),
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

async fn list_runs(State(state): State<Arc<AppState>>) -> Json<Vec<RunSummary>> {
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
