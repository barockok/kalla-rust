//! Kalla CLI - Command-line interface for the reconciliation engine
//!
//! NOTE: This crate is scheduled for deletion. It has been updated to compile
//! against the new simplified recipe schema but functionality is reduced.

use anyhow::Result;
use clap::{Parser, Subcommand};
use kalla_core::ReconciliationEngine;
use kalla_evidence::{EvidenceStore, MatchedRecord, RunMetadata, UnmatchedRecord};
use kalla_recipe::{validate_recipe, Recipe};
use std::path::PathBuf;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "kalla")]
#[command(about = "Universal Reconciliation Engine - Reconcile data using natural language")]
#[command(version)]
struct Cli {
    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a reconciliation using a recipe file
    Reconcile {
        /// Path to the recipe JSON file
        #[arg(short, long)]
        recipe: PathBuf,

        /// Output directory for evidence files
        #[arg(short, long, default_value = "./evidence")]
        output_dir: PathBuf,
    },

    /// Validate a recipe file
    ValidateRecipe {
        /// Path to the recipe JSON file
        recipe: PathBuf,
    },

    /// Generate a recipe from natural language (requires LLM API key)
    GenerateRecipe {
        /// Comma-separated paths to data sources
        #[arg(short, long)]
        sources: String,

        /// Natural language description of the reconciliation
        #[arg(short, long)]
        prompt: String,

        /// Output path for the generated recipe
        #[arg(short, long, default_value = "recipe.json")]
        output: PathBuf,
    },

    /// Show a summary report for a reconciliation run
    Report {
        /// Path to the evidence directory (or specific run)
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Set up logging
    let level = if cli.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    match cli.command {
        Commands::Reconcile { recipe, output_dir } => {
            run_reconciliation(&recipe, &output_dir).await?;
        }
        Commands::ValidateRecipe { recipe } => {
            validate_recipe_file(&recipe)?;
        }
        Commands::GenerateRecipe {
            sources,
            prompt,
            output,
        } => {
            generate_recipe(&sources, &prompt, &output).await?;
        }
        Commands::Report { path } => {
            show_report(&path)?;
        }
    }

    Ok(())
}

async fn run_reconciliation(recipe_path: &PathBuf, output_dir: &PathBuf) -> Result<()> {
    info!("Loading recipe from {:?}", recipe_path);

    // Load and validate recipe
    let recipe_content = std::fs::read_to_string(recipe_path)?;
    let recipe: Recipe = serde_json::from_str(&recipe_content)?;

    if let Err(errors) = validate_recipe(&recipe) {
        for error in &errors {
            eprintln!("Validation error: {}", error);
        }
        anyhow::bail!("Recipe validation failed with {} errors", errors.len());
    }

    info!("Recipe validated successfully");

    // Initialize engine and evidence store
    let engine = ReconciliationEngine::new();
    let store = EvidenceStore::new(output_dir)?;

    // Initialize run metadata
    let left_uri = recipe
        .sources
        .left
        .uri
        .as_deref()
        .unwrap_or("(file upload)");
    let right_uri = recipe
        .sources
        .right
        .uri
        .as_deref()
        .unwrap_or("(file upload)");
    let mut metadata = RunMetadata::new(
        recipe.recipe_id.clone(),
        left_uri.to_string(),
        right_uri.to_string(),
    );
    let run_path = store.init_run(&metadata)?;
    info!("Run ID: {}", metadata.run_id);

    // Register data sources
    register_source(&engine, &recipe.sources.left.alias, left_uri).await?;
    register_source(&engine, &recipe.sources.right.alias, right_uri).await?;

    // Execute match SQL directly
    info!("Executing match SQL");
    let mut all_matched: Vec<MatchedRecord> = Vec::new();
    let df = engine.sql(&recipe.match_sql).await?;
    let batches = df.collect().await?;

    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    info!("Match SQL produced {} rows", row_count);

    for batch in &batches {
        for i in 0..batch.num_rows() {
            all_matched.push(MatchedRecord::new(
                format!("row_{}", i),
                format!("row_{}", i),
                "match_sql".to_string(),
                1.0,
            ));
        }
    }

    metadata.matched_count = all_matched.len() as u64;

    // Derive unmatched using primary keys via ANTI JOIN
    let left_alias = &recipe.sources.left.alias;
    let right_alias = &recipe.sources.right.alias;
    let left_pk = &recipe.sources.left.primary_key;
    let right_pk = &recipe.sources.right.primary_key;

    let mut left_orphans: Vec<UnmatchedRecord> = Vec::new();
    let mut right_orphans: Vec<UnmatchedRecord> = Vec::new();

    if !left_pk.is_empty() && !right_pk.is_empty() {
        // Left unmatched
        let left_pk_cols = left_pk.join(", ");
        let right_pk_cols = right_pk.join(", ");
        let left_orphan_sql = format!(
            "SELECT {left_alias}.* FROM {left_alias} \
             LEFT JOIN {right_alias} ON {left_alias}.{lpk} = {right_alias}.{rpk} \
             WHERE {right_alias}.{rpk} IS NULL",
            left_alias = left_alias,
            right_alias = right_alias,
            lpk = left_pk_cols,
            rpk = right_pk_cols,
        );
        info!("Finding left orphans...");
        if let Ok(df) = engine.sql(&left_orphan_sql).await {
            if let Ok(batches) = df.collect().await {
                let count: usize = batches.iter().map(|b| b.num_rows()).sum();
                info!("Found {} left orphans", count);
                for batch in &batches {
                    for i in 0..batch.num_rows() {
                        left_orphans.push(UnmatchedRecord {
                            record_key: format!("row_{}", i),
                            attempted_rules: vec!["match_sql".to_string()],
                            closest_candidate: None,
                            rejection_reason: "No matching record found".to_string(),
                        });
                    }
                }
            }
        }

        // Right unmatched
        let right_orphan_sql = format!(
            "SELECT {right_alias}.* FROM {right_alias} \
             LEFT JOIN {left_alias} ON {right_alias}.{rpk} = {left_alias}.{lpk} \
             WHERE {left_alias}.{lpk} IS NULL",
            left_alias = left_alias,
            right_alias = right_alias,
            lpk = left_pk_cols,
            rpk = right_pk_cols,
        );
        info!("Finding right orphans...");
        if let Ok(df) = engine.sql(&right_orphan_sql).await {
            if let Ok(batches) = df.collect().await {
                let count: usize = batches.iter().map(|b| b.num_rows()).sum();
                info!("Found {} right orphans", count);
                for batch in &batches {
                    for i in 0..batch.num_rows() {
                        right_orphans.push(UnmatchedRecord {
                            record_key: format!("row_{}", i),
                            attempted_rules: vec!["match_sql".to_string()],
                            closest_candidate: None,
                            rejection_reason: "No matching record found".to_string(),
                        });
                    }
                }
            }
        }
    }

    metadata.unmatched_left_count = left_orphans.len() as u64;
    metadata.unmatched_right_count = right_orphans.len() as u64;

    // Write evidence files
    if !all_matched.is_empty() {
        store.write_matched(&metadata.run_id, &all_matched)?;
    }
    if !left_orphans.is_empty() {
        store.write_unmatched(&metadata.run_id, &left_orphans, "left")?;
    }
    if !right_orphans.is_empty() {
        store.write_unmatched(&metadata.run_id, &right_orphans, "right")?;
    }

    // Complete the run
    metadata.complete();
    store.update_metadata(&metadata)?;

    println!("\n=== Reconciliation Complete ===");
    println!("Run ID: {}", metadata.run_id);
    println!("Matched: {}", metadata.matched_count);
    println!("Unmatched Left: {}", metadata.unmatched_left_count);
    println!("Unmatched Right: {}", metadata.unmatched_right_count);
    println!("Results: {:?}", run_path);

    Ok(())
}

async fn register_source(engine: &ReconciliationEngine, alias: &str, uri: &str) -> Result<()> {
    if uri.starts_with("file://") {
        let path = uri.strip_prefix("file://").unwrap();
        if path.ends_with(".csv") {
            engine.register_csv(alias, path).await?;
            info!("Registered CSV '{}' as '{}'", path, alias);
        } else if path.ends_with(".parquet") {
            engine.register_parquet(alias, path).await?;
            info!("Registered Parquet '{}' as '{}'", path, alias);
        } else {
            anyhow::bail!("Unsupported file format: {}", path);
        }
    } else if uri.starts_with("postgres://") {
        anyhow::bail!(
            "Postgres support requires connection string parsing - not yet implemented in CLI"
        );
    } else {
        anyhow::bail!("Unsupported URI scheme: {}", uri);
    }

    Ok(())
}

fn validate_recipe_file(path: &PathBuf) -> Result<()> {
    info!("Validating recipe: {:?}", path);

    let content = std::fs::read_to_string(path)?;
    let recipe: Recipe = serde_json::from_str(&content)?;

    match validate_recipe(&recipe) {
        Ok(()) => {
            println!("Recipe is valid!");
            println!("  Recipe ID: {}", recipe.recipe_id);
            println!("  Name: {}", recipe.name);
            println!("  Match SQL: {}...", &recipe.match_sql[..recipe.match_sql.len().min(80)]);
            Ok(())
        }
        Err(errors) => {
            eprintln!("Recipe validation failed:");
            for error in &errors {
                eprintln!("  - {}", error);
            }
            anyhow::bail!("Validation failed with {} errors", errors.len());
        }
    }
}

async fn generate_recipe(sources: &str, prompt: &str, output: &PathBuf) -> Result<()> {
    use kalla_ai::prompt::{build_user_prompt, parse_recipe_response, SYSTEM_PROMPT};
    use kalla_ai::{extract_schema, LlmClient};

    info!("Generating recipe from natural language...");

    // Parse source paths
    let source_paths: Vec<&str> = sources.split(',').map(|s| s.trim()).collect();
    if source_paths.len() != 2 {
        anyhow::bail!(
            "Expected exactly 2 sources (comma-separated), got {}",
            source_paths.len()
        );
    }

    let left_path = source_paths[0];
    let right_path = source_paths[1];

    // Create engine and register sources
    let engine = ReconciliationEngine::new();

    // Register as CSV for now
    engine.register_csv("left", left_path).await?;
    engine.register_csv("right", right_path).await?;

    // Extract schemas (PII-safe)
    let left_schema = extract_schema(engine.context(), "left").await?;
    let right_schema = extract_schema(engine.context(), "right").await?;

    info!(
        "Extracted schemas: {} columns left, {} columns right",
        left_schema.columns.len(),
        right_schema.columns.len()
    );

    // Build prompt
    let left_uri = format!("file://{}", left_path);
    let right_uri = format!("file://{}", right_path);
    let user_prompt = build_user_prompt(&left_schema, &right_schema, prompt, &left_uri, &right_uri);

    // Call LLM
    let client = LlmClient::from_env()?;
    info!("Calling LLM API...");
    let response = client.generate(SYSTEM_PROMPT, &user_prompt).await?;

    // Parse response
    let recipe = parse_recipe_response(&response)?;

    // Validate
    if let Err(errors) = validate_recipe(&recipe) {
        eprintln!("Warning: Generated recipe has validation issues:");
        for error in &errors {
            eprintln!("  - {}", error);
        }
    }

    // Write to file
    let json = serde_json::to_string_pretty(&recipe)?;
    std::fs::write(output, &json)?;

    println!("\n=== Generated Recipe ===");
    println!("{}", json);
    println!("\nSaved to: {:?}", output);
    println!("\nPlease review the recipe before running reconciliation.");

    Ok(())
}

fn show_report(path: &PathBuf) -> Result<()> {
    // Check if this is a specific run or the evidence root
    let metadata_path = if path.join("metadata.json").exists() {
        path.join("metadata.json")
    } else if path.join("runs").exists() {
        // Find latest run
        let store = EvidenceStore::new(path)?;
        if let Some(latest) = store.latest_run()? {
            latest.join("metadata.json")
        } else {
            anyhow::bail!("No runs found in {:?}", path);
        }
    } else {
        anyhow::bail!("Invalid evidence path: {:?}", path);
    };

    let content = std::fs::read_to_string(&metadata_path)?;
    let metadata: RunMetadata = serde_json::from_str(&content)?;

    println!("\n=== Reconciliation Report ===");
    println!("Run ID: {}", metadata.run_id);
    println!("Recipe: {}", metadata.recipe_id);
    println!("Status: {:?}", metadata.status);
    println!();
    println!("Started: {}", metadata.started_at);
    if let Some(completed) = metadata.completed_at {
        println!("Completed: {}", completed);
    }
    println!();
    println!("Sources:");
    println!(
        "  Left:  {} ({} records)",
        metadata.left_source, metadata.left_record_count
    );
    println!(
        "  Right: {} ({} records)",
        metadata.right_source, metadata.right_record_count
    );
    println!();
    println!("Results:");
    println!("  Matched:         {}", metadata.matched_count);
    println!("  Unmatched Left:  {}", metadata.unmatched_left_count);
    println!("  Unmatched Right: {}", metadata.unmatched_right_count);

    let total_left = metadata.matched_count + metadata.unmatched_left_count;
    if total_left > 0 {
        let match_rate = (metadata.matched_count as f64 / total_left as f64) * 100.0;
        println!("\n  Match Rate: {:.1}%", match_rate);
    }

    Ok(())
}
