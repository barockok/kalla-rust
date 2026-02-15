# Architecture Review Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Restructure kalla into two deployable units (Next.js app + Rust worker) with simplified SQL-based recipes and single/scaled deployment modes.

**Architecture:** Next.js absorbs all API endpoints from kalla-server. Rust worker becomes stateless compute with HTTP (single mode) and NATS (scaled mode) job input. Recipe schema simplified to raw DataFusion SQL.

**Tech Stack:** Next.js 16, TypeScript, pg (node-postgres), Rust, Axum, DataFusion, object_store, async-nats

**Design doc:** `docs/plans/2026-02-15-architecture-review-design.md`

---

## Parallel Worktree Structure

| Worktree | Branch | Agent | Focus |
|----------|--------|-------|-------|
| wt-recipe | feat/new-recipe-schema | schema | New recipe types (Rust + TypeScript) |
| wt-worker | feat/worker-http | forge | Worker HTTP mode + stateless refactor |
| wt-api | feat/nextjs-api | api | Next.js API routes (absorb kalla-server) |
| wt-deploy | feat/deployment | ops | docker-compose files + deployment docs |

---

### Task 1: New Recipe Schema (wt-recipe)

**Files:**
- Modify: `crates/kalla-recipe/src/schema.rs`
- Modify: `crates/kalla-recipe/src/lib.rs`
- Delete: `crates/kalla-recipe/src/transpiler.rs`
- Create: `kalla-web/src/lib/recipe-types.ts`
- Modify: `crates/kalla-recipe/src/validation.rs` (if exists)

**Step 1: Define new Rust recipe types**

Replace the current `MatchRecipe` in `crates/kalla-recipe/src/schema.rs`:

```rust
use serde::{Deserialize, Serialize};

/// Source type determines how data is provided at execution time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    /// Persistent remote source (Postgres, BigQuery, Elasticsearch)
    Postgres,
    Bigquery,
    Elasticsearch,
    /// Disposable file source — schema stored, file uploaded each execution
    File,
}

/// A data source in a recipe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipeSource {
    pub alias: String,
    #[serde(rename = "type")]
    pub source_type: SourceType,
    /// Connection URI for persistent sources (postgres://, bigquery://)
    /// None for file sources (file is uploaded at execution time)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// Expected column names — required for file sources, optional for persistent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Vec<String>>,
    /// Primary key columns for deriving unmatched records
    pub primary_key: Vec<String>,
}

/// Recipe sources (left and right).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipeSources {
    pub left: RecipeSource,
    pub right: RecipeSource,
}

/// A reconciliation recipe — the core configuration unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recipe {
    pub recipe_id: String,
    pub name: String,
    pub description: String,
    /// The SQL query that DataFusion executes to produce matched records.
    /// References source aliases as table names.
    pub match_sql: String,
    /// Human-readable explanation of what the SQL does.
    pub match_description: String,
    /// Left and right data sources.
    pub sources: RecipeSources,
}
```

**Step 2: Define TypeScript recipe types**

Create `kalla-web/src/lib/recipe-types.ts`:

```typescript
export type SourceType = 'postgres' | 'bigquery' | 'elasticsearch' | 'file';

export interface RecipeSource {
  alias: string;
  type: SourceType;
  uri?: string;           // for persistent sources
  schema?: string[];      // expected columns (required for file sources)
  primary_key: string[];
}

export interface RecipeSources {
  left: RecipeSource;
  right: RecipeSource;
}

export interface Recipe {
  recipe_id: string;
  name: string;
  description: string;
  match_sql: string;
  match_description: string;
  sources: RecipeSources;
}

export interface JobPayload {
  run_id: string;
  callback_url: string;
  match_sql: string;
  sources: ResolvedSource[];
  output_path: string;
  primary_keys: Record<string, string[]>;
}

export interface ResolvedSource {
  alias: string;
  uri: string;  // always resolved at execution time
}

export interface WorkerProgress {
  run_id: string;
  stage: 'staging' | 'matching' | 'writing_results';
  source?: string;
  progress?: number;
  matched_count?: number;
  total_left?: number;
  total_right?: number;
}

export interface WorkerComplete {
  run_id: string;
  matched_count: number;
  unmatched_left_count: number;
  unmatched_right_count: number;
  output_paths: {
    matched: string;
    unmatched_left: string;
    unmatched_right: string;
  };
}

export interface WorkerError {
  run_id: string;
  error: string;
  stage: string;
}
```

**Step 3: Update kalla-recipe lib.rs**

Remove `Transpiler` re-exports, update `validate_recipe` to validate the new schema (check SQL is non-empty, sources have primary keys, file sources have schema).

**Step 4: Delete transpiler**

Remove `crates/kalla-recipe/src/transpiler.rs` and all references.

**Step 5: Update tests**

Update existing recipe tests to use new schema. Verify serialization/deserialization round-trips.

**Step 6: Run tests**

```bash
cargo test -p kalla-recipe
```

**Step 7: Commit**

```bash
git add -A && git commit -m "feat: simplified recipe schema with raw SQL match rules"
```

---

### Task 2: Worker HTTP Mode + Stateless Refactor (wt-worker)

**Files:**
- Modify: `crates/kalla-worker/src/main.rs`
- Create: `crates/kalla-worker/src/http_api.rs`
- Modify: `crates/kalla-worker/src/exec.rs`
- Modify: `crates/kalla-worker/src/stage.rs`
- Modify: `crates/kalla-worker/src/job_loop.rs`
- Modify: `crates/kalla-worker/src/config.rs`
- Modify: `crates/kalla-worker/src/health.rs`
- Modify: `crates/kalla-worker/Cargo.toml`

**Step 1: Add HTTP job submission types**

Create `crates/kalla-worker/src/http_api.rs`:

```rust
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
pub struct JobRequest {
    pub run_id: Uuid,
    pub callback_url: String,
    pub match_sql: String,
    pub sources: Vec<ResolvedSource>,
    pub output_path: String,
    pub primary_keys: std::collections::HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResolvedSource {
    pub alias: String,
    pub uri: String,
}

#[derive(Debug, Serialize)]
pub struct JobAccepted {
    pub run_id: Uuid,
    pub status: String,
}

/// POST /api/jobs — accept a job for processing
async fn submit_job(
    State(state): State<Arc<crate::WorkerState>>,
    Json(req): Json<JobRequest>,
) -> Result<Json<JobAccepted>, (StatusCode, String)> {
    let run_id = req.run_id;

    // Send job to the internal processing channel
    state.job_tx.send(req).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to enqueue job: {}", e))
    })?;

    Ok(Json(JobAccepted {
        run_id,
        status: "accepted".to_string(),
    }))
}

pub fn job_router(state: Arc<crate::WorkerState>) -> Router {
    Router::new()
        .route("/api/jobs", post(submit_job))
        .with_state(state)
}
```

**Step 2: Add callback client**

Add a callback module to report progress to the API:

```rust
// In http_api.rs or new callback.rs
use reqwest::Client;

pub struct CallbackClient {
    http: Client,
}

impl CallbackClient {
    pub fn new() -> Self {
        Self { http: Client::new() }
    }

    pub async fn report_progress(&self, callback_url: &str, progress: &serde_json::Value) -> anyhow::Result<()> {
        self.http.post(format!("{}/progress", callback_url))
            .json(progress)
            .send().await?;
        Ok(())
    }

    pub async fn report_complete(&self, callback_url: &str, result: &serde_json::Value) -> anyhow::Result<()> {
        self.http.post(format!("{}/complete", callback_url))
            .json(result)
            .send().await?;
        Ok(())
    }

    pub async fn report_error(&self, callback_url: &str, error: &serde_json::Value) -> anyhow::Result<()> {
        self.http.post(format!("{}/error", callback_url))
            .json(error)
            .send().await?;
        Ok(())
    }
}
```

**Step 3: Add reqwest dependency**

Add `reqwest` to `crates/kalla-worker/Cargo.toml`:
```toml
reqwest = { version = "0.12", features = ["json"] }
```

**Step 4: Refactor main.rs for dual-mode startup**

```rust
// Detect mode from environment
let mode = if config.nats_url.is_some() {
    WorkerMode::Scaled  // consume from NATS
} else {
    WorkerMode::Single  // accept jobs via HTTP
};
```

In single mode: start Axum server with health routes + job submission route, process jobs from an internal mpsc channel.
In scaled mode: connect to NATS, consume from JetStream (existing behavior).

**Step 5: Refactor config.rs**

Make `nats_url` and `database_url` optional:
```rust
pub struct WorkerConfig {
    pub worker_id: String,
    pub nats_url: Option<String>,      // None = single mode
    pub metrics_port: u16,
    pub max_parallel_chunks: usize,
    pub chunk_threshold_rows: u64,
    pub staging_path: String,          // local path or s3:// prefix
    pub callback_url: Option<String>,  // default callback for NATS mode
}
```

**Step 6: Refactor exec.rs — run match_sql directly**

Replace the transpiler-based execution with direct SQL:
- Accept `match_sql` string + `primary_keys` map
- Register sources as DataFusion tables
- Execute `match_sql` → collect matched records
- Derive unmatched_left: `SELECT * FROM left WHERE pk NOT IN (SELECT left_pk FROM matched)`
- Derive unmatched_right: same pattern
- Write all three result sets as Parquet to `output_path`
- Report progress via callback

**Step 7: Remove direct Postgres app-DB dependency**

Remove all `sqlx::query` calls that write to the app database (jobs table, run_staging_tracker). The worker only:
- Reads from data sources (Postgres where business data lives)
- Writes Parquet to object storage
- Reports status via HTTP callbacks

**Step 8: Configure object_store for local/S3**

Use `object_store` with local filesystem backend when no S3 is configured:
```rust
let store: Arc<dyn ObjectStore> = if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
    // S3/MinIO
    Arc::new(AmazonS3Builder::from_env().build()?)
} else {
    // Local filesystem
    Arc::new(LocalFileSystem::new_with_prefix(&config.staging_path)?)
};
```

**Step 9: Run tests and commit**

```bash
cargo test -p kalla-worker
cargo clippy -p kalla-worker -- -D warnings
git add -A && git commit -m "feat: worker dual-mode (HTTP/NATS) with stateless execution"
```

---

### Task 3: Next.js API Routes (wt-api)

**Files:**
- Create: `kalla-web/src/app/api/sources/route.ts`
- Create: `kalla-web/src/app/api/recipes/route.ts`
- Create: `kalla-web/src/app/api/recipes/[id]/route.ts`
- Create: `kalla-web/src/app/api/runs/route.ts`
- Create: `kalla-web/src/app/api/runs/[id]/route.ts`
- Create: `kalla-web/src/app/api/worker/progress/route.ts`
- Create: `kalla-web/src/app/api/worker/complete/route.ts`
- Create: `kalla-web/src/app/api/worker/error/route.ts`
- Create: `kalla-web/src/lib/db.ts`
- Create: `kalla-web/src/lib/worker-client.ts`
- Create: `kalla-web/src/lib/storage.ts`

**Step 1: Set up Postgres client**

Create `kalla-web/src/lib/db.ts`:

```typescript
import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
});

export default pool;
```

**Step 2: Create worker dispatch client**

Create `kalla-web/src/lib/worker-client.ts`:

```typescript
import { JobPayload } from './recipe-types';

const WORKER_URL = process.env.WORKER_URL || 'http://localhost:9090';
const NATS_URL = process.env.NATS_URL;

export async function dispatchJob(payload: JobPayload): Promise<void> {
  if (NATS_URL) {
    // Scaled mode: publish to NATS
    // (use nats.js client)
    throw new Error('NATS dispatch not yet implemented');
  } else {
    // Single mode: HTTP POST to worker
    const res = await fetch(`${WORKER_URL}/api/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      throw new Error(`Worker rejected job: ${await res.text()}`);
    }
  }
}
```

**Step 3: Sources API routes**

Create `kalla-web/src/app/api/sources/route.ts`:

```typescript
// GET /api/sources — list all registered sources
// POST /api/sources — register a new source
```

Port the logic from kalla-server's `list_sources` and `register_source` handlers. Sources are stored in Postgres `sources` table.

**Step 4: Recipes API routes**

Create `kalla-web/src/app/api/recipes/route.ts`:

```typescript
// GET /api/recipes — list all recipes
// POST /api/recipes — save a recipe (new schema with match_sql)
```

Create `kalla-web/src/app/api/recipes/[id]/route.ts`:
```typescript
// GET /api/recipes/:id — get recipe by ID
```

**Step 5: Runs API routes**

Create `kalla-web/src/app/api/runs/route.ts`:

```typescript
// GET /api/runs — list all runs
// POST /api/runs — create a run
//   1. Load recipe
//   2. For file sources: expect resolved URIs in request body
//   3. Build JobPayload with callback_url pointing to this API
//   4. Dispatch to worker (HTTP or NATS)
//   5. Return run_id + status: "submitted"
```

Create `kalla-web/src/app/api/runs/[id]/route.ts`:
```typescript
// GET /api/runs/:id — get run status + results
```

**Step 6: Worker callback routes**

Create `kalla-web/src/app/api/worker/progress/route.ts`:
```typescript
// POST /api/worker/progress — worker reports staging/matching progress
// Updates run_progress table in Postgres
```

Create `kalla-web/src/app/api/worker/complete/route.ts`:
```typescript
// POST /api/worker/complete — worker reports run completion
// Updates runs table with final counts and output paths
```

Create `kalla-web/src/app/api/worker/error/route.ts`:
```typescript
// POST /api/worker/error — worker reports failure
// Updates runs table with error status
```

**Step 7: SQL schema for Next.js managed tables**

Create or update `scripts/init.sql` with tables the Next.js API manages:
- `sources` (existing)
- `recipes` (existing, but schema column updated)
- `runs` (run_id, recipe_id, status, matched_count, unmatched_left_count, unmatched_right_count, output_paths, created_at, updated_at)
- `run_progress` (run_id, stage, progress, matched_count, total_left, total_right, updated_at)

**Step 8: Run frontend tests and commit**

```bash
cd kalla-web && npx tsc --noEmit && npx jest
git add -A && git commit -m "feat: Next.js API routes for sources, recipes, runs, worker callbacks"
```

---

### Task 4: Deployment Configs + Docs (wt-deploy)

**Files:**
- Create: `docker-compose.single.yml`
- Create: `docker-compose.scaled.yml`
- Modify: `docker-compose.yml` (simplify to dev setup)
- Create: `docs/deployment.md`
- Create: `kalla-web/Dockerfile` (update if needed for API routes)
- Modify: `.github/workflows/ci.yml` (update for new architecture)

**Step 1: docker-compose.single.yml**

```yaml
version: '3.8'
services:
  app:
    build:
      context: ./kalla-web
      dockerfile: Dockerfile
    ports:
      - "3000:3000"
    environment:
      - DATABASE_URL=postgres://kalla:kalla_secret@postgres:5432/kalla
      - WORKER_URL=http://worker:9090
      - ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}
    depends_on:
      postgres:
        condition: service_healthy

  worker:
    build:
      context: .
      dockerfile: crates/kalla-worker/Dockerfile
    ports:
      - "9090:9090"
    environment:
      - STAGING_PATH=/data/staging
    volumes:
      - staging_data:/data/staging
      - results_data:/data/results

  postgres:
    image: postgres:16-alpine
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: kalla
      POSTGRES_PASSWORD: kalla_secret
      POSTGRES_DB: kalla
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "kalla"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
  staging_data:
  results_data:
```

**Step 2: docker-compose.scaled.yml**

```yaml
version: '3.8'
services:
  app:
    build:
      context: ./kalla-web
      dockerfile: Dockerfile
    ports:
      - "3000:3000"
    environment:
      - DATABASE_URL=postgres://kalla:kalla_secret@postgres:5432/kalla
      - NATS_URL=nats://nats:4222
      - ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}
    depends_on:
      postgres:
        condition: service_healthy

  worker:
    build:
      context: .
      dockerfile: crates/kalla-worker/Dockerfile
    environment:
      - NATS_URL=nats://nats:4222
      - AWS_ENDPOINT_URL=http://minio:9000
      - AWS_ACCESS_KEY_ID=minioadmin
      - AWS_SECRET_ACCESS_KEY=minioadmin
      - STAGING_BUCKET=kalla-staging
    depends_on:
      - nats
      - minio
    deploy:
      replicas: 2

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: kalla
      POSTGRES_PASSWORD: kalla_secret
      POSTGRES_DB: kalla
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "kalla"]
      interval: 5s
      timeout: 5s
      retries: 5

  nats:
    image: nats:2-alpine
    command: ["--jetstream"]
    ports:
      - "4222:4222"
    volumes:
      - nats_data:/data

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio_data:/data

volumes:
  postgres_data:
  nats_data:
  minio_data:
```

**Step 3: Write deployment documentation**

Create `docs/deployment.md` covering:
- Development setup (local)
- Single VM production deployment
- Scaled K8s deployment
- Environment variable reference table
- Architecture diagrams for each mode

**Step 4: Update CI workflow**

Update `.github/workflows/ci.yml`:
- Integration tests use single-mode (HTTP to worker, no NATS)
- Docker build tests both single and scaled compose files
- Add benchmark job (triggered by `[perform-benchmark]` in commit message)

**Step 5: Commit**

```bash
git add -A && git commit -m "feat: deployment configs for single and scaled modes"
```

---

## Phase 2: Integration & Cleanup (after merge)

### Task 5: Merge and integrate

After all 4 branches merge to main:

1. Delete `kalla-server/` crate entirely
2. Delete `crates/kalla-ai/` crate (LLM moves to TypeScript)
3. Delete `crates/kalla-cli/` crate
4. Remove deleted crates from workspace `Cargo.toml`
5. Update `kalla-web` frontend components to use new API routes
6. Update CI to reflect new architecture
7. Run full test suite
8. Verify both docker-compose files work end-to-end
