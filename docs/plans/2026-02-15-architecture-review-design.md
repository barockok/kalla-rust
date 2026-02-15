# Kalla Architecture Review — Design Document

**Date:** 2026-02-15
**Status:** Approved
**Supersedes:** 2026-02-14-worker-autoscaling-design.md (deployment model), current recipe schema

## Goal

Redesign kalla's architecture around two principles: (1) simplify deployment to two components with single-VM and scaled modes, and (2) simplify the recipe model to raw DataFusion SQL built through conversation.

## Target Users

- Product Managers on financial products
- Finance/Accountants in fintech companies
- Non-technical — the most technical task is setting up a data source (with help)

---

## Architecture Overview

### Two Deployable Units

| Component | Tech | Role |
|-----------|------|------|
| **Kalla App** | Next.js | Web UI + API (agentic orchestrator, CRUD, Postgres) |
| **Kalla Worker** | Rust | Staging (any source → Parquet) + DataFusion execution |

### Two Deployment Modes

**Single mode** — one VM, no autoscaling:
- App talks to Worker via HTTP (Worker's Axum server)
- Worker writes staged Parquet to local filesystem via `object_store` local backend
- No NATS, no MinIO/S3, no Ballista
- Components: Next.js app + Rust worker + Postgres

**Scaled mode** — K8s with autoscaling:
- App publishes jobs to NATS JetStream
- Multiple Workers consume from NATS, write to S3/GCS
- Ballista scheduler + executors for distributed DataFusion
- HPA/KEDA scales workers based on queue depth
- Components: Next.js app + N x Rust workers + Postgres + NATS + S3/GCS + Ballista

**The Worker binary is the same in both modes.** It detects its mode from environment variables:
- `NATS_URL` present → scaled mode (consume from NATS)
- `NATS_URL` absent → single mode (accept jobs via HTTP)
- `AWS_ENDPOINT_URL` / `GCS_BUCKET` present → use S3/GCS
- Neither → use local filesystem via `object_store` local backend

---

## Recipe Schema Redesign

### Current (being replaced)

Complex schema with `match_rules[]`, operators (`eq`, `tolerance`, `contains`), patterns (`1:1`, `1:N`), conditions, thresholds, priorities. A `Transpiler` converts all of this to SQL.

### New Schema

```json
{
  "recipe_id": "monthly-invoice-payment",
  "name": "Invoice-Payment Reconciliation",
  "description": "Match invoices to payments by reference number and amount within 1% tolerance",
  "sources": {
    "left": {
      "alias": "invoices",
      "type": "postgres",
      "uri": "postgres://host/db?table=invoices",
      "primary_key": ["invoice_id"]
    },
    "right": {
      "alias": "payments",
      "type": "file",
      "schema": ["payment_id", "reference_number", "amount", "date"],
      "primary_key": ["payment_id"]
    }
  },
  "match_sql": "SELECT i.invoice_id, p.payment_id, i.amount AS left_amount, p.amount AS right_amount FROM invoices i JOIN payments p ON i.invoice_id = p.reference_number AND ABS(i.amount - p.amount) / NULLIF(i.amount, 0) < 0.01",
  "match_description": "Matches invoices to payments where the reference numbers are identical and the amounts are within 1% of each other."
}
```

### Key Changes

- `match_rules[]` → single `match_sql` string (DataFusion SQL)
- `match_description` — human-readable explanation shown in UI alongside collapsible SQL
- Source `type: "file"` stores expected `schema` (column names) instead of a URI. At execution time, UI prompts user to upload a file matching that schema.
- Source `type: "postgres"` (or `bigquery`, `elasticsearch`) stores the persistent URI.
- No `output` config — kalla always produces matched + auto-derives unmatched_left and unmatched_right using primary keys (LEFT ANTI JOIN).
- Users never write SQL directly — only the agentic conversation produces it.

---

## Data Flow

### Recipe Building (Conversation-Driven)

```
User: "I need to match our invoices against bank payments each month"
  |
Agent: asks about data sources
  |
User: "Invoices are in our Postgres DB, payments come as a CSV from the bank"
  |
Agent: connects to Postgres, inspects schema, asks user to upload sample CSV
  |
User: uploads sample CSV
  |
Agent: inspects CSV columns, proposes match logic in natural language
  |
User: "Yes but allow 1% tolerance on amounts"
  |
Agent: generates match_sql, shows SQL with explanation
  |
Agent: calls Worker to do a dry-run on sample data
  |
Agent: shows preview — "7 of 10 invoices matched, 3 unmatched"
  |
User: "Looks good, save it"
  |
Agent: saves recipe to Postgres (type:"file" for CSV, schema captured)
```

### File Handling

- **During recipe building:** User uploads CSV → Next.js stores as temp file in object storage → passed to Worker for dry-run → file discarded after session
- **During recipe execution:** UI checks recipe sources. For any `type: "file"` source, prompts user to upload. Validates uploaded file columns match the stored schema.
- **Files are always disposable.** The recipe stores the expected schema, never the file itself. Each execution requires fresh files.
- **Persistent sources** (Postgres, BigQuery, Elasticsearch) are stored as registered data sources. The recipe references them by alias. No re-upload needed.

### Validation Before Enqueuing

- File sources: UI validates uploaded file has the required columns
- URL sources: Agentic API verifies the Worker can access the URL before starting the run
- Persistent sources: Worker confirms connectivity during staging

### Run Execution

1. **File prompt** — UI checks recipe sources, prompts upload for `type: "file"` sources, validates schema.

2. **Upload & enqueue** — Uploaded files go to object storage. Next.js API creates run record in Postgres, dispatches to Worker (HTTP in single mode, NATS in scaled mode).

3. **Staging** — Worker stages all non-Parquet sources to Parquet:
   - CSV → read and write as Parquet
   - Postgres/BigQuery/Elasticsearch → extract rows, write as Parquet
   - Parquet sources → skip staging
   - Reports progress: `{"stage": "staging", "source": "invoices", "progress": 0.65}`

4. **Execution** — Worker runs `match_sql` against staged Parquet via DataFusion:
   - Registers all staged Parquets as DataFusion tables
   - Executes match SQL → produces matched set
   - Derives unmatched_left and unmatched_right via LEFT ANTI JOIN on primary keys
   - Reports progress: `{"stage": "matching", "matched": 847, "total_left": 1000}`

5. **Results** — Worker writes matched, unmatched_left, unmatched_right as Parquet to object storage. Updates run status via API callback with final counts.

---

## Component Boundaries

### Next.js API Owns All State

- Postgres: sources, recipes, runs, run_progress
- Object storage management: presigned upload URLs, file lifecycle
- NATS publishing (scaled mode only)
- REST endpoints for both frontend and Worker callbacks

### Rust Worker is Stateless Compute

- Receives self-contained job payload via HTTP (single) or NATS (scaled)
- Reads source data from connectors (Postgres data sources, object storage)
- Writes result Parquet to object storage
- Reports progress back to Next.js API via HTTP callbacks
- **No direct Postgres connection to the app database**

### Job Payload (Self-Contained)

```json
{
  "run_id": "uuid",
  "callback_url": "http://api:3000/api/worker/progress",
  "match_sql": "SELECT ... FROM left JOIN right ON ...",
  "sources": [
    {"alias": "invoices", "uri": "postgres://datahost/db?table=invoices"},
    {"alias": "payments", "uri": "s3://staging/tmp-abc123.csv"}
  ],
  "output_path": "s3://results/runs/uuid/",
  "primary_keys": {
    "invoices": ["invoice_id"],
    "payments": ["payment_id"]
  }
}
```

### Worker → API Callbacks

- `POST /api/worker/progress` — staging %, matching counts
- `POST /api/worker/complete` — final counts, output paths
- `POST /api/worker/error` — failure details

---

## Codebase Migration

### Stays in Rust (kalla-worker)

- `kalla-core` — ReconciliationEngine, DataFusion setup, UDFs
- `kalla-connectors` — PostgresConnector (future: BigQuery, Elasticsearch)
- `kalla-evidence` — Parquet evidence writing
- `kalla-worker` — staging, execution, health/metrics
- New: HTTP job submission endpoint (single mode)
- New: simplified executor that runs `match_sql` directly (no transpiler)

### Moves to Next.js (kalla-web absorbs kalla-server)

- All REST API endpoints: `/api/sources`, `/api/recipes`, `/api/runs`
- Postgres CRUD (sources, recipes, runs, run_progress)
- NATS publishing (scaled mode)
- Recipe validation (check SQL is parseable)
- File upload handling
- Run orchestration (create run → dispatch → track progress)

### Gets Deleted

- `kalla-server` crate — responsibilities split between Next.js and Worker
- `kalla-recipe` Transpiler, operator-based MatchRule, MatchCondition, ComparisonOp
- `kalla-ai` crate — LLM integration moves to Next.js (already has agentic orchestrator in TypeScript)
- `kalla-cli` crate — dev tool, not part of the product

### Gets Simplified

- `kalla-recipe` shrinks to schema types only (recipe with `match_sql`, source definitions) or gets removed entirely with types living as TypeScript in Next.js + a simple Rust struct in the Worker for deserialization

---

## DataFusion: Parquet Only

- DataFusion only works with Parquet files
- Any non-Parquet source goes through the staging pipeline first
- `object_store` crate provides seamless provider selection:
  - Local filesystem (single mode, no config needed)
  - S3 (with `AWS_ENDPOINT_URL`)
  - GCS (with `GCS_BUCKET`)
- The Worker code always uses the same `object_store` interface regardless of backend

---

## Benchmarking

### Trigger

CI benchmark runs only when commit message contains `[perform-benchmark]`. Scripts also run independently for controlled environments.

### Repository Structure

```
benchmarks/
  generate_data.py          # Generate test CSV/Parquet with N rows
  seed_postgres.py          # Seed a Postgres table with N rows
  run_benchmark.sh          # Entry point with args, runs scenarios, outputs report
  scenarios/
    csv_10k.json            # Stage 10k CSV -> Parquet
    csv_100k.json           # Stage 100k CSV -> Parquet
    postgres_10k.json       # Stage 10k Postgres rows -> Parquet
    postgres_100k.json      # Stage 100k Postgres rows -> Parquet
```

### What Gets Measured

1. **Staging speed** — time to convert source to Parquet (CSV 10k, 100k; Postgres 10k, 100k)
2. **Match speed** — time to execute match SQL on staged Parquet (10k x 10k, 100k x 100k)
3. **Memory peak** — RSS during staging and execution

### Output

Markdown report table with timings, written to `benchmarks/results/` and printed to CI summary.

### Usage

```bash
# Generate test data
python benchmarks/generate_data.py --rows 50000 --output /tmp/test.csv

# Run all benchmarks
./benchmarks/run_benchmark.sh --scenarios all

# Run specific scenario
./benchmarks/run_benchmark.sh --scenarios csv_10k
```

---

## Deployment Documentation

### 1. Development (local)

```bash
# Start dependencies
docker compose up postgres -d

# Run Worker
cd crates/kalla-worker && cargo run

# Run Next.js (web + API)
cd kalla-web && npm run dev
```

Worker uses local filesystem for staging (`./staging/`). Next.js calls Worker at `http://localhost:9090`. No NATS, no S3, no Ballista.

### 2. Single VM (production, small scale)

```yaml
# docker-compose.single.yml
services:
  app:        # Next.js (web + API), port 3000
  worker:     # Rust worker, port 9090
  postgres:   # App database
```

Worker uses local filesystem or mounted volume. App calls Worker via HTTP. Suitable for datasets up to ~1M rows. One `docker compose up` and done.

### 3. Scaled (K8s, large scale)

```yaml
# docker-compose.scaled.yml or K8s manifests
services:
  app:        # Next.js (web + API)
  worker:     # N replicas, consumes from NATS
  postgres:   # App database
  nats:       # Job queue (JetStream)
  s3:         # Object storage (or cloud S3/GCS)
  ballista:   # Distributed DataFusion (scheduler + executors)
```

App publishes jobs to NATS. Workers autoscale via HPA/KEDA. Ballista distributes heavy queries. S3/GCS for staging and results.

### Environment Variables

| Variable | Single | Scaled | Description |
|----------|--------|--------|-------------|
| `DATABASE_URL` | Required | Required | App Postgres connection |
| `WORKER_URL` | Required | - | Worker HTTP endpoint (single mode) |
| `NATS_URL` | - | Required | NATS broker (enables scaled mode) |
| `AWS_ENDPOINT_URL` | - | Required | S3 endpoint |
| `AWS_ACCESS_KEY_ID` | - | Required | S3 credentials |
| `GCS_BUCKET` | - | Alternative | GCS instead of S3 |
| `STAGING_PATH` | Optional | - | Local staging dir (default: `./staging/`) |
| `ANTHROPIC_API_KEY` | Required | Required | For agentic recipe builder |
