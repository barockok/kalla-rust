# Kalla - Universal Reconciliation Engine

Kalla is a high-performance data reconciliation engine built with Rust and powered by Apache DataFusion and Apache Ballista. It matches data across sources using raw SQL — built through conversation with an AI assistant, never written by hand.

## Architecture

![Kalla Architecture](docs/kalla-arch.svg)

> Editable version: open [`docs/architecture.excalidraw`](docs/architecture.excalidraw) in [excalidraw.com](https://excalidraw.com)

### Deployable Components

| Component | Technology | Role |
|-----------|------------|------|
| **Kalla App** | Next.js | Web UI + REST API + agentic recipe builder + file uploads + Postgres CRUD |
| **kallad** | Rust, DataFusion, Ballista | Unified binary — `scheduler` and `executor` subcommands |

### Deployment Modes

**Single node** — one machine, no executors:
- App dispatches jobs to the scheduler via HTTP
- Scheduler detects no executors (10s probe timeout) and falls back to local DataFusion
- Services: App + Scheduler + Postgres + MinIO

**Cluster** — distributed execution with Ballista:
- App dispatches jobs to the scheduler via HTTP
- Scheduler distributes query partitions across executors via gRPC/Arrow Flight
- Executors read source data directly from Postgres using `PostgresScanExec`
- Services: App + Scheduler + N Executors + Postgres

The `kallad` binary is the same in both modes. In single mode the scheduler runs alone; in cluster mode you add executor instances.

### Rust Crates

| Crate | Description |
|-------|-------------|
| `kallad` | Unified binary with `scheduler` and `executor` subcommands |
| `kalla-ballista` | Ballista integration — scheduler, executor, HTTP runner, codecs, lazy scan nodes |
| `kalla-core` | `ReconciliationEngine` wrapping DataFusion + financial UDFs (`tolerance_match`) |
| `kalla-connectors` | Partitioned data source connectors (Postgres, S3 CSV, S3 Parquet). See [connector README](crates/kalla-connectors/README.md) |
| `kalla-recipe` | Recipe schema types (match_sql, sources, primary keys) |
| `kalla-evidence` | Matched record audit trail (Parquet evidence store) |

### How Execution Works

1. App submits a `JobRequest` to the scheduler via `POST /api/jobs`
2. Scheduler registers source tables as `PostgresPartitionedTable` or `CsvByteRangeTable` (partitioned, lazy)
3. Scheduler probes the Ballista cluster with `SELECT 1` (10s timeout)
   - If executors respond: uses Ballista distributed execution
   - If timeout: falls back to local DataFusion
4. Executes `match_sql` — results streamed as Arrow RecordBatches
5. Computes unmatched counts from matched records (in-memory HashSet, no post-match queries)
6. Writes evidence (Parquet) and reports results back to the App via HTTP callback

### Distributed Partitioning & Join Strategy

When running in cluster mode, Ballista executes reconciliation joins across multiple executors in three phases:

**Phase 1 — Partitioned Scan (parallel I/O)**

Each source is divided into partitions for parallel reads. Executors fetch their assigned partitions directly from the data source:

- **Postgres** — `LIMIT/OFFSET` with `ORDER BY ctid` for deterministic row splits
- **S3 CSV** — byte-range reads with line-boundary handling

```
Executor 1: SELECT * FROM invoices ORDER BY ctid LIMIT 250000 OFFSET 0
Executor 2: SELECT * FROM invoices ORDER BY ctid LIMIT 250000 OFFSET 250000
Executor 3: SELECT * FROM payments ORDER BY ctid LIMIT 250000 OFFSET 0
Executor 4: SELECT * FROM payments ORDER BY ctid LIMIT 250000 OFFSET 250000
```

At this point data is partitioned by **position**, not by join key — matching rows could be on different executors.

**Phase 2 — Shuffle / Repartition (colocate by join key)**

Ballista inserts a shuffle exchange (`ShuffleWriterExec` → `ShuffleReaderExec`) that repartitions both sides by the join key hash. This ensures all rows with the same join key end up on the same executor:

```
Before shuffle:                          After shuffle (by join key hash):
┌─────────────────────────────┐          ┌─────────────────────────────┐
│ Exec 1: left rows 0-250K   │          │ Exec 1: all rows where      │
│ Exec 2: left rows 250K-500K│   ──►    │         hash(key) % N == 0  │
│ Exec 3: right rows 0-250K  │          │ Exec 2: all rows where      │
│ Exec 4: right rows 250K+   │          │         hash(key) % N == 1  │
└─────────────────────────────┘          └─────────────────────────────┘

Invoice #12345 (Exec 1) and Payment #12345 (Exec 3) → same executor after shuffle
```

**Phase 3 — Local Join (per executor)**

Each executor now holds all rows for its assigned join keys from both sides. It runs a local `HashJoinExec` — including UDF evaluation like `tolerance_match` — and produces matched pairs. Results are collected back at the scheduler.

```
Physical plan (simplified):

CoalescePartitionsExec          ← collect results at scheduler
└── HashJoinExec                ← join on invoice_id = reference_number
    ├── ShuffleReaderExec       ← left side, repartitioned by join key
    │   └── UnionExec
    │       ├── PostgresScanExec(left, partition 0)   [Executor 1]
    │       └── PostgresScanExec(left, partition 1)   [Executor 2]
    └── ShuffleReaderExec       ← right side, repartitioned by join key
        └── UnionExec
            ├── PostgresScanExec(right, partition 0)  [Executor 3]
            └── PostgresScanExec(right, partition 1)  [Executor 4]
```

This means position-based partitioning is used for **parallel I/O** only. The actual join uses **hash-based repartitioning** so that matching rows are always colocated, regardless of which partition they were originally read from.

## Agentic Recipe Builder

The core of Kalla is a 7-phase conversational agent that guides users from raw data to a working reconciliation recipe:

```mermaid
graph LR
    Greeting --> Intent --> Scoping --> Demonstration --> Inference --> Validation --> Execution
```

| Phase | What happens | Agent tools |
|-------|-------------|-------------|
| **Greeting** | Agent discovers available sources, user uploads files | `list_sources`, `get_source_preview`, `request_file_upload` |
| **Intent** | Confirm left & right sources, preview schemas | `list_sources`, `get_source_preview`, `request_file_upload` |
| **Scoping** | Apply filters, load sample data | `load_scoped`, `get_source_preview`, `request_file_upload` |
| **Demonstration** | Agent proposes record matches, user confirms/rejects via cards | `propose_match`, `get_source_preview` |
| **Inference** | Agent infers SQL rules from confirmed pairs, builds & saves recipe | `infer_rules`, `build_recipe`, `save_recipe`, `propose_match` |
| **Validation** | Run recipe on sample data, show results | `validate_recipe`, `run_sample`, `save_recipe` |
| **Execution** | Run recipe on full dataset with live progress | `run_full`, `validate_recipe` |

Each phase has prerequisites, context injections, and advancement conditions. The agent auto-advances when conditions are met (e.g., both schemas loaded → advance past intent).

### Chat UI Cards

The agent communicates through a mix of text and interactive cards:

| Card type | Component | Purpose |
|-----------|-----------|---------|
| `match_proposal` | `MatchProposalCard` | Side-by-side record comparison with accept/reject |
| `upload_request` | `UploadRequestCard` | Prompts user to upload CSV files |
| `progress` | `LiveProgressIndicator` | Live polling of run status |
| `result_summary` | `ResultSummary` | Match rate, counts, color-coded badge |
| `sample_table` | — | Inline data preview |

## Recipe Model

Recipes use raw DataFusion SQL — generated through conversation, never written by hand:

```json
{
  "recipe_id": "invoice-payment-match",
  "name": "Invoice-Payment Reconciliation",
  "description": "Match invoices to payments by reference and amount",
  "match_sql": "SELECT l.invoice_id, r.payment_id, l.amount, r.amount FROM left_src l JOIN right_src r ON l.invoice_id = r.reference_number AND tolerance_match(l.amount, r.amount, 0.02)",
  "match_description": "Matches invoices to payments where reference numbers match and amounts are within tolerance",
  "sources": {
    "left": {
      "alias": "invoices",
      "type": "postgres",
      "uri": "postgres://user:pass@host/db?table=invoices",
      "primary_key": ["invoice_id"],
      "schema": ["invoice_id", "customer_name", "amount", "currency", "status"]
    },
    "right": {
      "alias": "payments",
      "type": "postgres",
      "uri": "postgres://user:pass@host/db?table=payments",
      "primary_key": ["payment_id"],
      "schema": ["payment_id", "reference_number", "amount", "date", "currency"]
    }
  }
}
```

- `match_sql` uses `left_src` / `right_src` as fixed table aliases — the scheduler maps real URIs to these aliases at execution time
- `tolerance_match(a, b, threshold)` is a built-in UDF: `ABS(a - b) <= threshold`
- Unmatched counts are computed from matched records (no anti-join queries needed)
- Source types: `postgres` (partitioned reads), `csv_upload` (S3 CSV byte-range reads), `s3` (Parquet)

## Prerequisites

- **Docker** >= 24.0 and **Docker Compose** >= 2.20

For local development without Docker:
- **Rust** >= 1.85
- **Node.js** >= 22
- **PostgreSQL** >= 16
- **MinIO** (or any S3-compatible store, for CSV uploads)

## Quick Start

### Development (local)

```bash
# Start Postgres + MinIO
docker compose up -d

# Run the scheduler (single mode, no executors)
cargo run --bin kallad -- scheduler --http-port 9090

# Run Next.js app (web + API)
cd kalla-web && npm install && npm run dev
```

Docker Compose starts Postgres (port 5432) and MinIO (port 9000, console 9001) with auto-created `kalla-uploads` and `kalla-results` buckets. The scheduler falls back to local DataFusion when no executors are running. The App calls the scheduler at `http://localhost:9090`.

### File Upload Flow

Users upload CSV files through a presigned-URL flow:

1. Frontend requests a presigned URL from `/api/uploads/presign`
2. Frontend PUTs the file directly to MinIO using the presigned URL
3. Frontend confirms the upload via `/api/uploads/{id}/confirm`
4. The confirm endpoint parses the CSV, returns column names + row count
5. Files are attached to chat messages as `FileAttachment` objects
6. The agent inspects files via `/api/uploads/preview` (returns columns, row count, sample rows)

### Single VM (production)

```bash
docker compose -f docker-compose.single.yml up -d
```

Four services: app (port 3000), scheduler (port 9090), postgres, minio.

### Cluster (production)

```bash
docker compose -f docker-compose.cluster.yml up -d
```

Five services: app, scheduler (HTTP :8080 + gRPC :50050), executor-1, executor-2, postgres.

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for full deployment guide and environment variables.

## API Endpoints

### Chat & Agent

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/chat` | Send a message (with optional file attachments), get agent response |
| GET | `/api/chat/sessions` | List chat sessions |
| GET | `/api/chat/sessions/:id` | Get session with full message history |

### File Uploads

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/uploads/presign` | Get a presigned URL for S3 upload |
| POST | `/api/uploads/:id/confirm` | Confirm upload, parse CSV headers |
| POST | `/api/uploads/preview` | Preview file contents (columns, row count, sample rows) |

### Sources

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/sources` | List registered data sources |
| POST | `/api/sources` | Register a data source |
| GET | `/api/sources/:alias/preview` | Preview source schema and sample data |
| POST | `/api/sources/:alias/load-scoped` | Load filtered sample data |

### Recipes & Runs

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/recipes` | List recipes |
| POST | `/api/recipes` | Save a recipe |
| GET | `/api/recipes/:id` | Get recipe details |
| POST | `/api/runs` | Create and dispatch a reconciliation run |
| GET | `/api/runs/:id` | Get run status and results |

### Scheduler Callbacks (internal)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/worker/progress` | Matching progress updates |
| POST | `/api/worker/complete` | Run completion with counts and output paths |
| POST | `/api/worker/error` | Run failure details |

### Scheduler API (Rust — kallad)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/api/jobs` | Submit a reconciliation job |
| GET | `/metrics` | Prometheus metrics |

## Web UI Pages

| Route | Page | Description |
|-------|------|-------------|
| `/` | Home | Landing page |
| `/reconcile` | Reconcile | Chat-based agent interface for building recipes |
| `/sources` | Sources | Browse and register data sources |
| `/recipes` | Recipes | Browse saved recipes |
| `/runs` | Runs | List reconciliation runs |
| `/runs/:id` | Run Detail | Run results, progress, matched/unmatched counts |

## Project Structure

```
kalla/
├── crates/
│   ├── kallad/             # Unified binary (scheduler + executor subcommands)
│   ├── kalla-ballista/     # Ballista integration, HTTP runner, codecs, lazy scan nodes
│   ├── kalla-core/         # ReconciliationEngine (DataFusion) + financial UDFs
│   ├── kalla-connectors/   # Partitioned connectors (Postgres, S3 CSV, S3 Parquet)
│   ├── kalla-recipe/       # Recipe schema types
│   └── kalla-evidence/     # Audit trail storage (Parquet)
├── kalla-web/               # Next.js app
│   └── src/
│       ├── app/
│       │   ├── api/         # REST API routes
│       │   │   ├── chat/    # Agent chat + sessions
│       │   │   ├── uploads/ # Presign, confirm, preview
│       │   │   ├── sources/ # Source CRUD + preview + load-scoped
│       │   │   ├── recipes/ # Recipe CRUD
│       │   │   ├── runs/    # Run dispatch + status
│       │   │   └── worker/  # Scheduler callbacks (progress, complete, error)
│       │   ├── reconcile/   # Chat UI page
│       │   ├── sources/     # Sources browser page
│       │   ├── recipes/     # Recipes browser page
│       │   └── runs/        # Runs list + detail pages
│       ├── components/
│       │   ├── chat/        # ChatMessage, MatchProposalCard, UploadRequestCard, etc.
│       │   ├── ui/          # shadcn/ui primitives
│       │   ├── ResultSummary.tsx
│       │   ├── LiveProgressIndicator.tsx
│       │   └── SourcePreview.tsx
│       └── lib/
│           ├── agent.ts         # Agent orchestrator (Claude API, phase state machine)
│           ├── agent-tools.ts   # Tool implementations (list_sources, build_recipe, etc.)
│           ├── chat-types.ts    # Phase configs, card types, session types
│           ├── recipe-types.ts  # Recipe, JobPayload, Worker* types
│           ├── session-store.ts # In-memory session store with Postgres persistence
│           ├── worker-client.ts # HTTP dispatch to scheduler
│           ├── db.ts            # Postgres connection pool
│           └── api.ts           # Client-side API helpers
├── benchmarks/              # Performance benchmarks (Postgres 100k/1M/5M scenarios)
├── docs/
│   ├── DEPLOYMENT.md        # Deployment guide
│   └── plans/               # Design documents
├── scripts/
│   └── init.sql             # Database schema (sources, recipes, runs, sessions)
├── Dockerfile                     # Multi-stage build for kallad binary
├── docker-compose.yml             # Dev (Postgres + MinIO)
├── docker-compose.single.yml     # Single-node production
└── docker-compose.cluster.yml    # Cluster production (scheduler + executors)
```

## Development

### Running Tests

```bash
# Rust tests
cargo test --workspace

# Frontend unit tests
cd kalla-web && npm test

# Integration tests (requires Docker services running)
cd kalla-web && RUN_INTEGRATION=1 npx jest --verbose
```

### Building for Production

```bash
# Build the kallad binary
cargo build --release --bin kallad

# Build frontend
cd kalla-web && npm run build
```

## Environment Variables

### App (Next.js)

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | Postgres connection string |
| `WORKER_URL` | Yes | Scheduler HTTP endpoint (e.g. `http://scheduler:9090`) |
| `ANTHROPIC_API_KEY` | Yes | Claude API key for agentic builder |
| `ANTHROPIC_BASE_URL` | No | Custom Anthropic API endpoint |
| `S3_ENDPOINT` | Yes | S3/MinIO endpoint (e.g. `http://localhost:9000`) |
| `S3_ACCESS_KEY` | Yes | S3 access key |
| `S3_SECRET_KEY` | Yes | S3 secret key |
| `S3_UPLOADS_BUCKET` | No | Upload bucket (default: `kalla-uploads`) |

### kallad scheduler

| Variable | Required | Description |
|----------|----------|-------------|
| `RUST_LOG` | No | Log level (default: `info`) |
| `HTTP_PORT` | No | HTTP API port (default: `8080`) |
| `GRPC_PORT` | No | Ballista gRPC port (default: `50050`) |
| `BALLISTA_PARTITIONS` | No | Number of partitions for source reads (default: `4`) |
| `STAGING_PATH` | No | Local staging dir for evidence (default: `./staging`) |

### kallad executor

| Variable | Required | Description |
|----------|----------|-------------|
| `RUST_LOG` | No | Log level (default: `info`) |
| `SCHEDULER_HOST` | No | Scheduler hostname (default: `localhost`) |
| `SCHEDULER_PORT` | No | Scheduler gRPC port (default: `50050`) |
| `BIND_PORT` | No | Arrow Flight port (default: `50051`) |
| `BIND_GRPC_PORT` | No | gRPC port (default: `50052`) |
| `EXTERNAL_HOST` | No | Hostname advertised to scheduler (auto-detected if omitted) |

## Documentation

- [Deployment Guide](docs/deployment.md) — Single node, cluster, environment variables
- [Connector Interface](crates/kalla-connectors/README.md) — How connectors build queries, partitioning invariants
- [Ballista Cluster Mode](docs/plans/2026-02-17-ballista-cluster-mode.md) — Distributed execution design
- [Unified kallad Binary](docs/plans/2026-02-17-unified-kallad-binary-design.md) — Scheduler + executor consolidation

## License

MIT OR Apache-2.0
