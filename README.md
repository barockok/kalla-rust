# Kalla - Universal Reconciliation Engine

Kalla is a high-performance data reconciliation engine built with Rust and powered by Apache DataFusion. It matches data across sources using raw SQL — built through conversation with an AI assistant, never written by hand.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                           Kalla System                               │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────────────┐          ┌──────────────────────┐         │
│  │     Kalla App        │          │    Kalla Worker       │         │
│  │     (Next.js)        │─────────▶│    (Rust)             │         │
│  │                      │  HTTP    │                       │         │
│  │  • Web UI            │  (single │  • S3 CSV/Parquet     │         │
│  │  • REST API          │   mode)  │  • DataFusion SQL     │         │
│  │  • Agentic builder   │          │  • Result writing     │         │
│  │  • Postgres CRUD     │          │                       │         │
│  │  Port 3000           │          │  Port 9090            │         │
│  └──────┬───────┬───────┘          └───────────────────────┘         │
│         │       │                                                    │
│         ▼       ▼                                                    │
│  ┌────────────┐ ┌────────────┐                                      │
│  │ PostgreSQL │ │ MinIO (S3) │                                      │
│  │ Sources,   │ │ Uploads,   │                                      │
│  │ Recipes,   │ │ Results    │                                      │
│  │ Runs       │ │            │                                      │
│  └────────────┘ └────────────┘                                      │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Two Deployable Units

| Component | Technology | Role |
|-----------|------------|------|
| **Kalla App** | Next.js | Web UI + REST API + agentic recipe builder + file uploads + Postgres CRUD |
| **Kalla Worker** | Rust, DataFusion | Source loading (S3 CSV/Parquet) + SQL execution + result writing |

### Two Deployment Modes

**Single mode** — one VM, no autoscaling:
- App dispatches jobs to Worker via HTTP
- Worker loads sources from S3 (MinIO)
- Services: App + Worker + Postgres + MinIO

**Scaled mode** — K8s with autoscaling:
- App publishes jobs to NATS JetStream
- Multiple Workers consume from NATS, load from S3/GCS
- HPA/KEDA scales workers based on queue depth

The Worker binary is the same in both modes — it detects mode from environment variables.

### Rust Crates

| Crate | Description |
|-------|-------------|
| `kalla-core` | ReconciliationEngine wrapping DataFusion |
| `kalla-connectors` | Data source connectors (S3 CSV, S3 Parquet, PostgreSQL) |
| `kalla-recipe` | Recipe schema types (match_sql, sources, primary keys) |
| `kalla-evidence` | Matched record audit trail (Parquet) |
| `kalla-worker` | Source loading + SQL execution + HTTP/NATS job handling |

## Agentic Recipe Builder

The core of Kalla is a 7-phase conversational agent that guides users from raw data to a working reconciliation recipe:

```
greeting → intent → scoping → demonstration → inference → validation → execution
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
  "match_sql": "SELECT l.invoice_id, r.payment_id, l.amount, r.amount FROM left_src l JOIN right_src r ON l.invoice_id = r.reference_number AND ABS(l.amount - r.amount) / NULLIF(l.amount, 0) < 0.01",
  "match_description": "Matches invoices to payments where reference numbers match and amounts are within 1%",
  "sources": {
    "left": {
      "alias": "invoices",
      "type": "csv_upload",
      "uri": "s3://kalla-uploads/sessions/.../invoices.csv",
      "primary_key": ["invoice_id"],
      "schema": ["invoice_id", "customer_name", "amount", "currency", "status"]
    },
    "right": {
      "alias": "payments",
      "type": "csv_upload",
      "uri": "s3://kalla-uploads/sessions/.../payments.csv",
      "primary_key": ["payment_id"],
      "schema": ["payment_id", "reference_number", "amount", "date", "currency"]
    }
  }
}
```

- `match_sql` uses `left_src` / `right_src` as fixed table aliases — the worker maps real URIs to these aliases at execution time
- Kalla auto-derives unmatched records via LEFT ANTI JOIN
- Source types: `csv_upload` (uploaded files), `postgres`, `bigquery`, `elasticsearch`, `file` (schema-only, user uploads fresh data each run)

## Prerequisites

- **Docker** >= 24.0 and **Docker Compose** >= 2.20

For local development without Docker:
- **Rust** >= 1.85
- **Node.js** >= 22
- **PostgreSQL** >= 16
- **MinIO** (or any S3-compatible store)

## Quick Start

### Development (local)

```bash
# Start Postgres + MinIO
docker compose up -d

# Run Worker (single mode)
cd crates/kalla-worker && cargo run

# Run Next.js app (web + API)
cd kalla-web && npm install && npm run dev
```

Docker Compose starts Postgres (port 5432) and MinIO (port 9000, console 9001) with auto-created `kalla-uploads` and `kalla-results` buckets. The Worker loads sources from MinIO. The App calls the Worker at `http://localhost:9090`.

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

Four services: app (port 3000), worker (port 9090), postgres, minio.

### Scaled (K8s)

```bash
docker compose -f docker-compose.scaled.yml up -d
```

Five services: app, worker (N replicas), postgres, NATS (JetStream), MinIO (S3).

See [docs/deployment.md](docs/deployment.md) for full deployment guide and environment variables.

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

### Worker Callbacks (internal)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/worker/progress` | Staging/matching progress updates |
| POST | `/api/worker/complete` | Run completion with counts and output paths |
| POST | `/api/worker/error` | Run failure details |

### Worker API (Rust)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/api/jobs` | Submit job (single mode) |

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
│   ├── kalla-core/          # ReconciliationEngine (DataFusion)
│   ├── kalla-connectors/    # Data source connectors (S3 CSV, Parquet, Postgres)
│   ├── kalla-recipe/        # Recipe schema types
│   ├── kalla-evidence/      # Audit trail storage
│   └── kalla-worker/        # Source loading + execution + HTTP/NATS job handling
├── kalla-web/               # Next.js app
│   └── src/
│       ├── app/
│       │   ├── api/         # REST API routes
│       │   │   ├── chat/    # Agent chat + sessions
│       │   │   ├── uploads/ # Presign, confirm, preview
│       │   │   ├── sources/ # Source CRUD + preview + load-scoped
│       │   │   ├── recipes/ # Recipe CRUD
│       │   │   ├── runs/    # Run dispatch + status
│       │   │   └── worker/  # Worker callbacks (progress, complete, error)
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
│           ├── worker-client.ts # HTTP dispatch to Rust worker
│           ├── db.ts            # Postgres connection pool
│           └── api.ts           # Client-side API helpers
├── docs/
│   ├── deployment.md        # Deployment guide
│   └── plans/               # Design documents
├── scripts/
│   └── init.sql             # Database schema (sources, recipes, runs, sessions)
├── docker-compose.yml             # Dev (Postgres + MinIO)
├── docker-compose.single.yml     # Single-mode production
└── docker-compose.scaled.yml     # Scaled-mode production
```

## Development

### Running Tests

```bash
# Rust tests
cargo test --workspace

# Frontend unit tests (268 tests)
cd kalla-web && npm test

# Integration tests (requires Docker services running)
cd kalla-web && RUN_INTEGRATION=1 npx jest --verbose
```

### Building for Production

```bash
# Build worker binary
cargo build --release --bin kalla-worker

# Build frontend
cd kalla-web && npm run build
```

## Environment Variables

| Variable | Single | Scaled | Description |
|----------|--------|--------|-------------|
| `DATABASE_URL` | Required | Required | Postgres connection string |
| `WORKER_URL` | Required | - | Worker HTTP endpoint (single mode) |
| `NATS_URL` | - | Required | NATS broker (enables scaled mode) |
| `ANTHROPIC_API_KEY` | Required | Required | Claude API key for agentic builder |
| `ANTHROPIC_BASE_URL` | Optional | Optional | Custom Anthropic API endpoint |
| `S3_ENDPOINT` | Required | Required | S3/MinIO endpoint (e.g. `http://localhost:9000`) |
| `S3_ACCESS_KEY_ID` | Required | Required | S3 access key |
| `S3_SECRET_ACCESS_KEY` | Required | Required | S3 secret key |
| `S3_BUCKET` | Optional | Optional | Upload bucket (default: `kalla-uploads`) |
| `S3_REGION` | Optional | Optional | S3 region (default: `us-east-1`) |
| `AWS_ENDPOINT_URL` | - | Required | S3 endpoint for worker (scaled mode) |
| `STAGING_PATH` | Optional | - | Local staging dir (default: `./staging/`) |

## Documentation

- [Deployment Guide](docs/deployment.md) — Single VM, scaled K8s, environment variables
- [Architecture Design](docs/plans/2026-02-15-architecture-review-design.md) — Full design document

## License

MIT OR Apache-2.0
