# Kalla - Universal Reconciliation Engine

Kalla is a high-performance data reconciliation engine built with Rust and powered by Apache DataFusion. It matches data across sources using raw SQL — built through conversation with an AI assistant, never written by hand.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Kalla System                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────┐         ┌──────────────────────┐     │
│  │     Kalla App        │         │    Kalla Worker       │     │
│  │     (Next.js)        │────────▶│    (Rust)             │     │
│  │                      │  HTTP   │                       │     │
│  │  • Web UI            │  (single│  • Source staging     │     │
│  │  • REST API          │   mode) │  • DataFusion SQL     │     │
│  │  • Agentic builder   │         │  • Result writing     │     │
│  │  • Postgres CRUD     │         │                       │     │
│  │  Port 3000           │         │  Port 9090            │     │
│  └──────────┬───────────┘         └───────────────────────┘     │
│             │                                                   │
│             ▼                                                   │
│  ┌──────────────────────┐                                      │
│  │     PostgreSQL       │                                      │
│  │  Sources, Recipes,   │                                      │
│  │  Runs, Progress      │                                      │
│  └──────────────────────┘                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Two Deployable Units

| Component | Technology | Role |
|-----------|------------|------|
| **Kalla App** | Next.js | Web UI + REST API + agentic recipe builder + Postgres CRUD |
| **Kalla Worker** | Rust, DataFusion | Source staging (any format → Parquet) + SQL execution |

### Two Deployment Modes

**Single mode** — one VM, no autoscaling:
- App dispatches jobs to Worker via HTTP
- Worker uses local filesystem for staging
- No NATS, no S3 — just App + Worker + Postgres

**Scaled mode** — K8s with autoscaling:
- App publishes jobs to NATS JetStream
- Multiple Workers consume from NATS, write to S3/GCS
- HPA/KEDA scales workers based on queue depth

The Worker binary is the same in both modes — it detects mode from environment variables.

### Rust Crates

| Crate | Description |
|-------|-------------|
| `kalla-core` | ReconciliationEngine wrapping DataFusion |
| `kalla-connectors` | Data source connectors (CSV, Parquet, PostgreSQL) |
| `kalla-recipe` | Recipe schema types (match_sql, sources, primary keys) |
| `kalla-evidence` | Matched record audit trail (Parquet) |
| `kalla-worker` | Staging pipeline + execution + HTTP/NATS job handling |

## Recipe Model

Recipes use raw DataFusion SQL — generated through conversation, never written by hand:

```json
{
  "recipe_id": "invoice-payment-match",
  "name": "Invoice-Payment Reconciliation",
  "description": "Match invoices to payments by reference and amount",
  "match_sql": "SELECT i.invoice_id, p.payment_id, i.amount, p.amount FROM invoices i JOIN payments p ON i.invoice_id = p.reference_number AND ABS(i.amount - p.amount) / NULLIF(i.amount, 0) < 0.01",
  "match_description": "Matches invoices to payments where reference numbers match and amounts are within 1%",
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
  }
}
```

- `match_sql` defines what matches — Kalla auto-derives unmatched records via LEFT ANTI JOIN
- `type: "file"` sources store expected column names — users upload fresh files each run
- `type: "postgres"` (or `bigquery`, `elasticsearch`) sources store persistent URIs
- `match_description` provides human-readable explanation shown alongside collapsible SQL

## Prerequisites

- **Docker** >= 24.0 and **Docker Compose** >= 2.20

For local development without Docker:
- **Rust** >= 1.85
- **Node.js** >= 22
- **PostgreSQL** >= 16

## Quick Start

### Development (local)

```bash
# Start Postgres
docker compose up -d

# Run Worker (single mode — no NATS)
cd crates/kalla-worker && cargo run

# Run Next.js app (web + API)
cd kalla-web && npm install && npm run dev
```

Worker uses local filesystem for staging (`./staging/`). App calls Worker at `http://localhost:9090`.

### Single VM (production)

```bash
docker compose -f docker-compose.single.yml up -d
```

Three services: app (port 3000), worker (port 9090), postgres. One command, done.

### Scaled (K8s)

```bash
docker compose -f docker-compose.scaled.yml up -d
```

Five services: app, worker (N replicas), postgres, NATS (JetStream), MinIO (S3).

See [docs/deployment.md](docs/deployment.md) for full deployment guide and environment variables.

## API Endpoints

### App API (Next.js)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/sources` | List data sources |
| POST | `/api/sources` | Register a data source |
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

## Project Structure

```
kalla/
├── crates/
│   ├── kalla-core/        # ReconciliationEngine (DataFusion)
│   ├── kalla-connectors/  # Data source connectors (CSV, Parquet, Postgres)
│   ├── kalla-recipe/      # Recipe schema types
│   ├── kalla-evidence/    # Audit trail storage
│   └── kalla-worker/      # Staging + execution + HTTP/NATS job handling
├── kalla-web/             # Next.js app (web UI + API + agentic builder)
│   └── src/
│       ├── app/api/       # REST API routes
│       └── lib/           # DB, worker client, recipe types
├── docs/
│   ├── deployment.md      # Deployment guide
│   └── plans/             # Design documents
├── docker-compose.yml           # Dev (Postgres only)
├── docker-compose.single.yml   # Single-mode production
├── docker-compose.scaled.yml   # Scaled-mode production
└── scripts/
    └── init.sql           # Database schema
```

## Development

### Running Tests

```bash
# Rust tests
cargo test --workspace

# Frontend tests
cd kalla-web && npm test
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
| `DATABASE_URL` | Required | Required | App Postgres connection |
| `WORKER_URL` | Required | - | Worker HTTP endpoint (single mode) |
| `NATS_URL` | - | Required | NATS broker (enables scaled mode) |
| `AWS_ENDPOINT_URL` | - | Required | S3 endpoint |
| `STAGING_PATH` | Optional | - | Local staging dir (default: `./staging/`) |
| `ANTHROPIC_API_KEY` | Required | Required | For agentic recipe builder |

## Documentation

- [Deployment Guide](docs/deployment.md) — Single VM, scaled K8s, environment variables
- [Architecture Design](docs/plans/2026-02-15-architecture-review-design.md) — Full design document

## License

MIT OR Apache-2.0
