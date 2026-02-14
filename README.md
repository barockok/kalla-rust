# Kalla - Universal Reconciliation Engine

Kalla is a high-performance data reconciliation engine built with Rust and powered by Apache DataFusion. It provides flexible matching capabilities for financial reconciliation, data validation, and cross-system data verification — with an agentic AI assistant that builds match recipes through conversation.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            Kalla System                                  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐      ┌──────────────────┐      ┌───────────────┐      │
│  │  kalla-web   │─────▶│  kalla-server    │─────▶│  PostgreSQL   │      │
│  │  (Next.js)   │      │  (Rust/Axum)     │      │     16        │      │
│  │  Port 3000   │      │  Port 3001       │      │  Port 5432    │      │
│  └──────┬───────┘      └────────┬─────────┘      └───────────────┘      │
│         │                       │                                        │
│         │ Agentic Layer         │                                        │
│         │ (7-Phase State        ▼                                        │
│         │  Machine)    ┌──────────────────┐                              │
│         │              │   Rust Crates    │                              │
│         ▼              ├──────────────────┤                              │
│  ┌──────────────┐      │ kalla-core       │ DataFusion reconciliation   │
│  │  Anthropic   │      │ kalla-connectors │ CSV, Parquet, PostgreSQL    │
│  │  Claude API  │      │ kalla-recipe     │ Match rule config + SQL      │
│  └──────────────┘      │ kalla-evidence   │ Audit trail storage          │
│                        │ kalla-ai         │ Schema extraction + LLM      │
│                        │ kalla-cli        │ Command-line tool             │
│                        └──────────────────┘                              │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Technology | Description |
|-----------|------------|-------------|
| **kalla-web** | Next.js 16, React 19, TailwindCSS | Web UI with conversational agentic recipe builder |
| **kalla-server** | Rust, Axum, SQLx | REST API server exposing reconciliation capabilities |
| **kalla-core** | Rust, DataFusion | Core reconciliation engine with SQL-based matching |
| **kalla-connectors** | Rust | Data source connectors (CSV, Parquet, PostgreSQL) with scoped loading |
| **kalla-recipe** | Rust | Match recipe configuration, validation, and SQL transpilation |
| **kalla-evidence** | Rust | Audit trail and evidence storage |
| **kalla-ai** | Rust | Schema extraction and LLM-powered recipe generation |
| **kalla-cli** | Rust, Clap | CLI for reconcile, validate, generate, and report commands |
| **PostgreSQL** | PostgreSQL 16 | Metadata storage and data source |

### Agentic Recipe Builder

The web UI includes a conversational AI assistant that guides users through building reconciliation recipes. It uses a 7-phase declarative state machine:

| Phase | Purpose |
|-------|---------|
| **greeting** | Discover available data sources |
| **intent** | Confirm left/right sources and preview schemas |
| **scoping** | Define data subset with structured filter conditions |
| **demonstration** | Propose and confirm example match pairs |
| **inference** | Analyze confirmed pairs and build match rules |
| **validation** | Validate recipe and run on sample data |
| **execution** | Execute reconciliation on full scoped dataset |

Each phase has defined prerequisites, available tools, context injections, and advancement conditions. The orchestrator is generic — no phase-specific logic — and supports mid-turn phase advancement, retry budgets, and structured error recovery.

## Prerequisites

- **Docker** >= 24.0
- **Docker Compose** >= 2.20

For local development without Docker:
- **Rust** >= 1.85
- **Node.js** >= 22
- **PostgreSQL** >= 16

## Quick Start

### 1. Clone and Configure

```bash
git clone https://github.com/your-org/kalla.git
cd kalla

# Copy environment template
cp .env.example .env
```

### 2. Configure Environment Variables

Edit `.env` to set your configuration:

```env
# PostgreSQL Configuration
POSTGRES_USER=kalla
POSTGRES_PASSWORD=kalla_secret
POSTGRES_DB=kalla

# Backend Configuration
DATABASE_URL=postgres://kalla:kalla_secret@postgres:5432/kalla
RUST_LOG=info

# AI Configuration (required for agentic recipe builder)
ANTHROPIC_API_KEY=your_anthropic_api_key_here

# Frontend Configuration
NEXT_PUBLIC_API_URL=http://localhost:3001
```

### 3. Start Services

```bash
# Build and start all services
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f
```

### 4. Access the Application

- **Web UI**: http://localhost:3000
- **API**: http://localhost:3001
- **Health Check**: http://localhost:3001/health

### 5. Try the Agentic Recipe Builder

1. Open http://localhost:3000 in your browser
2. Start a new chat session
3. Tell the agent what you want to reconcile (e.g., "Match invoices with payments")
4. The agent will guide you through source selection, data scoping, match pair confirmation, and recipe generation

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/sources` | List registered data sources |
| POST | `/api/sources` | Register a new data source |
| GET | `/api/sources/:alias/primary-key` | Detect primary key columns |
| GET | `/api/sources/:alias/preview` | Preview source data and schema |
| POST | `/api/sources/:alias/load-scoped` | Load filtered subset of data |
| GET | `/api/recipes` | List saved recipes |
| POST | `/api/recipes` | Save a recipe |
| GET | `/api/recipes/:id` | Get a specific recipe |
| POST | `/api/recipes/validate` | Validate a match recipe |
| POST | `/api/recipes/validate-schema` | Validate recipe against source schemas |
| POST | `/api/recipes/generate` | Generate recipe using AI |
| GET | `/api/runs` | List reconciliation runs |
| POST | `/api/runs` | Create and execute a reconciliation run |
| GET | `/api/runs/:id` | Get run details and results |

See [docs/api-reference.md](docs/api-reference.md) for full API documentation with examples.

### Example: Register a Data Source

```bash
curl -X POST http://localhost:3001/api/sources \
  -H "Content-Type: application/json" \
  -d '{
    "alias": "invoices",
    "uri": "file:///app/testdata/invoices.csv"
  }'
```

### Example: Create a Reconciliation Run

```bash
curl -X POST http://localhost:3001/api/runs \
  -H "Content-Type: application/json" \
  -d '{
    "recipe": {
      "version": "1.0",
      "recipe_id": "invoice-payment-match",
      "sources": {
        "left": { "alias": "invoices_csv", "uri": "file:///app/testdata/invoices.csv" },
        "right": { "alias": "payments_csv", "uri": "file:///app/testdata/payments.csv" }
      },
      "match_rules": [
        {
          "name": "exact_match",
          "pattern": "1:1",
          "conditions": [
            { "left": "invoice_id", "op": "eq", "right": "reference_number" },
            { "left": "amount", "op": "eq", "right": "paid_amount" }
          ]
        }
      ],
      "output": {
        "matched": "matched_results",
        "unmatched_left": "unmatched_invoices",
        "unmatched_right": "unmatched_payments"
      }
    }
  }'
```

## Match Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| `eq` | Exact equality | `invoice_id = reference_number` |
| `tolerance` | Numeric tolerance matching | `amount ~ paid_amount (threshold: 0.02)` |
| `gt` / `lt` | Greater/Less than | `payment_date > invoice_date` |
| `gte` / `lte` | Greater/Less than or equal | `amount >= min_amount` |
| `contains` | String contains | `description contains 'INV'` |
| `startswith` | String prefix match | `ref startswith 'PAY-'` |
| `endswith` | String suffix match | `file endswith '.csv'` |

## Match Patterns

| Pattern | Description |
|---------|-------------|
| `1:1` | One-to-one matching (each record matches at most one) |
| `1:N` | One-to-many (one left record can match multiple right records) |
| `M:1` | Many-to-one (multiple left records can match one right record) |

## Project Structure

```
kalla/
├── crates/
│   ├── kalla-core/        # Core reconciliation engine (DataFusion)
│   ├── kalla-connectors/  # Data source connectors + scoped loading
│   ├── kalla-recipe/      # Recipe config, validation, SQL transpilation
│   ├── kalla-evidence/    # Audit trail storage
│   ├── kalla-ai/          # Schema extraction + LLM integration
│   └── kalla-cli/         # Command-line interface
├── kalla-server/          # REST API server (Axum)
├── kalla-web/             # Next.js web frontend + agentic layer
│   └── src/lib/           # State machine orchestrator, agent tools
├── scripts/
│   ├── init.sql           # Database schema and seed data
│   └── release.sh         # Release tagging script
├── testdata/              # Sample CSV files and recipes
├── docs/
│   ├── api-reference.md   # Full API documentation
│   ├── deployment-guide.md # Deployment and operations guide
│   └── plans/             # Design documents
├── docker-compose.yml     # Docker orchestration
├── CHANGELOG.md           # Release history
└── .env.example           # Environment template
```

## CLI Usage

```bash
# Run a reconciliation from a recipe file
kalla reconcile --recipe testdata/recipe.json --output-dir ./evidence

# Validate a recipe file
kalla validate-recipe testdata/recipe.json

# Generate a recipe from natural language
kalla generate-recipe \
  --sources "testdata/invoices.csv,testdata/payments.csv" \
  --prompt "Match invoices to payments by reference number and amount" \
  --output recipe.json

# View a reconciliation report
kalla report ./evidence

# Show version
kalla --version
```

## Development

### Running Locally (Without Docker)

**Backend:**
```bash
# Start PostgreSQL
docker compose up -d postgres db-init

# Set environment and run server
export DATABASE_URL=postgres://kalla:kalla_secret@localhost:5432/kalla
cd kalla-server
cargo run
```

**Frontend:**
```bash
cd kalla-web
npm install
export NEXT_PUBLIC_API_URL=http://localhost:3001
export SERVER_API_URL=http://localhost:3001
npm run dev
```

### Running Tests

```bash
# Rust tests
cargo test --workspace

# Frontend tests
cd kalla-web && npm test

# E2E tests (requires running services)
cd kalla-web && npx playwright test
```

### Building for Production

```bash
# Build all Rust crates
cargo build --release

# Build frontend
cd kalla-web
npm run build
```

## Docker Commands

```bash
# Build all images
docker compose build

# Start services
docker compose up -d

# Stop services
docker compose down

# Reset database (removes all data)
docker compose down -v
docker compose up -d

# View logs
docker compose logs -f server
docker compose logs -f web
docker compose logs -f postgres

# Execute SQL in PostgreSQL
docker exec -it kalla-postgres psql -U kalla -d kalla
```

## Sample Data

The project includes sample test data for invoice-to-payment reconciliation:

- `testdata/invoices.csv` - 15 sample invoices
- `testdata/payments.csv` - 14 sample payments
- `testdata/recipe.json` - Sample recipe matching invoices to payments

The database is also seeded with the same data in PostgreSQL tables (`invoices`, `payments`) plus pre-registered sources and a sample recipe.

Reconciliation scenarios covered:
- Exact matches (reference number + amount)
- Tolerance matches (bank wire fees causing small amount differences)
- Split payments (1:N — one invoice, multiple partial payments)
- Unmatched records (invoices without payments, orphan payments)
- Currency variations (USD, EUR, GBP)
- Name variations (for fuzzy matching)

## Documentation

- [API Reference](docs/api-reference.md) - Full endpoint documentation with examples
- [Deployment Guide](docs/deployment-guide.md) - Docker setup, environment variables, troubleshooting
- [Changelog](CHANGELOG.md) - Release history

## License

MIT OR Apache-2.0
