# Kalla - Universal Reconciliation Engine

Kalla is a high-performance data reconciliation engine built with Rust and powered by Apache DataFusion. It provides flexible matching capabilities for financial reconciliation, data validation, and cross-system data verification.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Kalla System                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐     ┌─────────────────┐     ┌──────────────┐   │
│  │  kalla-web  │────▶│  kalla-server   │────▶│  PostgreSQL  │   │
│  │  (Next.js)  │     │  (Rust/Axum)    │     │     16       │   │
│  │  Port 3000  │     │  Port 3001      │     │  Port 5432   │   │
│  └─────────────┘     └────────┬────────┘     └──────────────┘   │
│                               │                                 │
│                               ▼                                 │
│                    ┌─────────────────────┐                      │
│                    │    Rust Crates      │                      │
│                    ├─────────────────────┤                      │
│                    │ • kalla-core        │ Reconciliation engine│
│                    │ • kalla-connectors  │ Data source adapters │
│                    │ • kalla-recipe      │ Match rule configs   │
│                    │ • kalla-evidence    │ Audit trail storage  │
│                    │ • kalla-ai          │ LLM recipe generation│
│                    │ • kalla-cli         │ Command-line tool    │
│                    └─────────────────────┘                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Technology | Description |
|-----------|------------|-------------|
| **kalla-web** | Next.js 16, React 19, TailwindCSS | Web UI for managing sources, recipes, and viewing reconciliation results |
| **kalla-server** | Rust, Axum, SQLx | REST API server exposing reconciliation capabilities |
| **kalla-core** | Rust, DataFusion | Core reconciliation engine with SQL-based matching |
| **kalla-connectors** | Rust | Data source connectors (CSV, Parquet, PostgreSQL) |
| **kalla-recipe** | Rust | Match recipe configuration and validation |
| **kalla-evidence** | Rust | Audit trail and evidence storage |
| **kalla-ai** | Rust | AI/LLM-powered recipe generation |
| **PostgreSQL** | PostgreSQL 16 | Metadata storage and data source |

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

# AI/LLM Configuration (optional - for recipe generation)
LLM_API_KEY=your_api_key_here
LLM_API_URL=https://api.anthropic.com

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

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/sources` | List registered data sources |
| POST | `/api/sources` | Register a new data source |
| POST | `/api/recipes/validate` | Validate a match recipe |
| POST | `/api/recipes/generate` | Generate recipe using AI |
| GET | `/api/runs` | List reconciliation runs |
| POST | `/api/runs` | Create and execute a reconciliation run |
| GET | `/api/runs/:id` | Get run details and results |

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

Kalla supports various matching operations:

| Operation | Description | Example |
|-----------|-------------|---------|
| `eq` | Exact equality | `invoice_id = reference_number` |
| `tolerance` | Numeric tolerance matching | `amount ≈ paid_amount (±0.01)` |
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
│   ├── kalla-core/        # Core reconciliation engine
│   ├── kalla-connectors/  # Data source connectors
│   ├── kalla-recipe/      # Recipe configuration
│   ├── kalla-evidence/    # Audit trail storage
│   ├── kalla-ai/          # AI/LLM integration
│   └── kalla-cli/         # Command-line interface
├── kalla-server/          # REST API server
├── kalla-web/             # Next.js web frontend
├── scripts/
│   └── init.sql           # Database schema and seed data
├── testdata/              # Sample CSV files for testing
├── docker-compose.yml     # Docker orchestration
└── .env.example           # Environment template
```

## Development

### Running Locally (Without Docker)

**Backend:**
```bash
# Start PostgreSQL locally
# Set DATABASE_URL environment variable

cd kalla-server
cargo run
```

**Frontend:**
```bash
cd kalla-web
npm install
npm run dev
```

### Running Tests

```bash
cargo test --workspace
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

The sample data includes various reconciliation scenarios:
- Exact matches
- Partial payments
- Amount tolerances (wire fees)
- Duplicate payments
- Unmatched records
- Currency variations
- Name variations (fuzzy matching)

## License

MIT OR Apache-2.0
