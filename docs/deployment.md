# Kalla Deployment Guide

## Architecture Overview

Kalla consists of two deployable components:

| Component | Tech | Role |
|-----------|------|------|
| **Kalla App** | Next.js | Web UI + API (agentic orchestrator, CRUD, Postgres) |
| **Kalla Worker** | Rust | Stateless compute — staging sources to Parquet + DataFusion SQL execution |

The App owns all state (Postgres, run tracking, recipe storage). The Worker is stateless — it receives a self-contained job payload, executes it, writes results to object storage, and reports status back via HTTP callbacks.

### How the Worker Detects Its Mode

The Worker binary is the same in both deployment modes. It selects its mode based on environment variables at startup:

- `NATS_URL` **present** → scaled mode (consume jobs from NATS JetStream)
- `NATS_URL` **absent** → single mode (accept jobs via HTTP on port 9090)

Storage detection works the same way:

- `AWS_ENDPOINT_URL` or `GCS_BUCKET` present → use S3/GCS via `object_store`
- Neither present → use local filesystem

---

## 1. Development Setup (Local)

Run Postgres in Docker, everything else natively for fast iteration.

### Start Postgres

```bash
docker compose up -d
```

This uses the default `docker-compose.yml` which only starts Postgres (port 5432) with the schema from `scripts/init.sql`.

### Start the Worker

```bash
export RUST_LOG=debug
cd crates/kalla-worker
cargo run
```

The worker starts in **single mode** (no `NATS_URL`), listens on port 9090, and uses `./staging/` for local file storage.

### Start the App

```bash
cd kalla-web
npm install
export DATABASE_URL=postgres://kalla:kalla_secret@localhost:5432/kalla
export WORKER_URL=http://localhost:9090
export ANTHROPIC_API_KEY=sk-ant-...
npm run dev
```

The app is available at `http://localhost:3000`.

---

## 2. Single VM Production Deployment

One machine, no external dependencies beyond Docker. Good for datasets up to ~1M rows.

```bash
# Set your API key
export ANTHROPIC_API_KEY=sk-ant-...

# Start everything
docker compose -f docker-compose.single.yml up -d
```

### What Runs

| Service | Port | Notes |
|---------|------|-------|
| `app` | 3000 | Next.js (web + API) |
| `worker` | 9090 | Rust worker, single mode (HTTP) |
| `postgres` | 5432 | App database |

### How It Works

```
Browser --> App (:3000)
              |
              |--> Postgres (:5432)    [state]
              |--> Worker (:9090)      [compute]
                     |
                     |--> local volume  [staging + results]
                     |--> App callback  [progress/complete/error]
```

The app dispatches jobs to the worker via `POST /api/jobs`. The worker writes Parquet results to a shared Docker volume and reports status back to the app via HTTP callbacks at `/api/worker/progress`, `/api/worker/complete`, and `/api/worker/error`.

### Volumes

| Volume | Purpose |
|--------|---------|
| `postgres_data` | Database files |
| `staging_data` | Worker staging area (source → Parquet) |
| `results_data` | Reconciliation result Parquet files |

---

## 3. Scaled Deployment (K8s-Ready)

Multiple workers consuming from NATS, writing to S3-compatible object storage.

```bash
export ANTHROPIC_API_KEY=sk-ant-...
docker compose -f docker-compose.scaled.yml up -d
```

### What Runs

| Service | Port | Replicas | Notes |
|---------|------|----------|-------|
| `app` | 3000 | 1 | Next.js, publishes jobs to NATS |
| `worker` | — | 2 (configurable) | Consumes from NATS, writes to MinIO |
| `postgres` | 5432 | 1 | App database |
| `nats` | 4222 | 1 | JetStream job queue |
| `minio` | 9000, 9001 | 1 | S3-compatible object storage |

### How It Works

```
Browser --> App (:3000)
              |
              |--> Postgres (:5432)    [state]
              |--> NATS (:4222)        [job queue]
                     |
                     |--> Worker 1 ---> MinIO (:9000)   [staging + results]
                     |--> Worker 2 ---> MinIO (:9000)
                     |
                     Workers --> App callback [progress/complete/error]
```

The app publishes jobs to NATS JetStream. Workers consume jobs, stage source data to Parquet in MinIO, run DataFusion SQL, and write results back to MinIO. Progress is reported back to the app via HTTP callbacks.

### Scaling Workers

With Docker Compose:

```bash
docker compose -f docker-compose.scaled.yml up -d --scale worker=4
```

For Kubernetes, use HPA or KEDA to autoscale workers based on NATS queue depth.

### MinIO Console

The MinIO web console is available at `http://localhost:9001` (default credentials: `minioadmin`/`minioadmin`).

---

## 4. Environment Variable Reference

### App (Next.js)

| Variable | Single | Scaled | Default | Description |
|----------|--------|--------|---------|-------------|
| `DATABASE_URL` | Required | Required | — | Postgres connection string |
| `WORKER_URL` | Required | — | `http://localhost:9090` | Worker HTTP endpoint (single mode only) |
| `NATS_URL` | — | Required | — | NATS broker URL (enables scaled mode) |
| `ANTHROPIC_API_KEY` | Required | Required | — | API key for agentic recipe builder |

### Worker (Rust)

| Variable | Single | Scaled | Default | Description |
|----------|--------|--------|---------|-------------|
| `NATS_URL` | — | Required | — | NATS broker (presence enables scaled mode) |
| `STAGING_PATH` | Optional | — | `./staging/` | Local directory for staging Parquet files |
| `AWS_ENDPOINT_URL` | — | Required | — | S3-compatible endpoint (e.g., MinIO) |
| `AWS_ACCESS_KEY_ID` | — | Required | — | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | — | Required | — | S3 secret key |
| `AWS_REGION` | — | Optional | `us-east-1` | S3 region |
| `AWS_ALLOW_HTTP` | — | Optional | `false` | Allow HTTP (non-TLS) S3 connections |
| `STAGING_BUCKET` | — | Required | — | S3 bucket for staging data |
| `GCS_BUCKET` | — | Alternative | — | GCS bucket (alternative to S3) |
| `RUST_LOG` | Optional | Optional | `info` | Log level (`trace`, `debug`, `info`, `warn`, `error`) |

### Postgres

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `kalla` | Database user |
| `POSTGRES_PASSWORD` | `kalla_secret` | Database password |
| `POSTGRES_DB` | `kalla` | Database name |

---

## 5. Storage Configuration

### Single Mode (Local Filesystem)

The worker uses `object_store` with a local filesystem backend. Set `STAGING_PATH` to control where Parquet files are written (default: `./staging/`).

In Docker, this is a named volume mounted at `/data/staging`.

### Scaled Mode (S3/GCS)

The worker uses `object_store` with an S3 or GCS backend.

**S3 / MinIO:**
```bash
AWS_ENDPOINT_URL=http://minio:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
STAGING_BUCKET=kalla-staging
```

**GCS:**
```bash
GCS_BUCKET=kalla-staging
# Uses Application Default Credentials
```

The same `object_store` API is used regardless of backend — the worker code doesn't change between local and cloud storage.

---

## 6. Production Considerations

### Security

- **Change default Postgres password.** The default `kalla_secret` is for development only.
- **Restrict port exposure.** Don't expose Postgres (5432) or NATS (4222) to the public internet. Bind to `127.0.0.1` or remove port mappings.
- **Use HTTPS.** Place a reverse proxy (nginx, Caddy, Traefik) in front of the app for TLS termination.
- **Protect secrets.** Store `ANTHROPIC_API_KEY` and database credentials securely (`.env` with `chmod 600`, or a secrets manager in K8s).

### Reverse Proxy Example (Caddy)

```
kalla.example.com {
    reverse_proxy app:3000
}
```

All API routes are served by the Next.js app on port 3000 — no separate backend port.

### Health Checks

- **App:** `GET /` (Next.js default)
- **Worker:** `GET /health` (Axum health endpoint on port 9090)
- **Postgres:** `pg_isready -U kalla`

### Backups

```bash
# Postgres backup
docker compose exec -T postgres pg_dump -U kalla kalla > backup.sql

# Restore
docker compose exec -T postgres psql -U kalla kalla < backup.sql
```

For MinIO/S3 data, use `mc mirror` or cloud-native backup tools.

### Resource Requirements

| Deployment | RAM | CPU | Disk |
|------------|-----|-----|------|
| Development | 4 GB | 2 cores | 10 GB |
| Single VM | 4 GB | 2 cores | 20 GB |
| Scaled (per worker) | 2 GB | 1 core | — (uses S3) |
