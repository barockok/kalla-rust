# Kalla Deployment Guide

## Architecture Overview

Kalla consists of two deployable components:

| Component | Tech | Role |
|-----------|------|------|
| **Kalla App** | Next.js | Web UI + API (agentic orchestrator, CRUD, Postgres) |
| **kallad** | Rust | Unified binary with `scheduler` and `executor` subcommands |

The App owns all state (Postgres, run tracking, recipe storage). The `kallad` binary runs in two modes:

- **`kallad scheduler`** — HTTP API for job submission + Ballista scheduler for distributed query coordination
- **`kallad executor`** — Ballista executor that registers with the scheduler and executes query partitions

In single-node deployments, the scheduler runs DataFusion locally. In cluster deployments, the scheduler distributes work across executor instances.

### Architecture Diagram

```
Browser --> App (:3000)
              |
              |--> Postgres (:5432)           [state]
              |--> kallad scheduler (:9090)    [compute + coordination]
                     |
                     |--> kallad executor 1    [distributed execution]
                     |--> kallad executor 2
                     |--> ...
```

---

## 1. Development Setup (Local)

Run Postgres in Docker, everything else natively for fast iteration.

### Start Postgres

```bash
docker compose up -d
```

This uses the default `docker-compose.yml` which only starts Postgres (port 5432) with the schema from `scripts/init.sql`.

### Start the Scheduler

```bash
export RUST_LOG=debug
cargo run --bin kallad -- scheduler --http-port 9090
```

The scheduler starts in single mode, listens on port 9090 for HTTP job submissions, and uses local DataFusion for execution.

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
| `scheduler` | 9090 | kallad scheduler (HTTP + local DataFusion) |
| `postgres` | 5432 | App database |

### How It Works

```
Browser --> App (:3000)
              |
              |--> Postgres (:5432)              [state]
              |--> kallad scheduler (:9090)       [compute]
                     |
                     |--> local DataFusion        [query execution]
                     |--> App callback            [progress/complete/error]
```

The app dispatches jobs to the scheduler via `POST /api/jobs`. The scheduler executes queries using local DataFusion, writes results, and reports status back to the app via HTTP callbacks.

---

## 3. Cluster Deployment

Multiple executors for distributed query execution. Suitable for large datasets (1M+ rows).

```bash
export ANTHROPIC_API_KEY=sk-ant-...
docker compose -f docker-compose.cluster.yml up -d
```

### What Runs

| Service | Port | Replicas | Notes |
|---------|------|----------|-------|
| `app` | 3000 | 1 | Next.js, submits jobs via HTTP |
| `scheduler` | 8080, 50050 | 1 | kallad scheduler (HTTP + gRPC) |
| `executor-1` | — | 1 | kallad executor |
| `executor-2` | — | 1 | kallad executor |
| `postgres` | 5432 | 1 | App database |

### How It Works

```
Browser --> App (:3000)
              |
              |--> Postgres (:5432)                     [state]
              |--> kallad scheduler (:8080 HTTP, :50050 gRPC)
                     |
                     |--> kallad executor 1 (flight + gRPC)
                     |--> kallad executor 2 (flight + gRPC)
                     |
                     Executors read from Postgres via PostgresScanExec
                     Scheduler --> App callback [progress/complete/error]
```

The app submits jobs to the scheduler via HTTP. The scheduler coordinates distributed query execution across executors using Ballista's gRPC protocol. Executors read source data directly from Postgres.

### Scaling Executors

Add more executor services to `docker-compose.cluster.yml` following the pattern of `executor-1` and `executor-2`, ensuring unique `--external-host` values.

---

## 4. Environment Variable Reference

### App (Next.js)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | Postgres connection string |
| `WORKER_URL` | Yes | — | Scheduler HTTP endpoint (e.g., `http://scheduler:9090`) |
| `ANTHROPIC_API_KEY` | Yes | — | API key for agentic recipe builder |

### kallad scheduler

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RUST_LOG` | No | `info` | Log level (`trace`, `debug`, `info`, `warn`, `error`) |
| `BALLISTA_PARTITIONS` | No | — | Number of partitions for distributed execution |

CLI flags: `--http-port` (HTTP API port), `--grpc-port` (Ballista gRPC port)

### kallad executor

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RUST_LOG` | No | `info` | Log level |

CLI flags: `--scheduler-host`, `--scheduler-port`, `--flight-port`, `--grpc-port`, `--external-host`

### Postgres

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `kalla` | Database user |
| `POSTGRES_PASSWORD` | `kalla_secret` | Database password |
| `POSTGRES_DB` | `kalla` | Database name |

---

## 5. Health Checks

| Service | Endpoint | Method |
|---------|----------|--------|
| Scheduler | `GET /health` on HTTP port | HTTP |
| Postgres | `pg_isready -U kalla` | CLI |
| App | `GET /` | HTTP |

---

## 6. Production Considerations

### Security

- **Change default Postgres password.** The default `kalla_secret` is for development only.
- **Restrict port exposure.** Don't expose Postgres (5432) or gRPC ports to the public internet.
- **Use HTTPS.** Place a reverse proxy (nginx, Caddy, Traefik) in front of the app for TLS termination.
- **Protect secrets.** Store `ANTHROPIC_API_KEY` and database credentials securely.

### Reverse Proxy Example (Caddy)

```
kalla.example.com {
    reverse_proxy app:3000
}
```

### Backups

```bash
# Postgres backup
docker compose exec -T postgres pg_dump -U kalla kalla > backup.sql

# Restore
docker compose exec -T postgres psql -U kalla kalla < backup.sql
```

### Resource Requirements

| Deployment | RAM | CPU | Disk |
|------------|-----|-----|------|
| Development | 4 GB | 2 cores | 10 GB |
| Single VM | 4 GB | 2 cores | 20 GB |
| Cluster (per executor) | 2 GB | 1 core | minimal |
