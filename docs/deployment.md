# Kalla Deployment Guide

## Architecture Overview

Kalla consists of two deployable components:

| Component | Tech | Role |
|-----------|------|------|
| **Kalla App** | Next.js | Web UI + API (agentic orchestrator, CRUD, Postgres) |
| **kallad** | Rust, DataFusion, Ballista | Unified binary with `scheduler` and `executor` subcommands |

The App owns all state (Postgres, run tracking, recipe storage). The `kallad` binary runs in two roles:

- **`kallad scheduler`** — HTTP API for job submission + Ballista gRPC scheduler for distributed query coordination + embedded HTTP runner for reconciliation execution
- **`kallad executor`** — Ballista executor that registers with the scheduler and executes query partitions via Arrow Flight

In single-node deployments, the scheduler detects no executors (10s probe timeout) and runs DataFusion locally. In cluster deployments, the scheduler distributes partitioned reads across executor instances.

### Architecture Diagram

```
Browser --> App (:3000)
              |
              |--> Postgres (:5432)           [state]
              |--> kallad scheduler (:8080 HTTP + :50050 gRPC)
                     |
                     |--> kallad executor 1    [distributed execution]
                     |--> kallad executor 2
                     |--> ...
                     |
                     Executors read from Postgres via PostgresScanExec
                     Executors read from S3 via CsvRangeScanExec
```

### Execution Flow

1. App submits `POST /api/jobs` with `run_id`, `match_sql`, `sources[]`, `callback_url`
2. Scheduler registers each source as a partitioned `TableProvider`:
   - `postgres://` URIs → `PostgresPartitionedTable` (LIMIT/OFFSET with ORDER BY ctid)
   - `s3://` URIs → `CsvByteRangeTable` (byte-range reads)
3. Scheduler probes Ballista cluster with `SELECT 1` (10s timeout)
   - Executors respond → distributed execution via Ballista
   - Timeout → local DataFusion fallback
4. Executes `match_sql`, collects matched records
5. Computes unmatched counts from matched records (in-memory, no post-match queries)
6. Writes evidence (Parquet), reports results via HTTP callback to App

---

## 1. Development Setup (Local)

Run Postgres + MinIO in Docker, everything else natively for fast iteration.

### Start Infrastructure

```bash
docker compose up -d
```

This starts Postgres (port 5432) and MinIO (port 9000, console 9001) with auto-created `kalla-uploads` and `kalla-results` buckets.

### Start the Scheduler

```bash
export RUST_LOG=info
cargo run --bin kallad -- scheduler --http-port 9090
```

The scheduler starts in single mode (no executors), listens on port 9090 for HTTP job submissions, and uses local DataFusion for execution.

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

### Optional: Add Local Executors

To test distributed execution locally:

```bash
# Terminal 2: start executor 1
cargo run --bin kallad -- executor \
  --scheduler-host localhost --scheduler-port 50050 \
  --flight-port 50051 --grpc-port 50052

# Terminal 3: start executor 2
cargo run --bin kallad -- executor \
  --scheduler-host localhost --scheduler-port 50050 \
  --flight-port 50053 --grpc-port 50054
```

The scheduler's default gRPC port is 50050 (started alongside the HTTP server).

---

## 2. Single VM Production Deployment

One machine, no executors. Good for datasets up to ~1M rows.

```bash
export ANTHROPIC_API_KEY=sk-ant-...
docker compose -f docker-compose.single.yml up -d
```

### What Runs

| Service | Port | Notes |
|---------|------|-------|
| `app` | 3000 | Next.js (web + API) |
| `scheduler` | 9090 | kallad scheduler (HTTP + local DataFusion fallback) |
| `postgres` | 5432 | App database + source data |
| `minio` | 9000/9001 | S3-compatible storage for CSV uploads |

### How It Works

```
Browser --> App (:3000)
              |
              |--> Postgres (:5432)              [state + source data]
              |--> kallad scheduler (:9090)       [compute]
                     |
                     |--> local DataFusion        [query execution]
                     |--> App callback            [progress/complete/error]
```

The app dispatches jobs to the scheduler via `POST /api/jobs`. The scheduler registers sources as partitioned tables, executes queries using local DataFusion (no executors detected), writes evidence, and reports status back to the app via HTTP callbacks.

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
| `executor-1` | — | 1 | kallad executor (Flight :50051, gRPC :50052) |
| `executor-2` | — | 1 | kallad executor (Flight :50051, gRPC :50052) |
| `postgres` | 5432 | 1 | App database + source data |

### How It Works

```
Browser --> App (:3000)
              |
              |--> Postgres (:5432)
              |--> kallad scheduler (:8080 HTTP, :50050 gRPC)
                     |
                     |--> kallad executor 1 (flight + gRPC)
                     |--> kallad executor 2 (flight + gRPC)
                     |
                     Executors read from Postgres directly via PostgresScanExec
                     Scheduler --> App callback [progress/complete/error]
```

The app submits jobs to the scheduler via HTTP. The scheduler coordinates distributed query execution across executors using Ballista's gRPC protocol. Each executor reads its assigned partition directly from Postgres (or S3 for CSV sources).

### Scaling Executors

Add more executor services to `docker-compose.cluster.yml` following the pattern of `executor-1` and `executor-2`:

```yaml
executor-3:
  build:
    context: .
    dockerfile: Dockerfile
  command: ["executor", "--scheduler-host", "scheduler", "--scheduler-port", "50050",
            "--flight-port", "50051", "--grpc-port", "50052", "--external-host", "executor-3"]
  environment:
    RUST_LOG: ${RUST_LOG:-info}
  depends_on:
    scheduler:
      condition: service_started
```

Each executor must have a unique `--external-host` value (used by the scheduler to route work). Flight and gRPC ports can be the same across executors since each runs in its own container.

---

## 4. Environment Variable Reference

### App (Next.js)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | Postgres connection string |
| `WORKER_URL` | Yes | — | Scheduler HTTP endpoint (e.g., `http://scheduler:9090`) |
| `ANTHROPIC_API_KEY` | Yes | — | API key for agentic recipe builder |
| `S3_ENDPOINT` | No | — | MinIO/S3 endpoint for file uploads |
| `S3_ACCESS_KEY` | No | — | S3 access key |
| `S3_SECRET_KEY` | No | — | S3 secret key |
| `S3_UPLOADS_BUCKET` | No | `kalla-uploads` | S3 bucket for CSV uploads |

### kallad scheduler

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RUST_LOG` | No | `info` | Log level (`trace`, `debug`, `info`, `warn`, `error`) |
| `HTTP_PORT` | No | `8080` | HTTP API port |
| `GRPC_PORT` | No | `50050` | Ballista gRPC port |
| `BALLISTA_PARTITIONS` | No | `4` | Number of partitions for source reads |
| `STAGING_PATH` | No | `./staging` | Local staging dir for evidence files |
| `BIND_HOST` | No | `0.0.0.0` | Bind address |

CLI flags: `--http-port`, `--grpc-port`, `--partitions`, `--staging-path`, `--bind-host`

### kallad executor

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RUST_LOG` | No | `info` | Log level |
| `SCHEDULER_HOST` | No | `localhost` | Scheduler hostname |
| `SCHEDULER_PORT` | No | `50050` | Scheduler gRPC port |
| `BIND_PORT` | No | `50051` | Arrow Flight port |
| `BIND_GRPC_PORT` | No | `50052` | gRPC port |
| `BIND_HOST` | No | `0.0.0.0` | Bind address |
| `EXTERNAL_HOST` | No | auto-detect | Hostname advertised to scheduler |

CLI flags: `--scheduler-host`, `--scheduler-port`, `--flight-port`, `--grpc-port`, `--bind-host`, `--external-host`

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
| Scheduler | `GET /metrics` on HTTP port | Prometheus |
| Postgres | `pg_isready -U kalla` | CLI |
| App | `GET /` | HTTP |

---

## 6. Production Considerations

### Security

- **Change default Postgres password.** The default `kalla_secret` is for development only.
- **Restrict port exposure.** Don't expose Postgres (5432) or gRPC ports (50050-50052) to the public internet.
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
| Cluster (scheduler) | 2 GB | 2 cores | 10 GB |
| Cluster (per executor) | 2 GB | 1 core | minimal |

### Performance

Benchmark results on a single machine (Postgres source data):

| Dataset | Single Node | Cluster (2 executors) |
|---------|------------|----------------------|
| 100K rows | ~1.5s | ~1.9s |
| 1M rows | ~12s | ~9.6s |
| 5M rows | ~55s | ~44s |

Cluster mode shows increasing advantage as dataset size grows. The crossover point where cluster overhead is offset by parallelism is around 500K rows.
