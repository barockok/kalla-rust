# Unified `kallad` Binary Design

## Goal

Replace three standalone binaries (`kalla-worker`, `kalla-scheduler`, `kalla-executor`) with a single `kallad` binary using subcommands. Embed an HTTP job runner into the scheduler so the API talks directly to the scheduler — eliminating NATS, worker-side Postgres job tracking, and the standalone worker process.

## Architecture

```
kallad scheduler                    kallad executor (N replicas)
┌────────────────────┐              ┌──────────────────┐
│ Axum HTTP (:8080)  │              │ Ballista Executor │
│  POST /api/jobs    │              │ Flight (:50051)   │
│  GET /health       │              │ gRPC  (:50052)    │
│  GET /metrics      │              │                   │
├────────────────────┤              │ Connects to       │
│ Ballista gRPC      │◄────────────│ scheduler:50050   │
│ Scheduler (:50050) │              └───────────────────┘
├────────────────────┤
│ Runner (tokio task)│
│  - sql_stream()   │
│  - batch counting  │
│  - callbacks       │
│  - evidence write  │
└────────────────────┘

Mode detection:
- Executors connected → Ballista cluster execution
- No executors → local DataFusion (dev mode)
```

## Subcommands

```
kallad scheduler [--http-port 8080] [--grpc-port 50050]
kallad executor  [--scheduler-url localhost:50050] [--flight-port 50051] [--grpc-port 50052]
```

Env vars still work; CLI args override them.

## Scheduler Embedded HTTP Runner

The scheduler runs two servers concurrently (Axum HTTP + Ballista gRPC). When a job arrives via `POST /api/jobs`:

1. Choose engine: if executors connected → `ReconciliationEngine::new_cluster("df://localhost:{grpc_port}", KallaPhysicalCodec)`, else → `ReconciliationEngine::new()` (local DataFusion)
2. Register sources with partitioning (Postgres partitioned, CSV byte-range)
3. Execute `match_sql` via `engine.sql_stream()`, count progress per batch, report via callbacks
4. Count unmatched via LEFT ANTI JOIN
5. Write evidence, callback complete/error

Progress reporting uses batch counting from `sql_stream()` — as executor partitions complete, batches flow back to the runner, giving real progress.

## What's Removed

| Component | Reason |
|-----------|--------|
| NATS (async-nats, queue.rs, job_loop.rs) | API calls scheduler HTTP directly |
| Worker-side Postgres job tracking (heartbeat.rs, reaper.rs, jobs table) | Scheduler tracks in-flight jobs in-process; API detects timeout |
| 3 standalone binaries | Replaced by 1 unified binary |
| `docker-compose.scaled.yml` | Replaced by `docker-compose.cluster.yml` |
| Scaled-mode config (`nats_url`, `database_url`, heartbeat/reaper intervals) | No longer needed |

## What's Preserved

- `POST /api/jobs` contract — API needs zero changes
- Progress callbacks (batch-counting from sql_stream)
- Source registration with partitioning (Postgres, CSV)
- Custom codec (PostgresScanExec, CsvRangeScanExec)
- Health/metrics endpoints
- Local dev experience (`kallad scheduler` just works, no executors needed)

## Crate Structure

```
crates/
  kallad/                  ← NEW: thin CLI crate
    Cargo.toml             ← depends on kalla-ballista, kalla-worker
    src/main.rs            ← clap App: scheduler/executor subcommands

  kalla-ballista/          ← MODIFIED
    src/
      runner.rs            ← NEW: HTTP server + job execution via Ballista
      lib.rs               ← add pub mod runner, expose start_scheduler(), start_executor()
      bin/                 ← DELETE: kalla-scheduler.rs, kalla-executor.rs

  kalla-worker/            ← SIMPLIFIED: becomes library only
    src/
      main.rs              ← DELETE (entry point moves to kallad)
      lib.rs               ← expose single-mode logic as library function
      queue.rs             ← DELETE
      job_loop.rs          ← DELETE
      heartbeat.rs         ← DELETE
      reaper.rs            ← DELETE
      exec.rs              ← keep register_source(), count_unmatched(), extract_first_key()
      http_api.rs          ← keep JobRequest, CallbackClient types
      health.rs            ← keep
      metrics.rs           ← keep
      config.rs            ← simplify (remove NATS/DB/heartbeat/reaper fields)
```

## Deployment

### Docker (single image)

```dockerfile
FROM rust:1.83 AS builder
RUN cargo build --release --bin kallad

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/kallad /usr/local/bin/kallad
ENTRYPOINT ["kallad"]
```

### docker-compose.cluster.yml

```yaml
services:
  app:
    environment:
      WORKER_URL: http://scheduler:8080
  scheduler:
    image: kallad
    command: ["scheduler"]
    ports: ["8080:8080", "50050:50050"]
  executor:
    image: kallad
    command: ["executor", "--scheduler-url", "scheduler:50050"]
    deploy:
      replicas: 2
  postgres:
    image: postgres:16
  minio:
    image: minio/minio
```

### Kubernetes (3 components)

| Deployment | Replicas | Command | Service Ports |
|-----------|----------|---------|---------------|
| api | 1+ | Next.js | :3000 |
| scheduler | 1 (singleton) | `kallad scheduler` | :8080 (HTTP), :50050 (gRPC) |
| executor | N (HPA) | `kallad executor` | :50051 (Flight) |

## CI Changes

- Build step: `cargo build --release --bin kallad`
- Integration test: `kallad scheduler` (local mode, no executors)
- Cluster benchmark: `kallad scheduler` + 2x `kallad executor`
- Single-mode benchmark: `kallad scheduler` (auto-detects no executors)
