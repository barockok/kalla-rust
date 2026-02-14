# Worker Autoscaling Design

**Goal:** Make kalla workers scale horizontally across any orchestrator (Kubernetes HPA/KEDA, Cloud Run, VM auto-scaling groups) by exposing Prometheus metrics and using a message queue for job distribution. Use Ballista as the native DataFusion distributed execution layer.

**Tech Stack:** Rust, NATS JetStream (job queue), Ballista (distributed DataFusion), Prometheus (metrics), Kubernetes/KEDA (orchestration)

---

## Architecture

```
User -> Ingress -> API Server :3001
                       |
                       v
                  NATS JetStream
                  +-------------+
                  | Stage Queue  |  (extract non-native sources)
                  | Exec Queue   |  (run reconciliation via Ballista)
                  +------+------+
                         |
              +----------+----------+
              |          |          |
          Worker 0   Worker 1   Worker N
          :9090      :9090      :9090
          /metrics   /metrics   /metrics
              |
              v
     Ballista Cluster
     +------------------+
     | Scheduler :50050  |
     | Executor 0 :50051 | --> S3 / MinIO (Parquet)
     | Executor 1 :50051 | --> S3 / MinIO (Parquet)
     | Executor N :50051 | --> S3 / MinIO (Parquet)
     +------------------+
```

### Components

| Component | Responsibility | Stateless? | Scales on |
|-----------|---------------|------------|-----------|
| API Server | HTTP, sessions, chat, classify sources, push jobs, track run completion | Yes | CPU (standard HPA) |
| Worker | Pull jobs, stage extraction, plan partitions, submit to Ballista, write evidence, heartbeat, reap stale jobs | Yes | Queue depth + wait time + saturation (KEDA) |
| Ballista Scheduler | Distribute DataFusion query plans to executors | No (singleton) | Fixed (1 instance) |
| Ballista Executor | Execute DataFusion queries on Parquet/CSV | Yes | CPU + memory (HPA) |
| NATS JetStream | Stage queue + exec queue | No | Fixed (3-node cluster) |
| PostgreSQL | Job tracking, run metadata, heartbeats | No | Managed service |
| S3 / MinIO | Staged Parquet files, evidence output | No | Managed service |

### Design Decisions

1. **Hybrid source model** -- Ballista executors only read Parquet and CSV. All other sources (PostgreSQL, BigQuery, MySQL) are staged to Parquet on S3 first. This keeps executors simple and makes connectors independently extensible via stage workers.
2. **Two queues** -- Stage queue for extraction jobs, exec queue for reconciliation jobs. The exec job is blocked until all staging completes.
3. **Workers are deployment-agnostic** -- They expose Prometheus `/metrics` and `/health` endpoints. Any orchestrator (K8s, Cloud Run, VM ASG) can consume these to make scaling decisions.
4. **Ballista for compute, not coordination** -- Ballista handles distributed DataFusion execution. Job lifecycle (tracking, retries, completion) is handled by workers + Postgres.
5. **No separate reaper process** -- Every worker runs a reaper loop as a background tokio task. Atomic SQL prevents double-reclaims.

---

## Source Classification

When the API server receives a reconciliation run request, it classifies each source:

| Source Type | Classification | Staging Required? |
|-------------|---------------|-------------------|
| Parquet on S3 | Native | No -- executor reads directly |
| CSV on S3 | Native | No -- executor reads directly |
| Parquet local | Native | No -- register as ListingTable |
| CSV local | Native | No -- register as ListingTable |
| PostgreSQL | Non-native | Yes -- stage worker extracts to Parquet on S3 |
| BigQuery | Non-native | Yes -- stage worker extracts to Parquet on S3 |
| MySQL | Non-native | Yes -- stage worker extracts to Parquet on S3 |
| Elasticsearch | Non-native | Yes -- stage worker extracts to Parquet on S3 |

Adding a new data source means writing a new stage worker connector. Executors never change.

---

## Job Lifecycle

### Full Flow

```
POST /api/runs (recipe)
  |
  v
API Server
  1. Validate recipe
  2. Classify sources:
     - invoices.parquet (S3) -> native, skip staging
     - postgres://payments  -> needs staging
  3. Create run_staging_tracker in Postgres
  4. Push staging jobs to Stage Queue
  5. Exec job waits until all staging completes

Stage Workers (parallel)
  1. Pull staging job from Stage Queue
  2. Connect to source (e.g. PostgreSQL)
  3. If source > chunk_threshold_rows:
     a. COUNT(*) to estimate size
     b. Split into N chunks (up to max_parallel_chunks)
     c. Push N chunk-jobs back to Stage Queue
  4. If small or is a chunk-job:
     a. SELECT with scope filters
     b. Write as Parquet to S3
     c. ACK job
     d. Increment run_staging_tracker.completed_chunks

Completion Gate (atomic SQL)
  When last chunk ACKs:
    completed_chunks == total_chunks
    -> Push exec job to Exec Queue

Planner Worker
  1. Pull exec job from Exec Queue
  2. All sources are now Parquet on S3
  3. Transpile recipe -> SQL
  4. Submit query plan to Ballista scheduler
  5. Stream results back
  6. Write evidence to S3
  7. ACK job
  8. Mark run complete
```

### Parallel Staging

Large sources are split into parallel chunks for extraction:

```
Stage Job: "Extract postgres://payments (50M rows)"

  Stage Worker picks it up:
    Phase 1: Plan
      SELECT COUNT(*) -> 50M rows
      max_parallel_chunks: 10
      chunk_size: 50M / 10 = 5M rows each

    Phase 2: Fan out
      Push 10 chunk-jobs to Stage Queue:
        chunk-0: WHERE id >= 0     AND id < 5M    -> part-00.parquet
        chunk-1: WHERE id >= 5M    AND id < 10M   -> part-01.parquet
        ...
        chunk-9: WHERE id >= 45M   AND id < 50M   -> part-09.parquet

  Multiple workers pull chunks in parallel
  Each writes one Parquet part file to S3

  Result: s3://staging/run-123/payments/
            part-00.parquet
            part-01.parquet
            ...
            part-09.parquet
```

No combine step needed. DataFusion/Ballista natively reads a directory of Parquet files as one table via `ListingTable`.

### Configuration

```yaml
staging:
  max_parallel_chunks: 10        # max chunk-jobs per source
  chunk_threshold_rows: 1000000  # don't split below 1M rows
  partition_key: "id"            # column to partition on
```

---

## Completion Tracking

The API server creates a tracking record in PostgreSQL for each run:

```sql
CREATE TABLE run_staging_tracker (
    run_id          UUID PRIMARY KEY,
    status          TEXT NOT NULL DEFAULT 'staging',  -- staging | ready | executing | completed | failed
    total_chunks    INTEGER NOT NULL,
    completed_chunks INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Stage workers increment `completed_chunks` atomically on ACK:

```sql
UPDATE run_staging_tracker
SET completed_chunks = completed_chunks + 1,
    updated_at = now()
WHERE run_id = $1
RETURNING completed_chunks, total_chunks;
```

If `completed_chunks = total_chunks`, the worker that executed this query pushes the exec job to the Exec Queue. Postgres row-level locking guarantees only one worker wins.

---

## Job Health: Heartbeats, Timeouts, Recovery

### Job Record

```sql
CREATE TABLE jobs (
    job_id          UUID PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES run_staging_tracker(run_id),
    job_type        TEXT NOT NULL,  -- stage_plan | stage_chunk | exec
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | claimed | completed | failed
    claimed_by      TEXT,           -- worker identifier
    claimed_at      TIMESTAMPTZ,
    last_heartbeat  TIMESTAMPTZ,
    timeout_seconds INTEGER NOT NULL DEFAULT 300,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    payload         JSONB NOT NULL,  -- recipe, source config, chunk range, etc.
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Worker Behavior

```
Worker pulls job:
  UPDATE jobs SET status='claimed', claimed_by=$worker_id,
    claimed_at=now(), last_heartbeat=now(), attempts=attempts+1
  WHERE job_id=$1

While executing (every 30s):
  UPDATE jobs SET last_heartbeat=now() WHERE job_id=$1

On success:
  UPDATE jobs SET status='completed' WHERE job_id=$1

On failure:
  UPDATE jobs SET status='pending', claimed_by=NULL WHERE job_id=$1
  (job goes back to queue for retry)
```

### Reaper (runs inside every worker)

Every worker runs a reaper loop as a background tokio task (every 60 seconds):

```sql
-- Reclaim stale jobs (atomic, safe with multiple reapers)
UPDATE jobs
SET status = 'pending',
    claimed_by = NULL
WHERE status = 'claimed'
  AND last_heartbeat < now() - (timeout_seconds || ' seconds')::interval
  AND attempts < max_attempts
RETURNING job_id, run_id, job_type;
```

For jobs that exceed `max_attempts`:

```sql
UPDATE jobs SET status = 'failed'
WHERE status = 'claimed'
  AND last_heartbeat < now() - (timeout_seconds || ' seconds')::interval
  AND attempts >= max_attempts
RETURNING job_id, run_id;
```

When a job is marked failed, the reaper also marks the entire run as failed.

### Timeout Configuration

```yaml
jobs:
  stage_plan:
    timeout_seconds: 120
    heartbeat_interval: 30
    max_attempts: 3

  stage_chunk:
    timeout_seconds: 300
    heartbeat_interval: 30
    max_attempts: 3

  exec:
    timeout_seconds: 1800
    heartbeat_interval: 60
    max_attempts: 2

reaper:
  interval_seconds: 60

run:
  total_timeout_seconds: 7200   # 2 hour max for entire run
```

### Failure Scenarios

| Scenario | What happens |
|----------|-------------|
| Worker OOM-killed mid-chunk | No heartbeat, reaper reclaims after timeout, job retried |
| Network partition | Heartbeat fails, reaper reclaims, worker's late ACK ignored (job re-assigned) |
| Poison job (always crashes) | Retried max_attempts times, marked failed, run marked failed |
| Entire run too slow | total_timeout_seconds exceeded, reaper marks run failed, cancels remaining jobs |
| Worker finishes but ACK lost | Reaper reclaims, another worker re-executes, idempotent (overwrites same Parquet on S3) |

---

## Worker Process

Each worker is a standalone binary with three concurrent loops:

```
Worker Process
+-------------------------------+
|                               |
|  +----------+  +-----------+  |
|  | Job Loop  |  | Reaper    |  |
|  |           |  | (tokio    |  |
|  | pull()    |  |  interval)|  |
|  | heartbeat()|  |           |  |
|  | execute() |  | every 60s |  |
|  | ack()     |  | scan stale|  |
|  |           |  | reclaim   |  |
|  +----------+  +-----------+  |
|                               |
|  +---------------------------+|
|  | Metrics HTTP :9090        ||
|  | GET /metrics              ||
|  | GET /health               ||
|  | GET /ready                ||
|  +---------------------------+|
+-------------------------------+
```

### Endpoints

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `GET /health` | Liveness -- is the process alive? | `200` always, `503` if panicking |
| `GET /ready` | Readiness -- can it accept work? | `200` if connected to queue + database, `503` otherwise |
| `GET /metrics` | Prometheus scrape | Text format |

### Metrics

```prometheus
# -- Scaling signals --
kalla_stage_queue_depth 12
kalla_exec_queue_depth 3
kalla_queue_oldest_wait_seconds 45.2
kalla_worker_active_jobs 1

# -- Operational observability --
kalla_worker_current_job_type{type="stage_chunk"} 1
kalla_worker_current_job_duration_seconds 82.3
kalla_worker_jobs_completed_total{type="stage_chunk"} 142
kalla_worker_jobs_completed_total{type="stage_plan"} 15
kalla_worker_jobs_completed_total{type="exec"} 15
kalla_reaper_jobs_reclaimed_total 3
kalla_reaper_jobs_failed_total 1
kalla_worker_last_heartbeat_seconds_ago 2.1
kalla_worker_rows_processed_total 1250000
```

### Scaling Signals

| Metric | Meaning | Scale-up when |
|--------|---------|---------------|
| `kalla_stage_queue_depth + kalla_exec_queue_depth` | Jobs waiting | > 2 |
| `kalla_queue_oldest_wait_seconds` | Longest wait | > 30s |
| `avg(kalla_worker_active_jobs)` | Worker saturation | > 0.8 |

---

## Kubernetes Deployment

### Resource Topology

| Component | K8s Type | Replicas | Why |
|-----------|----------|----------|-----|
| API Server | Deployment | 2-10 (HPA) | Stateless HTTP handlers |
| Worker | Deployment | 1-50 (KEDA) | Stateless job consumers, pull from queue |
| Ballista Scheduler | Deployment | 1 | Singleton query plan distributor |
| Ballista Executor | StatefulSet | 2-20 (HPA) | Needs stable identity for scheduler tracking |
| NATS | StatefulSet | 3 | JetStream cluster, needs stable storage + identity |
| PostgreSQL | External | - | Managed service (Cloud SQL / RDS) |
| S3 | External | - | Managed object storage (or MinIO StatefulSet) |

### Autoscaler Definitions

```yaml
# 1. API Server -- standard CPU-based HPA
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: api-server
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: api-server
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          averageUtilization: 70

---
# 2. Worker -- KEDA scales on NATS queue depth + Prometheus metrics
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: worker
spec:
  scaleTargetRef:
    name: worker
  minReplicaCount: 1
  maxReplicaCount: 50
  cooldownPeriod: 300
  triggers:
    - type: nats-jetstream
      metadata:
        account: "$G"
        stream: kalla-jobs
        consumer: workers
        lagThreshold: "2"
    - type: prometheus
      metadata:
        serverAddress: http://prometheus:9090
        query: max(kalla_queue_oldest_wait_seconds)
        threshold: "30"
    - type: prometheus
      metadata:
        serverAddress: http://prometheus:9090
        query: avg(kalla_worker_active_jobs)
        threshold: "0.8"

---
# 3. Ballista Executors -- CPU/memory HPA
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ballista-executor
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: ballista-executor
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          averageUtilization: 75
    - type: Resource
      resource:
        name: memory
        target:
          averageUtilization: 80
```

### Network Flow

```
User -> Ingress -> api-server:3001
                       |
                       v
                  nats:4222 (push jobs)
                       |
                       v
                 worker (pull jobs)
                   |         |
                   v         v
             PostgreSQL    ballista-scheduler:50050
             (heartbeat,     |
              tracking)      v
                       ballista-executor:50051
                             |
                             v
                        S3 / MinIO
                       (read/write Parquet)
```

---

## What This Does NOT Change

- The Match Recipe JSON schema (kalla-recipe crate)
- The agentic AI orchestrator (TypeScript, 7-phase state machine)
- The frontend React components
- The evidence store format (Parquet)
- The API endpoints contract (existing REST API)

---

## Files to Change

### New Crate: kalla-worker

| File | Description |
|------|-------------|
| `crates/kalla-worker/Cargo.toml` | Dependencies: nats, prometheus, axum (metrics), ballista, kalla-core, kalla-connectors, kalla-recipe, kalla-evidence |
| `crates/kalla-worker/src/main.rs` | Binary entry point: start job loop, reaper, metrics server |
| `crates/kalla-worker/src/job_loop.rs` | Pull from NATS, classify job type, dispatch to handler |
| `crates/kalla-worker/src/stage.rs` | Stage handler: connect to source, plan chunks, extract to Parquet |
| `crates/kalla-worker/src/exec.rs` | Exec handler: transpile recipe, submit to Ballista, stream results, write evidence |
| `crates/kalla-worker/src/reaper.rs` | Background task: scan stale jobs, reclaim or fail |
| `crates/kalla-worker/src/heartbeat.rs` | Heartbeat loop for active jobs |
| `crates/kalla-worker/src/metrics.rs` | Prometheus metrics endpoint and gauge definitions |
| `crates/kalla-worker/src/health.rs` | /health and /ready endpoints |

### Modified

| File | Change |
|------|--------|
| `kalla-server/src/main.rs` | Remove in-process worker, add NATS client, push jobs to queue |
| `kalla-server/src/worker.rs` | Delete (replaced by kalla-worker crate) |
| `scripts/init.sql` | Add `jobs` and `run_staging_tracker` tables |
| `docker-compose.yml` | Add nats, ballista-scheduler, ballista-executor, kalla-worker services |
| `Cargo.toml` | Add kalla-worker to workspace members |

### New: Kubernetes Manifests

| File | Description |
|------|-------------|
| `k8s/api-server.yaml` | Deployment + Service + HPA |
| `k8s/worker.yaml` | Deployment + KEDA ScaledObject + PodMonitor |
| `k8s/ballista-scheduler.yaml` | Deployment + Service |
| `k8s/ballista-executor.yaml` | StatefulSet + HPA |
| `k8s/nats.yaml` | StatefulSet + headless Service (or use NATS Helm chart) |
| `k8s/keda-triggers.yaml` | KEDA ScaledObject with NATS + Prometheus triggers |
