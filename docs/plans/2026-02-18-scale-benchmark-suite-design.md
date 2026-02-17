# Kalla Scale Benchmark Suite — Design

## Goal

Demonstrate Kalla's reconciliation engine at scale: complex match patterns (1:N, M:1, M:N), 1B+ rows, autoscaling executors on GKE, with a single Terraform + script workflow that creates and tears down the entire environment.

## Architecture

```
Developer Laptop
│
├── bench.sh up    → Terraform → GKE Autopilot Cluster
├── bench.sh run   → kubectl   → Benchmark Job (in-cluster)
└── bench.sh down  → Terraform → Destroy everything

GKE Autopilot Cluster (kalla-bench namespace)
├── Deployment: kallad-scheduler (1 pod)
│   ├── HTTP :8080 (job submission + /metrics)
│   └── gRPC :50050 (Ballista scheduler)
├── Deployment: kallad-executor (0-16 pods, KEDA-scaled)
│   ├── Arrow Flight :50051
│   └── gRPC :50052
├── StatefulSet: postgres (1 pod, 200GB SSD PV)
│   └── :5432 (benchmark data source)
├── KEDA ScaledObject: executor autoscaler
│   └── Trigger: prometheus (kalla_runner_active_jobs > 0)
├── Job: data-seeder (seeds Postgres, exits)
└── Job: benchmark-runner (runs scenarios, collects reports)
```

## 1. New Benchmark Scenarios

### 1.1 Split Payments (1:N)

One invoice matched by multiple partial payments. Most common AP/AR pattern.

**Data shape:**
- Each invoice has 2-5 payments
- Payment amounts sum to invoice amount (within tolerance)
- `reference_number` contains the invoice_id for all partial payments

**match_sql:**
```sql
SELECT l.invoice_id, r.payment_id, l.amount AS invoice_amount,
       r.paid_amount AS payment_amount
FROM left_src l
JOIN right_src r ON l.invoice_id = r.reference_number
```

**Unmatched counting:** Works correctly — `distinct_left_keys` counts each invoice once even if matched by 3 payments.

**Data generation:** For N invoices, generate 2-5 payments each → ~3N payment rows. Split the invoice amount randomly into parts that sum to the original (with optional tolerance variance).

### 1.2 Batch Payments (M:1)

Multiple invoices settled by one bulk payment. Common in treasury/batch processing.

**Data shape:**
- Groups of 10-50 invoices share a `batch_ref`
- One payment per batch, amount = sum of group's invoice amounts
- Payment's `reference_number` = batch_ref

**New columns:** `batch_ref` on invoices table, `batch_ref` on payments table.

**match_sql:**
```sql
SELECT l.invoice_id, r.payment_id, l.batch_ref,
       l.amount AS invoice_amount, r.paid_amount AS batch_total
FROM left_src l
JOIN right_src r ON l.batch_ref = r.reference_number
```

**Unmatched counting:** Each of the M invoices maps to the same right key (batch payment). `distinct_right_keys` correctly counts one right match per batch.

### 1.3 Cross-match (M:N)

Multiple invoices matched to multiple payments via customer + currency + date window. Most complex pattern — shows full SQL flexibility.

**Data shape:**
- Multiple invoices per customer per month
- Multiple payments per customer per month
- Match on customer_id + currency + same month

**match_sql:**
```sql
SELECT l.invoice_id, r.payment_id, l.customer_id,
       l.amount AS invoice_amount, r.paid_amount AS payment_amount
FROM left_src l
JOIN right_src r
  ON l.customer_id = r.payer_id
  AND l.currency = r.currency
  AND SUBSTRING(l.invoice_date, 1, 7) = SUBSTRING(r.payment_date, 1, 7)
```

**Note:** This produces a cross-product per (customer, currency, month) — intentionally. Demonstrates the engine handling high fan-out joins.

### 1.4 Scenario Files

New scenario JSON files in `benchmarks/scenarios/`:

| File | Pattern | Rows | Mode |
|------|---------|------|------|
| `split_payments_1m.json` | 1:N | 1M invoices (~3M payments) | cluster |
| `split_payments_100m.json` | 1:N | 100M invoices (~300M payments) | cluster |
| `batch_payments_1m.json` | M:1 | 1M invoices (~20K payments) | cluster |
| `batch_payments_100m.json` | M:1 | 100M invoices (~2M payments) | cluster |
| `cross_match_1m.json` | M:N | 1M invoices, 1M payments | cluster |
| `cross_match_100m.json` | M:N | 100M invoices, 100M payments | cluster |
| `full_scale_1b.json` | 1:1 | 1B invoices | cluster |

### 1.5 Data Generation Changes

`benchmarks/datagen.py` gets new functions:
- `generate_split_payments(n, invoices)` — 2-5 payments per invoice
- `generate_batch_payments(n, invoices, batch_size=25)` — groups into batches
- `generate_cross_match_payments(n)` — independent payments matched by customer+month

`benchmarks/seed_postgres.py` gets:
- `--pattern` flag: `one_to_one` (default), `split`, `batch`, `cross`
- New table DDL with `batch_ref` column
- Chunked seeding (already exists) handles 1B rows via COPY

## 2. GCP Infrastructure (Terraform)

### 2.1 Directory Structure

```
infra/gcp-bench/
├── main.tf              # Provider, GKE cluster, Artifact Registry
├── variables.tf         # project, region, row count, executor count
├── outputs.tf           # cluster endpoint, report path
├── k8s.tf               # K8s resources (scheduler, executor, postgres, KEDA)
├── keda.tf              # KEDA Helm release + ScaledObject
├── docker.tf            # Build and push kallad image
├── bench.sh             # CLI wrapper (up/run/down/full)
├── k8s/
│   ├── namespace.yaml
│   ├── postgres.yaml    # StatefulSet + PVC + Service
│   ├── scheduler.yaml   # Deployment + Service
│   ├── executor.yaml    # Deployment (target for KEDA)
│   ├── seeder.yaml      # Job template
│   └── runner.yaml      # Job template
└── README.md
```

### 2.2 Terraform Resources

| Resource | Type | Purpose |
|----------|------|---------|
| GKE Autopilot cluster | `google_container_cluster` | Managed K8s, pay-per-pod |
| Artifact Registry | `google_artifact_registry_repository` | Store kallad Docker image |
| K8s namespace | `kubernetes_namespace` | `kalla-bench` isolation |
| Postgres StatefulSet | `kubernetes_stateful_set` | Self-managed Postgres with SSD PV |
| Postgres PVC | `kubernetes_persistent_volume_claim` | 200GB pd-ssd |
| Scheduler Deployment | `kubernetes_deployment` | 1 pod, kallad scheduler |
| Executor Deployment | `kubernetes_deployment` | 0-16 pods, kallad executor |
| KEDA | `helm_release` | KEDA operator for autoscaling |
| ScaledObject | `kubernetes_manifest` | KEDA trigger on active_jobs metric |

### 2.3 GKE Autopilot

GKE Autopilot removes node pool management:
- Pods request resources, Google provisions nodes automatically
- Pay only for pod resource requests (CPU, memory)
- No idle node costs when scaled to zero

Executor pod resources: `cpu: 4, memory: 8Gi` (e2-standard-4 equivalent).

### 2.4 Postgres StatefulSet

Single Postgres 16 pod with:
- 200GB `pd-ssd` PersistentVolume (enough for 1B rows of invoice+payment data, ~120GB)
- `shared_buffers=4GB`, `work_mem=256MB`, `max_connections=200`
- Exposed via ClusterIP Service at `postgres:5432`

## 3. CLI Script (`bench.sh`)

### 3.1 Commands

```bash
# Create infrastructure
./bench.sh up --project <gcp-project> --region us-central1

# Seed data + run benchmarks
./bench.sh run --rows 1000000000 --pattern all --executors 8

# Run specific pattern
./bench.sh run --rows 100000000 --pattern split --executors 4

# Tear down everything
./bench.sh down

# One-shot: up + run + down
./bench.sh full --project <gcp-project> --rows 1000000000
```

### 3.2 `up` Flow

1. Check prerequisites: `gcloud`, `terraform`, `kubectl`, `docker`
2. `gcloud auth application-default login` (if not already)
3. `terraform init && terraform apply -auto-approve`
4. Configure `kubectl` context for the new cluster
5. Wait for Postgres pod ready
6. Print connection info

### 3.3 `run` Flow

1. Scale executor deployment to `--executors N`
2. Wait for rollout complete
3. Run data seeder K8s Job (streams logs)
4. Run benchmark runner K8s Job (streams logs)
5. Copy report from pod to local `benchmarks/results/`
6. Print summary table

### 3.4 `down` Flow

1. `terraform destroy -auto-approve`
2. Clean up local kubectl context

### 3.5 `full` Flow

`up` → `run` → `down` in sequence. If any step fails, `down` still runs (cleanup).

## 4. KEDA Autoscaler

### 4.1 Trigger

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: kallad-executor
  namespace: kalla-bench
spec:
  scaleTargetRef:
    name: kallad-executor
  minReplicaCount: 0
  maxReplicaCount: 16
  cooldownPeriod: 60
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://kallad-scheduler:8080
        metricName: kalla_runner_active_jobs
        query: kalla_runner_active_jobs
        threshold: "1"
```

### 4.2 Behavior

| State | Executor Pods | Cost |
|-------|---------------|------|
| Idle (no jobs) | 0 | $0 |
| Job submitted | Scales to min (2) | ~$0.50/hr |
| Heavy load | Up to 16 | ~$4/hr |
| Job complete (60s cooldown) | Back to 0 | $0 |

### 4.3 Benchmark Pre-warm

For benchmarks, we bypass KEDA's scaling delay by explicitly scaling:

```bash
kubectl scale deployment kallad-executor --replicas=8
kubectl rollout status deployment/kallad-executor --timeout=120s
```

KEDA takes over after the benchmark completes, scaling back to 0.

## 5. Cost Estimate (1B row benchmark run)

| Component | Duration | Cost |
|-----------|----------|------|
| GKE Autopilot management | ~1hr | $0.10 |
| Scheduler pod (e2-standard-4) | ~1hr | $0.15 |
| 8 executor pods (e2-standard-4) | ~30min | $2.00 |
| Postgres pod (e2-standard-8) | ~1hr | $0.40 |
| 200GB pd-ssd | ~1hr | $0.03 |
| Data seeding I/O | one-time | $0.50 |
| **Total** | | **~$3-5** |

## 6. Non-Goals

- **Multi-cloud**: GCP only for now. The Terraform module is portable but we won't maintain AWS/Azure variants.
- **Production deployment**: This is for benchmarking only. No TLS, no auth, no multi-tenancy.
- **CI integration**: The `full` command could run in CI, but we won't set up GitHub Actions for it in this iteration.
- **CSV/S3 benchmarks on GCP**: Only Postgres source benchmarks. GCS-backed CSV could be added later.
