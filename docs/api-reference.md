# Kalla API Reference

Kalla is a reconciliation engine built on DataFusion and Ballista. The runner exposes an HTTP API for submitting reconciliation jobs and monitoring health.

**Default bind address:** `http://localhost:50050`

---

## Table of Contents

- [Overview](#overview)
- [Health & Readiness](#health--readiness)
  - [Health Check](#health-check)
  - [Readiness Check](#readiness-check)
  - [Metrics](#metrics)
- [Jobs](#jobs)
  - [Submit Job](#submit-job)
- [Data Structures](#data-structures)
  - [JobRequest](#jobrequest)
  - [ResolvedSource](#resolvedsource)
  - [FilterCondition](#filtercondition)
  - [Recipe](#recipe)
  - [RecipeSource](#recipesource)
  - [Callbacks](#callbacks)
- [Supported Data Sources](#supported-data-sources)
- [Error Handling](#error-handling)

---

## Overview

The Kalla runner accepts reconciliation jobs via `POST /api/jobs`. Each job specifies a `match_sql` query, resolved data sources, and an output path. The runner registers sources with DataFusion, executes the SQL, and writes matched/unmatched results to Parquet files.

Progress and completion are reported via HTTP callbacks to the `callback_url` provided in the job request.

---

## Health & Readiness

### Health Check

#### `GET /health`

Returns `200 OK` if the server is running.

**Response:** `200 OK` (plain text)

```bash
curl http://localhost:50050/health
```

---

### Readiness Check

#### `GET /ready`

Returns `200 OK` when the runner is ready to accept jobs.

```bash
curl http://localhost:50050/ready
```

---

### Metrics

#### `GET /metrics`

Returns Prometheus-format metrics for monitoring.

```bash
curl http://localhost:50050/metrics
```

---

## Jobs

### Submit Job

#### `POST /api/jobs`

Submits a reconciliation job for asynchronous execution. The job is queued and executed in the background. Progress and results are reported via callbacks.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `run_id` | UUID | yes | Unique identifier for this run |
| `callback_url` | string | yes | URL to receive progress and completion callbacks |
| `match_sql` | string | yes | DataFusion SQL query for matching records |
| `sources` | array | yes | Resolved data sources (see [ResolvedSource](#resolvedsource)) |
| `output_path` | string | yes | Directory path for output Parquet files |
| `primary_keys` | object | yes | Map of source alias to primary key column names |

**Example Request:**

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "callback_url": "http://localhost:3001/api/runs/a1b2c3d4/callback",
  "match_sql": "SELECT l.invoice_id, r.payment_id FROM left_src l JOIN right_src r ON l.invoice_id = r.reference_number AND tolerance_match(l.amount, r.paid_amount, 0.02)",
  "sources": [
    {
      "alias": "left_src",
      "uri": "postgres://user:pass@localhost:5432/db?table=invoices"
    },
    {
      "alias": "right_src",
      "uri": "postgres://user:pass@localhost:5432/db?table=payments"
    }
  ],
  "output_path": "/data/runs/a1b2c3d4",
  "primary_keys": {
    "left_src": ["invoice_id"],
    "right_src": ["payment_id"]
  }
}
```

**Response Body (200):**

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "accepted"
}
```

**Error Responses:**

| Status | Condition |
|---|---|
| 500 | Job queue is full or internal error |

**curl:**

```bash
curl -X POST http://localhost:50050/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "callback_url": "http://localhost:3001/api/runs/a1b2c3d4/callback",
    "match_sql": "SELECT l.invoice_id, r.payment_id FROM left_src l JOIN right_src r ON l.invoice_id = r.reference_number",
    "sources": [
      { "alias": "left_src", "uri": "postgres://user:pass@localhost:5432/db?table=invoices" },
      { "alias": "right_src", "uri": "postgres://user:pass@localhost:5432/db?table=payments" }
    ],
    "output_path": "/data/runs/a1b2c3d4",
    "primary_keys": {
      "left_src": ["invoice_id"],
      "right_src": ["payment_id"]
    }
  }'
```

---

## Data Structures

### JobRequest

The payload for `POST /api/jobs`.

```json
{
  "run_id": "UUID",
  "callback_url": "string",
  "match_sql": "string",
  "sources": [ResolvedSource],
  "output_path": "string",
  "primary_keys": { "alias": ["column"] }
}
```

| Field | Type | Description |
|---|---|---|
| `run_id` | UUID | Unique run identifier |
| `callback_url` | string | HTTP endpoint for progress/completion callbacks |
| `match_sql` | string | DataFusion SQL query referencing source aliases as table names |
| `sources` | array | List of [ResolvedSource](#resolvedsource) objects |
| `output_path` | string | Directory for output Parquet files (matched, unmatched_left, unmatched_right) |
| `primary_keys` | object | Maps each source alias to its primary key column(s) for unmatched derivation |

---

### ResolvedSource

A data source with a resolved URI, ready for registration.

```json
{
  "alias": "left_src",
  "uri": "postgres://user:pass@host:5432/db?table=invoices",
  "filters": [FilterCondition]
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `alias` | string | yes | Table alias used in `match_sql` |
| `uri` | string | yes | Data source URI (see [Supported Data Sources](#supported-data-sources)) |
| `filters` | array | no | Optional pre-filters applied at registration time (default: `[]`) |

---

### FilterCondition

Filters applied to sources before matching. Translated to SQL WHERE clauses.

```json
{
  "column": "amount",
  "op": "gte",
  "value": 1000
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `column` | string | yes | Column name to filter on |
| `op` | string | yes | Filter operator |
| `value` | varies | yes | Value to compare against |

**Filter operators:**

| Operator | Value Type | SQL Equivalent |
|---|---|---|
| `eq` | string or number | `column = value` |
| `neq` | string or number | `column != value` |
| `gt` | string or number | `column > value` |
| `gte` | string or number | `column >= value` |
| `lt` | string or number | `column < value` |
| `lte` | string or number | `column <= value` |
| `between` | array of 2 strings | `column BETWEEN a AND b` |
| `in` | array of strings | `column IN (a, b, ...)` |
| `like` | string | `column LIKE pattern` |

---

### Recipe

The recipe schema used by the orchestration layer. Recipes define what SQL to execute and which sources to use.

```json
{
  "recipe_id": "invoice-payment-match",
  "name": "Invoice-Payment Reconciliation",
  "description": "Match invoices to payments by reference number and amount",
  "match_sql": "SELECT l.invoice_id, r.payment_id FROM invoices l JOIN payments r ON l.invoice_id = r.reference_number AND tolerance_match(l.amount, r.paid_amount, 0.02)",
  "match_description": "Matches invoices to payments where reference numbers are identical and amounts are within absolute tolerance of 0.02",
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
      "schema": ["payment_id", "reference_number", "amount"],
      "primary_key": ["payment_id"]
    }
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `recipe_id` | string | yes | Unique recipe identifier |
| `name` | string | yes | Human-readable name |
| `description` | string | yes | Description of the reconciliation |
| `match_sql` | string | yes | DataFusion SQL query referencing source aliases as table names |
| `match_description` | string | yes | Human-readable explanation of the SQL logic |
| `sources` | object | yes | Left and right [RecipeSource](#recipesource) definitions |

---

### RecipeSource

A data source definition within a recipe.

```json
{
  "alias": "invoices",
  "type": "postgres",
  "uri": "postgres://host/db?table=invoices",
  "primary_key": ["invoice_id"]
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `alias` | string | yes | Source alias used in `match_sql` |
| `type` | string | yes | Source type: `"postgres"`, `"elasticsearch"`, or `"file"` |
| `uri` | string | conditional | Connection URI (required for persistent sources, omitted for file sources) |
| `schema` | array of strings | conditional | Expected column names (required for file sources, omitted for persistent) |
| `primary_key` | array of strings | yes | Primary key column(s) for unmatched record derivation |

**Source types:**

| Type | Description |
|---|---|
| `postgres` | PostgreSQL table via partitioned reads |
| `elasticsearch` | Elasticsearch index (planned) |
| `file` | Disposable file source — schema stored, file uploaded at execution time |

---

### Callbacks

The runner reports progress and completion to the `callback_url` via HTTP POST.

#### Progress Callback

Sent during job execution to report staging and matching progress.

**Staging progress:**

```json
{
  "stage": "staging",
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "progress": 0.5,
  "source": "left_src"
}
```

**Matching progress:**

```json
{
  "stage": "matching",
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "progress": 1.0,
  "matched_count": 4500
}
```

#### Completion Callback

Sent when the job finishes successfully.

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "matched_count": 4500,
  "unmatched_left_count": 500,
  "unmatched_right_count": 300,
  "output_paths": {
    "matched": "/data/runs/a1b2c3d4/matched.parquet",
    "unmatched_left": "/data/runs/a1b2c3d4/unmatched_left.parquet",
    "unmatched_right": "/data/runs/a1b2c3d4/unmatched_right.parquet"
  }
}
```

#### Error Callback

Sent when the job fails.

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "error": "Failed to connect to PostgreSQL: connection refused"
}
```

---

## Supported Data Sources

The runner supports the following URI schemes for data sources. Routing is handled by `register_source` based on URI prefix and file extension.

| URI Pattern | Connector | Description |
|---|---|---|
| `postgres://` or `postgresql://` | PostgresPartitionedTable | Partitioned reads via LIMIT/OFFSET with optional filter pushdown |
| `s3://.../*.csv` | CsvByteRangeTable | Byte-range partitioned CSV reads from S3-compatible storage |
| `*.csv` (local path) | DataFusion CSV | Local CSV file via DataFusion's built-in reader |
| `*` (local path, default) | DataFusion Parquet | Local Parquet file via DataFusion's built-in reader |

**PostgreSQL URI format:**

```
postgres://user:password@host:5432/database?table=table_name
```

The `table` query parameter is required and specifies the PostgreSQL table to read.

**S3 CSV URI format:**

```
s3://bucket/path/to/file.csv
```

Requires S3 credentials via environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, and optionally `AWS_ENDPOINT_URL` and `AWS_ALLOW_HTTP` for MinIO/LocalStack.

---

## Built-in UDFs

The runner registers custom DataFusion UDFs available in `match_sql`:

| Function | Signature | Description |
|---|---|---|
| `tolerance_match` | `tolerance_match(a, b, threshold) -> bool` | Returns `true` if `abs(a - b) <= threshold` (absolute tolerance) |

---

## Error Handling

All error responses return a plain-text error message with an appropriate HTTP status code.

| Status Code | Meaning |
|---|---|
| 200 | Success |
| 500 | Internal server error (queue full, execution failure) |
