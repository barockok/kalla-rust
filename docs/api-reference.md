# Kalla API Reference

Kalla consists of two services:

- **Kalla Web** — Next.js orchestration layer for managing sources, recipes, runs, uploads, and chat. Default: `http://localhost:3001`
- **Kalla Runner** — Rust reconciliation engine (DataFusion + Ballista) that executes matching jobs. Default: `http://localhost:50050`

---

## Table of Contents

- [Kalla Web API](#kalla-web-api)
  - [Sources](#sources)
  - [Recipes](#recipes)
  - [Runs](#runs)
  - [Uploads](#uploads)
  - [Chat](#chat)
  - [Worker Callbacks](#worker-callbacks)
- [Kalla Runner API](#kalla-runner-api)
  - [Health & Readiness](#health--readiness)
  - [Jobs](#jobs)
- [Data Structures](#data-structures)
  - [Recipe](#recipe)
  - [RecipeSource](#recipesource)
  - [ResolvedSource](#resolvedsource)
  - [FilterCondition](#filtercondition)
  - [JobRequest](#jobrequest)
  - [Callbacks](#callbacks)
- [Supported Data Sources](#supported-data-sources)
- [Built-in UDFs](#built-in-udfs)

---

# Kalla Web API

Base URL: `http://localhost:3001`

## Sources

### List Sources

#### `GET /api/sources`

Returns all registered data sources, ordered by creation date (newest first).

**Response (200):**

```json
[
  {
    "alias": "invoices",
    "uri": "postgres://host:5432/db?table=invoices",
    "source_type": "postgres",
    "status": "connected"
  }
]
```

---

### Register Source

#### `POST /api/sources`

Register or update a data source.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `alias` | string | yes | Unique source identifier |
| `uri` | string | yes | Connection URI |
| `source_type` | string | yes | Source type (`postgres`, `elasticsearch`, `file`) |

**Response (200):**

```json
{ "success": true, "message": "Source registered" }
```

---

### Preview Source

#### `GET /api/sources/:alias/preview`

Returns schema and sample rows from a registered source.

**Query Parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `limit` | number | 10 | Max rows to return (max 100) |

**Response (200):**

```json
{
  "alias": "invoices",
  "columns": [
    { "name": "id", "data_type": "integer", "nullable": false }
  ],
  "rows": [["1", "INV-001", "100.00"]],
  "total_rows": 5000,
  "preview_rows": 10
}
```

**Errors:** `404` source not found, `400` table name cannot be extracted from URI.

---

### Load Filtered Data

#### `POST /api/sources/:alias/load-scoped`

Returns rows from a source with optional filter conditions applied.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `conditions` | FilterCondition[] | no | Filter conditions (see [FilterCondition](#filtercondition)) |
| `limit` | number | no | Max rows (default 200, max 1000) |

**Response (200):**

```json
{
  "alias": "invoices",
  "columns": [{ "name": "id", "data_type": "integer", "nullable": false }],
  "rows": [["1", "INV-001", "100.00"]],
  "total_rows": 150,
  "preview_rows": 150
}
```

**Errors:** `404` source not found, `400` unknown column or operator.

---

## Recipes

### List Recipes

#### `GET /api/recipes`

Returns all recipes, ordered by creation date (newest first).

**Response (200):**

```json
[
  {
    "recipe_id": "invoice-payment-match",
    "name": "Invoice-Payment Reconciliation",
    "description": "Match invoices to payments",
    "match_sql": "SELECT ...",
    "match_description": "Matches by reference number",
    "sources": { "left": { ... }, "right": { ... } }
  }
]
```

---

### Create/Update Recipe

#### `POST /api/recipes`

Create a new recipe or update an existing one (upsert by `recipe_id`).

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `recipe_id` | string | yes | Unique recipe identifier |
| `name` | string | yes | Human-readable name |
| `description` | string | no | Description of the reconciliation |
| `match_sql` | string | yes | DataFusion SQL query |
| `match_description` | string | no | Human-readable explanation of the SQL |
| `sources` | object | yes | Left/right source definitions (see [RecipeSource](#recipesource)) |

**Response (200):**

```json
{ "success": true, "message": "Recipe saved" }
```

---

### Get Recipe

#### `GET /api/recipes/:id`

Returns a single recipe by ID.

**Response (200):** Recipe object.

**Errors:** `404` recipe not found.

---

## Runs

### List Runs

#### `GET /api/runs`

Returns all runs, ordered by creation date (newest first).

**Response (200):**

```json
[
  {
    "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "recipe_id": "invoice-payment-match",
    "status": "completed",
    "matched_count": 4500,
    "unmatched_left_count": 500,
    "unmatched_right_count": 300,
    "created_at": "2026-02-21T10:00:00Z",
    "updated_at": "2026-02-21T10:05:00Z"
  }
]
```

---

### Create Run

#### `POST /api/runs`

Create and dispatch a reconciliation run to the runner.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `recipe_id` | string | conditional | Fetch recipe from database (provide this OR `recipe`) |
| `recipe` | Recipe | conditional | Inline recipe object (provide this OR `recipe_id`) |
| `resolved_sources` | ResolvedSource[] | no | Optional source URI overrides |

**Response (200):**

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "submitted"
}
```

**Errors:** `404` recipe not found, `400` neither recipe_id nor recipe provided, `502` job dispatch to runner failed.

---

### Get Run Details

#### `GET /api/runs/:id`

Returns run details including the latest progress update.

**Response (200):**

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "recipe_id": "invoice-payment-match",
  "status": "running",
  "matched_count": 0,
  "unmatched_left_count": 0,
  "unmatched_right_count": 0,
  "output_matched": null,
  "output_unmatched_left": null,
  "output_unmatched_right": null,
  "error_message": null,
  "created_at": "2026-02-21T10:00:00Z",
  "updated_at": "2026-02-21T10:02:00Z",
  "latest_progress": {
    "stage": "matching",
    "progress": 0.75,
    "matched_count": 3000,
    "total_left": 5000,
    "total_right": 4800,
    "updated_at": "2026-02-21T10:02:00Z"
  }
}
```

**Errors:** `404` run not found.

---

## Uploads

### Request Presigned Upload URL

#### `POST /api/uploads/presign`

Returns a presigned S3 URL for direct CSV upload from the client.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `filename` | string | yes | File name (must end with `.csv`) |
| `session_id` | string | yes | Chat session ID for organizing uploads |

**Response (200):**

```json
{
  "upload_id": "uuid",
  "presigned_url": "https://s3.../presigned",
  "s3_uri": "s3://bucket/session_id/upload_id/filename.csv"
}
```

---

### Preview Uploaded CSV

#### `POST /api/uploads/preview`

Returns column names and sample rows from an uploaded CSV.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `s3_uri` | string | yes | S3 URI of the uploaded file |

**Response (200):**

```json
{
  "columns": ["invoice_id", "amount", "date"],
  "row_count": 1500,
  "sample": [
    { "invoice_id": "INV-001", "amount": "100.00", "date": "2026-01-15" }
  ]
}
```

**Errors:** `404` file not found, `400` invalid S3 URI.

---

### Confirm Upload

#### `POST /api/uploads/:uploadId/confirm`

Finalizes a file upload, returning confirmed metadata.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `s3_uri` | string | yes | S3 URI of the uploaded file |
| `filename` | string | yes | Original file name |

**Response (200):**

```json
{
  "upload_id": "uuid",
  "filename": "payments.csv",
  "s3_uri": "s3://bucket/session/upload/payments.csv",
  "columns": ["payment_id", "reference_number", "amount"],
  "row_count": 1500
}
```

**Errors:** `404` file not found, `400` empty CSV.

---

## Chat

### Create Chat Session

#### `POST /api/chat/sessions`

Creates a new empty chat session.

**Request Body:** None.

**Response (200):**

```json
{
  "session_id": "uuid",
  "phase": "initial",
  "status": "active"
}
```

---

### Get Chat Session

#### `GET /api/chat/sessions/:id`

Returns complete session state including message history.

**Response (200):** Full ChatSession object.

**Errors:** `404` session not found.

---

### Send Chat Message

#### `POST /api/chat`

Send a message or card response to the chat agent. Creates a new session if `session_id` is not provided.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `session_id` | string | no | Existing session ID (creates new if omitted) |
| `message` | string | conditional | User text message |
| `card_response` | object | conditional | Card action response (`action`, `card_id`, `value?`) |
| `files` | FileAttachment[] | no | Uploaded file metadata (`filename`, `columns`, `row_count`, `s3_uri`) |

**Response (200):**

```json
{
  "session_id": "uuid",
  "phase": "source-selection",
  "status": "active",
  "message": { "role": "assistant", "content": "..." },
  "recipe_draft": { ... }
}
```

---

## Worker Callbacks

These endpoints receive status updates from the Kalla runner during job execution.

### Report Progress

#### `POST /api/worker/progress`

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `run_id` | string | yes | Run identifier |
| `stage` | string | yes | Current stage (`staging` or `matching`) |
| `progress` | number | no | Progress fraction (0.0 to 1.0) |
| `matched_count` | number | no | Records matched so far |
| `total_left` | number | no | Total left-side records |
| `total_right` | number | no | Total right-side records |

**Response (200):** `{ "ok": true }`

---

### Report Completion

#### `POST /api/worker/complete`

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `run_id` | string | yes | Run identifier |
| `matched_count` | number | yes | Total matched records |
| `unmatched_left_count` | number | yes | Unmatched left records |
| `unmatched_right_count` | number | yes | Unmatched right records |
| `output_paths` | object | yes | Paths to output Parquet files |

**Response (200):** `{ "ok": true }`

---

### Report Error

#### `POST /api/worker/error`

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `run_id` | string | yes | Run identifier |
| `error` | string | yes | Error message |

**Response (200):** `{ "ok": true }`

---

# Kalla Runner API

Base URL: `http://localhost:50050`

The runner accepts reconciliation jobs, executes DataFusion SQL queries against registered data sources, and writes matched/unmatched results to Parquet files. Progress is reported via HTTP callbacks.

## Health & Readiness

### `GET /health`

Returns `200 OK` if the server is running.

### `GET /ready`

Returns `200 OK` when the runner is ready to accept jobs.

### `GET /metrics`

Returns Prometheus-format metrics for monitoring.

---

## Jobs

### Submit Job

#### `POST /api/jobs`

Submits a reconciliation job for asynchronous execution.

**Request Body:** See [JobRequest](#jobrequest).

**Example:**

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "callback_url": "http://localhost:3001/api/worker",
  "match_sql": "SELECT l.invoice_id, r.payment_id FROM left_src l JOIN right_src r ON l.invoice_id = r.reference_number AND tolerance_match(l.amount, r.paid_amount, 0.02)",
  "sources": [
    { "alias": "left_src", "uri": "postgres://user:pass@localhost:5432/db?table=invoices" },
    { "alias": "right_src", "uri": "postgres://user:pass@localhost:5432/db?table=payments" }
  ],
  "output_path": "/data/runs/a1b2c3d4",
  "primary_keys": {
    "left_src": ["invoice_id"],
    "right_src": ["payment_id"]
  }
}
```

**Response (200):**

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "accepted"
}
```

**Errors:** `500` queue full or internal error.

---

# Data Structures

## Recipe

Defines what SQL to execute and which sources to use.

```json
{
  "recipe_id": "invoice-payment-match",
  "name": "Invoice-Payment Reconciliation",
  "description": "Match invoices to payments by reference number and amount",
  "match_sql": "SELECT l.invoice_id, r.payment_id FROM invoices l JOIN payments r ON l.invoice_id = r.reference_number AND tolerance_match(l.amount, r.paid_amount, 0.02)",
  "match_description": "Matches invoices to payments where reference numbers are identical and amounts are within absolute tolerance of 0.02",
  "sources": {
    "left": { "alias": "invoices", "type": "postgres", "uri": "postgres://host/db?table=invoices", "primary_key": ["invoice_id"] },
    "right": { "alias": "payments", "type": "file", "schema": ["payment_id", "reference_number", "amount"], "primary_key": ["payment_id"] }
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

## RecipeSource

A data source definition within a recipe.

| Field | Type | Required | Description |
|---|---|---|---|
| `alias` | string | yes | Source alias used in `match_sql` |
| `type` | string | yes | Source type: `postgres`, `elasticsearch`, or `file` |
| `uri` | string | conditional | Connection URI (required for persistent sources) |
| `schema` | string[] | conditional | Expected column names (required for `file` sources) |
| `primary_key` | string[] | yes | Primary key column(s) for unmatched record derivation |

---

## ResolvedSource

A data source with a resolved URI, ready for the runner.

| Field | Type | Required | Description |
|---|---|---|---|
| `alias` | string | yes | Table alias used in `match_sql` |
| `uri` | string | yes | Data source URI (see [Supported Data Sources](#supported-data-sources)) |
| `filters` | FilterCondition[] | no | Pre-filters applied at registration time |

---

## FilterCondition

Filters applied to sources before matching, translated to SQL WHERE clauses.

```json
{ "column": "amount", "op": "gte", "value": 1000 }
```

| Field | Type | Required | Description |
|---|---|---|---|
| `column` | string | yes | Column name to filter on |
| `op` | string | yes | Filter operator |
| `value` | varies | yes | Value to compare against |

**Operators:**

| Operator | Value Type | SQL Equivalent |
|---|---|---|
| `eq` | string or number | `column = value` |
| `neq` | string or number | `column != value` |
| `gt` | string or number | `column > value` |
| `gte` | string or number | `column >= value` |
| `lt` | string or number | `column < value` |
| `lte` | string or number | `column <= value` |
| `between` | array of 2 values | `column BETWEEN a AND b` |
| `in` | array of values | `column IN (a, b, ...)` |
| `like` | string | `column LIKE pattern` |

---

## JobRequest

The payload for `POST /api/jobs` on the runner.

| Field | Type | Description |
|---|---|---|
| `run_id` | UUID | Unique run identifier |
| `callback_url` | string | HTTP endpoint for progress/completion callbacks |
| `match_sql` | string | DataFusion SQL query referencing source aliases as table names |
| `sources` | ResolvedSource[] | List of resolved data sources |
| `output_path` | string | Directory for output Parquet files (`matched`, `unmatched_left`, `unmatched_right`) |
| `primary_keys` | object | Maps each source alias to its primary key column(s) |

---

## Callbacks

The runner reports status to the web service via the worker callback endpoints.

**Staging progress:** `POST /api/worker/progress`

```json
{ "run_id": "...", "stage": "staging", "progress": 0.5, "source": "left_src" }
```

**Matching progress:** `POST /api/worker/progress`

```json
{ "run_id": "...", "stage": "matching", "progress": 1.0, "matched_count": 4500 }
```

**Completion:** `POST /api/worker/complete`

```json
{
  "run_id": "...",
  "matched_count": 4500,
  "unmatched_left_count": 500,
  "unmatched_right_count": 300,
  "output_paths": {
    "matched": "/data/runs/.../matched.parquet",
    "unmatched_left": "/data/runs/.../unmatched_left.parquet",
    "unmatched_right": "/data/runs/.../unmatched_right.parquet"
  }
}
```

**Error:** `POST /api/worker/error`

```json
{ "run_id": "...", "error": "Failed to connect to PostgreSQL: connection refused" }
```

---

# Supported Data Sources

The runner routes data sources by URI scheme and file extension via `register_source`.

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

The `table` query parameter is required.

**S3 CSV URI format:**

```
s3://bucket/path/to/file.csv
```

Requires environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, and optionally `AWS_ENDPOINT_URL` and `AWS_ALLOW_HTTP` for MinIO/LocalStack.

---

# Built-in UDFs

Custom DataFusion UDFs available in `match_sql`:

| Function | Signature | Description |
|---|---|---|
| `tolerance_match` | `tolerance_match(a, b, threshold) -> bool` | Returns `true` if `abs(a - b) <= threshold` (absolute tolerance) |
