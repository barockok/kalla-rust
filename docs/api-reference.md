# Kalla API Reference

Kalla is a Universal Reconciliation Engine with a Rust/Axum backend. This document provides a complete reference for all HTTP endpoints exposed by the Kalla server.

**Base URL:** `http://localhost:3001`

---

## Table of Contents

- [Overview](#overview)
- [Health Check](#health-check)
- [Data Sources](#data-sources)
  - [List Sources](#list-sources)
  - [Register Source](#register-source)
  - [Detect Primary Key](#detect-primary-key)
  - [Preview Source](#preview-source)
  - [Load Scoped Data](#load-scoped-data)
- [Recipes](#recipes)
  - [List Recipes](#list-recipes)
  - [Save Recipe](#save-recipe)
  - [Get Recipe](#get-recipe)
  - [Validate Recipe](#validate-recipe)
  - [Validate Recipe Schema](#validate-recipe-schema)
  - [Generate Recipe](#generate-recipe)
- [Reconciliation Runs](#reconciliation-runs)
  - [Create Run](#create-run)
  - [List Runs](#list-runs)
  - [Get Run](#get-run)
- [Data Structures](#data-structures)
  - [MatchRecipe](#matchrecipe)
  - [MatchRule](#matchrule)
  - [MatchCondition](#matchcondition)
  - [FilterCondition](#filtercondition)
- [Error Handling](#error-handling)

---

## Overview

All endpoints return JSON unless otherwise noted. The server enables CORS for all origins, methods, and headers.

Supported data source URI schemes:

| Scheme | Format | Example |
|---|---|---|
| `file://` | Local CSV or Parquet files | `file:///data/invoices.csv` |
| `postgres://` | PostgreSQL table | `postgres://user:pass@host:5432/db?table=invoices` |

---

## Health Check

### `GET /health`

Returns a plain-text health status. Use this to verify the server is running.

**Response:**

```
OK
```

**curl:**

```bash
curl http://localhost:3001/health
```

---

## Data Sources

### List Sources

#### `GET /api/sources`

Returns all registered data sources.

**Response Body:**

```json
[
  {
    "alias": "invoices",
    "uri": "postgres://user:pass@localhost:5432/db?table=invoices",
    "source_type": "postgres",
    "status": "connected"
  },
  {
    "alias": "payments",
    "uri": "file:///data/payments.csv",
    "source_type": "csv",
    "status": "connected"
  }
]
```

**curl:**

```bash
curl http://localhost:3001/api/sources
```

---

### Register Source

#### `POST /api/sources`

Registers a new data source with the engine. For file-based sources (`file://`), the file is immediately registered with the query engine. For PostgreSQL sources (`postgres://`), the URI is stored and the connection is established lazily on first use.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `alias` | string | yes | Unique identifier for the source |
| `uri` | string | yes | Data source URI (see supported schemes above) |

```json
{
  "alias": "invoices",
  "uri": "postgres://user:pass@localhost:5432/db?table=invoices"
}
```

**Response Body (200):**

```json
{
  "success": true,
  "message": "Registered 'postgres://user:pass@localhost:5432/db?table=invoices' as 'invoices'"
}
```

**Error Responses:**

| Status | Condition |
|---|---|
| 400 | Unsupported URI scheme (not `file://` or `postgres://`) |
| 400 | Unsupported file format (not `.csv` or `.parquet`) |
| 400 | File not found or unreadable |

**curl:**

```bash
curl -X POST http://localhost:3001/api/sources \
  -H "Content-Type: application/json" \
  -d '{
    "alias": "invoices",
    "uri": "postgres://user:pass@localhost:5432/mydb?table=invoices"
  }'
```

---

### Detect Primary Key

#### `GET /api/sources/:alias/primary-key`

Uses heuristic analysis to detect the likely primary key column(s) for a registered source. The source must already be registered and loaded into the query engine.

**Path Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `alias` | string | The source alias |

**Response Body (200):**

```json
{
  "alias": "invoices",
  "detected_keys": ["invoice_id"],
  "confidence": "high"
}
```

The `confidence` field is `"high"` when at least one key is detected, and `"low"` when no keys are detected.

**Error Responses:**

| Status | Condition |
|---|---|
| 404 | Source not found or not loaded into the engine |

**curl:**

```bash
curl http://localhost:3001/api/sources/invoices/primary-key
```

---

### Preview Source

#### `GET /api/sources/:alias/preview`

Returns schema information and a sample of rows from the source. The source is lazily registered with the query engine if not already loaded.

**Path Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `alias` | string | The source alias |

**Query Parameters:**

| Parameter | Type | Default | Max | Description |
|---|---|---|---|---|
| `limit` | integer | 10 | 100 | Number of preview rows to return |

**Response Body (200):**

```json
{
  "alias": "invoices",
  "columns": [
    { "name": "invoice_id", "data_type": "Utf8", "nullable": false },
    { "name": "amount", "data_type": "Float64", "nullable": true },
    { "name": "date", "data_type": "Utf8", "nullable": true }
  ],
  "rows": [
    ["INV-001", "1500.00", "2024-01-15"],
    ["INV-002", "2300.50", "2024-01-16"]
  ],
  "total_rows": 5000,
  "preview_rows": 2
}
```

**Fields:**

| Field | Type | Description |
|---|---|---|
| `alias` | string | The source alias |
| `columns` | array | Column metadata (name, Arrow data type, nullability) |
| `rows` | array of arrays | Row data as string values |
| `total_rows` | integer | Total number of rows in the source |
| `preview_rows` | integer | Number of rows returned in this response |

**Error Responses:**

| Status | Condition |
|---|---|
| 404 | Source not found or could not be registered |
| 500 | Query execution failure |

**curl:**

```bash
curl "http://localhost:3001/api/sources/invoices/preview?limit=5"
```

---

### Load Scoped Data

#### `POST /api/sources/:alias/load-scoped`

Loads a filtered subset of a source into the query engine and returns a preview. For PostgreSQL sources, the filtered data is pushed down to the database. For file-based sources, filtering is applied via SQL WHERE clauses in DataFusion.

**Path Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `alias` | string | The source alias |

**Request Body:**

| Field | Type | Required | Default | Max | Description |
|---|---|---|---|---|---|
| `conditions` | array | yes | -- | -- | Array of filter conditions |
| `limit` | integer | no | 200 | 1000 | Maximum rows to return |

Each condition in the `conditions` array is a [FilterCondition](#filtercondition) object.

```json
{
  "conditions": [
    { "column": "status", "op": "eq", "value": "active" },
    { "column": "amount", "op": "gte", "value": 1000 },
    { "column": "invoice_date", "op": "between", "value": ["2024-01-01", "2024-01-31"] }
  ],
  "limit": 50
}
```

**Response Body (200):**

Same structure as [Preview Source](#preview-source), but filtered to matching rows.

```json
{
  "alias": "invoices",
  "columns": [
    { "name": "invoice_id", "data_type": "Utf8", "nullable": false },
    { "name": "amount", "data_type": "Float64", "nullable": true },
    { "name": "status", "data_type": "Utf8", "nullable": true },
    { "name": "invoice_date", "data_type": "Utf8", "nullable": true }
  ],
  "rows": [
    ["INV-042", "1500.00", "active", "2024-01-15"],
    ["INV-087", "2300.50", "active", "2024-01-22"]
  ],
  "total_rows": 2,
  "preview_rows": 2
}
```

**Error Responses:**

| Status | Condition |
|---|---|
| 400 | Invalid URI or connection parameters |
| 404 | Source not found |
| 500 | Database connection failure or query error |

**curl:**

```bash
curl -X POST http://localhost:3001/api/sources/invoices/load-scoped \
  -H "Content-Type: application/json" \
  -d '{
    "conditions": [
      { "column": "status", "op": "eq", "value": "active" },
      { "column": "amount", "op": "gt", "value": 500 }
    ],
    "limit": 100
  }'
```

---

## Recipes

### List Recipes

#### `GET /api/recipes`

Returns all saved recipes.

**Response Body (200):**

```json
[
  {
    "recipe_id": "invoice-payment-match",
    "name": "Invoice to Payment Reconciliation",
    "description": "Matches invoices to payments by ID and amount",
    "config": { ... }
  }
]
```

The `config` field contains the full [MatchRecipe](#matchrecipe) object serialized as JSON.

**curl:**

```bash
curl http://localhost:3001/api/recipes
```

---

### Save Recipe

#### `POST /api/recipes`

Saves a new recipe or updates an existing one. The recipe configuration is validated before saving. If a `DATABASE_URL` is configured, the recipe is persisted to PostgreSQL with an upsert on `recipe_id`.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `recipe_id` | string | yes | Unique recipe identifier |
| `name` | string | yes | Human-readable name |
| `description` | string | no | Optional description |
| `config` | MatchRecipe | yes | Full recipe configuration |

```json
{
  "recipe_id": "invoice-payment-match",
  "name": "Invoice to Payment Reconciliation",
  "description": "Matches invoices to payments by ID and amount tolerance",
  "config": {
    "version": "1.0",
    "recipe_id": "invoice-payment-match",
    "sources": {
      "left": {
        "alias": "invoices",
        "uri": "postgres://user:pass@localhost:5432/db?table=invoices",
        "primary_key": ["invoice_id"]
      },
      "right": {
        "alias": "payments",
        "uri": "postgres://user:pass@localhost:5432/db?table=payments",
        "primary_key": ["payment_id"]
      }
    },
    "match_rules": [
      {
        "name": "exact_id_match",
        "pattern": "1:1",
        "conditions": [
          { "left": "invoice_id", "op": "eq", "right": "payment_ref" }
        ],
        "priority": 1
      }
    ],
    "output": {
      "matched": "matched_results.parquet",
      "unmatched_left": "orphan_invoices.parquet",
      "unmatched_right": "orphan_payments.parquet"
    }
  }
}
```

**Response Body (200):**

```json
{
  "success": true,
  "message": "Recipe 'invoice-payment-match' saved successfully"
}
```

**Error Responses:**

| Status | Condition |
|---|---|
| 400 | Recipe validation failed (invalid config) |
| 500 | Database persistence error or serialization failure |

**curl:**

```bash
curl -X POST http://localhost:3001/api/recipes \
  -H "Content-Type: application/json" \
  -d '{
    "recipe_id": "invoice-payment-match",
    "name": "Invoice to Payment Reconciliation",
    "config": {
      "version": "1.0",
      "recipe_id": "invoice-payment-match",
      "sources": {
        "left": {
          "alias": "invoices",
          "uri": "postgres://user:pass@localhost:5432/db?table=invoices",
          "primary_key": ["invoice_id"]
        },
        "right": {
          "alias": "payments",
          "uri": "postgres://user:pass@localhost:5432/db?table=payments",
          "primary_key": ["payment_id"]
        }
      },
      "match_rules": [
        {
          "name": "exact_id_match",
          "pattern": "1:1",
          "conditions": [
            { "left": "invoice_id", "op": "eq", "right": "payment_ref" }
          ],
          "priority": 1
        }
      ],
      "output": {
        "matched": "matched.parquet",
        "unmatched_left": "unmatched_left.parquet",
        "unmatched_right": "unmatched_right.parquet"
      }
    }
  }'
```

---

### Get Recipe

#### `GET /api/recipes/:id`

Returns a single saved recipe by its ID.

**Path Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `id` | string | The recipe ID |

**Response Body (200):**

```json
{
  "recipe_id": "invoice-payment-match",
  "name": "Invoice to Payment Reconciliation",
  "description": "Matches invoices to payments by ID and amount",
  "config": { ... }
}
```

**Error Responses:**

| Status | Condition |
|---|---|
| 404 | Recipe not found |

**curl:**

```bash
curl http://localhost:3001/api/recipes/invoice-payment-match
```

---

### Validate Recipe

#### `POST /api/recipes/validate`

Validates a MatchRecipe configuration without saving it. Checks structural correctness, required fields, and rule consistency.

**Request Body:**

A full [MatchRecipe](#matchrecipe) JSON object.

```json
{
  "version": "1.0",
  "recipe_id": "test-recipe",
  "sources": {
    "left": { "alias": "invoices", "uri": "file:///data/invoices.csv" },
    "right": { "alias": "payments", "uri": "file:///data/payments.csv" }
  },
  "match_rules": [
    {
      "name": "exact_match",
      "pattern": "1:1",
      "conditions": [
        { "left": "invoice_id", "op": "eq", "right": "payment_ref" }
      ],
      "priority": 1
    }
  ],
  "output": {
    "matched": "matched.parquet",
    "unmatched_left": "unmatched_left.parquet",
    "unmatched_right": "unmatched_right.parquet"
  }
}
```

**Response Body (200) -- valid:**

```json
{
  "valid": true,
  "errors": []
}
```

**Response Body (200) -- invalid:**

```json
{
  "valid": false,
  "errors": [
    "match_rules: at least one match rule is required",
    "output.matched: path must not be empty"
  ]
}
```

**curl:**

```bash
curl -X POST http://localhost:3001/api/recipes/validate \
  -H "Content-Type: application/json" \
  -d '{
    "version": "1.0",
    "recipe_id": "test",
    "sources": {
      "left": { "alias": "a", "uri": "file:///a.csv" },
      "right": { "alias": "b", "uri": "file:///b.csv" }
    },
    "match_rules": [],
    "output": {
      "matched": "matched.parquet",
      "unmatched_left": "unmatched_left.parquet",
      "unmatched_right": "unmatched_right.parquet"
    }
  }'
```

---

### Validate Recipe Schema

#### `POST /api/recipes/validate-schema`

Validates a recipe against the actual schemas of the registered sources. Both sources must already be loaded into the query engine. This checks that field references in match rules correspond to real columns in the data.

**Request Body:**

A full [MatchRecipe](#matchrecipe) JSON object (same as validate).

**Response Body (200):**

```json
{
  "valid": true,
  "errors": [],
  "warnings": [],
  "resolved_fields": [
    ["invoices.invoice_id", "Utf8"],
    ["payments.payment_ref", "Utf8"],
    ["invoices.amount", "Float64"],
    ["payments.paid_amount", "Float64"]
  ]
}
```

**Response Body (200) -- with errors:**

```json
{
  "valid": false,
  "errors": [
    {
      "rule_name": "exact_match",
      "field": "nonexistent_col",
      "source": "left",
      "message": "Field 'nonexistent_col' not found in source 'invoices'",
      "suggestion": "Did you mean 'invoice_id'?"
    }
  ],
  "warnings": ["Type mismatch: invoices.amount is Float64 but payments.paid_amount is Int64"],
  "resolved_fields": []
}
```

**Error Responses:**

| Status | Condition |
|---|---|
| 400 | Left or right source not found in the engine |

**curl:**

```bash
curl -X POST http://localhost:3001/api/recipes/validate-schema \
  -H "Content-Type: application/json" \
  -d '{
    "version": "1.0",
    "recipe_id": "test",
    "sources": {
      "left": { "alias": "invoices", "uri": "file:///data/invoices.csv" },
      "right": { "alias": "payments", "uri": "file:///data/payments.csv" }
    },
    "match_rules": [
      {
        "name": "exact_match",
        "pattern": "1:1",
        "conditions": [
          { "left": "invoice_id", "op": "eq", "right": "payment_ref" }
        ],
        "priority": 1
      }
    ],
    "output": {
      "matched": "matched.parquet",
      "unmatched_left": "unmatched_left.parquet",
      "unmatched_right": "unmatched_right.parquet"
    }
  }'
```

---

### Generate Recipe

#### `POST /api/recipes/generate`

Uses an LLM to automatically generate a MatchRecipe from a natural language prompt and the schemas of two registered sources. Requires a configured LLM API key (via environment variables).

Both sources must be registered before calling this endpoint. They are lazily loaded into the query engine if not already present.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `left_source` | string | yes | Alias of the left source |
| `right_source` | string | yes | Alias of the right source |
| `prompt` | string | yes | Natural language description of the reconciliation logic |

```json
{
  "left_source": "invoices",
  "right_source": "payments",
  "prompt": "Match invoices to payments by invoice ID and amount within 2% tolerance"
}
```

**Response Body (200) -- success:**

```json
{
  "recipe": {
    "version": "1.0",
    "recipe_id": "generated-invoices-payments",
    "sources": {
      "left": { "alias": "invoices", "uri": "registered://invoices", "primary_key": ["invoice_id"] },
      "right": { "alias": "payments", "uri": "registered://payments", "primary_key": ["payment_id"] }
    },
    "match_rules": [
      {
        "name": "id_and_amount_match",
        "pattern": "1:1",
        "conditions": [
          { "left": "invoice_id", "op": "eq", "right": "payment_ref" },
          { "left": "amount", "op": "tolerance", "right": "paid_amount", "threshold": 0.02 }
        ],
        "priority": 1
      }
    ],
    "output": {
      "matched": "matched.parquet",
      "unmatched_left": "unmatched_left.parquet",
      "unmatched_right": "unmatched_right.parquet"
    }
  },
  "error": null
}
```

**Response Body (200) -- LLM parse failure:**

```json
{
  "recipe": null,
  "error": "Failed to parse recipe: expected JSON object at line 1"
}
```

**Error Responses:**

| Status | Condition |
|---|---|
| 400 | Source not registered or schema extraction failed |
| 500 | LLM client not configured or LLM call failed |

**curl:**

```bash
curl -X POST http://localhost:3001/api/recipes/generate \
  -H "Content-Type: application/json" \
  -d '{
    "left_source": "invoices",
    "right_source": "payments",
    "prompt": "Match invoices to payments by invoice ID and amount within 2% tolerance"
  }'
```

---

## Reconciliation Runs

### Create Run

#### `POST /api/runs`

Creates a new reconciliation run and starts execution in the background. The recipe is validated before the run starts. The reconciliation is executed asynchronously -- the endpoint returns immediately with a `run_id` while processing continues in the background.

**Request Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `recipe` | MatchRecipe | yes | Full recipe configuration to execute |

```json
{
  "recipe": {
    "version": "1.0",
    "recipe_id": "invoice-payment-match",
    "sources": {
      "left": {
        "alias": "invoices",
        "uri": "postgres://user:pass@localhost:5432/db?table=invoices",
        "primary_key": ["invoice_id"]
      },
      "right": {
        "alias": "payments",
        "uri": "postgres://user:pass@localhost:5432/db?table=payments",
        "primary_key": ["payment_id"]
      }
    },
    "match_rules": [
      {
        "name": "exact_id_match",
        "pattern": "1:1",
        "conditions": [
          { "left": "invoice_id", "op": "eq", "right": "payment_ref" }
        ],
        "priority": 1
      },
      {
        "name": "amount_tolerance",
        "pattern": "1:1",
        "conditions": [
          { "left": "amount", "op": "tolerance", "right": "paid_amount", "threshold": 0.02 }
        ],
        "priority": 2
      }
    ],
    "output": {
      "matched": "matched_results.parquet",
      "unmatched_left": "orphan_invoices.parquet",
      "unmatched_right": "orphan_payments.parquet"
    }
  }
}
```

**Response Body (200):**

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "running"
}
```

**Error Responses:**

| Status | Condition |
|---|---|
| 400 | Recipe validation failed |
| 500 | Evidence store initialization error |

**curl:**

```bash
curl -X POST http://localhost:3001/api/runs \
  -H "Content-Type: application/json" \
  -d '{
    "recipe": {
      "version": "1.0",
      "recipe_id": "invoice-payment-match",
      "sources": {
        "left": {
          "alias": "invoices",
          "uri": "postgres://user:pass@localhost:5432/db?table=invoices",
          "primary_key": ["invoice_id"]
        },
        "right": {
          "alias": "payments",
          "uri": "postgres://user:pass@localhost:5432/db?table=payments",
          "primary_key": ["payment_id"]
        }
      },
      "match_rules": [
        {
          "name": "exact_id_match",
          "pattern": "1:1",
          "conditions": [
            { "left": "invoice_id", "op": "eq", "right": "payment_ref" }
          ],
          "priority": 1
        }
      ],
      "output": {
        "matched": "matched.parquet",
        "unmatched_left": "unmatched_left.parquet",
        "unmatched_right": "unmatched_right.parquet"
      }
    }
  }'
```

---

### List Runs

#### `GET /api/runs`

Returns summary information for all reconciliation runs.

**Response Body (200):**

```json
[
  {
    "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "recipe_id": "invoice-payment-match",
    "status": "Completed",
    "started_at": "2024-01-15T10:30:00Z",
    "matched_count": 4500,
    "unmatched_left_count": 150,
    "unmatched_right_count": 75
  },
  {
    "run_id": "f1e2d3c4-b5a6-7890-fedc-ba0987654321",
    "recipe_id": "invoice-payment-match",
    "status": "Running",
    "started_at": "2024-01-15T11:00:00Z",
    "matched_count": 0,
    "unmatched_left_count": 0,
    "unmatched_right_count": 0
  }
]
```

**Fields:**

| Field | Type | Description |
|---|---|---|
| `run_id` | UUID | Unique run identifier |
| `recipe_id` | string | ID of the recipe used |
| `status` | string | `"Running"`, `"Completed"`, or `"Failed"` |
| `started_at` | string | ISO 8601 timestamp |
| `matched_count` | integer | Number of successfully matched record pairs |
| `unmatched_left_count` | integer | Orphan records from the left source |
| `unmatched_right_count` | integer | Orphan records from the right source |

**curl:**

```bash
curl http://localhost:3001/api/runs
```

---

### Get Run

#### `GET /api/runs/:id`

Returns full metadata for a single reconciliation run, including timing and record counts.

**Path Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `id` | UUID | The run ID |

**Response Body (200):**

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "recipe_id": "invoice-payment-match",
  "started_at": "2024-01-15T10:30:00Z",
  "completed_at": "2024-01-15T10:30:45Z",
  "left_source": "postgres://user:pass@localhost:5432/db?table=invoices",
  "right_source": "postgres://user:pass@localhost:5432/db?table=payments",
  "left_record_count": 5000,
  "right_record_count": 4800,
  "matched_count": 4500,
  "unmatched_left_count": 500,
  "unmatched_right_count": 300,
  "status": "completed"
}
```

**Fields:**

| Field | Type | Description |
|---|---|---|
| `run_id` | UUID | Unique run identifier |
| `recipe_id` | string | ID of the recipe used |
| `started_at` | string | ISO 8601 timestamp for run start |
| `completed_at` | string or null | ISO 8601 timestamp for run completion (null if still running) |
| `left_source` | string | URI of the left data source |
| `right_source` | string | URI of the right data source |
| `left_record_count` | integer | Total records in the left source |
| `right_record_count` | integer | Total records in the right source |
| `matched_count` | integer | Successfully matched record pairs |
| `unmatched_left_count` | integer | Orphan records from the left source |
| `unmatched_right_count` | integer | Orphan records from the right source |
| `status` | string | `"running"`, `"completed"`, or `"failed"` |

**Error Responses:**

| Status | Condition |
|---|---|
| 404 | Run not found |

**curl:**

```bash
curl http://localhost:3001/api/runs/a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

---

## Data Structures

### MatchRecipe

The core configuration object for defining a reconciliation job.

```json
{
  "version": "1.0",
  "recipe_id": "string",
  "sources": {
    "left": {
      "alias": "string",
      "uri": "string",
      "primary_key": ["string"]
    },
    "right": {
      "alias": "string",
      "uri": "string",
      "primary_key": ["string"]
    }
  },
  "match_rules": [ ... ],
  "output": {
    "matched": "string",
    "unmatched_left": "string",
    "unmatched_right": "string"
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `version` | string | yes | Schema version, currently `"1.0"` |
| `recipe_id` | string | yes | Unique identifier for this recipe |
| `sources` | object | yes | Left and right data source definitions |
| `sources.left` | DataSource | yes | Left data source |
| `sources.right` | DataSource | yes | Right data source |
| `match_rules` | array | yes | Ordered list of match rules |
| `output` | object | yes | Output file paths for results |

**DataSource fields:**

| Field | Type | Required | Description |
|---|---|---|---|
| `alias` | string | yes | Alias used to reference this source in queries |
| `uri` | string | yes | Data source URI |
| `primary_key` | array of strings | no | Primary key column name(s) |

**Output fields:**

| Field | Type | Required | Description |
|---|---|---|---|
| `matched` | string | yes | Output path for matched record pairs |
| `unmatched_left` | string | yes | Output path for unmatched left records |
| `unmatched_right` | string | yes | Output path for unmatched right records |

---

### MatchRule

Defines a single matching rule within a recipe.

```json
{
  "name": "exact_id_and_amount",
  "pattern": "1:1",
  "conditions": [ ... ],
  "priority": 1
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Unique name for this rule |
| `pattern` | string | yes | Match cardinality: `"1:1"`, `"1:N"`, or `"M:1"` |
| `conditions` | array | yes | Conditions that must all be satisfied for a match |
| `priority` | integer | no | Rule priority (lower number = higher priority) |

**Match patterns:**

| Pattern | Description |
|---|---|
| `1:1` | Each left record matches at most one right record |
| `1:N` | One left record can match multiple right records |
| `M:1` | Multiple left records can match one right record |

---

### MatchCondition

Defines a single condition within a match rule, comparing a field from the left source to a field from the right source.

```json
{
  "left": "amount",
  "op": "tolerance",
  "right": "paid_amount",
  "threshold": 0.02
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `left` | string | yes | Column name from the left source |
| `op` | string | yes | Comparison operator |
| `right` | string | yes | Column name from the right source |
| `threshold` | number | no | Threshold value for tolerance-based matching |

**Comparison operators:**

| Operator | Description |
|---|---|
| `eq` | Exact equality |
| `tolerance` | Numeric match within a relative threshold (e.g., 0.02 = 2%) |
| `gt` | Left value greater than right value |
| `lt` | Left value less than right value |
| `gte` | Left value greater than or equal to right value |
| `lte` | Left value less than or equal to right value |
| `contains` | Left string contains right string |
| `startswith` | Left string starts with right string |
| `endswith` | Left string ends with right string |

---

### FilterCondition

Used in the [Load Scoped Data](#load-scoped-data) endpoint to filter source data. Each condition specifies a column, an operator, and a value to compare against.

```json
{
  "column": "status",
  "op": "eq",
  "value": "active"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `column` | string | yes | Column name to filter on |
| `op` | string | yes | Filter operator |
| `value` | varies | yes | Value to compare against (type depends on operator) |

**Filter operators and expected value types:**

| Operator | Value Type | Example | SQL Equivalent |
|---|---|---|---|
| `eq` | string or number | `"active"` or `100` | `column = value` |
| `neq` | string or number | `"inactive"` | `column != value` |
| `gt` | string or number | `500` | `column > value` |
| `gte` | string or number | `500` | `column >= value` |
| `lt` | string or number | `1000` | `column < value` |
| `lte` | string or number | `1000` | `column <= value` |
| `between` | array of 2 strings | `["2024-01-01", "2024-01-31"]` | `column BETWEEN a AND b` |
| `in` | array of strings | `["food", "drink"]` | `column IN (a, b)` |
| `like` | string | `"%acme%"` | `column LIKE pattern` |

---

## Error Handling

All error responses return a plain-text error message as the response body with an appropriate HTTP status code.

| Status Code | Meaning |
|---|---|
| 200 | Success |
| 400 | Bad request (invalid input, unsupported format, validation failure) |
| 404 | Resource not found (source, recipe, or run) |
| 500 | Internal server error (database failure, query error, LLM error) |

Example error response:

```
HTTP/1.1 400 Bad Request
Content-Type: text/plain

Invalid recipe: match_rules: at least one match rule is required
```

Errors from Axum are returned as a tuple of `(StatusCode, String)`, so the response body is a plain string rather than a JSON object.
