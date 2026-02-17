# TODO

## Priority 1: Must-Have for Public Release

### Results & Feedback
- [ ] **Result summary with stats** - Display match rate, unmatched counts, and potential issues after each run
- [ ] **Live progress indicator** - Show real-time progress during reconciliation runs

### Source Setup Experience
- [ ] **Field preview** - Display available columns when configuring a data source

### Reusable Uploaded Files
- [ ] **"Save as data source" checkbox on upload** - When uploading CSV/Parquet, show a checkmark to flag the file as reusable. Checked = file is persisted to storage and registered in the sources list. Unchecked = ephemeral (current behavior, session-only)
- [ ] **Persist flagged uploads** - Copy flagged files to a permanent storage path (S3 bucket or local volume) instead of temp/session storage. Register in `sources` table with `source_type = 'csv_upload'` or `'parquet'` and a user-provided alias
- [ ] **Show uploads in sources list** - Reusable uploads appear alongside Postgres and other sources on the `/sources` page, selectable in future recipes without re-uploading

## Priority 2: Improves Adoption

### Guided Setup
- [x] **Primary key confirmation** - Prompt user to confirm detected primary key before matching
- [x] **Smart field name resolution** - Auto-resolve common variations (underscores, dashes, casing)
- [x] **Recipe schema validation** - Validate match rules against source schema before running

### Source Preview
- [x] **Row preview from source** - Show sample rows when exploring a data source

## Priority 3: Future Enhancements

### Extras Data Sources
Reference/supplementary data sources that augment reconciliation beyond the core left/right match.
Registered once, reusable across recipes. Joined into `match_sql` as additional tables.

- [ ] **Extras source model** - Extend recipe schema to support `extras: RecipeSource[]` alongside left/right. Each extra is registered as a named table available in `match_sql`
- [ ] **Whitelist/blacklist filtering** - e.g. a "whitelisted consumers" list to filter matches: `JOIN extras_whitelist w ON l.consumer_id = w.consumer_id`
- [ ] **Lookup/reference tables** - e.g. currency exchange rates, product catalogs, account mappings. Enrich matched records with reference data
- [ ] **Exclusion lists** - Exclude known false positives or already-reconciled records from future runs
- [ ] **Threshold tables** - Per-category tolerance amounts instead of a single global threshold (e.g. different tolerance per currency or product type)
- [ ] **Agent support for extras** - Teach the agent to suggest and configure extras during the Scoping/Inference phases. New tools: `add_extra_source`, `list_extras`
- [ ] **DB schema migration** - Add `extras JSONB` column to `recipes` table (or extend existing `sources` JSONB)

Example recipe with extras:
```json
{
  "sources": {
    "left": { "alias": "invoices", ... },
    "right": { "alias": "payments", ... }
  },
  "extras": [
    { "alias": "whitelisted_consumers", "type": "postgres", "uri": "...", "role": "filter" },
    { "alias": "fx_rates", "type": "csv_upload", "role": "lookup" }
  ],
  "match_sql": "SELECT l.*, r.* FROM left_src l JOIN right_src r ON ... JOIN whitelisted_consumers w ON l.consumer_id = w.id LEFT JOIN fx_rates fx ON l.currency = fx.currency"
}
```

### Derived Primary Keys (JSON extraction, regex, UDFs)
Real-world data often has no clean primary key — the key is buried inside a JSON string field,
needs regex extraction, or must be composed from multiple fields with transformations.

- [ ] **JSON extraction UDFs** - Register DataFusion UDFs to extract keys from JSON string fields. e.g. `json_extract(metadata, '$.transaction_id')` or `json_value(payload, '$.header.ref_no')` to pull a PK from a raw JSON column
- [ ] **Regex extraction UDF** - `regex_extract(field, pattern, group)` to derive a key from messy string fields. e.g. `regex_extract(description, 'REF-(\d+)', 1)` extracts `REF-12345` → `12345`
- [ ] **Composite derived keys** - Allow `primary_key` in recipes to be an expression, not just column names. e.g. `CONCAT(source_code, '-', regex_extract(ref_field, '\d+', 0))` as a virtual PK
- [ ] **Agent awareness** - During Demonstration/Inference phases, when the agent detects no obvious PK or sees JSON columns, it should suggest extraction strategies and preview derived keys before building the recipe
- [ ] **Key preview in UI** - When configuring a derived PK, show a preview of extracted values from sample rows so the user can verify before committing

Example scenarios:
```sql
-- PK buried in a JSON string column
SELECT json_extract(l.raw_data, '$.invoice_number') AS pk, ...
  FROM left_src l JOIN right_src r
  ON json_extract(l.raw_data, '$.invoice_number') = r.reference_id

-- PK needs regex from a description field like "Payment for INV-00421 dated 2024-01-15"
SELECT regex_extract(l.narration, 'INV-(\d+)', 1) AS pk, ...
  FROM left_src l JOIN right_src r
  ON regex_extract(l.narration, 'INV-(\d+)', 1) = r.invoice_number

-- Composite: combine multiple fields with transformation
SELECT CONCAT(l.branch_code, '-', LPAD(CAST(l.seq AS VARCHAR), 6, '0')) AS pk, ...
```

### Anchor Source (completeness direction)
Reconciliation often cares about completeness from one side only. Setting a source as **anchor** means:
"fully reconciled" = every anchor row has a match, regardless of unmatched rows on the other side.

e.g. Left (invoices) is anchor with 1,000 rows, Right (payments) has 1,200 rows.
If all 1,000 invoices match → **100% reconciled**. The 200 extra payments are informational, not failures.

- [ ] **Anchor flag on recipe source** - Add `"anchor": true` to left or right source in recipe config. Only one side can be anchor. Determines which side drives the match rate calculation
- [ ] **Match rate based on anchor** - `match_rate = matched_count / anchor_total` instead of current symmetric calculation. Unmatched rows on the non-anchor side reported separately as "surplus" not "unmatched"
- [ ] **Result summary adapts to anchor** - Show "All 1,000 invoices matched (200 surplus payments)" instead of "1,000 matched, 200 unmatched". Green = anchor fully covered, even if the other side has extras
- [ ] **Agent sets anchor during Intent phase** - Agent asks "Which source should be fully accounted for?" and sets anchor accordingly. Common patterns: GL is anchor vs bank statement, invoices anchor vs payments
- [ ] **UI indicator** - Show anchor badge on the designated source in recipe detail and run results

Example recipe:
```json
{
  "sources": {
    "left": { "alias": "invoices", "anchor": true, "primary_key": ["invoice_id"], ... },
    "right": { "alias": "payments", "anchor": false, "primary_key": ["payment_id"], ... }
  }
}
```

Result interpretation:
```
Anchor: invoices (left)
Anchor total:     1,000
Matched:          1,000  → 100% reconciled ✓
Right total:      1,200
Right surplus:      200  (payments with no matching invoice — informational)
```

### Automated Reconciliation Pipelines
End-to-end automation: trigger → ingest → reconcile → report delivery. No human in the loop.
A pipeline ties together: a trigger (how it starts), source bindings (what data), a recipe (how to match),
and report transports (where results go when done).

- [ ] **Pipeline config model** - New `pipelines` table/entity defining the full automation chain:
  - `trigger` — what kicks off the run (email, schedule, webhook, file drop)
  - `source_bindings` — map trigger inputs to recipe sources (e.g. email attachment → left source)
  - `recipe_id` — which recipe to execute
  - `transports` — where to send results when done (email, S3, webhook, Slack)
- [ ] **Email trigger** - Monitor an inbox (IMAP/mailgun/SES inbound). When an email arrives matching rules (sender, subject pattern), extract CSV/Excel attachments as source data and kick off the reconciliation
- [ ] **Schedule trigger** - Cron-based runs (daily at 6am, end of month, etc.) pulling from pre-configured sources like a Postgres table that refreshes nightly
- [ ] **Webhook trigger** - `POST /api/pipelines/:id/trigger` with optional file payload. Allows external systems (ERP, banking portals) to push data and trigger a run
- [ ] **File drop trigger** - Watch an S3 prefix or local directory. New file lands → pipeline runs
- [ ] **Report transports** — configurable per pipeline, multiple transports per run:
  - `email` — send result summary + CSV/Excel of matched/unmatched to recipients
  - `s3` — write Parquet/CSV results to a specific S3 path
  - `webhook` — POST result payload to an external URL (for downstream systems)
  - `slack` — send summary message to a Slack channel
- [ ] **Pipeline run history** - Track each automated run: trigger event, input files, recipe used, result, transport delivery status
- [ ] **Agent-assisted pipeline builder** - Conversational setup: "Run this recipe every time finance@company.com sends the daily bank statement, and email the report to ops-team@company.com"
- [ ] **Error handling & alerts** - When a pipeline run fails (bad data, missing columns, zero matches), send alert via configured transport instead of silently failing

Example pipeline config:
```json
{
  "pipeline_id": "daily-bank-recon",
  "name": "Daily Bank Reconciliation",
  "trigger": {
    "type": "email",
    "inbox": "recon@company.com",
    "rules": { "sender_contains": "bank.com", "subject_pattern": "Daily Statement.*" }
  },
  "source_bindings": {
    "left": { "type": "postgres", "uri": "postgres://...?table=gl_entries", "filter": "date = CURRENT_DATE" },
    "right": { "type": "email_attachment", "filename_pattern": "*.csv" }
  },
  "recipe_id": "gl-bank-match",
  "transports": [
    { "type": "email", "to": ["ops@company.com", "finance@company.com"], "attach": ["summary", "unmatched_csv"] },
    { "type": "s3", "path": "s3://kalla-results/daily-bank/{date}/", "format": "parquet" },
    { "type": "slack", "channel": "#finance-ops", "template": "summary_only" }
  ]
}
```

Flow:
```
Email arrives (bank statement CSV)
  → Pipeline triggers
  → Attachment extracted as right source
  → Left source loaded from Postgres (today's GL entries)
  → Recipe "gl-bank-match" executes
  → Results: 950/1000 matched
  → Transport 1: Email sent to ops + finance with summary + unmatched CSV
  → Transport 2: Full results written to S3 as Parquet
  → Transport 3: Slack message "#finance-ops: Daily bank recon — 95% matched, 50 unmatched"
```

### Table Name Isolation for Concurrent Runs
Currently `match_sql` uses hardcoded `left_src` / `right_src` table names registered in DataFusion.
If multiple reconciliation runs execute concurrently in the same worker process, table names will collide —
run B overwrites run A's `left_src` registration, corrupting results or crashing.

Need to decide: **per-run SessionContext** or **run-prefixed table names**?

- [ ] **Audit current isolation** - Verify whether the worker creates a new `SessionContext` per run or reuses a shared one. If shared → confirmed conflict risk
- [ ] **Option A: SessionContext per run (preferred)** - Each run gets its own isolated `SessionContext`. Tables stay named `left_src`/`right_src`, no SQL rewriting needed. Recipe SQL stays clean. Context is dropped when run completes. Simple, no naming gymnastics
- [ ] **Option B: Run-prefixed table names** - Register tables as `{run_id}_left_src` / `{run_id}_right_src`. Requires rewriting `match_sql` at execution time to substitute table names. More complex, but allows a single shared context with visibility across runs (useful for debugging)
- [ ] **Extras table isolation** - Same problem applies to extras sources. If using prefixed names: `{run_id}_whitelisted_consumers`, etc. If using per-run context: no issue
- [ ] **Staging/evidence path isolation** - Ensure output paths are also run-scoped: `/data/staging/{run_id}/` not just `/data/staging/`. Prevent result files from overwriting each other

Recommendation: **Option A** (per-run SessionContext) is simpler and keeps recipe SQL portable.
Option B only if there's a need to inspect multiple runs' tables simultaneously.

```
Concurrent runs with per-run SessionContext:
┌─────────────────────────────────┐  ┌─────────────────────────────────┐
│ Run abc-123                     │  │ Run def-456                     │
│ SessionContext {                │  │ SessionContext {                │
│   left_src  → invoices_jan     │  │   left_src  → invoices_feb     │
│   right_src → payments_jan     │  │   right_src → payments_feb     │
│ }                               │  │ }                               │
│ match_sql: SELECT ... FROM      │  │ match_sql: SELECT ... FROM      │
│   left_src JOIN right_src ...   │  │   left_src JOIN right_src ...   │
└─────────────────────────────────┘  └─────────────────────────────────┘
No conflict — isolated contexts, same SQL
```

### Engine/API
- [ ] **Split Server and Worker** - Separate API server from reconciliation worker for independent scaling
- [ ] **Ballista cluster support** - Enable DataFusion distributed execution for large datasets
- [ ] **Virtual fields** - Define computed columns in recipes
- [ ] **Datatype detection** - Infer field types from source data
- [ ] **Unified type system** - Normalize datatypes across different adapters
- [ ] **Adapter abstraction** - Ensure clean interface for adding new connectors (S3, GCS, MySQL, BigQuery, Snowflake)
- [ ] **Recipe parameters** - Accept runtime arguments for dates, tolerance amounts, filters

## Development Automation
- [ ] **Sample data generator** - Build pipeline to generate test datasets for verification
