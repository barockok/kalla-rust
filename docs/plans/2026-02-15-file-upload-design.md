# File Upload in Chat — Design Document

**Date:** 2026-02-15
**Status:** Approved

## Goal

Add file upload capability to the chat UI so users can provide CSV files both as samples during recipe building and as actual data sources during reconciliation execution. Files are always disposable — the recipe stores expected schema, never the file itself.

## Two Upload Scenarios

1. **During recipe building (sample):** User uploads CSV in chat → agent inspects columns and sample rows → uses it for dry-run → file discarded after session.
2. **During recipe execution (actual):** UI checks recipe sources, prompts upload for `type: "file"` sources → validates columns match stored schema → Worker processes the file → file discarded after run.

## Approach: Presigned URL Upload

Files never pass through Next.js. The frontend gets a presigned URL from the API and uploads directly to object storage (MinIO locally, S3/GCS in production).

---

## 1. Upload Infrastructure

### Presign Endpoint

`POST /api/uploads/presign`

```json
// Request
{ "filename": "payments.csv", "session_id": "uuid" }

// Response
{
  "upload_id": "uuid",
  "presigned_url": "https://minio:9000/kalla-uploads/...",
  "s3_uri": "s3://kalla-uploads/{session_id}/{upload_id}/payments.csv"
}
```

Uses `@aws-sdk/s3-request-presigner` to generate presigned PUT URLs. Files stored under `s3://kalla-uploads/{session_id}/{upload_id}/{filename}`.

### Confirm Endpoint

`POST /api/uploads/{upload_id}/confirm`

- Called after frontend completes the S3 upload
- Reads CSV header row from S3 to extract column names
- Returns: `{ upload_id, filename, s3_uri, columns: [...], row_count }`

### MinIO in Dev

MinIO added to `docker-compose.yml` (ports 9000/9001). Auto-creates `kalla-uploads` and `kalla-results` buckets on startup.

Environment variables:
- `S3_ENDPOINT` — MinIO/S3 endpoint URL
- `S3_ACCESS_KEY` — access key
- `S3_SECRET_KEY` — secret key
- `S3_BUCKET` — uploads bucket name (default: `kalla-uploads`)

---

## 2. Chat UI — File Attachment

### Always-Available Upload

- **Paperclip button** next to text input, accepts `.csv` files
- **Drag-and-drop** on entire chat area with visual drop zone indicator

### Upload Flow

1. User picks/drops file → file pill appears above input (filename + size + X to remove)
2. Frontend calls `/api/uploads/presign` → gets presigned URL
3. Frontend uploads to S3 via PUT with progress bar on the pill
4. Frontend calls `/api/uploads/{upload_id}/confirm` → gets columns + row count
5. User hits Send → message sent with `files` array attached

### Message Format

```json
{
  "message": "here's our bank payments",
  "files": [{
    "upload_id": "uuid",
    "filename": "payments.csv",
    "s3_uri": "s3://kalla-uploads/session/upload/payments.csv",
    "columns": ["payment_id", "reference_number", "amount", "date"],
    "row_count": 1450
  }]
}
```

### File Message Bubble

User's message shows a file card with filename, column count, row count.

### Agent Upload Request Card

New card type `upload_request` the agent can send when it needs a file. Shows a drop zone inline in the chat with optional context message (e.g., "Please upload your bank payments CSV"). Same presign → upload → confirm flow, then auto-sends the file as a message.

---

## 3. Agent Integration

### Agent Sees Files

When a message has `files`, the agent receives file metadata: filename, columns, row count, and S3 URI.

### Updated Agent Tools

- `get_source_preview` — accepts `s3_uri` parameter to inspect uploaded file data directly (not just registered source aliases)
- `run_sample` / `run_full` — job payload takes `sources[].uri`. For file sources, the S3 URI from the upload is used. No worker changes needed.

### New Agent Tool: `request_file_upload`

- Agent calls this when it needs a file from the user
- Returns an `upload_request` card with optional context message
- Agent waits for user's next message which will contain the file

### Recipe Building Flow

1. Agent learns one source is a file → calls `request_file_upload`
2. User uploads → agent gets columns and S3 URI
3. Agent uses `get_source_preview` with S3 URI to see sample rows
4. Agent builds `match_sql` using the file's columns
5. Agent calls `run_sample` with uploaded file's S3 URI for dry-run
6. Recipe saved with `type: "file"`, `schema: [columns]`, no URI stored

### Recipe Execution Flow

1. User clicks "Run" on a saved recipe
2. UI checks sources — for `type: "file"`, shows upload prompt
3. User uploads → frontend validates columns match `recipe.sources[].schema`
4. If mismatch → error ("Expected columns: X, Y, Z. Got: A, B, C")
5. If match → `POST /api/runs` with S3 URIs of uploaded files
6. Worker processes normally

---

## 4. Cleanup & Lifecycle

### Session Files

When a chat session ends or expires, all files under `s3://kalla-uploads/{session_id}/` are deleted. Cleanup triggered on session close.

### Run Files

After a run completes (success or error), uploaded source files are cleaned up. Result Parquet files stay in `s3://kalla-results/runs/{run_id}/`.

### No Cleanup Cron

Keep it simple. Cleanup on session close and run completion. Leaked files (crashes) are cheap — future S3 lifecycle policy can handle them.

### Bucket Layout

```
kalla-uploads/                          # Temp source files (disposable)
  {session_id}/
    {upload_id}/
      payments.csv

kalla-results/                          # Run output (persistent)
  runs/{run_id}/
    matched.parquet
    unmatched_left.parquet
    unmatched_right.parquet
```

---

## File Types

CSV only (`.csv`). Most common format for bank exports and ERP exports.

## Dependencies

- `@aws-sdk/client-s3` — S3 client for presign and read operations
- `@aws-sdk/s3-request-presigner` — presigned URL generation
- `csv-parse` — CSV header parsing in confirm endpoint
- MinIO container in docker-compose.yml
