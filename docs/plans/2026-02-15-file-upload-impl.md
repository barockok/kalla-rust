# File Upload in Chat — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add CSV file upload to the chat UI via presigned S3 URLs, with both user-initiated (paperclip/drag-drop) and agent-initiated (upload_request card) flows.

**Architecture:** Frontend gets presigned URL from Next.js API, uploads directly to MinIO/S3, then sends file metadata with chat messages. Agent sees files and can use them for preview and reconciliation. Files are disposable — cleaned up after session or run.

**Tech Stack:** @aws-sdk/client-s3, @aws-sdk/s3-request-presigner, csv-parse, MinIO (dev), React drag-and-drop

---

## Task 1: MinIO in Docker Compose + S3 Client Library

Add MinIO to dev docker-compose and install S3 SDK in kalla-web.

**Files:**
- Modify: `docker-compose.yml`
- Create: `kalla-web/src/lib/s3-client.ts`
- Modify: `kalla-web/package.json` (via npm install)

**Step 1: Add MinIO service to docker-compose.yml**

Add after the `postgres` service block in `docker-compose.yml`:

```yaml
  minio:
    image: minio/minio:latest
    container_name: kalla-minio
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data
    environment:
      MINIO_ROOT_USER: ${S3_ACCESS_KEY:-minioadmin}
      MINIO_ROOT_PASSWORD: ${S3_SECRET_KEY:-minioadmin}
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin;
      mc mb --ignore-existing local/kalla-uploads;
      mc mb --ignore-existing local/kalla-results;
      exit 0;
      "
```

Add to `volumes:` section:
```yaml
  minio_data:
```

**Step 2: Install S3 SDK packages**

Run: `cd kalla-web && npm install @aws-sdk/client-s3 @aws-sdk/s3-request-presigner csv-parse`

**Step 3: Create S3 client singleton**

Create `kalla-web/src/lib/s3-client.ts`:

```typescript
import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectsCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

const s3 = new S3Client({
  region: 'us-east-1',
  endpoint: process.env.S3_ENDPOINT || 'http://localhost:9000',
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY || 'minioadmin',
    secretAccessKey: process.env.S3_SECRET_KEY || 'minioadmin',
  },
});

const UPLOADS_BUCKET = process.env.S3_UPLOADS_BUCKET || 'kalla-uploads';

export async function createPresignedUploadUrl(key: string): Promise<string> {
  const command = new PutObjectCommand({
    Bucket: UPLOADS_BUCKET,
    Key: key,
  });
  return getSignedUrl(s3, command, { expiresIn: 3600 });
}

export async function getObject(key: string): Promise<ReadableStream | null> {
  const command = new GetObjectCommand({
    Bucket: UPLOADS_BUCKET,
    Key: key,
  });
  const response = await s3.send(command);
  return response.Body?.transformToWebStream() ?? null;
}

export async function deleteSessionFiles(sessionId: string): Promise<void> {
  const listCommand = new ListObjectsV2Command({
    Bucket: UPLOADS_BUCKET,
    Prefix: `${sessionId}/`,
  });
  const listed = await s3.send(listCommand);
  if (!listed.Contents?.length) return;

  const deleteCommand = new DeleteObjectsCommand({
    Bucket: UPLOADS_BUCKET,
    Delete: {
      Objects: listed.Contents.map((obj) => ({ Key: obj.Key })),
    },
  });
  await s3.send(deleteCommand);
}

export { s3, UPLOADS_BUCKET };
```

**Step 4: Restart docker compose to include MinIO**

Run: `cd "/Users/barock/Library/Mobile Documents/com~apple~CloudDocs/Code/kalla" && docker compose down && docker compose up -d`

Expected: Postgres + MinIO + minio-init running. minio-init creates buckets and exits.

**Step 5: Verify MinIO is working**

Run: `curl -sf http://localhost:9000/minio/health/live`

Expected: Returns OK or 200

**Step 6: Commit**

```bash
git add docker-compose.yml kalla-web/package.json kalla-web/package-lock.json kalla-web/src/lib/s3-client.ts
git commit -m "feat: add MinIO to dev compose and S3 client library"
```

---

## Task 2: Upload API Endpoints (presign + confirm)

**Files:**
- Create: `kalla-web/src/app/api/uploads/presign/route.ts`
- Create: `kalla-web/src/app/api/uploads/[uploadId]/confirm/route.ts`

**Step 1: Create presign endpoint**

Create `kalla-web/src/app/api/uploads/presign/route.ts`:

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { v4 as uuidv4 } from 'uuid';
import { createPresignedUploadUrl } from '@/lib/s3-client';

export async function POST(req: NextRequest) {
  const { filename, session_id } = await req.json();

  if (!filename || !session_id) {
    return NextResponse.json(
      { error: 'filename and session_id are required' },
      { status: 400 }
    );
  }

  if (!filename.endsWith('.csv')) {
    return NextResponse.json(
      { error: 'Only CSV files are supported' },
      { status: 400 }
    );
  }

  const uploadId = uuidv4();
  const key = `${session_id}/${uploadId}/${filename}`;
  const s3Uri = `s3://kalla-uploads/${key}`;

  const presignedUrl = await createPresignedUploadUrl(key);

  return NextResponse.json({
    upload_id: uploadId,
    presigned_url: presignedUrl,
    s3_uri: s3Uri,
  });
}
```

**Step 2: Create confirm endpoint**

Create `kalla-web/src/app/api/uploads/[uploadId]/confirm/route.ts`:

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { getObject, UPLOADS_BUCKET } from '@/lib/s3-client';
import { S3Client, HeadObjectCommand } from '@aws-sdk/client-s3';
import { parse } from 'csv-parse/sync';

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ uploadId: string }> }
) {
  const { uploadId } = await params;
  const { s3_uri, filename } = await req.json();

  if (!s3_uri || !filename) {
    return NextResponse.json(
      { error: 's3_uri and filename are required' },
      { status: 400 }
    );
  }

  // Extract key from s3_uri: "s3://kalla-uploads/session/upload/file.csv"
  const key = s3_uri.replace(`s3://${UPLOADS_BUCKET}/`, '');

  try {
    const stream = await getObject(key);
    if (!stream) {
      return NextResponse.json({ error: 'File not found' }, { status: 404 });
    }

    // Read enough of the file to get headers and count rows
    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];
    let done = false;

    while (!done) {
      const result = await reader.read();
      if (result.done) {
        done = true;
      } else {
        chunks.push(result.value);
      }
    }

    const text = new TextDecoder().decode(
      Uint8Array.from(chunks.flatMap((c) => Array.from(c)))
    );
    const records = parse(text, { columns: false, skip_empty_lines: true });

    if (records.length === 0) {
      return NextResponse.json({ error: 'CSV file is empty' }, { status: 400 });
    }

    const columns: string[] = records[0];
    const rowCount = records.length - 1; // Subtract header row

    return NextResponse.json({
      upload_id: uploadId,
      filename,
      s3_uri,
      columns,
      row_count: rowCount,
    });
  } catch (err) {
    return NextResponse.json(
      { error: `Failed to read file: ${err}` },
      { status: 500 }
    );
  }
}
```

**Step 3: Verify endpoints with curl**

Start the Next.js dev server and test:

```bash
# Get presigned URL
curl -X POST http://localhost:3000/api/uploads/presign \
  -H "Content-Type: application/json" \
  -d '{"filename": "test.csv", "session_id": "test-session"}'

# Should return { upload_id, presigned_url, s3_uri }
```

**Step 4: Commit**

```bash
git add kalla-web/src/app/api/uploads/
git commit -m "feat: add presign and confirm upload API endpoints"
```

---

## Task 3: Chat Types — File Support

**Files:**
- Modify: `kalla-web/src/lib/chat-types.ts`

**Step 1: Add FileAttachment type and update ChatMessage**

In `kalla-web/src/lib/chat-types.ts`, add after the existing type definitions (around line 10):

```typescript
export interface FileAttachment {
  upload_id: string;
  filename: string;
  s3_uri: string;
  columns: string[];
  row_count: number;
}
```

Add `files` field to `ChatMessage` interface (the one with `role`, `segments`, `timestamp`):

```typescript
export interface ChatMessage {
  role: 'agent' | 'user';
  segments: ChatSegment[];
  timestamp: string;
  files?: FileAttachment[];
}
```

Add `upload_request` to the card_type union in `ChatSegment`:

```typescript
card_type?: 'select' | 'confirm' | 'match_proposal' | 'rule_summary' | 'progress' | 'result_summary' | 'upload_request';
```

**Step 2: Commit**

```bash
git add kalla-web/src/lib/chat-types.ts
git commit -m "feat: add FileAttachment type and upload_request card type"
```

---

## Task 4: Chat UI — Paperclip Button + Drag-and-Drop + File Pill

**Files:**
- Modify: `kalla-web/src/app/reconcile/page.tsx`
- Create: `kalla-web/src/components/chat/FileUploadPill.tsx`
- Create: `kalla-web/src/components/chat/FileMessageCard.tsx`
- Create: `kalla-web/src/lib/upload-client.ts`

**Step 1: Create the upload client helper**

Create `kalla-web/src/lib/upload-client.ts`:

```typescript
import type { FileAttachment } from './chat-types';

export interface UploadProgress {
  phase: 'presigning' | 'uploading' | 'confirming' | 'done' | 'error';
  percent: number;
  error?: string;
}

export async function uploadFile(
  file: File,
  sessionId: string,
  onProgress: (progress: UploadProgress) => void
): Promise<FileAttachment> {
  // 1. Get presigned URL
  onProgress({ phase: 'presigning', percent: 0 });

  const presignRes = await fetch('/api/uploads/presign', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ filename: file.name, session_id: sessionId }),
  });

  if (!presignRes.ok) {
    const err = await presignRes.json();
    throw new Error(err.error || 'Failed to get presigned URL');
  }

  const { upload_id, presigned_url, s3_uri } = await presignRes.json();

  // 2. Upload to S3 via presigned URL
  onProgress({ phase: 'uploading', percent: 10 });

  const uploadRes = await fetch(presigned_url, {
    method: 'PUT',
    body: file,
    headers: { 'Content-Type': 'text/csv' },
  });

  if (!uploadRes.ok) {
    throw new Error('Failed to upload file to storage');
  }

  onProgress({ phase: 'uploading', percent: 80 });

  // 3. Confirm upload and get metadata
  onProgress({ phase: 'confirming', percent: 85 });

  const confirmRes = await fetch(`/api/uploads/${upload_id}/confirm`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ s3_uri, filename: file.name }),
  });

  if (!confirmRes.ok) {
    const err = await confirmRes.json();
    throw new Error(err.error || 'Failed to confirm upload');
  }

  const attachment: FileAttachment = await confirmRes.json();
  onProgress({ phase: 'done', percent: 100 });

  return attachment;
}
```

**Step 2: Create FileUploadPill component**

Create `kalla-web/src/components/chat/FileUploadPill.tsx`:

```tsx
'use client';

import type { FileAttachment } from '@/lib/chat-types';
import type { UploadProgress } from '@/lib/upload-client';

interface FileUploadPillProps {
  filename: string;
  progress: UploadProgress | null;
  attachment: FileAttachment | null;
  onRemove: () => void;
}

export function FileUploadPill({ filename, progress, attachment, onRemove }: FileUploadPillProps) {
  const isUploading = progress && progress.phase !== 'done' && progress.phase !== 'error';
  const isError = progress?.phase === 'error';
  const isDone = progress?.phase === 'done' || attachment !== null;

  return (
    <div className="flex items-center gap-2 rounded-lg border bg-muted px-3 py-1.5 text-sm">
      <svg className="h-4 w-4 shrink-0 text-muted-foreground" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
      </svg>
      <span className="truncate max-w-[200px]">{filename}</span>
      {isUploading && (
        <div className="h-1.5 w-20 rounded-full bg-muted-foreground/20">
          <div
            className="h-full rounded-full bg-primary transition-all"
            style={{ width: `${progress.percent}%` }}
          />
        </div>
      )}
      {isDone && attachment && (
        <span className="text-xs text-muted-foreground">
          {attachment.columns.length} cols, {attachment.row_count.toLocaleString()} rows
        </span>
      )}
      {isError && (
        <span className="text-xs text-destructive">{progress.error || 'Upload failed'}</span>
      )}
      <button
        onClick={onRemove}
        className="ml-auto shrink-0 text-muted-foreground hover:text-foreground"
        aria-label="Remove file"
      >
        <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  );
}
```

**Step 3: Create FileMessageCard component**

Create `kalla-web/src/components/chat/FileMessageCard.tsx`:

```tsx
import type { FileAttachment } from '@/lib/chat-types';

interface FileMessageCardProps {
  file: FileAttachment;
}

export function FileMessageCard({ file }: FileMessageCardProps) {
  return (
    <div className="flex items-center gap-2 rounded-lg border bg-muted/50 px-3 py-2 text-sm">
      <svg className="h-5 w-5 shrink-0 text-muted-foreground" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
      </svg>
      <div className="min-w-0">
        <div className="truncate font-medium">{file.filename}</div>
        <div className="text-xs text-muted-foreground">
          {file.columns.length} columns, {file.row_count.toLocaleString()} rows
        </div>
      </div>
    </div>
  );
}
```

**Step 4: Modify reconcile page to add paperclip, drag-and-drop, and file state**

Modify `kalla-web/src/app/reconcile/page.tsx`:

The key changes are:
1. Add file state: `pendingFile`, `uploadProgress`, `fileAttachment`
2. Add `useRef` for hidden file input
3. Add drag-and-drop handlers on chat area
4. Add paperclip button next to Send button
5. Show FileUploadPill above the input when a file is staged
6. Include `files` array when sending message
7. Show FileMessageCard in user messages that have files

This is a significant modification to the page — the full updated file should:
- Import `FileUploadPill`, `FileMessageCard`, `uploadFile`, `UploadProgress`, `FileAttachment`
- Add state: `pendingFile: File | null`, `uploadProgress: UploadProgress | null`, `fileAttachment: FileAttachment | null`, `isDragging: boolean`
- Add `fileInputRef = useRef<HTMLInputElement>(null)`
- Add `handleFileSelect(file: File)` — sets pendingFile, calls `uploadFile()`, stores attachment
- Add `handleDragOver`, `handleDragLeave`, `handleDrop` on the chat container
- Modify `handleSubmit` to include `files: [fileAttachment]` in the POST body when a file is attached, then clear file state
- Add hidden `<input type="file" accept=".csv">` triggered by paperclip button
- Render `FileUploadPill` above the input area when `pendingFile` is set
- In the message list, render `FileMessageCard` for messages that have `files`

**Step 5: Commit**

```bash
git add kalla-web/src/lib/upload-client.ts kalla-web/src/components/chat/FileUploadPill.tsx kalla-web/src/components/chat/FileMessageCard.tsx kalla-web/src/app/reconcile/page.tsx
git commit -m "feat: add file upload UI with paperclip, drag-drop, and file pills"
```

---

## Task 5: Chat API — Accept File Metadata in Messages

**Files:**
- Modify: `kalla-web/src/app/api/chat/route.ts`
- Modify: `kalla-web/src/lib/session-store.ts`

**Step 1: Update chat route to accept files**

In `kalla-web/src/app/api/chat/route.ts`, modify the POST handler to extract `files` from the request body and pass them through to the session message:

```typescript
const { session_id, message, card_response, files } = await req.json();
```

When creating the user message, include files:

```typescript
const userMessage: ChatMessage = {
  role: 'user',
  segments: [{ type: 'text', content: message }],
  timestamp: new Date().toISOString(),
  files: files || undefined,
};
```

**Step 2: Commit**

```bash
git add kalla-web/src/app/api/chat/route.ts
git commit -m "feat: accept file attachments in chat API"
```

---

## Task 6: Agent Integration — File-Aware Tools

**Files:**
- Modify: `kalla-web/src/lib/agent.ts` (TOOL_DEFINITIONS + system prompt)
- Modify: `kalla-web/src/lib/agent-tools.ts` (new tool + updated tools)

**Step 1: Add request_file_upload tool definition**

In `kalla-web/src/lib/agent.ts`, add to the `TOOL_DEFINITIONS` array:

```typescript
{
  name: 'request_file_upload',
  description: 'Ask the user to upload a CSV file. Use this when you need a file from the user (e.g., sample data, source file for reconciliation). Returns an upload_request card in the chat.',
  input_schema: {
    type: 'object' as const,
    properties: {
      message: {
        type: 'string',
        description: 'Context message to show the user explaining what file is needed and why',
      },
    },
    required: ['message'],
  },
},
```

**Step 2: Update get_source_preview tool to accept s3_uri**

In the `TOOL_DEFINITIONS`, modify the `get_source_preview` tool to add an optional `s3_uri` parameter:

```typescript
s3_uri: {
  type: 'string',
  description: 'S3 URI of an uploaded file to preview (alternative to alias for uploaded files)',
},
```

**Step 3: Implement request_file_upload in agent-tools.ts**

In `kalla-web/src/lib/agent-tools.ts`, add the tool implementation:

```typescript
async function requestFileUpload(input: { message: string }): Promise<Record<string, unknown>> {
  return {
    card_type: 'upload_request',
    message: input.message,
  };
}
```

And in the `executeTool` switch, add the case:

```typescript
case 'request_file_upload':
  return requestFileUpload(input as { message: string });
```

**Step 4: Update get_source_preview to handle s3_uri**

In `kalla-web/src/lib/agent-tools.ts`, modify `getSourcePreview` to also accept `s3_uri`:

```typescript
async function getSourcePreview(
  input: { alias?: string; s3_uri?: string }
): Promise<Record<string, unknown>> {
  if (input.s3_uri) {
    // Fetch preview from uploaded file
    const res = await fetch(`${apiBase}/api/uploads/preview`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ s3_uri: input.s3_uri }),
    });
    return res.json();
  }
  // Existing alias-based logic
  const res = await fetch(`${apiBase}/api/sources/${input.alias}/preview`);
  return res.json();
}
```

**Step 5: Update agent system prompt to mention files**

In `kalla-web/src/lib/agent.ts`, in `buildSystemPrompt()`, add to the system prompt text:

```
When a user sends a message with attached files, you can see the file metadata (filename, columns, row_count, s3_uri). Use get_source_preview with the s3_uri to inspect the file data. Use request_file_upload to ask the user for a file when you need one.
```

**Step 6: Handle upload_request card in agent response**

In `kalla-web/src/lib/agent.ts`, in the tool result handling section of `runAgent()`, when the tool is `request_file_upload`, create a card segment in the agent message:

```typescript
if (toolName === 'request_file_upload') {
  // Add upload_request card to agent response
  agentSegments.push({
    type: 'card',
    card_type: 'upload_request',
    card_id: `upload-${Date.now()}`,
    data: toolResult,
  });
}
```

**Step 7: Commit**

```bash
git add kalla-web/src/lib/agent.ts kalla-web/src/lib/agent-tools.ts
git commit -m "feat: add file-aware agent tools (request_file_upload, s3_uri preview)"
```

---

## Task 7: Upload Request Card Component

**Files:**
- Create: `kalla-web/src/components/chat/UploadRequestCard.tsx`
- Modify: `kalla-web/src/components/chat/ChatMessage.tsx`

**Step 1: Create UploadRequestCard component**

Create `kalla-web/src/components/chat/UploadRequestCard.tsx`:

```tsx
'use client';

import { useRef, useState, useCallback } from 'react';
import type { FileAttachment } from '@/lib/chat-types';
import type { UploadProgress } from '@/lib/upload-client';
import { uploadFile } from '@/lib/upload-client';
import { FileUploadPill } from './FileUploadPill';

interface UploadRequestCardProps {
  message: string;
  sessionId: string;
  onFileUploaded: (attachment: FileAttachment) => void;
  disabled?: boolean;
}

export function UploadRequestCard({ message, sessionId, onFileUploaded, disabled }: UploadRequestCardProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [progress, setProgress] = useState<UploadProgress | null>(null);
  const [attachment, setAttachment] = useState<FileAttachment | null>(null);
  const [isDragging, setIsDragging] = useState(false);

  const handleFile = useCallback(async (f: File) => {
    if (!f.name.endsWith('.csv')) return;
    setFile(f);
    try {
      const result = await uploadFile(f, sessionId, setProgress);
      setAttachment(result);
      onFileUploaded(result);
    } catch (err) {
      setProgress({ phase: 'error', percent: 0, error: String(err) });
    }
  }, [sessionId, onFileUploaded]);

  if (disabled || attachment) {
    return attachment ? (
      <div className="rounded-lg border bg-muted/50 p-3">
        <p className="mb-2 text-sm text-muted-foreground">{message}</p>
        <FileUploadPill filename={attachment.filename} progress={null} attachment={attachment} onRemove={() => {}} />
      </div>
    ) : null;
  }

  return (
    <div
      className={`rounded-lg border-2 border-dashed p-4 transition-colors ${isDragging ? 'border-primary bg-primary/5' : 'border-muted-foreground/25'}`}
      onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
      onDragLeave={() => setIsDragging(false)}
      onDrop={(e) => {
        e.preventDefault();
        setIsDragging(false);
        const f = e.dataTransfer.files[0];
        if (f) handleFile(f);
      }}
    >
      <p className="mb-3 text-sm">{message}</p>
      {file ? (
        <FileUploadPill filename={file.name} progress={progress} attachment={attachment} onRemove={() => { setFile(null); setProgress(null); }} />
      ) : (
        <button
          onClick={() => fileInputRef.current?.click()}
          className="rounded-md bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90"
        >
          Choose CSV file
        </button>
      )}
      <input
        ref={fileInputRef}
        type="file"
        accept=".csv"
        className="hidden"
        onChange={(e) => {
          const f = e.target.files?.[0];
          if (f) handleFile(f);
        }}
      />
    </div>
  );
}
```

**Step 2: Register UploadRequestCard in ChatMessage**

In `kalla-web/src/components/chat/ChatMessage.tsx`, import and render:

```typescript
import { UploadRequestCard } from './UploadRequestCard';
import { FileMessageCard } from './FileMessageCard';
```

In the segment rendering, add a case for `upload_request` card_type:

```tsx
{segment.card_type === 'upload_request' && (
  <UploadRequestCard
    message={segment.data?.message as string}
    sessionId={sessionId}
    onFileUploaded={onFileUploaded}
  />
)}
```

Also render file cards for user messages with files:

```tsx
{message.files?.map((file) => (
  <FileMessageCard key={file.upload_id} file={file} />
))}
```

Note: `ChatMessage` component will need `sessionId` and `onFileUploaded` props passed down from the reconcile page.

**Step 3: Commit**

```bash
git add kalla-web/src/components/chat/UploadRequestCard.tsx kalla-web/src/components/chat/ChatMessage.tsx
git commit -m "feat: add UploadRequestCard and FileMessageCard to chat"
```

---

## Task 8: Upload Preview Endpoint (for agent)

**Files:**
- Create: `kalla-web/src/app/api/uploads/preview/route.ts`

**Step 1: Create preview endpoint**

The agent needs to preview uploaded file data. Create `kalla-web/src/app/api/uploads/preview/route.ts`:

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { getObject, UPLOADS_BUCKET } from '@/lib/s3-client';
import { parse } from 'csv-parse/sync';

export async function POST(req: NextRequest) {
  const { s3_uri } = await req.json();

  if (!s3_uri) {
    return NextResponse.json({ error: 's3_uri is required' }, { status: 400 });
  }

  const key = s3_uri.replace(`s3://${UPLOADS_BUCKET}/`, '');

  try {
    const stream = await getObject(key);
    if (!stream) {
      return NextResponse.json({ error: 'File not found' }, { status: 404 });
    }

    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];
    let done = false;

    while (!done) {
      const result = await reader.read();
      if (result.done) {
        done = true;
      } else {
        chunks.push(result.value);
      }
    }

    const text = new TextDecoder().decode(
      Uint8Array.from(chunks.flatMap((c) => Array.from(c)))
    );
    const records = parse(text, { columns: true, skip_empty_lines: true });

    const columns = records.length > 0 ? Object.keys(records[0]) : [];
    const sample = records.slice(0, 10); // First 10 rows as preview

    return NextResponse.json({
      columns,
      row_count: records.length,
      sample,
    });
  } catch (err) {
    return NextResponse.json(
      { error: `Failed to read file: ${err}` },
      { status: 500 }
    );
  }
}
```

**Step 2: Commit**

```bash
git add kalla-web/src/app/api/uploads/preview/route.ts
git commit -m "feat: add upload preview endpoint for agent file inspection"
```

---

## Task 9: Integration Test — Full Upload Flow

**Files:**
- Create: `kalla-web/__tests__/upload-flow.test.ts`

**Step 1: Write integration test**

Create `kalla-web/__tests__/upload-flow.test.ts`:

```typescript
import { describe, test, expect, beforeAll } from '@jest/globals';

const BASE_URL = 'http://localhost:3000';

describe('File Upload Flow', () => {
  const sessionId = `test-session-${Date.now()}`;

  test('presign returns upload_id and presigned_url', async () => {
    const res = await fetch(`${BASE_URL}/api/uploads/presign`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename: 'test.csv', session_id: sessionId }),
    });

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.upload_id).toBeDefined();
    expect(body.presigned_url).toContain('http');
    expect(body.s3_uri).toContain('kalla-uploads');
  });

  test('rejects non-csv files', async () => {
    const res = await fetch(`${BASE_URL}/api/uploads/presign`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename: 'test.xlsx', session_id: sessionId }),
    });

    expect(res.status).toBe(400);
  });

  test('full upload and confirm flow', async () => {
    // 1. Get presigned URL
    const presignRes = await fetch(`${BASE_URL}/api/uploads/presign`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename: 'payments.csv', session_id: sessionId }),
    });
    const { upload_id, presigned_url, s3_uri } = await presignRes.json();

    // 2. Upload CSV to S3
    const csvContent = 'payment_id,reference_number,amount,date\n1,INV-001,100.00,2024-01-15\n2,INV-002,250.50,2024-01-16\n';
    const uploadRes = await fetch(presigned_url, {
      method: 'PUT',
      body: csvContent,
      headers: { 'Content-Type': 'text/csv' },
    });
    expect(uploadRes.ok).toBe(true);

    // 3. Confirm upload
    const confirmRes = await fetch(`${BASE_URL}/api/uploads/${upload_id}/confirm`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ s3_uri, filename: 'payments.csv' }),
    });

    expect(confirmRes.status).toBe(200);
    const body = await confirmRes.json();
    expect(body.columns).toEqual(['payment_id', 'reference_number', 'amount', 'date']);
    expect(body.row_count).toBe(2);
  });
});
```

Note: This test requires Next.js dev server + MinIO running. It's an integration test, not a unit test.

**Step 2: Commit**

```bash
git add kalla-web/__tests__/upload-flow.test.ts
git commit -m "test: add integration test for file upload flow"
```

---

## Task 10: End-to-End Manual Verification

**Steps:**

1. Start all services: `docker compose up -d` (Postgres + MinIO)
2. Start worker: `RUST_LOG=info STAGING_PATH=./staging cargo run --bin kalla-worker`
3. Start Next.js: `cd kalla-web && DATABASE_URL=postgres://kalla:kalla_secret@localhost:5432/kalla WORKER_URL=http://localhost:9090 S3_ENDPOINT=http://localhost:9000 npm run dev`
4. Open http://localhost:3000/reconcile
5. Start a conversation
6. Click paperclip → select a CSV → verify upload pill shows progress → verify columns/rows shown
7. Send message with file attached → verify file card appears in message bubble
8. Verify agent acknowledges the file and can inspect it
9. Drag-and-drop a CSV onto the chat → verify same flow
10. Verify MinIO console at http://localhost:9001 shows uploaded files

**Final commit:**

```bash
git add -A
git commit -m "feat: file upload in chat — complete implementation"
```
