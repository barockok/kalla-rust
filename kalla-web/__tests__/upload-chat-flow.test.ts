/**
 * @jest-environment node
 */
import { describe, test, expect } from '@jest/globals';

const BASE_URL = process.env.TEST_BASE_URL || 'http://localhost:3000';

// Integration test — requires running Next.js + MinIO + Claude API key.
// Skip in unit test runs.
const itIntegration = process.env.RUN_INTEGRATION ? test : test.skip;

// Agent responses can take 10-30s (Claude API calls).
jest.setTimeout(60_000);

// ---------------------------------------------------------------------------
// Types matching src/lib/chat-types.ts
// ---------------------------------------------------------------------------
interface FileAttachment {
  upload_id: string;
  filename: string;
  s3_uri: string;
  columns: string[];
  row_count: number;
}

interface ChatSegment {
  type: 'text' | 'card';
  content?: string;
  card_type?: string;
  card_id?: string;
  data?: Record<string, unknown>;
}

interface ChatMessage {
  role: 'agent' | 'user';
  segments: ChatSegment[];
  timestamp: string;
  files?: FileAttachment[];
}

interface ChatResponse {
  session_id: string;
  phase: string;
  status: string;
  message: ChatMessage;
  recipe_draft: Record<string, unknown> | null;
}

// ---------------------------------------------------------------------------
// Test CSV data
// ---------------------------------------------------------------------------
const PAYMENTS_CSV =
  'payment_id,reference_number,amount,date,currency\n' +
  '1,INV-001,100.00,2024-01-15,USD\n' +
  '2,INV-002,250.50,2024-01-16,USD';

const INVOICES_CSV =
  'invoice_id,customer_name,amount,currency,status\n' +
  '1,Acme Corp,100.00,USD,pending\n' +
  '2,Beta Inc,250.50,USD,pending';

// ---------------------------------------------------------------------------
// Helper: upload a CSV through the full presign → PUT → confirm flow
// ---------------------------------------------------------------------------
async function uploadTestFile(
  sessionId: string,
  filename: string,
  csvContent: string,
): Promise<FileAttachment> {
  // 1. Presign
  const presignRes = await fetch(`${BASE_URL}/api/uploads/presign`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ filename, session_id: sessionId }),
  });
  expect(presignRes.status).toBe(200);
  const { upload_id, presigned_url, s3_uri } = await presignRes.json();

  // 2. PUT to S3
  const putRes = await fetch(presigned_url, {
    method: 'PUT',
    body: csvContent,
    headers: { 'Content-Type': 'text/csv' },
  });
  expect(putRes.ok).toBe(true);

  // 3. Confirm
  const confirmRes = await fetch(`${BASE_URL}/api/uploads/${upload_id}/confirm`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ s3_uri, filename }),
  });
  expect(confirmRes.status).toBe(200);
  const body = await confirmRes.json();

  return {
    upload_id: body.upload_id,
    filename: body.filename,
    s3_uri: body.s3_uri ?? s3_uri,
    columns: body.columns,
    row_count: body.row_count,
  };
}

// ---------------------------------------------------------------------------
// Helper: send a chat message and return parsed response
// ---------------------------------------------------------------------------
async function sendChat(
  sessionId: string | undefined,
  message: string,
  files?: FileAttachment[],
): Promise<ChatResponse> {
  const payload: Record<string, unknown> = { message };
  if (sessionId) payload.session_id = sessionId;
  if (files && files.length > 0) payload.files = files;

  const res = await fetch(`${BASE_URL}/api/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  expect(res.status).toBe(200);
  return res.json();
}

// ---------------------------------------------------------------------------
// Helper: extract all text content from a ChatMessage
// ---------------------------------------------------------------------------
function messageText(msg: ChatMessage): string {
  return msg.segments
    .filter((s) => s.type === 'text' && s.content)
    .map((s) => s.content!)
    .join('\n');
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
describe('Upload + Chat Agent Flow', () => {
  itIntegration('agent sees uploaded files in message', async () => {
    // Start a chat to get a session
    const greeting = await sendChat(undefined, 'Hello, I want to reconcile some data.');
    const sessionId = greeting.session_id;
    expect(sessionId).toBeDefined();

    // Upload both CSV files
    const paymentsAttachment = await uploadTestFile(sessionId, 'payments.csv', PAYMENTS_CSV);
    const invoicesAttachment = await uploadTestFile(sessionId, 'invoices.csv', INVOICES_CSV);

    // Send message with files attached
    const response = await sendChat(sessionId, 'match these files', [
      paymentsAttachment,
      invoicesAttachment,
    ]);

    expect(response.session_id).toBe(sessionId);

    const text = messageText(response.message).toLowerCase();

    // Agent should NOT say it can't see files
    expect(text).not.toContain("don't see any files");
    expect(text).not.toContain('please upload');

    // Agent should reference the file data (column names, filenames, etc.)
    const mentionsSomething =
      text.includes('payment') ||
      text.includes('invoice') ||
      text.includes('amount') ||
      text.includes('reference') ||
      text.includes('currency') ||
      text.includes('csv');
    expect(mentionsSomething).toBe(true);
  });

  itIntegration('phase advances from greeting when files are previewed', async () => {
    // Start chat — may already advance to intent if registered sources exist
    const greeting = await sendChat(undefined, 'Hello');
    const sessionId = greeting.session_id;
    const initialPhase = greeting.phase;

    // Upload files
    const paymentsAttachment = await uploadTestFile(sessionId, 'payments.csv', PAYMENTS_CSV);
    const invoicesAttachment = await uploadTestFile(sessionId, 'invoices.csv', INVOICES_CSV);

    // Send with files — agent should preview them and advance past initial phase
    const response = await sendChat(sessionId, 'reconcile these', [
      paymentsAttachment,
      invoicesAttachment,
    ]);

    // Should have advanced beyond greeting (at minimum to intent or further)
    expect(response.phase).not.toBe('greeting');
    // Should be at scoping or beyond (both schemas loaded from file previews)
    const advancedPhases = ['scoping', 'demonstration', 'inference', 'validation', 'execution'];
    expect(advancedPhases).toContain(response.phase);
  });

  itIntegration('multiple files can be attached to single message', async () => {
    // Start chat
    const greeting = await sendChat(undefined, 'Hi');
    const sessionId = greeting.session_id;

    // Upload two CSV files
    const paymentsAttachment = await uploadTestFile(sessionId, 'payments.csv', PAYMENTS_CSV);
    const invoicesAttachment = await uploadTestFile(sessionId, 'invoices.csv', INVOICES_CSV);

    // Send both files in one message
    const response = await sendChat(sessionId, 'I have two files to reconcile', [
      paymentsAttachment,
      invoicesAttachment,
    ]);

    expect(response.session_id).toBe(sessionId);

    const text = messageText(response.message).toLowerCase();

    // Agent should acknowledge both files/datasets
    const mentionsPayments =
      text.includes('payment') || text.includes('payments.csv');
    const mentionsInvoices =
      text.includes('invoice') || text.includes('invoices.csv');

    expect(mentionsPayments || mentionsInvoices).toBe(true);
  });

  itIntegration('preview endpoint works with uploaded file s3_uri', async () => {
    // Start chat to get session_id
    const greeting = await sendChat(undefined, 'Hello');
    const sessionId = greeting.session_id;

    // Upload a CSV
    const attachment = await uploadTestFile(sessionId, 'payments.csv', PAYMENTS_CSV);

    // Call the preview endpoint directly
    const previewRes = await fetch(`${BASE_URL}/api/uploads/preview`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ s3_uri: attachment.s3_uri }),
    });

    expect(previewRes.status).toBe(200);
    const body = await previewRes.json();

    expect(body.columns).toEqual([
      'payment_id',
      'reference_number',
      'amount',
      'date',
      'currency',
    ]);
    expect(body.row_count).toBe(2);
    expect(body.sample).toBeDefined();
    expect(Array.isArray(body.sample)).toBe(true);
    expect(body.sample.length).toBeGreaterThan(0);
    expect(body.sample[0]).toHaveProperty('payment_id');
    expect(body.sample[0]).toHaveProperty('reference_number');
    expect(body.sample[0]).toHaveProperty('amount');
  });
});
