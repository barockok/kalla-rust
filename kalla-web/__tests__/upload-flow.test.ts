import { describe, test, expect } from '@jest/globals';

const BASE_URL = process.env.TEST_BASE_URL || 'http://localhost:3000';

// Integration test — requires running Next.js + MinIO.
// Skip in unit test runs (no fetch in Jest Node environment).
const itIntegration = process.env.RUN_INTEGRATION ? test : test.skip;

describe('File Upload Flow', () => {
  const sessionId = `test-session-${Date.now()}`;

  itIntegration('presign returns upload_id and presigned_url', async () => {
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

  itIntegration('rejects non-csv files', async () => {
    const res = await fetch(`${BASE_URL}/api/uploads/presign`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename: 'test.xlsx', session_id: sessionId }),
    });

    expect(res.status).toBe(400);
  });

  itIntegration('full upload and confirm flow', async () => {
    // 1. Get presigned URL
    const presignRes = await fetch(`${BASE_URL}/api/uploads/presign`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename: 'payments.csv', session_id: sessionId }),
    });
    expect(presignRes.status).toBe(200);
    const { upload_id, presigned_url, s3_uri } = await presignRes.json();

    // 2. Upload CSV to S3 via presigned URL
    const csvContent = 'payment_id,reference_number,amount,date\n1,INV-001,100.00,2024-01-15\n2,INV-002,250.50,2024-01-16\n';
    const uploadRes = await fetch(presigned_url, {
      method: 'PUT',
      body: csvContent,
      headers: { 'Content-Type': 'text/csv' },
    });
    expect(uploadRes.ok).toBe(true);

    // 3. Confirm upload and get metadata
    const confirmRes = await fetch(`${BASE_URL}/api/uploads/${upload_id}/confirm`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ s3_uri, filename: 'payments.csv' }),
    });
    expect(confirmRes.status).toBe(200);
    const body = await confirmRes.json();
    expect(body.columns).toEqual(['payment_id', 'reference_number', 'amount', 'date']);
    expect(body.row_count).toBe(2);
    expect(body.upload_id).toBe(upload_id);
  });

  itIntegration('preview returns sample rows', async () => {
    // First do a full upload
    const presignRes = await fetch(`${BASE_URL}/api/uploads/presign`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename: 'preview-test.csv', session_id: sessionId }),
    });
    const { presigned_url, s3_uri } = await presignRes.json();

    const csvContent = 'id,name,amount\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n';
    await fetch(presigned_url, {
      method: 'PUT',
      body: csvContent,
      headers: { 'Content-Type': 'text/csv' },
    });

    // Preview the uploaded file
    const previewRes = await fetch(`${BASE_URL}/api/uploads/preview`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ s3_uri }),
    });
    expect(previewRes.status).toBe(200);
    const body = await previewRes.json();
    expect(body.columns).toEqual(['id', 'name', 'amount']);
    expect(body.row_count).toBe(3);
    expect(body.sample).toHaveLength(3);
    expect(body.sample[0]).toEqual({ id: '1', name: 'Alice', amount: '100' });
  });
});
