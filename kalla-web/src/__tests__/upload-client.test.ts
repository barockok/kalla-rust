import { uploadFile } from '@/lib/upload-client';
import type { UploadProgress } from '@/lib/upload-client';

// ---------------------------------------------------------------------------
// Global fetch mock
// ---------------------------------------------------------------------------
const mockFetch = jest.fn();
global.fetch = mockFetch;

beforeEach(() => {
  mockFetch.mockReset();
});

function okJson(data: unknown) {
  return { ok: true, json: async () => data, statusText: 'OK' };
}

function failJson(statusText: string, body: Record<string, unknown> = {}) {
  return { ok: false, statusText, json: async () => body };
}

function makeFile(name: string, content: string): File {
  return new File([content], name, { type: 'text/csv' });
}

describe('uploadFile', () => {
  const sessionId = 'sess-1';

  test('happy path: presign → upload → confirm → done', async () => {
    const progress: UploadProgress[] = [];
    const onProgress = (p: UploadProgress) => progress.push({ ...p });

    // 1. Presign
    mockFetch.mockResolvedValueOnce(
      okJson({
        upload_id: 'u1',
        presigned_url: 'https://s3.example.com/put',
        s3_uri: 's3://bucket/key.csv',
      }),
    );

    // 2. PUT to S3
    mockFetch.mockResolvedValueOnce({ ok: true });

    // 3. Confirm
    mockFetch.mockResolvedValueOnce(
      okJson({
        upload_id: 'u1',
        filename: 'test.csv',
        s3_uri: 's3://bucket/key.csv',
        columns: ['a', 'b'],
        row_count: 5,
      }),
    );

    const file = makeFile('test.csv', 'a,b\n1,2\n3,4');
    const result = await uploadFile(file, sessionId, onProgress);

    expect(result).toEqual({
      upload_id: 'u1',
      filename: 'test.csv',
      s3_uri: 's3://bucket/key.csv',
      columns: ['a', 'b'],
      row_count: 5,
    });

    // Check progress calls
    expect(progress.map((p) => p.phase)).toEqual([
      'presigning',
      'uploading',
      'uploading',
      'confirming',
      'done',
    ]);
    expect(progress[progress.length - 1].percent).toBe(100);

    // Verify fetch calls
    expect(mockFetch).toHaveBeenCalledTimes(3);

    // Presign call
    const [presignUrl, presignOpts] = mockFetch.mock.calls[0];
    expect(presignUrl).toBe('/api/uploads/presign');
    expect(presignOpts.method).toBe('POST');
    expect(JSON.parse(presignOpts.body)).toEqual({
      filename: 'test.csv',
      session_id: sessionId,
    });

    // PUT call
    const [putUrl, putOpts] = mockFetch.mock.calls[1];
    expect(putUrl).toBe('https://s3.example.com/put');
    expect(putOpts.method).toBe('PUT');

    // Confirm call
    const [confirmUrl, confirmOpts] = mockFetch.mock.calls[2];
    expect(confirmUrl).toBe('/api/uploads/u1/confirm');
    expect(confirmOpts.method).toBe('POST');
  });

  test('presign failure: sets error progress and throws', async () => {
    const progress: UploadProgress[] = [];
    const onProgress = (p: UploadProgress) => progress.push({ ...p });

    mockFetch.mockResolvedValueOnce(failJson('Bad Request', { error: 'Invalid session' }));

    const file = makeFile('bad.csv', 'a\n1');
    await expect(uploadFile(file, sessionId, onProgress)).rejects.toThrow('Invalid session');

    expect(progress.some((p) => p.phase === 'error')).toBe(true);
    const errProgress = progress.find((p) => p.phase === 'error')!;
    expect(errProgress.error).toBe('Invalid session');
  });

  test('presign failure: fallback error message when JSON parse fails', async () => {
    const progress: UploadProgress[] = [];
    const onProgress = (p: UploadProgress) => progress.push({ ...p });

    mockFetch.mockResolvedValueOnce({
      ok: false,
      statusText: 'Internal Server Error',
      json: async () => { throw new Error('not json'); },
    });

    const file = makeFile('bad.csv', 'a\n1');
    await expect(uploadFile(file, sessionId, onProgress)).rejects.toThrow('Presign failed');
  });

  test('S3 upload failure: sets error progress and throws', async () => {
    const progress: UploadProgress[] = [];
    const onProgress = (p: UploadProgress) => progress.push({ ...p });

    // Presign OK
    mockFetch.mockResolvedValueOnce(
      okJson({ upload_id: 'u1', presigned_url: 'https://s3/put', s3_uri: 's3://b/k' }),
    );
    // PUT fails
    mockFetch.mockResolvedValueOnce({ ok: false, statusText: 'Forbidden' });

    const file = makeFile('test.csv', 'a\n1');
    await expect(uploadFile(file, sessionId, onProgress)).rejects.toThrow('Upload failed: Forbidden');

    expect(progress.some((p) => p.phase === 'error')).toBe(true);
  });

  test('confirm failure: sets error progress and throws', async () => {
    const progress: UploadProgress[] = [];
    const onProgress = (p: UploadProgress) => progress.push({ ...p });

    // Presign OK
    mockFetch.mockResolvedValueOnce(
      okJson({ upload_id: 'u1', presigned_url: 'https://s3/put', s3_uri: 's3://b/k' }),
    );
    // PUT OK
    mockFetch.mockResolvedValueOnce({ ok: true });
    // Confirm fails
    mockFetch.mockResolvedValueOnce(failJson('Unprocessable Entity', { error: 'Bad CSV format' }));

    const file = makeFile('test.csv', 'a\n1');
    await expect(uploadFile(file, sessionId, onProgress)).rejects.toThrow('Bad CSV format');

    expect(progress.some((p) => p.phase === 'error')).toBe(true);
  });

  test('confirm failure: fallback error message when JSON parse fails', async () => {
    const progress: UploadProgress[] = [];
    const onProgress = (p: UploadProgress) => progress.push({ ...p });

    // Presign OK
    mockFetch.mockResolvedValueOnce(
      okJson({ upload_id: 'u1', presigned_url: 'https://s3/put', s3_uri: 's3://b/k' }),
    );
    // PUT OK
    mockFetch.mockResolvedValueOnce({ ok: true });
    // Confirm fails with non-JSON body
    mockFetch.mockResolvedValueOnce({
      ok: false,
      statusText: 'Internal Server Error',
      json: async () => { throw new Error('not json'); },
    });

    const file = makeFile('test.csv', 'a\n1');
    await expect(uploadFile(file, sessionId, onProgress)).rejects.toThrow('Confirm failed');
  });
});
