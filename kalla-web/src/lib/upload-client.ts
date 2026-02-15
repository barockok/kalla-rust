import type { FileAttachment } from '@/lib/chat-types';

export interface UploadProgress {
  phase: 'presigning' | 'uploading' | 'confirming' | 'done' | 'error';
  percent: number;
  error?: string;
}

export async function uploadFile(
  file: File,
  sessionId: string,
  onProgress: (p: UploadProgress) => void,
): Promise<FileAttachment> {
  // 1. Presign
  onProgress({ phase: 'presigning', percent: 10 });

  const presignRes = await fetch('/api/uploads/presign', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ filename: file.name, session_id: sessionId }),
  });

  if (!presignRes.ok) {
    const err = await presignRes.json().catch(() => ({ error: 'Presign failed' }));
    const msg = err.error || 'Presign failed';
    onProgress({ phase: 'error', percent: 0, error: msg });
    throw new Error(msg);
  }

  const { upload_id, presigned_url, s3_uri } = await presignRes.json();

  // 2. Upload to S3
  onProgress({ phase: 'uploading', percent: 30 });

  const uploadRes = await fetch(presigned_url, {
    method: 'PUT',
    headers: { 'Content-Type': 'text/csv' },
    body: file,
  });

  if (!uploadRes.ok) {
    const msg = `Upload failed: ${uploadRes.statusText}`;
    onProgress({ phase: 'error', percent: 0, error: msg });
    throw new Error(msg);
  }

  onProgress({ phase: 'uploading', percent: 70 });

  // 3. Confirm
  onProgress({ phase: 'confirming', percent: 80 });

  const confirmRes = await fetch(`/api/uploads/${upload_id}/confirm`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ s3_uri, filename: file.name }),
  });

  if (!confirmRes.ok) {
    const err = await confirmRes.json().catch(() => ({ error: 'Confirm failed' }));
    const msg = err.error || 'Confirm failed';
    onProgress({ phase: 'error', percent: 0, error: msg });
    throw new Error(msg);
  }

  const attachment: FileAttachment = await confirmRes.json();

  // 4. Done
  onProgress({ phase: 'done', percent: 100 });

  return attachment;
}
