import { JobPayload } from './recipe-types';

const WORKER_URL = process.env.WORKER_URL || 'http://localhost:9090';
const NATS_URL = process.env.NATS_URL;

export async function dispatchJob(payload: JobPayload): Promise<void> {
  if (NATS_URL) {
    throw new Error('NATS dispatch not yet implemented');
  }

  const res = await fetch(`${WORKER_URL}/api/jobs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    throw new Error(`Worker rejected job: ${await res.text()}`);
  }
}
