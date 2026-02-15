import { NextResponse } from 'next/server';
import pool from '@/lib/db';
import type { WorkerError } from '@/lib/recipe-types';

export async function POST(request: Request) {
  const body: WorkerError = await request.json();

  await pool.query(
    `UPDATE runs SET status = 'failed', error_message = $2, updated_at = NOW() WHERE id = $1`,
    [body.run_id, body.error]
  );

  return NextResponse.json({ ok: true });
}
