import { NextResponse } from 'next/server';
import pool from '@/lib/db';
import type { WorkerProgress } from '@/lib/recipe-types';

export async function POST(request: Request) {
  const body: WorkerProgress = await request.json();

  await pool.query(
    `INSERT INTO run_progress (run_id, stage, progress, matched_count, total_left, total_right, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, NOW())
     ON CONFLICT (run_id) DO UPDATE
       SET stage = $2, progress = $3, matched_count = $4, total_left = $5, total_right = $6, updated_at = NOW()`,
    [
      body.run_id,
      body.stage,
      body.progress ?? 0,
      body.matched_count ?? 0,
      body.total_left ?? 0,
      body.total_right ?? 0,
    ]
  );

  await pool.query(
    `UPDATE runs SET status = 'running', updated_at = NOW() WHERE id = $1 AND status != 'running'`,
    [body.run_id]
  );

  return NextResponse.json({ ok: true });
}
