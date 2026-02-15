import { NextResponse } from 'next/server';
import pool from '@/lib/db';
import type { WorkerComplete } from '@/lib/recipe-types';

export async function POST(request: Request) {
  const body: WorkerComplete = await request.json();

  await pool.query(
    `UPDATE runs SET
       status = 'completed',
       matched_count = $2,
       unmatched_left_count = $3,
       unmatched_right_count = $4,
       output_matched = $5,
       output_unmatched_left = $6,
       output_unmatched_right = $7,
       updated_at = NOW()
     WHERE id = $1`,
    [
      body.run_id,
      body.matched_count,
      body.unmatched_left_count,
      body.unmatched_right_count,
      body.output_paths.matched,
      body.output_paths.unmatched_left,
      body.output_paths.unmatched_right,
    ]
  );

  return NextResponse.json({ ok: true });
}
