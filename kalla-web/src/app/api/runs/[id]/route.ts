import { NextResponse } from 'next/server';
import pool from '@/lib/db';

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;

  const { rows: runRows } = await pool.query(
    `SELECT id as run_id, recipe_id, status, matched_count,
            unmatched_left_count, unmatched_right_count,
            output_matched, output_unmatched_left, output_unmatched_right,
            error_message, created_at, updated_at
     FROM runs WHERE id = $1`,
    [id]
  );

  if (runRows.length === 0) {
    return NextResponse.json({ error: 'Run not found' }, { status: 404 });
  }

  const run = runRows[0];

  const { rows: progressRows } = await pool.query(
    `SELECT stage, progress, matched_count, total_left, total_right, updated_at
     FROM run_progress WHERE run_id = $1 ORDER BY updated_at DESC LIMIT 1`,
    [id]
  );

  return NextResponse.json({
    ...run,
    latest_progress: progressRows[0] || null,
  });
}
