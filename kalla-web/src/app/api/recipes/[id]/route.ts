import { NextResponse } from 'next/server';
import pool from '@/lib/db';

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const { rows } = await pool.query(
    `SELECT recipe_id, name, description, match_sql, match_description, sources
     FROM recipes WHERE recipe_id = $1`,
    [id]
  );

  if (rows.length === 0) {
    return NextResponse.json({ error: 'Recipe not found' }, { status: 404 });
  }

  return NextResponse.json(rows[0]);
}
