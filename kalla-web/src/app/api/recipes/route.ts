import { NextResponse } from 'next/server';
import pool from '@/lib/db';
import type { Recipe } from '@/lib/recipe-types';

export async function GET() {
  const { rows } = await pool.query(
    `SELECT recipe_id, name, description, match_sql, match_description, sources
     FROM recipes ORDER BY created_at DESC`
  );
  return NextResponse.json(rows);
}

export async function POST(request: Request) {
  const body: Recipe = await request.json();

  if (!body.recipe_id || !body.name || !body.match_sql || !body.sources) {
    return NextResponse.json(
      { error: 'recipe_id, name, match_sql, and sources are required' },
      { status: 400 }
    );
  }

  await pool.query(
    `INSERT INTO recipes (recipe_id, name, description, match_sql, match_description, sources)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (recipe_id) DO UPDATE
       SET name = $2, description = $3, match_sql = $4, match_description = $5, sources = $6, updated_at = NOW()`,
    [
      body.recipe_id,
      body.name,
      body.description || '',
      body.match_sql,
      body.match_description || '',
      JSON.stringify(body.sources),
    ]
  );

  return NextResponse.json({
    success: true,
    message: `Recipe '${body.recipe_id}' saved`,
  });
}
