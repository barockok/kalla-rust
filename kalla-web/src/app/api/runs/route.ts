import { NextResponse } from 'next/server';
import { v4 as uuidv4 } from 'uuid';
import pool from '@/lib/db';
import { dispatchJob } from '@/lib/worker-client';
import type { Recipe, ResolvedSource } from '@/lib/recipe-types';

export async function GET() {
  const { rows } = await pool.query(
    `SELECT id as run_id, recipe_id, status, matched_count, unmatched_left_count,
            unmatched_right_count, created_at, updated_at
     FROM runs ORDER BY created_at DESC`
  );
  return NextResponse.json(rows);
}

interface CreateRunBody {
  recipe_id?: string;
  recipe?: Recipe;
  resolved_sources?: ResolvedSource[];
}

export async function POST(request: Request) {
  const body: CreateRunBody = await request.json();

  let recipe: Recipe;

  if (body.recipe) {
    recipe = body.recipe;
  } else if (body.recipe_id) {
    const { rows } = await pool.query(
      `SELECT recipe_id, name, description, match_sql, match_description, sources
       FROM recipes WHERE recipe_id = $1`,
      [body.recipe_id]
    );
    if (rows.length === 0) {
      return NextResponse.json({ error: 'Recipe not found' }, { status: 404 });
    }
    recipe = rows[0];
  } else {
    return NextResponse.json(
      { error: 'recipe_id or inline recipe required' },
      { status: 400 }
    );
  }

  const sources =
    typeof recipe.sources === 'string'
      ? JSON.parse(recipe.sources)
      : recipe.sources;

  const runId = uuidv4();
  const outputPath = `runs/${runId}`;

  const resolvedSources: ResolvedSource[] = body.resolved_sources || [
    { alias: sources.left.alias, uri: sources.left.uri || '' },
    { alias: sources.right.alias, uri: sources.right.uri || '' },
  ];

  const primaryKeys: Record<string, string[]> = {
    [sources.left.alias]: sources.left.primary_key,
    [sources.right.alias]: sources.right.primary_key,
  };

  const callbackUrl = `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000'}/api/worker`;

  try {
    await dispatchJob({
      run_id: runId,
      callback_url: callbackUrl,
      match_sql: recipe.match_sql,
      sources: resolvedSources,
      output_path: outputPath,
      primary_keys: primaryKeys,
    });
  } catch (err) {
    return NextResponse.json(
      { error: `Failed to dispatch job: ${err instanceof Error ? err.message : err}` },
      { status: 502 }
    );
  }

  await pool.query(
    `INSERT INTO runs (id, recipe_id, status, created_at, updated_at)
     VALUES ($1, $2, 'submitted', NOW(), NOW())`,
    [runId, recipe.recipe_id]
  );

  return NextResponse.json({ run_id: runId, status: 'submitted' });
}
