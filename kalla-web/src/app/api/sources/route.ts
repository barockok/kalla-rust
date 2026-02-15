import { NextResponse } from 'next/server';
import pool from '@/lib/db';

export async function GET() {
  const { rows } = await pool.query(
    'SELECT alias, uri, source_type, status FROM sources ORDER BY created_at DESC'
  );
  return NextResponse.json(rows);
}

export async function POST(request: Request) {
  const { alias, uri, source_type } = await request.json();

  if (!alias || !uri || !source_type) {
    return NextResponse.json(
      { error: 'alias, uri, and source_type are required' },
      { status: 400 }
    );
  }

  await pool.query(
    `INSERT INTO sources (alias, uri, source_type, status)
     VALUES ($1, $2, $3, 'connected')
     ON CONFLICT (alias) DO UPDATE SET uri = $2, source_type = $3, status = 'connected', updated_at = NOW()`,
    [alias, uri, source_type]
  );

  return NextResponse.json({
    success: true,
    message: `Registered '${alias}' as ${source_type}`,
  });
}
