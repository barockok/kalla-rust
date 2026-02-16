import { NextResponse } from 'next/server';
import pool from '@/lib/db';

/**
 * GET /api/sources/:alias/preview?limit=10
 *
 * Returns schema info and sample rows for a registered data source.
 * Parses the source URI to extract the table name and queries Postgres directly.
 */
export async function GET(
  request: Request,
  { params }: { params: Promise<{ alias: string }> },
) {
  const { alias } = await params;
  const url = new URL(request.url);
  const limit = Math.min(Number(url.searchParams.get('limit') || '10'), 100);

  // Look up the source by alias
  const { rows: sources } = await pool.query(
    'SELECT alias, uri, source_type FROM sources WHERE alias = $1',
    [alias],
  );

  if (sources.length === 0) {
    return NextResponse.json({ error: `Source '${alias}' not found` }, { status: 404 });
  }

  const source = sources[0];

  // Extract table name from URI (e.g. postgres://...?table=invoices)
  const tableMatch = source.uri.match(/[?&]table=([^&]+)/);
  if (!tableMatch) {
    return NextResponse.json(
      { error: `Cannot extract table from URI: ${source.uri}` },
      { status: 400 },
    );
  }
  const tableName = tableMatch[1];

  // Validate table name (prevent SQL injection)
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(tableName)) {
    return NextResponse.json({ error: 'Invalid table name' }, { status: 400 });
  }

  // Get column info from information_schema
  const { rows: columnRows } = await pool.query(
    `SELECT column_name, data_type, is_nullable
     FROM information_schema.columns
     WHERE table_name = $1 AND table_schema = 'public'
     ORDER BY ordinal_position`,
    [tableName],
  );

  const columns = columnRows.map((c) => ({
    name: c.column_name,
    data_type: c.data_type,
    nullable: c.is_nullable === 'YES',
  }));

  // Get total row count
  const { rows: countRows } = await pool.query(
    `SELECT COUNT(*)::int AS total FROM "${tableName}"`,
  );
  const totalRows = countRows[0].total;

  // Get sample rows
  const { rows: dataRows } = await pool.query(
    `SELECT * FROM "${tableName}" LIMIT $1`,
    [limit],
  );

  // Convert to string[][] format matching SourcePreview
  const colNames = columns.map((c) => c.name);
  const rows = dataRows.map((row) =>
    colNames.map((col) => (row[col] === null ? '' : String(row[col]))),
  );

  return NextResponse.json({
    alias,
    columns,
    rows,
    total_rows: totalRows,
    preview_rows: rows.length,
  });
}
