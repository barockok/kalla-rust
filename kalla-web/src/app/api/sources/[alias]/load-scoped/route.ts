import { NextResponse } from 'next/server';
import pool from '@/lib/db';

interface FilterCondition {
  column: string;
  op: string;
  value: unknown;
}

const VALID_OPS: Record<string, string> = {
  eq: '=',
  neq: '!=',
  gt: '>',
  gte: '>=',
  lt: '<',
  lte: '<=',
  like: 'LIKE',
};

/**
 * POST /api/sources/:alias/load-scoped
 *
 * Load filtered rows from a registered data source.
 * Body: { conditions: FilterCondition[], limit?: number }
 */
export async function POST(
  request: Request,
  { params }: { params: Promise<{ alias: string }> },
) {
  const { alias } = await params;
  const body = await request.json();
  const conditions: FilterCondition[] = body.conditions || [];
  const limit = Math.min(Number(body.limit || 200), 1000);

  // Look up the source by alias
  const { rows: sources } = await pool.query(
    'SELECT alias, uri, source_type FROM sources WHERE alias = $1',
    [alias],
  );

  if (sources.length === 0) {
    return NextResponse.json({ error: `Source '${alias}' not found` }, { status: 404 });
  }

  const source = sources[0];

  // Extract table name from URI
  const tableMatch = source.uri.match(/[?&]table=([^&]+)/);
  if (!tableMatch) {
    return NextResponse.json(
      { error: `Cannot extract table from URI: ${source.uri}` },
      { status: 400 },
    );
  }
  const tableName = tableMatch[1];

  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(tableName)) {
    return NextResponse.json({ error: 'Invalid table name' }, { status: 400 });
  }

  // Get column info
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

  const validColumns = new Set(columns.map((c) => c.name));

  // Build WHERE clause from conditions
  const whereParts: string[] = [];
  const values: unknown[] = [];
  let paramIdx = 1;

  for (const cond of conditions) {
    // Validate column name exists
    if (!validColumns.has(cond.column)) {
      return NextResponse.json(
        { error: `Unknown column: ${cond.column}` },
        { status: 400 },
      );
    }

    const colRef = `"${cond.column}"`;

    if (cond.op === 'between' && Array.isArray(cond.value) && cond.value.length === 2) {
      whereParts.push(`${colRef} BETWEEN $${paramIdx} AND $${paramIdx + 1}`);
      values.push(cond.value[0], cond.value[1]);
      paramIdx += 2;
    } else if (cond.op === 'in' && Array.isArray(cond.value)) {
      const placeholders = cond.value.map(() => `$${paramIdx++}`).join(', ');
      whereParts.push(`${colRef} IN (${placeholders})`);
      values.push(...cond.value);
    } else {
      const sqlOp = VALID_OPS[cond.op];
      if (!sqlOp) {
        return NextResponse.json({ error: `Unknown operator: ${cond.op}` }, { status: 400 });
      }
      // Cast to text for comparison to handle date strings against date columns
      whereParts.push(`${colRef}::text ${sqlOp} $${paramIdx}`);
      values.push(cond.value);
      paramIdx++;
    }
  }

  const whereClause = whereParts.length > 0 ? `WHERE ${whereParts.join(' AND ')}` : '';

  // Query with filters
  values.push(limit);
  const { rows: dataRows } = await pool.query(
    `SELECT * FROM "${tableName}" ${whereClause} LIMIT $${paramIdx}`,
    values,
  );

  // Convert to string[][] format
  const colNames = columns.map((c) => c.name);
  const rows = dataRows.map((row) =>
    colNames.map((col) => (row[col] === null ? '' : String(row[col]))),
  );

  return NextResponse.json({
    alias,
    columns,
    rows,
    total_rows: rows.length,
    preview_rows: rows.length,
  });
}
