import { NextResponse } from 'next/server';
import pool from '@/lib/db';
import { getObject, UPLOADS_BUCKET } from '@/lib/s3-client';
import { parse } from 'csv-parse/sync';

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

/* ------------------------------------------------------------------ */
/*  CSV source handler                                                 */
/* ------------------------------------------------------------------ */

async function loadCsvSource(
  alias: string,
  uri: string,
  conditions: FilterCondition[],
  limit: number,
) {
  const prefix = `s3://${UPLOADS_BUCKET}/`;
  if (!uri.startsWith(prefix)) {
    return NextResponse.json({ error: `Invalid CSV URI: ${uri}` }, { status: 400 });
  }
  const key = uri.slice(prefix.length);

  const stream = await getObject(key);
  if (!stream) {
    return NextResponse.json({ error: 'CSV file not found in S3' }, { status: 404 });
  }

  // Read stream
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (value) chunks.push(value);
    if (done) break;
  }
  const totalLength = chunks.reduce((acc, c) => acc + c.length, 0);
  const buffer = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    buffer.set(chunk, offset);
    offset += chunk.length;
  }
  const csvText = new TextDecoder().decode(buffer);

  const records: Record<string, string>[] = parse(csvText, {
    columns: true,
    skip_empty_lines: true,
  });

  if (records.length === 0) {
    return NextResponse.json({
      alias,
      columns: [],
      rows: [],
      total_rows: 0,
      preview_rows: 0,
    });
  }

  const colNames = Object.keys(records[0]);
  const columns = colNames.map((name) => ({
    name,
    data_type: 'text',
    nullable: true,
  }));

  // Apply in-memory filters
  let filtered = records;
  for (const cond of conditions) {
    if (!colNames.includes(cond.column)) continue; // skip unknown columns
    filtered = filtered.filter((row) => {
      const cellVal = row[cond.column] ?? '';
      if (cond.op === 'eq') return cellVal === String(cond.value);
      if (cond.op === 'neq') return cellVal !== String(cond.value);
      if (cond.op === 'like') {
        const pattern = String(cond.value).replace(/%/g, '.*').replace(/_/g, '.');
        return new RegExp(pattern, 'i').test(cellVal);
      }
      if (cond.op === 'between' && Array.isArray(cond.value) && cond.value.length === 2) {
        return cellVal >= String(cond.value[0]) && cellVal <= String(cond.value[1]);
      }
      if (cond.op === 'gte') return cellVal >= String(cond.value);
      if (cond.op === 'lte') return cellVal <= String(cond.value);
      if (cond.op === 'gt') return cellVal > String(cond.value);
      if (cond.op === 'lt') return cellVal < String(cond.value);
      return true;
    });
  }

  const limited = filtered.slice(0, limit);
  const rows = limited.map((row) => colNames.map((col) => row[col] ?? ''));

  return NextResponse.json({
    alias,
    columns,
    rows,
    total_rows: filtered.length,
    preview_rows: rows.length,
  });
}

/* ------------------------------------------------------------------ */
/*  DB source handler                                                  */
/* ------------------------------------------------------------------ */

async function loadDbSource(
  alias: string,
  uri: string,
  conditions: FilterCondition[],
  limit: number,
) {
  const tableMatch = uri.match(/[?&]table=([^&]+)/);
  if (!tableMatch) {
    return NextResponse.json(
      { error: `Cannot extract table from URI: ${uri}` },
      { status: 400 },
    );
  }
  const tableName = tableMatch[1];

  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(tableName)) {
    return NextResponse.json({ error: 'Invalid table name' }, { status: 400 });
  }

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

  const whereParts: string[] = [];
  const values: unknown[] = [];
  let paramIdx = 1;

  for (const cond of conditions) {
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
      whereParts.push(`${colRef}::text ${sqlOp} $${paramIdx}`);
      values.push(cond.value);
      paramIdx++;
    }
  }

  const whereClause = whereParts.length > 0 ? `WHERE ${whereParts.join(' AND ')}` : '';

  values.push(limit);
  const { rows: dataRows } = await pool.query(
    `SELECT * FROM "${tableName}" ${whereClause} LIMIT $${paramIdx}`,
    values,
  );

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

/* ------------------------------------------------------------------ */
/*  Route handler                                                      */
/* ------------------------------------------------------------------ */

/**
 * POST /api/sources/:alias/load-scoped
 *
 * Load filtered rows from a registered data source (DB or CSV).
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

  const { rows: sources } = await pool.query(
    'SELECT alias, uri, source_type FROM sources WHERE alias = $1',
    [alias],
  );

  if (sources.length === 0) {
    return NextResponse.json({ error: `Source '${alias}' not found` }, { status: 404 });
  }

  const source = sources[0];

  if (source.source_type === 'csv') {
    return loadCsvSource(alias, source.uri, conditions, limit);
  }

  return loadDbSource(alias, source.uri, conditions, limit);
}
