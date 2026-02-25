import { NextResponse } from 'next/server';

const WORKER_URL = process.env.WORKER_URL || 'http://localhost:9090';

/**
 * POST /api/sources/:alias/load-scoped
 *
 * Thin proxy to the Rust backend which handles all data loading.
 * Body is forwarded as-is: { conditions: FilterCondition[], limit?: number }
 */
export async function POST(
  request: Request,
  { params }: { params: Promise<{ alias: string }> },
) {
  const { alias } = await params;
  const body = await request.text();

  try {
    const res = await fetch(
      `${WORKER_URL}/api/sources/${encodeURIComponent(alias)}/load-scoped`,
      { method: 'POST', headers: { 'Content-Type': 'application/json' }, body },
    );
    const data = await res.json();
    return NextResponse.json(data, { status: res.status });
  } catch (error: any) {
    return NextResponse.json(
      { error: `Backend unavailable: ${error.message}` },
      { status: 502 },
    );
  }
}
