import { NextRequest, NextResponse } from 'next/server';
import { getSession } from '@/lib/session-store';

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const session = getSession(id);
  if (!session) {
    return NextResponse.json({ error: 'Session not found' }, { status: 404 });
  }
  return NextResponse.json(session);
}
