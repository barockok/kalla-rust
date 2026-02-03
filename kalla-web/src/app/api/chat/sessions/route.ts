import { NextResponse } from 'next/server';
import { createSession } from '@/lib/session-store';

export async function POST() {
  const session = createSession();
  return NextResponse.json({
    session_id: session.id,
    phase: session.phase,
    status: session.status,
  });
}
