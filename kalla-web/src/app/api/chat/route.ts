import { NextRequest, NextResponse } from 'next/server';
import { createSession, getSession, updateSession, addMessage } from '@/lib/session-store';
import { runAgent } from '@/lib/agent';
import type { ChatMessage, CardResponse, ChatSession, FileAttachment } from '@/lib/chat-types';

export async function POST(request: NextRequest) {
  let session: ReturnType<typeof getSession> | ReturnType<typeof createSession> | undefined;
  try {
    const body = await request.json();
    const { session_id, message, card_response, files } = body as {
      session_id?: string;
      message?: string;
      card_response?: CardResponse;
      files?: FileAttachment[];
    };

    // Get or create session
    session = session_id ? getSession(session_id) : undefined;
    if (!session) {
      session = createSession();
    }

    // Build user message text
    let userText: string;
    if (card_response) {
      userText = `[Card response: ${card_response.action} on ${card_response.card_id}${card_response.value !== undefined ? `, value: ${JSON.stringify(card_response.value)}` : ''}]`;

      // Handle match confirmation
      if (card_response.action === 'confirm' && card_response.card_id.startsWith('match-')) {
        const matchData = card_response.value as { left: Record<string, unknown>; right: Record<string, unknown> } | undefined;
        if (matchData) {
          const pairs = [...session.confirmed_pairs, matchData];
          updateSession(session.id, { confirmed_pairs: pairs });
          session = getSession(session.id)!;
        }
      }

      // Handle validation approval
      if (card_response.action === 'approve' && card_response.card_id.startsWith('validation-')) {
        updateSession(session.id, { validation_approved: true });
        session = getSession(session.id)!;
      }
    } else if (message) {
      userText = message;
    } else {
      return NextResponse.json({ error: 'Either message or card_response required' }, { status: 400 });
    }

    // Add user message to session
    const userMsg: ChatMessage = {
      role: 'user',
      segments: [{ type: 'text', content: userText }],
      timestamp: new Date().toISOString(),
      files: files || undefined,
    };
    addMessage(session.id, userMsg);

    // Run the agent (ALL phase management happens inside)
    const agentResponse = await runAgent(session, userText);

    // Apply phase transition and session updates in a single write to avoid
    // a race window where the phase advances but prerequisite data is missing.
    const updates: Partial<ChatSession> = { ...agentResponse.sessionUpdates };
    if (agentResponse.phaseTransition) {
      updates.phase = agentResponse.phaseTransition;
    }
    if (Object.keys(updates).length > 0) {
      updateSession(session.id, updates);
    }

    // Add agent message
    const agentMsg: ChatMessage = {
      role: 'agent',
      segments: agentResponse.segments,
      timestamp: new Date().toISOString(),
    };
    addMessage(session.id, agentMsg);

    session = getSession(session.id)!;

    return NextResponse.json({
      session_id: session.id,
      phase: session.phase,
      status: session.status,
      message: agentMsg,
      recipe_draft: session.recipe_draft,
    });
  } catch (err) {
    console.error('Chat API error:', err);
    const errorSegment = {
      type: 'text' as const,
      content: `Error: ${err instanceof Error ? err.message : 'Something went wrong'}`,
    };
    const errorMsg: ChatMessage = {
      role: 'agent' as const,
      segments: [errorSegment],
      timestamp: new Date().toISOString(),
    };

    return NextResponse.json({
      session_id: session?.id || null,
      phase: session?.phase || 'greeting',
      status: session?.status || 'error',
      message: errorMsg,
      recipe_draft: null,
    });
  }
}
