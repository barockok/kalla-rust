import { NextRequest, NextResponse } from 'next/server';
import { createSession, getSession, updateSession, addMessage } from '@/lib/session-store';
import { runAgent } from '@/lib/agent';
import type { ChatMessage, CardResponse } from '@/lib/chat-types';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { session_id, message, card_response } = body as {
      session_id?: string;
      message?: string;
      card_response?: CardResponse;
    };

    // Get or create session
    let session = session_id ? getSession(session_id) : undefined;
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
    };
    addMessage(session.id, userMsg);

    // Detect intent from user message for phase transitions
    if (session.phase === 'intent' && !session.left_source_alias) {
      const words = userText.toLowerCase().split(/\s+/);
      if (words.includes('invoices') || words.includes('invoice')) {
        updateSession(session.id, { left_source_alias: 'invoices' });
      }
      if (words.includes('payments') || words.includes('payment')) {
        updateSession(session.id, { right_source_alias: 'payments' });
      }
      session = getSession(session.id)!;
      if (session.left_source_alias && session.right_source_alias) {
        updateSession(session.id, { phase: 'sampling' });
        session = getSession(session.id)!;
      }
    }

    // Run the agent
    const agentResponse = await runAgent(session, userText);

    // Apply phase transitions
    if (agentResponse.phaseTransition) {
      updateSession(session.id, { phase: agentResponse.phaseTransition });
    }
    if (agentResponse.sessionUpdates) {
      updateSession(session.id, agentResponse.sessionUpdates);
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
    return NextResponse.json(
      { error: err instanceof Error ? err.message : 'Internal error' },
      { status: 500 }
    );
  }
}
