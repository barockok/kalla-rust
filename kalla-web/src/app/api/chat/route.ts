import { NextRequest, NextResponse } from 'next/server';
import { createSession, getSession, updateSession, addMessage } from '@/lib/session-store';
import { runAgent } from '@/lib/agent';
import { detectSourceAliases } from '@/lib/intent-detection';
import { listSources } from '@/lib/agent-tools';
import type { ChatMessage, CardResponse } from '@/lib/chat-types';

export async function POST(request: NextRequest) {
  let session: ReturnType<typeof getSession> | ReturnType<typeof createSession> | undefined;
  try {
    const body = await request.json();
    const { session_id, message, card_response } = body as {
      session_id?: string;
      message?: string;
      card_response?: CardResponse;
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
      try {
        const sources = await listSources();
        const detected = detectSourceAliases(
          userText,
          sources.map((s) => ({ alias: s.alias, source_type: s.source_type })),
        );
        if (detected.left) {
          updateSession(session.id, { left_source_alias: detected.left });
        }
        if (detected.right) {
          updateSession(session.id, { right_source_alias: detected.right });
        }
        session = getSession(session.id)!;
        if (session.left_source_alias && session.right_source_alias) {
          updateSession(session.id, { phase: 'sampling' });
          session = getSession(session.id)!;
        }
      } catch {
        // If source listing fails, let the agent handle it via tool calls
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
