import { v4 as uuidv4 } from 'uuid';
import type { ChatSession, ChatMessage } from './chat-types';

// ---------------------------------------------------------------------------
// In-memory session store with optional PostgreSQL persistence.
//
// Sufficient for single-instance deployment. Every mutation is fire-and-forget
// persisted to PostgreSQL (when DATABASE_URL is configured) so sessions survive
// server restarts.
// ---------------------------------------------------------------------------

const sessions = new Map<string, ChatSession>();

// ---------------------------------------------------------------------------
// PostgreSQL persistence (optional, fire-and-forget)
// ---------------------------------------------------------------------------

async function persistSession(session: ChatSession): Promise<void> {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) return;

  try {
    // Dynamic import keeps `pg` out of client bundles.
    const { Pool } = await import('pg');
    const pool = new Pool({ connectionString: dbUrl });

    await pool.query(
      `INSERT INTO chat_sessions (
         id, status, phase,
         left_source_alias, right_source_alias,
         recipe_draft,
         sample_left, sample_right,
         sample_criteria_left, sample_criteria_right,
         confirmed_pairs, messages, updated_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, NOW())
       ON CONFLICT (id) DO UPDATE SET
         status = $2, phase = $3,
         left_source_alias = $4, right_source_alias = $5,
         recipe_draft = $6,
         sample_left = $7, sample_right = $8,
         sample_criteria_left = $9, sample_criteria_right = $10,
         confirmed_pairs = $11, messages = $12,
         updated_at = NOW()`,
      [
        session.id,
        session.status,
        session.phase,
        session.left_source_alias,
        session.right_source_alias,
        session.recipe_draft ? JSON.stringify(session.recipe_draft) : null,
        session.sample_left ? JSON.stringify(session.sample_left) : null,
        session.sample_right ? JSON.stringify(session.sample_right) : null,
        session.sample_criteria_left,
        session.sample_criteria_right,
        JSON.stringify(session.confirmed_pairs),
        JSON.stringify(session.messages),
      ],
    );

    await pool.end();
  } catch (err) {
    // Persistence failures are non-fatal — the in-memory store is authoritative.
    console.error('Failed to persist session:', err);
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/** Create a brand-new session with sensible defaults. */
export function createSession(): ChatSession {
  const now = new Date().toISOString();
  const session: ChatSession = {
    id: uuidv4(),
    status: 'active',
    phase: 'greeting',
    left_source_alias: null,
    right_source_alias: null,
    recipe_draft: null,
    sample_left: null,
    sample_right: null,
    sample_criteria_left: null,
    sample_criteria_right: null,
    confirmed_pairs: [],
    messages: [],
    created_at: now,
    updated_at: now,
  };

  sessions.set(session.id, session);
  // Fire-and-forget — do not await.
  persistSession(session);
  return session;
}

/** Retrieve a session by ID (in-memory only). */
export function getSession(id: string): ChatSession | undefined {
  return sessions.get(id);
}

/** Apply a partial update to a session and persist. */
export function updateSession(
  id: string,
  updates: Partial<
    Pick<
      ChatSession,
      | 'status'
      | 'phase'
      | 'left_source_alias'
      | 'right_source_alias'
      | 'recipe_draft'
      | 'sample_left'
      | 'sample_right'
      | 'sample_criteria_left'
      | 'sample_criteria_right'
      | 'confirmed_pairs'
    >
  >,
): ChatSession | undefined {
  const session = sessions.get(id);
  if (!session) return undefined;

  Object.assign(session, updates, { updated_at: new Date().toISOString() });
  persistSession(session);
  return session;
}

/** Append a message to the session's conversation history and persist. */
export function addMessage(id: string, message: ChatMessage): ChatSession | undefined {
  const session = sessions.get(id);
  if (!session) return undefined;

  session.messages.push(message);
  session.updated_at = new Date().toISOString();
  persistSession(session);
  return session;
}

/** Remove a session from the in-memory store. */
export function deleteSession(id: string): boolean {
  return sessions.delete(id);
}
