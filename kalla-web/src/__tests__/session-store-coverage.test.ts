// ---------------------------------------------------------------------------
// Additional session-store tests targeting uncovered lines:
//   - addMessage (lines 144-151)
//   - deleteSession (lines 155-156)
//   - persistSession PostgreSQL path (lines 25-66, 69)
// ---------------------------------------------------------------------------

let counter = 0;
jest.mock('uuid', () => ({
  v4: () => `coverage-uuid-${++counter}`,
}));

// Mock the pg module so dynamic `import('pg')` resolves to our fake Pool.
const mockQuery = jest.fn().mockResolvedValue({});
const mockEnd = jest.fn().mockResolvedValue(undefined);
jest.mock('pg', () => ({
  Pool: jest.fn().mockImplementation(() => ({
    query: mockQuery,
    end: mockEnd,
  })),
}));

import {
  createSession,
  getSession,
  addMessage,
  deleteSession,
} from '@/lib/session-store';
import type { ChatMessage } from '@/lib/chat-types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeMessage(overrides?: Partial<ChatMessage>): ChatMessage {
  return {
    role: 'user',
    segments: [{ type: 'text', content: 'hello' }],
    timestamp: new Date().toISOString(),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('session-store coverage', () => {
  const savedDatabaseUrl = process.env.DATABASE_URL;

  afterEach(() => {
    // Restore DATABASE_URL to its original value (or remove it).
    if (savedDatabaseUrl === undefined) {
      delete process.env.DATABASE_URL;
    } else {
      process.env.DATABASE_URL = savedDatabaseUrl;
    }
    mockQuery.mockClear();
    mockEnd.mockClear();
  });

  // -----------------------------------------------------------------------
  // addMessage
  // -----------------------------------------------------------------------

  describe('addMessage', () => {
    it('appends a message and returns the updated session', () => {
      const session = createSession();
      const msg = makeMessage();

      const result = addMessage(session.id, msg);

      expect(result).toBeDefined();
      expect(result!.messages).toContainEqual(msg);
      expect(result!.messages.length).toBe(1);
      // updated_at should have been refreshed
      expect(result!.updated_at).toBeDefined();
    });

    it('returns undefined for a non-existent session id', () => {
      const result = addMessage('does-not-exist', makeMessage());
      expect(result).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // deleteSession
  // -----------------------------------------------------------------------

  describe('deleteSession', () => {
    it('removes a session and getSession returns undefined afterwards', () => {
      const session = createSession();
      expect(getSession(session.id)).toBeDefined();

      const deleted = deleteSession(session.id);
      expect(deleted).toBe(true);
      expect(getSession(session.id)).toBeUndefined();
    });

    it('returns false when deleting a non-existent session', () => {
      const result = deleteSession('non-existent-id');
      expect(result).toBe(false);
    });
  });

  // -----------------------------------------------------------------------
  // persistSession – PostgreSQL path
  // -----------------------------------------------------------------------

  describe('persistence with DATABASE_URL', () => {
    it('calls pool.query and pool.end when DATABASE_URL is set', async () => {
      process.env.DATABASE_URL = 'postgres://test:test@localhost:5432/testdb';

      const session = createSession();

      // persistSession is fire-and-forget, give it a tick to resolve.
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockQuery).toHaveBeenCalled();
      // The first argument to query should be the SQL string
      const callArgs = mockQuery.mock.calls[0];
      expect(callArgs[0]).toContain('INSERT INTO chat_sessions');
      // The params array should include the session id as the first element
      expect(callArgs[1][0]).toBe(session.id);

      expect(mockEnd).toHaveBeenCalled();
    });

    it('persists when addMessage triggers persistSession', async () => {
      process.env.DATABASE_URL = 'postgres://test:test@localhost:5432/testdb';

      const session = createSession();

      // Clear mocks from createSession's persist call
      await new Promise((resolve) => setTimeout(resolve, 50));
      mockQuery.mockClear();
      mockEnd.mockClear();

      const msg = makeMessage({ role: 'agent' });
      addMessage(session.id, msg);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockQuery).toHaveBeenCalled();
      // The messages param (index 15 in the params array) should contain the message
      const params = mockQuery.mock.calls[0][1];
      const messagesJson = params[15];
      expect(JSON.parse(messagesJson)).toContainEqual(msg);

      expect(mockEnd).toHaveBeenCalled();
    });
  });

  // -----------------------------------------------------------------------
  // persistSession – error handling
  // -----------------------------------------------------------------------

  describe('persistence error handling', () => {
    it('catches and logs errors when pool.query throws', async () => {
      process.env.DATABASE_URL = 'postgres://test:test@localhost:5432/testdb';

      const dbError = new Error('connection refused');
      mockQuery.mockRejectedValueOnce(dbError);

      const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

      createSession();

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(consoleSpy).toHaveBeenCalledWith(
        'Failed to persist session:',
        dbError,
      );

      consoleSpy.mockRestore();
    });
  });
});
