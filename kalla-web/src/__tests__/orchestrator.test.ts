import type { ChatSession, ChatPhase, PhaseConfig } from '@/lib/chat-types';
import { PHASES, PHASE_ORDER } from '@/lib/chat-types';

// We test the orchestrator helpers in isolation.
// The full runAgent function requires mocking Anthropic SDK.

// Import the helpers we'll extract
import {
  checkPrerequisites,
  buildContextInjections,
  getPhaseTools,
} from '@/lib/agent';

function makeSession(overrides: Partial<ChatSession> = {}): ChatSession {
  return {
    id: 'test-session',
    status: 'active',
    phase: 'greeting',
    left_source_alias: null,
    right_source_alias: null,
    recipe_draft: null,
    sample_left: null,
    sample_right: null,
    confirmed_pairs: [],
    messages: [],
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    sources_list: null,
    schema_left: null,
    schema_right: null,
    scope_left: null,
    scope_right: null,
    validation_approved: false,
    ...overrides,
  };
}

describe('orchestrator helpers', () => {
  describe('checkPrerequisites', () => {
    test('greeting phase has no prerequisites -- always passes', () => {
      const session = makeSession();
      expect(() => checkPrerequisites(PHASES.greeting, session)).not.toThrow();
    });

    test('intent phase requires sources_list -- throws when null', () => {
      const session = makeSession({ phase: 'intent' });
      expect(() => checkPrerequisites(PHASES.intent, session)).toThrow(
        /sources_list/,
      );
    });

    test('intent phase passes when sources_list is populated', () => {
      const session = makeSession({
        phase: 'intent',
        sources_list: [{ alias: 'a', uri: '', source_type: 'csv', status: 'ok' }],
      });
      expect(() => checkPrerequisites(PHASES.intent, session)).not.toThrow();
    });

    test('scoping phase requires schema_left and schema_right', () => {
      const session = makeSession({ phase: 'scoping' });
      expect(() => checkPrerequisites(PHASES.scoping, session)).toThrow();
    });

    test('treats empty arrays as missing prerequisites', () => {
      const session = makeSession({
        phase: 'demonstration',
        sample_left: [],
        sample_right: [],
      });
      expect(() => checkPrerequisites(PHASES.demonstration, session)).toThrow(
        /sample_left/,
      );
    });
  });

  describe('buildContextInjections', () => {
    test('returns empty string for greeting (no injections)', () => {
      const session = makeSession();
      const result = buildContextInjections(PHASES.greeting, session);
      expect(result).toBe('');
    });

    test('injects sources_list for intent phase', () => {
      const session = makeSession({
        sources_list: [
          { alias: 'invoices', uri: 'file://inv.csv', source_type: 'csv', status: 'connected' },
        ],
      });
      const result = buildContextInjections(PHASES.intent, session);
      expect(result).toContain('invoices');
      expect(result).toContain('AVAILABLE SOURCES');
    });

    test('injects schemas for scoping phase', () => {
      const session = makeSession({
        schema_left: [{ name: 'id', data_type: 'Int64', nullable: false }],
        schema_right: [{ name: 'ref', data_type: 'Utf8', nullable: true }],
      });
      const result = buildContextInjections(PHASES.scoping, session);
      expect(result).toContain('LEFT SOURCE SCHEMA');
      expect(result).toContain('RIGHT SOURCE SCHEMA');
      expect(result).toContain('id');
      expect(result).toContain('ref');
    });

    test('truncates sample data to 20 rows', () => {
      const rows = Array.from({ length: 25 }, (_, i) => ({ id: String(i) }));
      const session = makeSession({ sample_left: rows, sample_right: rows });
      const result = buildContextInjections(PHASES.demonstration, session);
      expect(result).toContain('Showing 20 of 25 rows');
    });
  });

  describe('getPhaseTools', () => {
    test('returns only tools defined for the phase', () => {
      const tools = getPhaseTools('greeting');
      expect(tools).toHaveLength(1);
      expect(tools[0].name).toBe('list_sources');
    });

    test('excludes tools from exhausted retry set', () => {
      const exhausted = new Set(['list_sources']);
      const tools = getPhaseTools('greeting', exhausted);
      expect(tools).toHaveLength(0);
    });

    test('scoping phase includes load_scoped', () => {
      const tools = getPhaseTools('scoping');
      const names = tools.map((t) => t.name);
      expect(names).toContain('load_scoped');
      expect(names).not.toContain('load_sample');
    });
  });
});
