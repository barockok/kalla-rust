import Anthropic from '@anthropic-ai/sdk';
import type { ChatSession, ChatPhase, PhaseConfig } from '@/lib/chat-types';
import { PHASES, PHASE_ORDER } from '@/lib/chat-types';
import {
  checkPrerequisites,
  buildContextInjections,
  getPhaseTools,
  runAgent,
} from '@/lib/agent';

// Mock Anthropic SDK
jest.mock('@anthropic-ai/sdk', () => ({
  __esModule: true,
  default: jest.fn(),
}));

// Mock fetch globally
const mockFetch = jest.fn();
global.fetch = mockFetch as unknown as typeof fetch;

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

// ---------------------------------------------------------------------------
// Helper unit tests (no SDK mock needed)
// ---------------------------------------------------------------------------

beforeEach(() => {
  mockFetch.mockReset();
});

describe('orchestrator helpers', () => {
  describe('checkPrerequisites', () => {
    test('greeting phase has no prerequisites -- always passes', () => {
      const session = makeSession();
      expect(() => checkPrerequisites(PHASES.greeting, session)).not.toThrow();
    });

    test('intent phase has no prerequisites (relaxed for file uploads)', () => {
      const session = makeSession({ phase: 'intent' });
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
      expect(tools).toHaveLength(3);
      const names = tools.map(t => t.name);
      expect(names).toContain('list_sources');
      expect(names).toContain('get_source_preview');
      expect(names).toContain('request_file_upload');
    });

    test('excludes tools from exhausted retry set', () => {
      const exhausted = new Set(['list_sources', 'get_source_preview', 'request_file_upload']);
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

// ---------------------------------------------------------------------------
// Integration tests for runAgent (mocked Anthropic SDK + fetch)
// ---------------------------------------------------------------------------

describe('runAgent integration', () => {
  let mockCreate: jest.Mock;

  beforeEach(() => {
    mockCreate = jest.fn();
    (Anthropic as unknown as jest.Mock).mockImplementation(() => ({
      messages: { create: mockCreate },
    }));
    mockFetch.mockReset();
    mockCreate.mockReset();
    process.env.ANTHROPIC_API_KEY = 'test-key';
  });

  afterEach(() => {
    delete process.env.ANTHROPIC_API_KEY;
  });

  // --- Greeting Phase ---

  test('greeting: list_sources populates sources and advances to intent', async () => {
    mockCreate
      .mockResolvedValueOnce({
        content: [
          { type: 'tool_use', id: 'tu-1', name: 'list_sources', input: {} },
        ],
      })
      .mockResolvedValueOnce({
        content: [
          { type: 'text', text: 'Welcome! You have 2 sources available.' },
        ],
      });

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => [
        { alias: 'invoices', uri: 'file://inv.csv', source_type: 'csv', status: 'connected' },
        { alias: 'payments', uri: 'file://pay.csv', source_type: 'csv', status: 'connected' },
      ],
    });

    const session = makeSession();
    const result = await runAgent(session, 'Hello');

    expect(result.sessionUpdates?.sources_list).toHaveLength(2);
    expect(result.phaseTransition).toBe('intent');
    expect(result.segments).toContainEqual(
      expect.objectContaining({ type: 'text', content: expect.stringContaining('Welcome') }),
    );
  });

  // --- Intent Phase ---

  test('intent: get_source_preview x2 sets schemas, aliases, and advances to scoping', async () => {
    mockCreate
      .mockResolvedValueOnce({
        content: [
          { type: 'tool_use', id: 'tu-1', name: 'get_source_preview', input: { alias: 'invoices' } },
          { type: 'tool_use', id: 'tu-2', name: 'get_source_preview', input: { alias: 'payments' } },
        ],
      })
      .mockResolvedValueOnce({
        content: [
          { type: 'text', text: 'Both sources loaded. What data range do you want?' },
        ],
      });

    const leftPreview = {
      alias: 'invoices',
      columns: [
        { name: 'id', data_type: 'Int64', nullable: false },
        { name: 'amount', data_type: 'Float64', nullable: false },
      ],
      rows: [['1', '100.00']],
      total_rows: 100,
      preview_rows: 1,
    };
    const rightPreview = {
      alias: 'payments',
      columns: [
        { name: 'pay_id', data_type: 'Int64', nullable: false },
        { name: 'total', data_type: 'Float64', nullable: false },
      ],
      rows: [['1', '100.00']],
      total_rows: 50,
      preview_rows: 1,
    };
    mockFetch
      .mockResolvedValueOnce({ ok: true, json: async () => leftPreview })
      .mockResolvedValueOnce({ ok: true, json: async () => rightPreview });

    const session = makeSession({
      phase: 'intent',
      sources_list: [
        { alias: 'invoices', uri: 'file://inv.csv', source_type: 'csv', status: 'connected' },
        { alias: 'payments', uri: 'file://pay.csv', source_type: 'csv', status: 'connected' },
      ],
    });
    const result = await runAgent(session, 'Reconcile invoices with payments');

    expect(result.sessionUpdates?.schema_left).toEqual(leftPreview.columns);
    expect(result.sessionUpdates?.schema_right).toEqual(rightPreview.columns);
    expect(result.sessionUpdates?.left_source_alias).toBe('invoices');
    expect(result.sessionUpdates?.right_source_alias).toBe('payments');
    expect(result.phaseTransition).toBe('scoping');
  });

  // --- Scoping Phase ---

  test('scoping: load_scoped x2 populates samples and advances to demonstration', async () => {
    mockCreate
      .mockResolvedValueOnce({
        content: [
          { type: 'tool_use', id: 'tu-1', name: 'load_scoped', input: {
            alias: 'invoices',
            conditions: [{ column: 'date', op: 'gte', value: '2024-01-01' }],
          }},
          { type: 'tool_use', id: 'tu-2', name: 'load_scoped', input: {
            alias: 'payments',
            conditions: [{ column: 'date', op: 'gte', value: '2024-01-01' }],
          }},
        ],
      })
      .mockResolvedValueOnce({
        content: [
          { type: 'text', text: 'Data loaded. Let me propose some matches.' },
        ],
      });

    mockFetch
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          alias: 'invoices',
          columns: [
            { name: 'id', data_type: 'Int64', nullable: false },
            { name: 'amount', data_type: 'Float64', nullable: false },
          ],
          rows: [['1', '100.00'], ['2', '200.00']],
          total_rows: 2,
          preview_rows: 2,
        }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          alias: 'payments',
          columns: [
            { name: 'pay_id', data_type: 'Int64', nullable: false },
            { name: 'total', data_type: 'Float64', nullable: false },
          ],
          rows: [['1', '100.00'], ['2', '200.00']],
          total_rows: 2,
          preview_rows: 2,
        }),
      });

    const session = makeSession({
      phase: 'scoping',
      left_source_alias: 'invoices',
      right_source_alias: 'payments',
      schema_left: [
        { name: 'id', data_type: 'Int64', nullable: false },
        { name: 'amount', data_type: 'Float64', nullable: false },
      ],
      schema_right: [
        { name: 'pay_id', data_type: 'Int64', nullable: false },
        { name: 'total', data_type: 'Float64', nullable: false },
      ],
    });
    const result = await runAgent(session, 'Load January 2024 data');

    expect(result.sessionUpdates?.sample_left).toHaveLength(2);
    expect(result.sessionUpdates?.sample_right).toHaveLength(2);
    expect(result.sessionUpdates?.scope_left).toHaveLength(1);
    expect(result.sessionUpdates?.scope_right).toHaveLength(1);
    expect(result.phaseTransition).toBe('demonstration');
  });

  // --- Demonstration Phase ---

  test('demonstration: propose_match emits match_proposal card', async () => {
    mockCreate
      .mockResolvedValueOnce({
        content: [
          { type: 'text', text: 'I found a potential match.' },
          { type: 'tool_use', id: 'tu-1', name: 'propose_match', input: {
            left_row: { id: '1', amount: '100' },
            right_row: { pay_id: '1', total: '100' },
            reasoning: 'Same amount and date',
          }},
        ],
      })
      .mockResolvedValueOnce({
        content: [
          { type: 'text', text: 'Please confirm or reject this match.' },
        ],
      });

    const session = makeSession({
      phase: 'demonstration',
      sample_left: [{ id: '1', amount: '100' }],
      sample_right: [{ pay_id: '1', total: '100' }],
      schema_left: [{ name: 'id', data_type: 'Int64', nullable: false }],
      schema_right: [{ name: 'pay_id', data_type: 'Int64', nullable: false }],
    });
    const result = await runAgent(session, 'Start matching');

    const cards = result.segments.filter((s) => s.type === 'card');
    expect(cards).toHaveLength(1);
    expect(cards[0].card_type).toBe('match_proposal');
    expect(cards[0].data).toHaveProperty('left');
    expect(cards[0].data).toHaveProperty('right');
    expect(cards[0].data).toHaveProperty('reasoning');
  });

  // --- Inference Phase ---

  test('inference: infer_rules + build_recipe advances to validation', async () => {
    mockCreate
      .mockResolvedValueOnce({
        content: [
          { type: 'tool_use', id: 'tu-1', name: 'infer_rules', input: {
            left_columns: ['id', 'amount'],
            right_columns: ['pay_id', 'total'],
          }},
        ],
      })
      .mockResolvedValueOnce({
        content: [
          { type: 'text', text: 'I found matching rules. Building recipe.' },
          { type: 'tool_use', id: 'tu-2', name: 'build_recipe', input: {
            name: 'Invoice-Payment Match',
            description: 'Match invoices to payments by amount',
            match_sql: 'SELECT * FROM left_src JOIN right_src ON left_src.amount = right_src.total',
            match_description: 'Join on amount = total',
            left_alias: 'invoices',
            right_alias: 'payments',
            left_uri: 'file://inv.csv',
            right_uri: 'file://pay.csv',
            left_pk: ['id'],
            right_pk: ['pay_id'],
            left_schema: ['id', 'amount'],
            right_schema: ['pay_id', 'total'],
          }},
        ],
      })
      .mockResolvedValueOnce({
        content: [
          { type: 'text', text: 'Recipe built. Ready for validation.' },
        ],
      });

    const session = makeSession({
      phase: 'inference',
      confirmed_pairs: [
        { left: { id: '1', amount: '100' }, right: { pay_id: '1', total: '100' } },
        { left: { id: '2', amount: '200' }, right: { pay_id: '2', total: '200' } },
        { left: { id: '3', amount: '300' }, right: { pay_id: '3', total: '300' } },
      ],
      schema_left: [
        { name: 'id', data_type: 'Int64', nullable: false },
        { name: 'amount', data_type: 'Float64', nullable: false },
      ],
      schema_right: [
        { name: 'pay_id', data_type: 'Int64', nullable: false },
        { name: 'total', data_type: 'Float64', nullable: false },
      ],
    });
    const result = await runAgent(session, 'Build the matching rules');

    expect(result.sessionUpdates?.recipe_draft).toBeDefined();
    expect(result.sessionUpdates?.recipe_draft).toHaveProperty('match_sql');
    expect(result.phaseTransition).toBe('validation');
  });

  // --- Validation Phase ---

  test('validation: run_sample emits result_summary card', async () => {
    mockCreate
      .mockResolvedValueOnce({
        content: [
          { type: 'tool_use', id: 'tu-1', name: 'validate_recipe', input: {
            recipe: { recipe_id: 'recipe-1', name: 'test', match_sql: 'SELECT 1', sources: {} },
          }},
        ],
      })
      .mockResolvedValueOnce({
        content: [
          { type: 'text', text: 'Recipe is valid. Running sample...' },
          { type: 'tool_use', id: 'tu-2', name: 'run_sample', input: { recipe_id: 'recipe-1' } },
        ],
      })
      .mockResolvedValueOnce({
        content: [
          { type: 'text', text: 'Here are the results. Please review.' },
        ],
      });

    // run_sample: POST /api/runs (validate_recipe is local, no fetch needed)
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ run_id: 'run-1', status: 'submitted' }),
    });
    // run_sample: poll GET /api/runs/run-1
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ run_id: 'run-1', status: 'Completed', matched_count: 5, unmatched_left_count: 1, unmatched_right_count: 1 }),
    });

    const session = makeSession({
      phase: 'validation',
      recipe_draft: { recipe_id: 'recipe-1', name: 'test', match_sql: 'SELECT 1', sources: {} },
      schema_left: [{ name: 'id', data_type: 'Int64', nullable: false }],
      schema_right: [{ name: 'pay_id', data_type: 'Int64', nullable: false }],
    });
    const result = await runAgent(session, 'Validate and run');

    const cards = result.segments.filter((s) => s.type === 'card');
    expect(cards.some((c) => c.card_type === 'result_summary')).toBe(true);
  });

  // --- Execution Phase ---

  test('execution: run_full sets status to running', async () => {
    mockCreate
      .mockResolvedValueOnce({
        content: [
          { type: 'tool_use', id: 'tu-1', name: 'run_full', input: { recipe_id: 'recipe-1' } },
        ],
      })
      .mockResolvedValueOnce({
        content: [
          { type: 'text', text: 'Running on full data. You will be notified when done.' },
        ],
      });

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ run_id: 'run-full-1', status: 'Running' }),
    });

    const session = makeSession({
      phase: 'execution',
      recipe_draft: { recipe_id: 'recipe-1', match_sql: 'SELECT 1' },
      validation_approved: true,
    });
    const result = await runAgent(session, 'Run on all data');

    expect(result.sessionUpdates?.status).toBe('running');
  });

  // --- Mid-turn Phase Advancement ---

  test('mid-turn: greeting -> intent -> scoping in one turn', async () => {
    // Turn 1: Claude calls list_sources
    mockCreate.mockResolvedValueOnce({
      content: [
        { type: 'tool_use', id: 'tu-1', name: 'list_sources', input: {} },
      ],
    });
    // After list_sources succeeds and phase advances to intent,
    // Claude now has get_source_preview available and uses it
    mockCreate.mockResolvedValueOnce({
      content: [
        { type: 'text', text: 'Found your sources. Let me check the schemas.' },
        { type: 'tool_use', id: 'tu-2', name: 'get_source_preview', input: { alias: 'invoices' } },
        { type: 'tool_use', id: 'tu-3', name: 'get_source_preview', input: { alias: 'payments' } },
      ],
    });
    // After both previews and phase advances to scoping, Claude responds with text
    mockCreate.mockResolvedValueOnce({
      content: [
        { type: 'text', text: 'Both sources are loaded. What time range do you want to reconcile?' },
      ],
    });

    // Backend: list_sources
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => [
        { alias: 'invoices', uri: 'file://inv.csv', source_type: 'csv', status: 'connected' },
        { alias: 'payments', uri: 'file://pay.csv', source_type: 'csv', status: 'connected' },
      ],
    });
    // Backend: get_source_preview invoices
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        alias: 'invoices',
        columns: [{ name: 'id', data_type: 'Int64', nullable: false }],
        rows: [['1']],
        total_rows: 100,
        preview_rows: 1,
      }),
    });
    // Backend: get_source_preview payments
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        alias: 'payments',
        columns: [{ name: 'pay_id', data_type: 'Int64', nullable: false }],
        rows: [['1']],
        total_rows: 50,
        preview_rows: 1,
      }),
    });

    const session = makeSession({ phase: 'greeting' });
    const result = await runAgent(session, 'Reconcile invoices with payments');

    // Should have advanced through greeting -> intent -> scoping
    expect(result.phaseTransition).toBe('scoping');
    expect(result.sessionUpdates?.sources_list).toHaveLength(2);
    expect(result.sessionUpdates?.schema_left).toBeDefined();
    expect(result.sessionUpdates?.schema_right).toBeDefined();
    expect(result.sessionUpdates?.left_source_alias).toBe('invoices');
    expect(result.sessionUpdates?.right_source_alias).toBe('payments');

    // Verify Claude was called 3 times (once per tool_use cycle + final text)
    expect(mockCreate).toHaveBeenCalledTimes(3);

    // Verify the second call has intent-phase tools (including get_source_preview)
    const secondCallArgs = mockCreate.mock.calls[1][0];
    const toolNames = secondCallArgs.tools.map((t: { name: string }) => t.name);
    expect(toolNames).toContain('get_source_preview');
  });

  // --- Error Recovery ---

  test('error recovery: tool failures are tracked and tool is removed after max retries', async () => {
    // Claude tries list_sources
    mockCreate.mockResolvedValueOnce({
      content: [
        { type: 'tool_use', id: 'tu-1', name: 'list_sources', input: {} },
      ],
    });
    // Claude retries list_sources
    mockCreate.mockResolvedValueOnce({
      content: [
        { type: 'tool_use', id: 'tu-2', name: 'list_sources', input: {} },
      ],
    });
    // After exhaustion (no tools left), Claude returns text
    mockCreate.mockResolvedValueOnce({
      content: [
        { type: 'text', text: 'I could not reach the data sources. Please check the backend.' },
      ],
    });

    // Backend fails both times
    mockFetch
      .mockResolvedValueOnce({ ok: false, statusText: 'Connection refused' })
      .mockResolvedValueOnce({ ok: false, statusText: 'Connection refused' });

    const session = makeSession();
    const result = await runAgent(session, 'Hello');

    // Phase should NOT advance
    expect(result.phaseTransition).toBeUndefined();
    expect(result.sessionUpdates?.sources_list).toBeUndefined();

    // Claude was called 3 times: 2 tool_use cycles + 1 final text
    expect(mockCreate).toHaveBeenCalledTimes(3);

    // The third call should have list_sources exhausted (2 remaining: get_source_preview, request_file_upload)
    const thirdCallArgs = mockCreate.mock.calls[2][0];
    expect(thirdCallArgs.tools).toHaveLength(2);
    expect(thirdCallArgs.tools.map((t: { name: string }) => t.name)).not.toContain('list_sources');
  });

  // --- Context Injections per Phase ---

  test('system prompt includes correct context injections for each phase', async () => {
    // Demonstration phase should inject schemas, samples, and confirmed pairs
    mockCreate.mockResolvedValueOnce({
      content: [{ type: 'text', text: 'Looking at the data...' }],
    });

    const session = makeSession({
      phase: 'demonstration',
      sample_left: [{ id: '1', amount: '100' }],
      sample_right: [{ pay_id: '1', total: '100' }],
      schema_left: [{ name: 'id', data_type: 'Int64', nullable: false }],
      schema_right: [{ name: 'pay_id', data_type: 'Int64', nullable: false }],
      confirmed_pairs: [
        { left: { id: '1', amount: '100' }, right: { pay_id: '1', total: '100' } },
      ],
    });
    await runAgent(session, 'Match these');

    const callArgs = mockCreate.mock.calls[0][0];
    const systemPrompt = callArgs.system;
    expect(systemPrompt).toContain('LEFT SOURCE SCHEMA');
    expect(systemPrompt).toContain('RIGHT SOURCE SCHEMA');
    expect(systemPrompt).toContain('LEFT SOURCE DATA');
    expect(systemPrompt).toContain('RIGHT SOURCE DATA');
    expect(systemPrompt).toContain('CONFIRMED MATCH PAIRS');
    expect(systemPrompt).toContain('CURRENT PHASE: demonstration');
  });

  test('greeting phase system prompt has no context injections', async () => {
    mockCreate.mockResolvedValueOnce({
      content: [{ type: 'text', text: 'Hello!' }],
    });

    const session = makeSession();
    await runAgent(session, 'Hi');

    const callArgs = mockCreate.mock.calls[0][0];
    const systemPrompt = callArgs.system;
    expect(systemPrompt).toContain('CURRENT PHASE: greeting');
    expect(systemPrompt).not.toContain('LEFT SOURCE SCHEMA');
    expect(systemPrompt).not.toContain('AVAILABLE SOURCES');
  });

  test('validation phase injects recipe_draft and schemas', async () => {
    mockCreate.mockResolvedValueOnce({
      content: [{ type: 'text', text: 'Checking the recipe.' }],
    });

    const session = makeSession({
      phase: 'validation',
      recipe_draft: { recipe_id: 'recipe-1', match_sql: 'SELECT 1', sources: {} },
      schema_left: [{ name: 'id', data_type: 'Int64', nullable: false }],
      schema_right: [{ name: 'pay_id', data_type: 'Int64', nullable: false }],
    });
    await runAgent(session, 'Validate this');

    const callArgs = mockCreate.mock.calls[0][0];
    const systemPrompt = callArgs.system;
    expect(systemPrompt).toContain('CURRENT RECIPE DRAFT');
    expect(systemPrompt).toContain('LEFT SOURCE SCHEMA');
    expect(systemPrompt).toContain('RIGHT SOURCE SCHEMA');
    expect(systemPrompt).not.toContain('LEFT SOURCE DATA');
  });

  // --- Prerequisite Failure ---

  test('runAgent throws if phase prerequisites are not met', async () => {
    const session = makeSession({
      phase: 'scoping',
      // Missing schema_left and schema_right
    });

    await expect(runAgent(session, 'Load data')).rejects.toThrow(
      /prerequisites not met/,
    );

    // Claude should never have been called
    expect(mockCreate).not.toHaveBeenCalled();
  });

  // --- Conversation History ---

  test('conversation history includes prior messages as text', async () => {
    mockCreate.mockResolvedValueOnce({
      content: [{ type: 'text', text: 'I see your sources.' }],
    });

    const session = makeSession({
      phase: 'intent',
      sources_list: [
        { alias: 'invoices', uri: 'file://inv.csv', source_type: 'csv', status: 'connected' },
      ],
      messages: [
        {
          role: 'user',
          segments: [{ type: 'text', content: 'Hello' }],
          timestamp: '2024-01-01T00:00:00Z',
        },
        {
          role: 'agent',
          segments: [{ type: 'text', content: 'Welcome to Kalla!' }],
          timestamp: '2024-01-01T00:00:01Z',
        },
      ],
    });
    await runAgent(session, 'Reconcile invoices');

    const callArgs = mockCreate.mock.calls[0][0];
    const messages = callArgs.messages;
    // Prior messages + current message
    expect(messages).toHaveLength(3);
    expect(messages[0]).toEqual({ role: 'user', content: 'Hello' });
    expect(messages[1]).toEqual({ role: 'assistant', content: 'Welcome to Kalla!' });
    expect(messages[2]).toEqual({ role: 'user', content: 'Reconcile invoices' });
  });

  // --- API Error Handling ---

  test('Anthropic API error is caught and returned as text segment', async () => {
    mockCreate.mockRejectedValueOnce(new Error('Rate limit exceeded'));

    const session = makeSession();
    const result = await runAgent(session, 'Hello');

    expect(result.segments).toHaveLength(1);
    expect(result.segments[0].type).toBe('text');
    expect(result.segments[0].content).toContain('Rate limit exceeded');
  });
});
