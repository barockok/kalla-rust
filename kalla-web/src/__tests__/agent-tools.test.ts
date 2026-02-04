import { executeTool } from '@/lib/agent-tools';
import type { ChatSession } from '@/lib/chat-types';

// Mock fetch globally
const mockFetch = jest.fn();
global.fetch = mockFetch;

function makeSession(overrides: Partial<ChatSession> = {}): ChatSession {
  return {
    id: 'test-session',
    status: 'active',
    phase: 'scoping',
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

describe('agent-tools', () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  test('load_scoped calls /api/sources/:alias/load-scoped with POST', async () => {
    const responseData = {
      alias: 'invoices',
      columns: [{ name: 'id', data_type: 'Int64', nullable: false }],
      rows: [['1']],
      total_rows: 1,
      preview_rows: 1,
    };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => responseData,
    });

    const result = await executeTool(
      'load_scoped',
      {
        alias: 'invoices',
        conditions: [{ column: 'date', op: 'gte', value: '2024-01-01' }],
        limit: 100,
      },
      makeSession(),
    );

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toContain('/api/sources/invoices/load-scoped');
    expect(options.method).toBe('POST');
    const body = JSON.parse(options.body);
    expect(body.conditions).toHaveLength(1);
    expect(body.conditions[0].op).toBe('gte');
    expect(body.limit).toBe(100);
    expect(result).toEqual(responseData);
  });

  test('executeTool throws for unknown tool', async () => {
    await expect(
      executeTool('nonexistent_tool', {}, makeSession()),
    ).rejects.toThrow('Unknown tool: nonexistent_tool');
  });

  test('load_sample is no longer a valid tool', async () => {
    await expect(
      executeTool('load_sample', { alias: 'test' }, makeSession()),
    ).rejects.toThrow('Unknown tool: load_sample');
  });
});
