import {
  listSources,
  getSourcePreview,
  loadScoped,
  proposeMatch,
  inferRules,
  buildRecipe,
  validateRecipe,
  runSample,
  runFull,
  pollRunStatus,
  executeTool,
} from '@/lib/agent-tools';
import type { ChatSession } from '@/lib/chat-types';

// ---------------------------------------------------------------------------
// Global fetch mock
// ---------------------------------------------------------------------------
const mockFetch = jest.fn();
global.fetch = mockFetch;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------
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

function okJson(data: unknown) {
  return { ok: true, json: async () => data, statusText: 'OK' };
}

function failRes(statusText = 'Internal Server Error') {
  return { ok: false, statusText, json: async () => ({}) };
}

// ===========================================================================
// Tests
// ===========================================================================

describe('agent-tools coverage', () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  // -------------------------------------------------------------------------
  // 1. listSources
  // -------------------------------------------------------------------------
  describe('listSources', () => {
    test('success – returns array of sources', async () => {
      const data = [{ alias: 'inv', uri: 'file://inv.csv', source_type: 'csv', status: 'ready' }];
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await listSources();
      expect(result).toEqual(data);
      expect(mockFetch).toHaveBeenCalledWith(expect.stringContaining('/api/sources'));
    });

    test('error – throws on non-ok response', async () => {
      mockFetch.mockResolvedValueOnce(failRes('Not Found'));
      await expect(listSources()).rejects.toThrow('Failed to list sources: Not Found');
    });
  });

  // -------------------------------------------------------------------------
  // 2. getSourcePreview
  // -------------------------------------------------------------------------
  describe('getSourcePreview', () => {
    test('success – fetches preview with default limit', async () => {
      const data = { alias: 'inv', columns: [], rows: [], total_rows: 0, preview_rows: 0 };
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await getSourcePreview('inv');
      expect(result).toEqual(data);
      const url: string = mockFetch.mock.calls[0][0];
      expect(url).toContain('/api/sources/inv/preview?limit=10');
    });

    test('success – fetches preview with custom limit', async () => {
      const data = { alias: 'inv', columns: [], rows: [], total_rows: 5, preview_rows: 5 };
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await getSourcePreview('inv', 25);
      expect(result).toEqual(data);
      const url: string = mockFetch.mock.calls[0][0];
      expect(url).toContain('limit=25');
    });

    test('error – throws on non-ok response', async () => {
      mockFetch.mockResolvedValueOnce(failRes('Bad Request'));
      await expect(getSourcePreview('bad')).rejects.toThrow('Failed to get preview for bad: Bad Request');
    });
  });

  // -------------------------------------------------------------------------
  // 3. loadScoped – error case (success already tested in existing suite)
  // -------------------------------------------------------------------------
  describe('loadScoped', () => {
    test('error – throws on non-ok response', async () => {
      mockFetch.mockResolvedValueOnce(failRes('Bad Gateway'));
      await expect(
        loadScoped('inv', [{ column: 'date', op: 'gte', value: '2024-01-01' }]),
      ).rejects.toThrow('Failed to load scoped data from inv: Bad Gateway');
    });
  });

  // -------------------------------------------------------------------------
  // 4. proposeMatch
  // -------------------------------------------------------------------------
  describe('proposeMatch', () => {
    test('returns correct structure', () => {
      const left = { id: '1', name: 'Alice' };
      const right = { id: '2', name: 'Alice A.' };
      const reasoning = 'Name similarity';

      const result = proposeMatch(left, right, reasoning);
      expect(result).toEqual({ left, right, reasoning });
    });
  });

  // -------------------------------------------------------------------------
  // 5. inferRules
  // -------------------------------------------------------------------------
  describe('inferRules', () => {
    test('empty confirmed pairs – returns empty array', () => {
      const result = inferRules([], ['name'], ['name']);
      expect(result).toEqual([]);
    });

    test('exact match > 70% – produces eq rule', () => {
      // 4 out of 4 pairs match exactly on "name" (100% > 70%)
      const pairs = [
        { left: { name: 'Alice' }, right: { name: 'Alice' } },
        { left: { name: 'Bob' }, right: { name: 'Bob' } },
        { left: { name: 'Carol' }, right: { name: 'Carol' } },
        { left: { name: 'Dave' }, right: { name: 'Dave' } },
      ];
      const rules = inferRules(pairs, ['name'], ['name']);
      expect(rules).toHaveLength(1);
      expect(rules[0].op).toBe('eq');
      expect(rules[0].confidence).toBe(1);
      expect(rules[0].left_col).toBe('name');
      expect(rules[0].right_col).toBe('name');
      expect(rules[0].reasoning).toContain('100%');
    });

    test('tolerance match > 50% – produces tolerance rule', () => {
      // 3 out of 4 pairs are within 5% tolerance (75% > 50%), 0 exact
      // (the 4th pair is way off so it's not matched)
      const pairs = [
        { left: { amount: '100' }, right: { amount: '101' } },   // 1% diff
        { left: { amount: '200' }, right: { amount: '205' } },   // 2.5% diff
        { left: { amount: '300' }, right: { amount: '310' } },   // 3.3% diff
        { left: { amount: '400' }, right: { amount: '500' } },   // 25% diff – outside 5%
      ];
      const rules = inferRules(pairs, ['amount'], ['amount']);
      expect(rules).toHaveLength(1);
      expect(rules[0].op).toBe('tolerance');
      expect(rules[0].confidence).toBe(0.75);
    });

    test('combined exact + tolerance > 70% – produces tolerance rule', () => {
      // 2 exact (50%) + 1 tolerance (25%) = 75% combined > 70%
      // tolerance alone is only 25%, not > 50%, so it must be the combined path
      const pairs = [
        { left: { val: '100' }, right: { val: '100' } },     // exact
        { left: { val: '200' }, right: { val: '200' } },     // exact
        { left: { val: '300' }, right: { val: '305' } },     // ~1.67% → tolerance
        { left: { val: '400' }, right: { val: '999' } },     // way off
      ];
      const rules = inferRules(pairs, ['val'], ['val']);
      expect(rules).toHaveLength(1);
      expect(rules[0].op).toBe('tolerance');
      expect(rules[0].confidence).toBe(0.75);
    });

    test('no matches – returns empty array', () => {
      const pairs = [
        { left: { name: 'Alice' }, right: { name: 'Zara' } },
        { left: { name: 'Bob' }, right: { name: 'Yuki' } },
        { left: { name: 'Carol' }, right: { name: 'Xena' } },
        { left: { name: 'Dave' }, right: { name: 'Wu' } },
      ];
      const rules = inferRules(pairs, ['name'], ['name']);
      expect(rules).toEqual([]);
    });

    test('skips pairs where left or right value is empty', () => {
      // 2 exact matches out of 4 total pairs => 50% (not > 70%)
      // The two empty-value pairs are skipped in the match count but
      // total still = 4, so confidence = 2/4 = 50%
      const pairs = [
        { left: { name: 'Alice' }, right: { name: 'Alice' } },
        { left: { name: 'Bob' }, right: { name: 'Bob' } },
        { left: { name: '' }, right: { name: 'Carol' } },     // skipped (leftVal empty)
        { left: { name: 'Dave' }, right: { name: '' } },       // skipped (rightVal empty)
      ];
      const rules = inferRules(pairs, ['name'], ['name']);
      // 2 exact / 4 total = 50%, not > 70%, so no rule
      expect(rules).toEqual([]);
    });

    test('handles null/undefined values gracefully', () => {
      const pairs = [
        { left: { col: null }, right: { col: undefined } },
        { left: { col: 'X' }, right: { col: 'X' } },
      ];
      // null → '' which is empty, so pair 0 skipped; 1 exact / 2 total = 50%
      const rules = inferRules(pairs as any, ['col'], ['col']);
      expect(rules).toEqual([]);
    });

    test('rules are sorted by confidence descending', () => {
      // Two column pairs: colA has 100% exact, colB has ~75% exact
      const pairs = [
        { left: { colA: 'a', colB: '1' }, right: { rA: 'a', rB: '1' } },
        { left: { colA: 'b', colB: '2' }, right: { rA: 'b', rB: '2' } },
        { left: { colA: 'c', colB: '3' }, right: { rA: 'c', rB: '3' } },
        { left: { colA: 'd', colB: 'x' }, right: { rA: 'd', rB: 'y' } },
      ];
      const rules = inferRules(pairs, ['colA', 'colB'], ['rA', 'rB']);
      // colA→rA: 4/4=100%, colB→rB: 3/4=75%
      expect(rules.length).toBeGreaterThanOrEqual(2);
      expect(rules[0].confidence).toBeGreaterThanOrEqual(rules[1].confidence);
    });
  });

  // -------------------------------------------------------------------------
  // 6. buildRecipe
  // -------------------------------------------------------------------------
  describe('buildRecipe', () => {
    test('returns correct structure', () => {
      const rules = [
        { name: 'rule1', pattern: 'exact', conditions: [{ left: 'id', op: 'eq', right: 'id' }] },
      ];

      const now = Date.now();
      jest.spyOn(Date, 'now').mockReturnValue(now);

      const recipe = buildRecipe('left_ds', 'right_ds', 'file://left.csv', 'file://right.csv', ['id'], ['id'], rules);

      expect(recipe.version).toBe('1.0');
      expect(recipe.recipe_id).toBe(`recipe-${now}`);
      expect(recipe.sources).toEqual({
        left: { alias: 'left_ds', uri: 'file://left.csv', primary_key: ['id'] },
        right: { alias: 'right_ds', uri: 'file://right.csv', primary_key: ['id'] },
      });
      expect(recipe.match_rules).toEqual([{ ...rules[0], priority: 1 }]);
      expect(recipe.output).toEqual({
        matched: 'evidence/matched.parquet',
        unmatched_left: 'evidence/unmatched_left.parquet',
        unmatched_right: 'evidence/unmatched_right.parquet',
      });

      (Date.now as jest.Mock).mockRestore();
    });
  });

  // -------------------------------------------------------------------------
  // 7. validateRecipe
  // -------------------------------------------------------------------------
  describe('validateRecipe', () => {
    test('success – returns validation result', async () => {
      const data = { valid: true, errors: [] };
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await validateRecipe({ version: '1.0' });
      expect(result).toEqual(data);
      const [url, opts] = mockFetch.mock.calls[0];
      expect(url).toContain('/api/recipes/validate');
      expect(opts.method).toBe('POST');
    });

    test('error – throws on non-ok response', async () => {
      mockFetch.mockResolvedValueOnce(failRes('Unprocessable Entity'));
      await expect(validateRecipe({})).rejects.toThrow('Validation request failed: Unprocessable Entity');
    });
  });

  // -------------------------------------------------------------------------
  // 8. runSample
  // -------------------------------------------------------------------------
  describe('runSample', () => {
    test('success – returns run info', async () => {
      const data = { run_id: 'run-1', status: 'Running' };
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await runSample({ version: '1.0' });
      expect(result).toEqual(data);
      const [url, opts] = mockFetch.mock.calls[0];
      expect(url).toContain('/api/runs');
      expect(opts.method).toBe('POST');
      expect(JSON.parse(opts.body)).toEqual({ recipe: { version: '1.0' } });
    });

    test('error – throws on non-ok response', async () => {
      mockFetch.mockResolvedValueOnce(failRes('Service Unavailable'));
      await expect(runSample({})).rejects.toThrow('Run creation failed: Service Unavailable');
    });
  });

  // -------------------------------------------------------------------------
  // 9. runFull – delegates to runSample
  // -------------------------------------------------------------------------
  describe('runFull', () => {
    test('delegates to runSample and returns same result', async () => {
      const data = { run_id: 'run-full-1', status: 'Running' };
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await runFull({ version: '1.0' });
      expect(result).toEqual(data);
      const [url, opts] = mockFetch.mock.calls[0];
      expect(url).toContain('/api/runs');
      expect(opts.method).toBe('POST');
    });
  });

  // -------------------------------------------------------------------------
  // 10. pollRunStatus
  // -------------------------------------------------------------------------
  describe('pollRunStatus', () => {
    beforeEach(() => {
      jest.useFakeTimers();
    });

    afterEach(() => {
      jest.useRealTimers();
    });

    test('returns immediately when status is not Running', async () => {
      const data = { run_id: 'r1', status: 'Completed', matched: 42 };
      mockFetch.mockResolvedValueOnce(okJson(data));

      const promise = pollRunStatus('r1', 5000, 500);
      const result = await promise;
      expect(result).toEqual(data);
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });

    test('polls until status transitions from Running to Completed', async () => {
      const running = { run_id: 'r2', status: 'Running' };
      const completed = { run_id: 'r2', status: 'Completed', matched: 10 };

      mockFetch
        .mockResolvedValueOnce(okJson(running))
        .mockResolvedValueOnce(okJson(completed));

      const promise = pollRunStatus('r2', 10000, 500);

      // Flush the first fetch + setTimeout
      await jest.advanceTimersByTimeAsync(500);

      const result = await promise;
      expect(result).toEqual(completed);
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    test('throws on fetch error during polling', async () => {
      mockFetch.mockResolvedValueOnce(failRes('Gateway Timeout'));

      const promise = pollRunStatus('r3', 5000, 500);
      await expect(promise).rejects.toThrow('Failed to get run status: Gateway Timeout');
    });

    test('throws timeout error when maxWaitMs exceeded', async () => {
      // Mock Date.now to control the deadline logic
      let callCount = 0;
      const dateNowSpy = jest.spyOn(Date, 'now').mockImplementation(() => {
        callCount++;
        // First call (setting deadline): returns 1000 → deadline = 1000 + 2000 = 3000
        // Second call (while check): returns 2000 → 2000 < 3000, enters loop
        // Third call (while check after sleep): returns 4000 → 4000 >= 3000, exits loop
        if (callCount === 1) return 1000;
        if (callCount === 2) return 2000;
        return 4000;
      });

      const running = { run_id: 'r4', status: 'Running' };
      mockFetch.mockResolvedValue(okJson(running));

      // Capture the promise rejection immediately so it doesn't become unhandled
      let caughtError: Error | undefined;
      const promise = pollRunStatus('r4', 2000, 500).catch((err) => {
        caughtError = err;
      });

      // Advance timer so the setTimeout in the loop resolves
      await jest.advanceTimersByTimeAsync(500);

      await promise;

      expect(caughtError).toBeDefined();
      expect(caughtError!.message).toBe('Run r4 timed out after 2000ms');

      dateNowSpy.mockRestore();
    });
  });

  // -------------------------------------------------------------------------
  // 11. executeTool – all remaining tool names
  // -------------------------------------------------------------------------
  describe('executeTool dispatches', () => {
    test('list_sources', async () => {
      const data = [{ alias: 'src1', uri: 'f://x', source_type: 'csv', status: 'ready' }];
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await executeTool('list_sources', {}, makeSession());
      expect(result).toEqual(data);
    });

    test('get_source_preview with default limit', async () => {
      const data = { alias: 'a', columns: [], rows: [], total_rows: 0, preview_rows: 0 };
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await executeTool('get_source_preview', { alias: 'a' }, makeSession());
      expect(result).toEqual(data);
      const url: string = mockFetch.mock.calls[0][0];
      expect(url).toContain('limit=10');
    });

    test('get_source_preview with custom limit', async () => {
      const data = { alias: 'a', columns: [], rows: [], total_rows: 0, preview_rows: 0 };
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await executeTool('get_source_preview', { alias: 'a', limit: 50 }, makeSession());
      expect(result).toEqual(data);
      const url: string = mockFetch.mock.calls[0][0];
      expect(url).toContain('limit=50');
    });

    test('propose_match', async () => {
      const result = await executeTool(
        'propose_match',
        { left_row: { id: '1' }, right_row: { id: '2' }, reasoning: 'test' },
        makeSession(),
      );
      expect(result).toEqual({ left: { id: '1' }, right: { id: '2' }, reasoning: 'test' });
    });

    test('infer_rules', async () => {
      const pairs = [
        { left: { name: 'A' }, right: { name: 'A' } },
        { left: { name: 'B' }, right: { name: 'B' } },
        { left: { name: 'C' }, right: { name: 'C' } },
        { left: { name: 'D' }, right: { name: 'D' } },
      ];
      const session = makeSession({ confirmed_pairs: pairs });

      const result = await executeTool(
        'infer_rules',
        { left_columns: ['name'], right_columns: ['name'] },
        session,
      );
      expect(Array.isArray(result)).toBe(true);
      expect((result as any[])[0].op).toBe('eq');
    });

    test('build_recipe', async () => {
      const rules = [{ name: 'r1', pattern: 'exact', conditions: [{ left: 'id', op: 'eq', right: 'id' }] }];

      const result = await executeTool(
        'build_recipe',
        {
          left_alias: 'L',
          right_alias: 'R',
          left_uri: 'file://l.csv',
          right_uri: 'file://r.csv',
          left_pk: ['id'],
          right_pk: ['id'],
          rules,
        },
        makeSession(),
      );
      expect((result as any).version).toBe('1.0');
      expect((result as any).sources.left.alias).toBe('L');
    });

    test('validate_recipe', async () => {
      const data = { valid: true, errors: [] };
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await executeTool('validate_recipe', { recipe: { v: 1 } }, makeSession());
      expect(result).toEqual(data);
    });

    test('run_sample', async () => {
      const data = { run_id: 'run-x', status: 'Running' };
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await executeTool('run_sample', { recipe: {} }, makeSession());
      expect(result).toEqual(data);
    });

    test('run_full', async () => {
      const data = { run_id: 'run-y', status: 'Running' };
      mockFetch.mockResolvedValueOnce(okJson(data));

      const result = await executeTool('run_full', { recipe: {} }, makeSession());
      expect(result).toEqual(data);
    });
  });
});
