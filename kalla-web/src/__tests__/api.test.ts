import {
  healthCheck,
  listSources,
  registerSource,
  listRecipes,
  getRecipe,
  saveRecipe,
  validateRecipe,
  validateRecipeSchema,
  generateRecipe,
  createRun,
  listRuns,
  getRun,
  MatchRecipe,
} from '@/lib/api';

const mockFetch = jest.fn();
global.fetch = mockFetch;

const API_BASE = 'http://localhost:3001';

// Reusable fixture for a minimal MatchRecipe
const sampleRecipe: MatchRecipe = {
  version: '1',
  recipe_id: 'recipe-1',
  sources: {
    left: { alias: 'left_src', uri: 'file://left.csv' },
    right: { alias: 'right_src', uri: 'file://right.csv' },
  },
  match_rules: [
    {
      name: 'rule-1',
      pattern: '1:1',
      conditions: [{ left: 'id', op: 'eq', right: 'id' }],
    },
  ],
  output: {
    matched: 'matched.csv',
    unmatched_left: 'unmatched_left.csv',
    unmatched_right: 'unmatched_right.csv',
  },
};

beforeEach(() => {
  mockFetch.mockReset();
});

// ---------------------------------------------------------------------------
// healthCheck
// ---------------------------------------------------------------------------
describe('healthCheck', () => {
  it('calls GET /health and returns text', async () => {
    mockFetch.mockResolvedValueOnce({
      text: () => Promise.resolve('OK'),
    });

    const result = await healthCheck();

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/health`);
    expect(result).toBe('OK');
  });

  it('returns text even when response is not ok (no res.ok check)', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      text: () => Promise.resolve('Internal Server Error'),
    });

    const result = await healthCheck();
    expect(result).toBe('Internal Server Error');
  });
});

// ---------------------------------------------------------------------------
// listSources
// ---------------------------------------------------------------------------
describe('listSources', () => {
  it('calls GET /api/sources and returns parsed JSON', async () => {
    const sources = [
      { alias: 'inv', uri: 'file://inv.csv', source_type: 'csv', status: 'connected' },
    ];
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(sources),
    });

    const result = await listSources();

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/api/sources`);
    expect(result).toEqual(sources);
  });

  it('throws "Failed to fetch sources" when response is not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
    });

    await expect(listSources()).rejects.toThrow('Failed to fetch sources');
  });
});

// ---------------------------------------------------------------------------
// registerSource
// ---------------------------------------------------------------------------
describe('registerSource', () => {
  it('calls POST /api/sources with alias and uri, returns JSON', async () => {
    const body = { success: true, message: 'Source registered' };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(body),
    });

    const result = await registerSource('my_alias', 'file://data.csv');

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/api/sources`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ alias: 'my_alias', uri: 'file://data.csv' }),
    });
    expect(result).toEqual(body);
  });

  it('throws with error text from response when not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      text: () => Promise.resolve('Invalid source URI'),
    });

    await expect(registerSource('bad', 'nope')).rejects.toThrow('Invalid source URI');
  });
});

// ---------------------------------------------------------------------------
// listRecipes
// ---------------------------------------------------------------------------
describe('listRecipes', () => {
  it('calls GET /api/recipes and returns parsed JSON', async () => {
    const recipes = [
      { recipe_id: 'r1', name: 'Test', description: null, config: sampleRecipe },
    ];
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(recipes),
    });

    const result = await listRecipes();

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/api/recipes`);
    expect(result).toEqual(recipes);
  });

  it('throws "Failed to fetch recipes" when response is not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
    });

    await expect(listRecipes()).rejects.toThrow('Failed to fetch recipes');
  });
});

// ---------------------------------------------------------------------------
// getRecipe
// ---------------------------------------------------------------------------
describe('getRecipe', () => {
  it('calls GET /api/recipes/:id and returns parsed JSON', async () => {
    const recipe = { recipe_id: 'r1', name: 'Test', description: 'desc', config: sampleRecipe };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(recipe),
    });

    const result = await getRecipe('r1');

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/api/recipes/r1`);
    expect(result).toEqual(recipe);
  });

  it('throws "Recipe not found" when response is not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
    });

    await expect(getRecipe('nonexistent')).rejects.toThrow('Recipe not found');
  });
});

// ---------------------------------------------------------------------------
// saveRecipe
// ---------------------------------------------------------------------------
describe('saveRecipe', () => {
  it('calls POST /api/recipes with recipe data and returns JSON', async () => {
    const body = { success: true, message: 'Recipe saved' };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(body),
    });

    const result = await saveRecipe('r1', 'My Recipe', 'A description', sampleRecipe);

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/api/recipes`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        recipe_id: 'r1',
        name: 'My Recipe',
        description: 'A description',
        config: sampleRecipe,
      }),
    });
    expect(result).toEqual(body);
  });

  it('sends null description correctly', async () => {
    const body = { success: true, message: 'Recipe saved' };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(body),
    });

    await saveRecipe('r1', 'My Recipe', null, sampleRecipe);

    const callBody = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(callBody.description).toBeNull();
  });

  it('throws with error text from response when not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 422,
      text: () => Promise.resolve('Validation failed: missing name'),
    });

    await expect(saveRecipe('r1', '', null, sampleRecipe)).rejects.toThrow(
      'Validation failed: missing name'
    );
  });
});

// ---------------------------------------------------------------------------
// validateRecipe
// ---------------------------------------------------------------------------
describe('validateRecipe', () => {
  it('calls POST /api/recipes/validate and returns JSON directly', async () => {
    const validation = { valid: true, errors: [] };
    mockFetch.mockResolvedValueOnce({
      json: () => Promise.resolve(validation),
    });

    const result = await validateRecipe(sampleRecipe);

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/api/recipes/validate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(sampleRecipe),
    });
    expect(result).toEqual(validation);
  });

  it('returns JSON even when response is not ok (no res.ok check)', async () => {
    const validation = { valid: false, errors: ['some server error'] };
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: () => Promise.resolve(validation),
    });

    const result = await validateRecipe(sampleRecipe);
    expect(result).toEqual(validation);
  });
});

// ---------------------------------------------------------------------------
// validateRecipeSchema
// ---------------------------------------------------------------------------
describe('validateRecipeSchema', () => {
  it('calls POST /api/recipes/validate-schema and returns JSON', async () => {
    const schemaResult = {
      valid: true,
      errors: [],
      warnings: [],
      resolved_fields: [['left.id', 'Int64']],
    };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(schemaResult),
    });

    const result = await validateRecipeSchema(sampleRecipe);

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/api/recipes/validate-schema`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(sampleRecipe),
    });
    expect(result).toEqual(schemaResult);
  });

  it('throws with error text from response when not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      text: () => Promise.resolve('Schema validation error: source not found'),
    });

    await expect(validateRecipeSchema(sampleRecipe)).rejects.toThrow(
      'Schema validation error: source not found'
    );
  });
});

// ---------------------------------------------------------------------------
// generateRecipe
// ---------------------------------------------------------------------------
describe('generateRecipe', () => {
  it('calls POST /api/recipes/generate with sources and prompt, returns JSON', async () => {
    const generated = { recipe: sampleRecipe };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(generated),
    });

    const result = await generateRecipe('left_src', 'right_src', 'Match by id');

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/api/recipes/generate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        left_source: 'left_src',
        right_source: 'right_src',
        prompt: 'Match by id',
      }),
    });
    expect(result).toEqual(generated);
  });

  it('throws with error text from response when not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      text: () => Promise.resolve('LLM generation failed'),
    });

    await expect(generateRecipe('a', 'b', 'prompt')).rejects.toThrow('LLM generation failed');
  });
});

// ---------------------------------------------------------------------------
// createRun
// ---------------------------------------------------------------------------
describe('createRun', () => {
  it('calls POST /api/runs with recipe and returns JSON', async () => {
    const runResult = { run_id: 'run-1', status: 'Running' };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(runResult),
    });

    const result = await createRun(sampleRecipe);

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/api/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ recipe: sampleRecipe }),
    });
    expect(result).toEqual(runResult);
  });

  it('throws with error text from response when not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      text: () => Promise.resolve('Invalid recipe configuration'),
    });

    await expect(createRun(sampleRecipe)).rejects.toThrow('Invalid recipe configuration');
  });
});

// ---------------------------------------------------------------------------
// listRuns
// ---------------------------------------------------------------------------
describe('listRuns', () => {
  it('calls GET /api/runs and returns parsed JSON directly', async () => {
    const runs = [
      {
        run_id: 'run-1',
        recipe_id: 'recipe-1',
        status: 'Completed',
        started_at: '2025-01-01T00:00:00Z',
        matched_count: 10,
        unmatched_left_count: 2,
        unmatched_right_count: 3,
      },
    ];
    mockFetch.mockResolvedValueOnce({
      json: () => Promise.resolve(runs),
    });

    const result = await listRuns();

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/api/runs`);
    expect(result).toEqual(runs);
  });

  it('returns JSON even when response is not ok (no res.ok check)', async () => {
    const runs: never[] = [];
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: () => Promise.resolve(runs),
    });

    const result = await listRuns();
    expect(result).toEqual(runs);
  });
});

// ---------------------------------------------------------------------------
// getRun
// ---------------------------------------------------------------------------
describe('getRun', () => {
  it('calls GET /api/runs/:id and returns parsed JSON', async () => {
    const run = {
      run_id: 'run-1',
      recipe_id: 'recipe-1',
      started_at: '2025-01-01T00:00:00Z',
      completed_at: '2025-01-01T00:01:00Z',
      left_source: 'file://left.csv',
      right_source: 'file://right.csv',
      left_record_count: 100,
      right_record_count: 200,
      matched_count: 80,
      unmatched_left_count: 20,
      unmatched_right_count: 120,
      status: 'Completed' as const,
    };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(run),
    });

    const result = await getRun('run-1');

    expect(mockFetch).toHaveBeenCalledWith(`${API_BASE}/api/runs/run-1`);
    expect(result).toEqual(run);
  });

  it('throws "Run not found" when response is not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
    });

    await expect(getRun('nonexistent')).rejects.toThrow('Run not found');
  });
});
