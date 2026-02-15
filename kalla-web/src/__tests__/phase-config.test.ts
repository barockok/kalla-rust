import type {
  ChatPhase,
  ChatSession,
  FilterCondition,
  PhaseConfig,
  ContextInjection,
} from '@/lib/chat-types';
import { PHASES, PHASE_ORDER } from '@/lib/chat-types';

describe('PhaseConfig types and PHASES config', () => {
  test('ChatPhase includes scoping instead of sampling', () => {
    const phases: ChatPhase[] = [
      'greeting', 'intent', 'scoping', 'demonstration',
      'inference', 'validation', 'execution',
    ];
    expect(phases).toHaveLength(7);
  });

  test('FilterCondition accepts valid ops', () => {
    const filter: FilterCondition = {
      column: 'invoice_date',
      op: 'between',
      value: ['2024-01-01', '2024-01-31'],
    };
    expect(filter.op).toBe('between');
  });

  test('PHASES config has all 7 phases', () => {
    expect(Object.keys(PHASES)).toHaveLength(7);
    expect(PHASES.greeting).toBeDefined();
    expect(PHASES.scoping).toBeDefined();
    expect(PHASES.execution).toBeDefined();
  });

  test('PHASE_ORDER is correct sequence', () => {
    expect(PHASE_ORDER).toEqual([
      'greeting', 'intent', 'scoping', 'demonstration',
      'inference', 'validation', 'execution',
    ]);
  });

  test('each phase config has required fields', () => {
    for (const [name, config] of Object.entries(PHASES)) {
      expect(config.name).toBe(name);
      expect(config.tools).toBeDefined();
      expect(config.instructions).toBeTruthy();
      expect(config.prerequisites).toBeDefined();
      expect(config.contextInjections).toBeDefined();
      expect(config.advancesWhen).toBeInstanceOf(Function);
      expect(config.errorPolicy).toBeDefined();
      expect(config.errorPolicy.maxRetriesPerTool).toBeGreaterThan(0);
      expect(['inform_user', 'skip_phase']).toContain(config.errorPolicy.onExhausted);
    }
  });

  test('greeting advancesWhen sources_list is populated or schema_left exists', () => {
    const session = { sources_list: null, schema_left: null } as unknown as ChatSession;
    expect(PHASES.greeting.advancesWhen(session)).toBe(false);
    // Advances when sources_list is populated (registered sources path)
    session.sources_list = [{ alias: 'inv', uri: '', source_type: 'csv', status: 'ok' }];
    expect(PHASES.greeting.advancesWhen(session)).toBe(true);
    // Also advances when schema_left exists (file upload path)
    session.sources_list = null;
    session.schema_left = [{ name: 'id', data_type: 'string', nullable: true }];
    expect(PHASES.greeting.advancesWhen(session)).toBe(true);
  });

  test('intent advancesWhen both schemas populated', () => {
    const session = { schema_left: null, schema_right: null } as unknown as ChatSession;
    expect(PHASES.intent.advancesWhen(session)).toBe(false);
    session.schema_left = [{ name: 'id', data_type: 'Int64', nullable: false }];
    expect(PHASES.intent.advancesWhen(session)).toBe(false);
    session.schema_right = [{ name: 'id', data_type: 'Int64', nullable: false }];
    expect(PHASES.intent.advancesWhen(session)).toBe(true);
  });

  test('scoping advancesWhen both samples populated and non-empty', () => {
    const session = { sample_left: null, sample_right: null } as unknown as ChatSession;
    expect(PHASES.scoping.advancesWhen(session)).toBe(false);
    session.sample_left = [];
    session.sample_right = [];
    expect(PHASES.scoping.advancesWhen(session)).toBe(false);
    session.sample_left = [{ id: '1' }];
    session.sample_right = [{ id: '2' }];
    expect(PHASES.scoping.advancesWhen(session)).toBe(true);
  });

  test('demonstration advancesWhen 3+ confirmed pairs', () => {
    const session = { confirmed_pairs: [] } as unknown as ChatSession;
    expect(PHASES.demonstration.advancesWhen(session)).toBe(false);
    session.confirmed_pairs = [
      { left: {}, right: {} },
      { left: {}, right: {} },
    ];
    expect(PHASES.demonstration.advancesWhen(session)).toBe(false);
    session.confirmed_pairs.push({ left: {}, right: {} });
    expect(PHASES.demonstration.advancesWhen(session)).toBe(true);
  });

  test('inference advancesWhen recipe_draft is populated', () => {
    const session = { recipe_draft: null } as unknown as ChatSession;
    expect(PHASES.inference.advancesWhen(session)).toBe(false);
    session.recipe_draft = { version: '1.0' };
    expect(PHASES.inference.advancesWhen(session)).toBe(true);
  });

  test('validation advancesWhen validation_approved is true', () => {
    const session = { validation_approved: false } as unknown as ChatSession;
    expect(PHASES.validation.advancesWhen(session)).toBe(false);
    session.validation_approved = true;
    expect(PHASES.validation.advancesWhen(session)).toBe(true);
  });

  test('execution advancesWhen always returns false (terminal)', () => {
    const session = {} as unknown as ChatSession;
    expect(PHASES.execution.advancesWhen(session)).toBe(false);
  });
});
