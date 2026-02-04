// Chat session types shared between API routes and frontend

// --- Phase & Status Types ---

export type ChatPhase =
  | 'greeting'
  | 'intent'
  | 'scoping'
  | 'demonstration'
  | 'inference'
  | 'validation'
  | 'execution';

export type SessionStatus = 'active' | 'recipe_ready' | 'running' | 'completed';

// --- Filter Conditions (source-agnostic) ---

export type FilterOp = 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'between' | 'in' | 'like';

export interface FilterCondition {
  column: string;
  op: FilterOp;
  value: string | number | string[] | [string, string];
}

// --- Card Types ---

export type CardType =
  | 'select'
  | 'confirm'
  | 'sample_table'
  | 'match_proposal'
  | 'rule_summary'
  | 'progress'
  | 'result_summary';

// --- Message Types ---

export interface ChatSegment {
  type: 'text' | 'card';
  content?: string;
  card_type?: CardType;
  card_id?: string;
  data?: Record<string, unknown>;
}

export interface ChatMessage {
  role: 'agent' | 'user';
  segments: ChatSegment[];
  timestamp: string;
}

export interface CardResponse {
  card_id: string;
  action: string;
  value?: unknown;
}

// --- Session ---

export interface ChatSession {
  id: string;
  status: SessionStatus;
  phase: ChatPhase;
  left_source_alias: string | null;
  right_source_alias: string | null;
  recipe_draft: Record<string, unknown> | null;
  sample_left: Record<string, unknown>[] | null;
  sample_right: Record<string, unknown>[] | null;
  confirmed_pairs: Array<{ left: Record<string, unknown>; right: Record<string, unknown> }>;
  messages: ChatMessage[];
  created_at: string;
  updated_at: string;

  // New fields for state machine
  sources_list: SourceInfo[] | null;
  schema_left: ColumnInfo[] | null;
  schema_right: ColumnInfo[] | null;
  scope_left: FilterCondition[] | null;
  scope_right: FilterCondition[] | null;
  validation_approved: boolean;
}

// --- Source Data (from Rust backend) ---

export interface SourceInfo {
  alias: string;
  uri: string;
  source_type: string;
  status: string;
}

export interface ColumnInfo {
  name: string;
  data_type: string;
  nullable: boolean;
}

export interface SourcePreview {
  alias: string;
  columns: ColumnInfo[];
  rows: string[][];
  total_rows: number;
  preview_rows: number;
}

// --- Agent Tool Definitions ---

export const AGENT_TOOLS = [
  'list_sources',
  'get_source_preview',
  'load_scoped',
  'propose_match',
  'infer_rules',
  'build_recipe',
  'validate_recipe',
  'run_sample',
  'run_full',
] as const;

export type AgentTool = (typeof AGENT_TOOLS)[number];

// --- Phase Config ---

export type ContextInjection =
  | 'sources_list'
  | 'schema_left'
  | 'schema_right'
  | 'sample_left'
  | 'sample_right'
  | 'confirmed_pairs'
  | 'recipe_draft';

export interface PhaseConfig {
  name: ChatPhase;
  tools: AgentTool[];
  instructions: string;
  prerequisites: {
    sessionFields: (keyof ChatSession)[];
  };
  contextInjections: ContextInjection[];
  advancesWhen: (session: ChatSession) => boolean;
  errorPolicy: {
    maxRetriesPerTool: number;
    onExhausted: 'inform_user' | 'skip_phase';
  };
}

export const PHASE_ORDER: ChatPhase[] = [
  'greeting', 'intent', 'scoping', 'demonstration',
  'inference', 'validation', 'execution',
];

export const PHASES: Record<ChatPhase, PhaseConfig> = {
  greeting: {
    name: 'greeting',
    tools: ['list_sources'],
    instructions: 'Greet the user. Use list_sources to see what data sources are available. Tell the user what sources they have and ask what they want to reconcile.',
    prerequisites: { sessionFields: [] },
    contextInjections: [],
    advancesWhen: (s) => s.sources_list !== null,
    errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
  },
  intent: {
    name: 'intent',
    tools: ['list_sources', 'get_source_preview'],
    instructions: 'The user has stated what they want to reconcile. Confirm the left and right sources. Use get_source_preview on both sources to understand the data structure. You must preview both sources before proceeding.',
    prerequisites: { sessionFields: ['sources_list'] },
    contextInjections: ['sources_list'],
    advancesWhen: (s) => s.schema_left !== null && s.schema_right !== null,
    errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
  },
  scoping: {
    name: 'scoping',
    tools: ['list_sources', 'get_source_preview', 'load_scoped'],
    instructions: `The user's sources have been confirmed and schemas are loaded. Ask the user what subset of data they want to reconcile (e.g., date range, customer segment, amount range). Use load_scoped to load filtered data from each source into the working set. Pass structured filter conditions — the backend handles translation to the source's native query language.`,
    prerequisites: { sessionFields: ['schema_left', 'schema_right'] },
    contextInjections: ['schema_left', 'schema_right'],
    advancesWhen: (s) => s.sample_left !== null && s.sample_right !== null && s.sample_left.length > 0 && s.sample_right.length > 0,
    errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
  },
  demonstration: {
    name: 'demonstration',
    tools: ['get_source_preview', 'propose_match'],
    instructions: 'Examine the scoped data. Propose candidate matches using propose_match. Explain your reasoning for each match. The user will confirm or reject. Build up at least 3 confirmed pairs before moving on.',
    prerequisites: { sessionFields: ['sample_left', 'sample_right'] },
    contextInjections: ['schema_left', 'schema_right', 'sample_left', 'sample_right', 'confirmed_pairs'],
    advancesWhen: (s) => s.confirmed_pairs.length >= 3,
    errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
  },
  inference: {
    name: 'inference',
    tools: ['infer_rules', 'build_recipe', 'propose_match'],
    instructions: 'Analyze the confirmed match pairs using infer_rules. Propose matching rules to the user in plain language. Once rules are agreed upon, build the recipe using build_recipe.',
    prerequisites: { sessionFields: ['confirmed_pairs'] },
    contextInjections: ['schema_left', 'schema_right', 'confirmed_pairs'],
    advancesWhen: (s) => s.recipe_draft !== null,
    errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
  },
  validation: {
    name: 'validation',
    tools: ['validate_recipe', 'run_sample', 'get_source_preview'],
    instructions: 'Validate the recipe using validate_recipe. Run it on the scoped data using run_sample. Present the results to the user. Ask if they want to adjust the rules or approve.',
    prerequisites: { sessionFields: ['recipe_draft'] },
    contextInjections: ['recipe_draft', 'schema_left', 'schema_right'],
    advancesWhen: (s) => s.validation_approved === true,
    errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
  },
  execution: {
    name: 'execution',
    tools: ['run_full', 'validate_recipe'],
    instructions: 'The user has approved the recipe. Run it on the full scoped dataset using run_full. Present the results summary.',
    prerequisites: { sessionFields: ['recipe_draft'] },
    contextInjections: ['recipe_draft'],
    advancesWhen: () => false,
    errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
  },
};
