// Chat session types shared between API routes and frontend

// --- Phase & Status Types ---

export type ChatPhase =
  | 'greeting'
  | 'intent'
  | 'sampling'
  | 'demonstration'
  | 'inference'
  | 'validation'
  | 'execution';

export type SessionStatus = 'active' | 'recipe_ready' | 'running' | 'completed';

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
  sample_criteria_left: string | null;
  sample_criteria_right: string | null;
  confirmed_pairs: Array<{ left: Record<string, unknown>; right: Record<string, unknown> }>;
  messages: ChatMessage[];
  created_at: string;
  updated_at: string;
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
  'load_sample',
  'propose_match',
  'infer_rules',
  'build_recipe',
  'validate_recipe',
  'run_sample',
  'run_full',
] as const;

export type AgentTool = (typeof AGENT_TOOLS)[number];

// Phase-to-tool availability mapping: which tools are callable in each phase
export const PHASE_TOOLS: Record<ChatPhase, AgentTool[]> = {
  greeting: ['list_sources'],
  intent: ['list_sources', 'get_source_preview'],
  sampling: ['list_sources', 'get_source_preview', 'load_sample'],
  demonstration: ['list_sources', 'get_source_preview', 'load_sample', 'propose_match'],
  inference: [
    'list_sources',
    'get_source_preview',
    'load_sample',
    'propose_match',
    'infer_rules',
    'build_recipe',
  ],
  validation: ['list_sources', 'get_source_preview', 'validate_recipe', 'run_sample'],
  execution: ['list_sources', 'get_source_preview', 'validate_recipe', 'run_full'],
};
