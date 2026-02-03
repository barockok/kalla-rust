# Agentic Recipe Builder — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the form-based `/reconcile` page with a conversational, agentic chat interface where users demonstrate matches by example and an LLM agent infers reconciliation rules.

**Architecture:** Next.js API routes host the agentic layer (session management, LLM orchestration via Anthropic SDK with tool calling, SSE streaming). The existing Rust service on port 3001 handles all data operations (source listing, sample loading, recipe validation, reconciliation execution). The React frontend renders a chat UI with dynamic interactive cards.

**Tech Stack:** Next.js 16 (API routes + React), Anthropic SDK (`@anthropic-ai/sdk`), Server-Sent Events, Playwright (integration tests), PostgreSQL (chat session persistence), existing Rust/Axum backend (unchanged).

---

## Task 1: Install Dependencies

**Files:**
- Modify: `kalla-web/package.json`

**Step 1: Install production dependencies**

Run:
```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npm install @anthropic-ai/sdk uuid
```
Expected: packages added to dependencies

**Step 2: Install dev dependencies**

Run:
```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npm install -D @playwright/test @types/uuid
```
Expected: packages added to devDependencies

**Step 3: Install Playwright browsers**

Run:
```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npx playwright install chromium
```
Expected: Chromium browser downloaded for Playwright

**Step 4: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/package.json kalla-web/package-lock.json
git commit -m "feat: add anthropic sdk, uuid, and playwright dependencies"
```

---

## Task 2: Chat Session Types & Shared Constants

**Files:**
- Create: `kalla-web/src/lib/chat-types.ts`

**Step 1: Write the type definitions file**

```typescript
// Chat session types shared between API routes and frontend

export type ChatPhase =
  | 'greeting'
  | 'intent'
  | 'sampling'
  | 'demonstration'
  | 'inference'
  | 'validation'
  | 'execution';

export type SessionStatus = 'active' | 'recipe_ready' | 'running' | 'completed';

export type CardType =
  | 'select'
  | 'confirm'
  | 'sample_table'
  | 'match_proposal'
  | 'rule_summary'
  | 'progress'
  | 'result_summary';

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

// Source info returned from Rust backend
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

// Tool definitions for the agent
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

export type AgentTool = typeof AGENT_TOOLS[number];

// Phase-to-tool availability mapping
export const PHASE_TOOLS: Record<ChatPhase, AgentTool[]> = {
  greeting: ['list_sources'],
  intent: ['list_sources', 'get_source_preview'],
  sampling: ['list_sources', 'get_source_preview', 'load_sample'],
  demonstration: ['list_sources', 'get_source_preview', 'load_sample', 'propose_match'],
  inference: ['list_sources', 'get_source_preview', 'load_sample', 'propose_match', 'infer_rules', 'build_recipe'],
  validation: ['list_sources', 'get_source_preview', 'validate_recipe', 'run_sample'],
  execution: ['list_sources', 'get_source_preview', 'validate_recipe', 'run_full'],
};
```

**Step 2: Run TypeScript check**

Run: `cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npx tsc --noEmit src/lib/chat-types.ts`
Expected: No errors

**Step 3: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/src/lib/chat-types.ts
git commit -m "feat: add chat session types and phase-tool mapping"
```

---

## Task 3: Chat Session Store (In-Memory + DB Persistence)

**Files:**
- Create: `kalla-web/src/lib/session-store.ts`

**Step 1: Write the session store**

```typescript
import { v4 as uuidv4 } from 'uuid';
import type { ChatSession, ChatMessage, ChatPhase, SessionStatus } from './chat-types';

// In-memory session store (sufficient for single-instance deployment)
// Persists to PostgreSQL on every mutation for durability
const sessions = new Map<string, ChatSession>();

const RUST_API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3001';
const DB_URL = process.env.DATABASE_URL || '';

async function persistSession(session: ChatSession): Promise<void> {
  if (!DB_URL) return;

  try {
    // Use dynamic import to avoid bundling pg in client
    const { Pool } = await import('pg');
    const pool = new Pool({ connectionString: DB_URL });

    await pool.query(
      `INSERT INTO chat_sessions (id, status, phase, left_source_alias, right_source_alias,
        recipe_draft, sample_left, sample_right, sample_criteria_left, sample_criteria_right,
        confirmed_pairs, messages, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
       ON CONFLICT (id) DO UPDATE SET
        status = $2, phase = $3, left_source_alias = $4, right_source_alias = $5,
        recipe_draft = $6, sample_left = $7, sample_right = $8,
        sample_criteria_left = $9, sample_criteria_right = $10,
        confirmed_pairs = $11, messages = $12, updated_at = NOW()`,
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
      ]
    );
    await pool.end();
  } catch (err) {
    console.error('Failed to persist session:', err);
  }
}

export function createSession(): ChatSession {
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
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
  sessions.set(session.id, session);
  persistSession(session);
  return session;
}

export function getSession(id: string): ChatSession | undefined {
  return sessions.get(id);
}

export function updateSession(
  id: string,
  updates: Partial<Pick<ChatSession, 'status' | 'phase' | 'left_source_alias' | 'right_source_alias' | 'recipe_draft' | 'sample_left' | 'sample_right' | 'sample_criteria_left' | 'sample_criteria_right' | 'confirmed_pairs'>>
): ChatSession | undefined {
  const session = sessions.get(id);
  if (!session) return undefined;

  Object.assign(session, updates, { updated_at: new Date().toISOString() });
  persistSession(session);
  return session;
}

export function addMessage(id: string, message: ChatMessage): ChatSession | undefined {
  const session = sessions.get(id);
  if (!session) return undefined;

  session.messages.push(message);
  session.updated_at = new Date().toISOString();
  persistSession(session);
  return session;
}

export function deleteSession(id: string): boolean {
  return sessions.delete(id);
}
```

**Step 2: Run TypeScript check**

Run: `cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npx tsc --noEmit src/lib/session-store.ts`
Expected: No errors (pg may need type stubs — if so, install `@types/pg` or use a fetch-based approach to Postgres)

**Step 3: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/src/lib/session-store.ts
git commit -m "feat: add chat session store with PostgreSQL persistence"
```

---

## Task 4: Agent Tool Executor (Calls Rust Backend)

**Files:**
- Create: `kalla-web/src/lib/agent-tools.ts`

**Step 1: Write the tool executor**

This module implements all 9 agent tools as functions that call the Rust backend HTTP API. The LLM invokes these via tool_use blocks, and this code translates them into Rust API calls.

```typescript
import type { ChatSession, SourceInfo, SourcePreview } from './chat-types';

const RUST_API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3001';

// Tool: list_sources
export async function listSources(): Promise<SourceInfo[]> {
  const res = await fetch(`${RUST_API}/api/sources`);
  if (!res.ok) throw new Error(`Failed to list sources: ${res.statusText}`);
  return res.json();
}

// Tool: get_source_preview
export async function getSourcePreview(
  alias: string,
  limit: number = 10
): Promise<SourcePreview> {
  const res = await fetch(`${RUST_API}/api/sources/${alias}/preview?limit=${limit}`);
  if (!res.ok) throw new Error(`Failed to get preview for ${alias}: ${res.statusText}`);
  return res.json();
}

// Tool: load_sample — loads filtered rows from a source
export async function loadSample(
  alias: string,
  criteria: string,
  limit: number = 50
): Promise<SourcePreview> {
  // For now, we use the preview endpoint with a limit.
  // The criteria would be used as a SQL WHERE clause in future Rust endpoint.
  // Current implementation: load preview with higher limit as a sample.
  const res = await fetch(`${RUST_API}/api/sources/${alias}/preview?limit=${limit}`);
  if (!res.ok) throw new Error(`Failed to load sample from ${alias}: ${res.statusText}`);
  return res.json();
}

// Tool: propose_match — agent proposes a match between two rows
export function proposeMatch(
  leftRow: Record<string, string>,
  rightRow: Record<string, string>,
  reasoning: string
): { left: Record<string, string>; right: Record<string, string>; reasoning: string } {
  return { left: leftRow, right: rightRow, reasoning };
}

// Tool: infer_rules — analyze confirmed match pairs to find column mapping rules
export function inferRules(
  confirmedPairs: Array<{ left: Record<string, unknown>; right: Record<string, unknown> }>,
  leftColumns: string[],
  rightColumns: string[]
): Array<{ left_col: string; right_col: string; op: string; confidence: number; reasoning: string }> {
  const rules: Array<{ left_col: string; right_col: string; op: string; confidence: number; reasoning: string }> = [];

  for (const leftCol of leftColumns) {
    for (const rightCol of rightColumns) {
      let exactMatches = 0;
      let toleranceMatches = 0;
      let total = confirmedPairs.length;

      for (const pair of confirmedPairs) {
        const leftVal = String(pair.left[leftCol] ?? '');
        const rightVal = String(pair.right[rightCol] ?? '');

        if (!leftVal || !rightVal) continue;

        // Exact match check
        if (leftVal === rightVal) {
          exactMatches++;
          continue;
        }

        // Numeric tolerance check
        const leftNum = parseFloat(leftVal);
        const rightNum = parseFloat(rightVal);
        if (!isNaN(leftNum) && !isNaN(rightNum)) {
          const diff = Math.abs(leftNum - rightNum);
          const pctDiff = leftNum !== 0 ? diff / Math.abs(leftNum) : diff;
          if (pctDiff < 0.05) { // 5% tolerance
            toleranceMatches++;
            continue;
          }
        }
      }

      if (total > 0) {
        const exactConfidence = exactMatches / total;
        const toleranceConfidence = toleranceMatches / total;

        if (exactConfidence > 0.7) {
          rules.push({
            left_col: leftCol,
            right_col: rightCol,
            op: 'eq',
            confidence: exactConfidence,
            reasoning: `${Math.round(exactConfidence * 100)}% of pairs have exact match on ${leftCol} = ${rightCol}`,
          });
        } else if (toleranceConfidence > 0.5 || (exactConfidence + toleranceConfidence) > 0.7) {
          rules.push({
            left_col: leftCol,
            right_col: rightCol,
            op: 'tolerance',
            confidence: exactConfidence + toleranceConfidence,
            reasoning: `${Math.round((exactConfidence + toleranceConfidence) * 100)}% match with tolerance on ${leftCol} ≈ ${rightCol}`,
          });
        }
      }
    }
  }

  // Sort by confidence descending and deduplicate
  rules.sort((a, b) => b.confidence - a.confidence);
  return rules;
}

// Tool: build_recipe — assemble a MatchRecipe from inferred rules
export function buildRecipe(
  leftAlias: string,
  rightAlias: string,
  leftUri: string,
  rightUri: string,
  leftPk: string[],
  rightPk: string[],
  rules: Array<{ name: string; pattern: string; conditions: Array<{ left: string; op: string; right: string; threshold?: number }> }>
): Record<string, unknown> {
  return {
    version: '1.0',
    recipe_id: `recipe-${Date.now()}`,
    sources: {
      left: { alias: leftAlias, uri: leftUri, primary_key: leftPk },
      right: { alias: rightAlias, uri: rightUri, primary_key: rightPk },
    },
    match_rules: rules.map((r, i) => ({
      ...r,
      priority: i + 1,
    })),
    output: {
      matched: 'evidence/matched.parquet',
      unmatched_left: 'evidence/unmatched_left.parquet',
      unmatched_right: 'evidence/unmatched_right.parquet',
    },
  };
}

// Tool: validate_recipe
export async function validateRecipe(recipe: Record<string, unknown>): Promise<{ valid: boolean; errors: string[] }> {
  const res = await fetch(`${RUST_API}/api/recipes/validate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(recipe),
  });
  if (!res.ok) throw new Error(`Validation request failed: ${res.statusText}`);
  return res.json();
}

// Tool: run_sample — execute recipe on sample data
export async function runSample(recipe: Record<string, unknown>): Promise<{ run_id: string; status: string }> {
  const res = await fetch(`${RUST_API}/api/runs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ recipe }),
  });
  if (!res.ok) throw new Error(`Run creation failed: ${res.statusText}`);
  return res.json();
}

// Tool: run_full — execute recipe on full dataset
export async function runFull(recipe: Record<string, unknown>): Promise<{ run_id: string; status: string }> {
  // Same endpoint as run_sample — the Rust backend always runs on the full registered source
  return runSample(recipe);
}

// Poll run status until completion
export async function pollRunStatus(
  runId: string,
  maxWaitMs: number = 30000,
  intervalMs: number = 1000
): Promise<Record<string, unknown>> {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const res = await fetch(`${RUST_API}/api/runs/${runId}`);
    if (!res.ok) throw new Error(`Failed to get run status: ${res.statusText}`);
    const run = await res.json();
    if (run.status !== 'Running') return run;
    await new Promise(resolve => setTimeout(resolve, intervalMs));
  }
  throw new Error('Run timed out');
}

// Execute a tool by name with arguments
export async function executeTool(
  toolName: string,
  args: Record<string, unknown>,
  session: ChatSession
): Promise<unknown> {
  switch (toolName) {
    case 'list_sources':
      return listSources();

    case 'get_source_preview':
      return getSourcePreview(
        args.alias as string,
        (args.limit as number) || 10
      );

    case 'load_sample':
      return loadSample(
        args.alias as string,
        (args.criteria as string) || '',
        (args.limit as number) || 50
      );

    case 'propose_match':
      return proposeMatch(
        args.left_row as Record<string, string>,
        args.right_row as Record<string, string>,
        args.reasoning as string
      );

    case 'infer_rules':
      return inferRules(
        session.confirmed_pairs,
        args.left_columns as string[],
        args.right_columns as string[]
      );

    case 'build_recipe':
      return buildRecipe(
        args.left_alias as string,
        args.right_alias as string,
        args.left_uri as string,
        args.right_uri as string,
        args.left_pk as string[],
        args.right_pk as string[],
        args.rules as Array<{ name: string; pattern: string; conditions: Array<{ left: string; op: string; right: string; threshold?: number }> }>
      );

    case 'validate_recipe':
      return validateRecipe(args.recipe as Record<string, unknown>);

    case 'run_sample':
      return runSample(args.recipe as Record<string, unknown>);

    case 'run_full':
      return runFull(args.recipe as Record<string, unknown>);

    default:
      throw new Error(`Unknown tool: ${toolName}`);
  }
}
```

**Step 2: Run TypeScript check**

Run: `cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npx tsc --noEmit src/lib/agent-tools.ts`
Expected: No errors

**Step 3: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/src/lib/agent-tools.ts
git commit -m "feat: add agent tool executor for Rust backend integration"
```

---

## Task 5: Anthropic Agent Orchestrator (LLM + Tool Calling + SSE)

**Files:**
- Create: `kalla-web/src/lib/agent.ts`

**Step 1: Write the agent orchestrator**

This is the core agentic layer. It:
- Builds system prompts with phase awareness and context injection
- Sends messages to Claude via the Anthropic SDK with tool definitions
- Handles the tool_use → tool_result loop
- Streams text responses via callback
- Manages phase transitions

```typescript
import Anthropic from '@anthropic-ai/sdk';
import type { ChatSession, ChatPhase, ChatSegment } from './chat-types';
import { PHASE_TOOLS } from './chat-types';
import { executeTool } from './agent-tools';

// Load API key from environment — user puts ANTHROPIC_API_KEY in .env
const getClient = () => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error('ANTHROPIC_API_KEY not set');
  return new Anthropic({ apiKey });
};

// Tool definitions for Claude's tool_use
const TOOL_DEFINITIONS: Anthropic.Tool[] = [
  {
    name: 'list_sources',
    description: 'List all registered data sources with their aliases, URIs, types, and connection status.',
    input_schema: {
      type: 'object' as const,
      properties: {},
      required: [],
    },
  },
  {
    name: 'get_source_preview',
    description: 'Get schema info and sample rows from a registered data source. Returns column names, types, and a preview of data rows.',
    input_schema: {
      type: 'object' as const,
      properties: {
        alias: { type: 'string', description: 'The alias of the data source to preview' },
        limit: { type: 'number', description: 'Max rows to return (default 10, max 100)' },
      },
      required: ['alias'],
    },
  },
  {
    name: 'load_sample',
    description: 'Load a filtered sample of rows from a data source for match demonstration. Use this to get a workable subset of data.',
    input_schema: {
      type: 'object' as const,
      properties: {
        alias: { type: 'string', description: 'The alias of the data source' },
        criteria: { type: 'string', description: 'Filter criteria (e.g., date range, customer ID)' },
        limit: { type: 'number', description: 'Max rows to load (default 50)' },
      },
      required: ['alias'],
    },
  },
  {
    name: 'propose_match',
    description: 'Propose a candidate match between a left row and a right row. Include reasoning about why they might match. The user will confirm or reject.',
    input_schema: {
      type: 'object' as const,
      properties: {
        left_row: { type: 'object', description: 'The left source row data as key-value pairs' },
        right_row: { type: 'object', description: 'The right source row data as key-value pairs' },
        reasoning: { type: 'string', description: 'Explanation of why these rows might match' },
      },
      required: ['left_row', 'right_row', 'reasoning'],
    },
  },
  {
    name: 'infer_rules',
    description: 'Analyze confirmed match pairs to detect column-level matching rules. Returns candidate rules with confidence scores.',
    input_schema: {
      type: 'object' as const,
      properties: {
        left_columns: { type: 'array', items: { type: 'string' }, description: 'Column names from left source' },
        right_columns: { type: 'array', items: { type: 'string' }, description: 'Column names from right source' },
      },
      required: ['left_columns', 'right_columns'],
    },
  },
  {
    name: 'build_recipe',
    description: 'Build a complete MatchRecipe from the inferred rules and source configuration.',
    input_schema: {
      type: 'object' as const,
      properties: {
        left_alias: { type: 'string' },
        right_alias: { type: 'string' },
        left_uri: { type: 'string' },
        right_uri: { type: 'string' },
        left_pk: { type: 'array', items: { type: 'string' }, description: 'Left source primary key columns' },
        right_pk: { type: 'array', items: { type: 'string' }, description: 'Right source primary key columns' },
        rules: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              name: { type: 'string' },
              pattern: { type: 'string', enum: ['1:1', '1:N', 'M:1'] },
              conditions: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    left: { type: 'string' },
                    op: { type: 'string', enum: ['eq', 'tolerance', 'gt', 'lt', 'gte', 'lte', 'contains', 'startswith', 'endswith'] },
                    right: { type: 'string' },
                    threshold: { type: 'number' },
                  },
                  required: ['left', 'op', 'right'],
                },
              },
            },
            required: ['name', 'pattern', 'conditions'],
          },
        },
      },
      required: ['left_alias', 'right_alias', 'left_uri', 'right_uri', 'left_pk', 'right_pk', 'rules'],
    },
  },
  {
    name: 'validate_recipe',
    description: 'Validate a recipe structure and field references against source schemas.',
    input_schema: {
      type: 'object' as const,
      properties: {
        recipe: { type: 'object', description: 'The complete MatchRecipe to validate' },
      },
      required: ['recipe'],
    },
  },
  {
    name: 'run_sample',
    description: 'Execute the recipe against loaded sample data. Returns match statistics.',
    input_schema: {
      type: 'object' as const,
      properties: {
        recipe: { type: 'object', description: 'The MatchRecipe to run' },
      },
      required: ['recipe'],
    },
  },
  {
    name: 'run_full',
    description: 'Execute the recipe against the full datasets. Returns a run_id for tracking.',
    input_schema: {
      type: 'object' as const,
      properties: {
        recipe: { type: 'object', description: 'The MatchRecipe to run on full data' },
      },
      required: ['recipe'],
    },
  },
];

function buildSystemPrompt(session: ChatSession): string {
  const lines: string[] = [
    'You are a reconciliation assistant for Kalla, a data reconciliation engine.',
    'Your job is to help users build reconciliation recipes by demonstrating matches with examples.',
    '',
    'BEHAVIORAL RULES:',
    '- Infer matching logic when confident. Ask clarifying questions only when ambiguous.',
    '- Ask one question at a time. Keep responses concise.',
    '- Never show raw JSON to the user unless they ask. Present rules in plain language.',
    '- Be conversational and helpful. Guide the user through the process step by step.',
    '- When proposing matches, explain your reasoning clearly.',
    '- After confirming matches, analyze the patterns and propose rules.',
    '- Stop asking for more examples once patterns are unambiguous.',
    '',
    `CURRENT PHASE: ${session.phase}`,
    `Available tools in this phase: ${PHASE_TOOLS[session.phase].join(', ')}`,
    '',
  ];

  // Phase-specific instructions
  switch (session.phase) {
    case 'greeting':
      lines.push('PHASE INSTRUCTIONS: Greet the user. Use list_sources to see what data sources are available. Tell the user what sources they have and ask what they want to reconcile.');
      break;
    case 'intent':
      lines.push('PHASE INSTRUCTIONS: The user has stated what they want to reconcile. Confirm the left and right sources. Use get_source_preview to understand the data structure if needed.');
      break;
    case 'sampling':
      lines.push('PHASE INSTRUCTIONS: Ask the user for filter criteria to narrow down each source to a workable sample. Load samples using load_sample.');
      break;
    case 'demonstration':
      lines.push('PHASE INSTRUCTIONS: Examine the loaded sample data. Propose candidate matches using propose_match. The user will confirm or reject. Build up a set of confirmed pairs.');
      break;
    case 'inference':
      lines.push('PHASE INSTRUCTIONS: Analyze confirmed match pairs using infer_rules. Propose the matching rules to the user. Build the recipe using build_recipe once rules are agreed upon.');
      break;
    case 'validation':
      lines.push('PHASE INSTRUCTIONS: Validate the recipe using validate_recipe. Run it on sample data using run_sample. Show results and let the user iterate if needed.');
      break;
    case 'execution':
      lines.push('PHASE INSTRUCTIONS: The user has approved the recipe. Run it on the full dataset using run_full. Show the results summary.');
      break;
  }

  // Context injection
  if (session.left_source_alias || session.right_source_alias) {
    lines.push('');
    lines.push('SELECTED SOURCES:');
    if (session.left_source_alias) lines.push(`- Left: ${session.left_source_alias}`);
    if (session.right_source_alias) lines.push(`- Right: ${session.right_source_alias}`);
  }

  if (session.confirmed_pairs.length > 0) {
    lines.push('');
    lines.push(`CONFIRMED MATCH PAIRS: ${session.confirmed_pairs.length} pairs confirmed so far.`);
  }

  if (session.recipe_draft) {
    lines.push('');
    lines.push('CURRENT RECIPE DRAFT:');
    lines.push(JSON.stringify(session.recipe_draft, null, 2));
  }

  return lines.join('\n');
}

// Convert session messages to Anthropic message format
function toAnthropicMessages(messages: Array<{ role: string; content: string }>): Anthropic.MessageParam[] {
  return messages.map(m => ({
    role: m.role === 'user' ? 'user' : 'assistant',
    content: m.content,
  })) as Anthropic.MessageParam[];
}

// Filter tools based on current phase
function getPhaseTools(phase: ChatPhase): Anthropic.Tool[] {
  const allowed = PHASE_TOOLS[phase];
  return TOOL_DEFINITIONS.filter(t => allowed.includes(t.name as any));
}

export interface AgentResponse {
  segments: ChatSegment[];
  phaseTransition?: ChatPhase;
  sessionUpdates?: Partial<ChatSession>;
}

export async function runAgent(
  session: ChatSession,
  userMessage: string,
  onTextChunk?: (text: string) => void
): Promise<AgentResponse> {
  const client = getClient();
  const systemPrompt = buildSystemPrompt(session);
  const tools = getPhaseTools(session.phase);

  // Build conversation history for Claude
  const conversationMessages: Anthropic.MessageParam[] = [];
  for (const msg of session.messages) {
    const textContent = msg.segments
      .filter(s => s.type === 'text')
      .map(s => s.content)
      .join('\n');
    if (textContent) {
      conversationMessages.push({
        role: msg.role === 'user' ? 'user' : 'assistant',
        content: textContent,
      });
    }
  }

  // Add current user message
  conversationMessages.push({ role: 'user', content: userMessage });

  const segments: ChatSegment[] = [];
  let sessionUpdates: Partial<ChatSession> = {};
  let phaseTransition: ChatPhase | undefined;

  // Tool-use loop: keep calling Claude until we get a final text response
  let continueLoop = true;
  let currentMessages = conversationMessages;

  while (continueLoop) {
    const response = await client.messages.create({
      model: process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-20250514',
      max_tokens: 4096,
      system: systemPrompt,
      tools,
      messages: currentMessages,
    });

    continueLoop = false;

    for (const block of response.content) {
      if (block.type === 'text') {
        segments.push({ type: 'text', content: block.text });
        if (onTextChunk) onTextChunk(block.text);
      } else if (block.type === 'tool_use') {
        // Execute the tool
        try {
          const result = await executeTool(block.name, block.input as Record<string, unknown>, session);

          // Detect phase transitions based on tool usage
          if (block.name === 'list_sources' && session.phase === 'greeting') {
            phaseTransition = 'intent';
          } else if (block.name === 'load_sample' && session.phase === 'sampling') {
            phaseTransition = 'demonstration';
          } else if (block.name === 'infer_rules') {
            phaseTransition = 'inference';
          } else if (block.name === 'build_recipe') {
            phaseTransition = 'validation';
            sessionUpdates.recipe_draft = result as Record<string, unknown>;
          } else if (block.name === 'validate_recipe') {
            phaseTransition = 'validation';
          } else if (block.name === 'run_full') {
            phaseTransition = 'execution';
            sessionUpdates.status = 'running';
          }

          // Store sample data in session
          if (block.name === 'load_sample' || block.name === 'get_source_preview') {
            const preview = result as { alias: string; rows: string[][]; columns: Array<{ name: string }> };
            if (session.left_source_alias && preview.alias === session.left_source_alias) {
              sessionUpdates.sample_left = preview.rows.map((row, _i) => {
                const obj: Record<string, unknown> = {};
                preview.columns.forEach((col, j) => { obj[col.name] = row[j]; });
                return obj;
              });
            } else if (session.right_source_alias && preview.alias === session.right_source_alias) {
              sessionUpdates.sample_right = preview.rows.map((row, _i) => {
                const obj: Record<string, unknown> = {};
                preview.columns.forEach((col, j) => { obj[col.name] = row[j]; });
                return obj;
              });
            }
          }

          // Add tool result and continue the loop
          currentMessages = [
            ...currentMessages,
            { role: 'assistant', content: response.content },
            {
              role: 'user',
              content: [{
                type: 'tool_result',
                tool_use_id: block.id,
                content: JSON.stringify(result),
              }],
            },
          ] as Anthropic.MessageParam[];

          continueLoop = true;
        } catch (err) {
          const errorMsg = err instanceof Error ? err.message : 'Tool execution failed';

          currentMessages = [
            ...currentMessages,
            { role: 'assistant', content: response.content },
            {
              role: 'user',
              content: [{
                type: 'tool_result',
                tool_use_id: block.id,
                content: JSON.stringify({ error: errorMsg }),
                is_error: true,
              }],
            },
          ] as Anthropic.MessageParam[];

          continueLoop = true;
        }
      }
    }
  }

  return { segments, phaseTransition, sessionUpdates };
}
```

**Step 2: Run TypeScript check**

Run: `cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npx tsc --noEmit src/lib/agent.ts`
Expected: No errors

**Step 3: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/src/lib/agent.ts
git commit -m "feat: add Anthropic agent orchestrator with tool calling loop"
```

---

## Task 6: Chat API Route (POST /api/chat)

**Files:**
- Create: `kalla-web/src/app/api/chat/route.ts`

**Step 1: Write the API route**

This Next.js API route handles:
- `POST /api/chat` — Send a message, get SSE-streamed agent response
- Creates sessions on first message (no session_id provided)
- Handles card responses (confirm/reject matches)

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { createSession, getSession, updateSession, addMessage } from '@/lib/session-store';
import { runAgent } from '@/lib/agent';
import type { ChatMessage, ChatSegment, CardResponse } from '@/lib/chat-types';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { session_id, message, card_response } = body as {
      session_id?: string;
      message?: string;
      card_response?: CardResponse;
    };

    // Get or create session
    let session = session_id ? getSession(session_id) : undefined;
    if (!session) {
      session = createSession();
    }

    // Build user message text
    let userText: string;
    if (card_response) {
      // Card interaction — translate to text for the LLM
      userText = `[Card response: ${card_response.action} on ${card_response.card_id}${card_response.value !== undefined ? `, value: ${JSON.stringify(card_response.value)}` : ''}]`;

      // Handle match confirmation/rejection
      if (card_response.action === 'confirm' && card_response.card_id.startsWith('match-')) {
        const matchData = card_response.value as { left: Record<string, unknown>; right: Record<string, unknown> } | undefined;
        if (matchData) {
          const pairs = [...session.confirmed_pairs, matchData];
          updateSession(session.id, { confirmed_pairs: pairs });
          session = getSession(session.id)!;
        }
      }
    } else if (message) {
      userText = message;
    } else {
      return NextResponse.json({ error: 'Either message or card_response required' }, { status: 400 });
    }

    // Add user message to session
    const userMsg: ChatMessage = {
      role: 'user',
      segments: [{ type: 'text', content: userText }],
      timestamp: new Date().toISOString(),
    };
    addMessage(session.id, userMsg);

    // Detect intent from user message for phase transitions
    if (session.phase === 'intent' && !session.left_source_alias) {
      // Try to detect source aliases from user message
      const words = userText.toLowerCase().split(/\s+/);
      // Simple heuristic — look for common patterns
      if (words.includes('invoices') || words.includes('invoice')) {
        updateSession(session.id, { left_source_alias: 'invoices' });
      }
      if (words.includes('payments') || words.includes('payment')) {
        updateSession(session.id, { right_source_alias: 'payments' });
      }
      session = getSession(session.id)!;

      if (session.left_source_alias && session.right_source_alias) {
        updateSession(session.id, { phase: 'sampling' });
        session = getSession(session.id)!;
      }
    }

    // Run the agent
    const agentResponse = await runAgent(session, userText);

    // Apply phase transitions
    if (agentResponse.phaseTransition) {
      updateSession(session.id, { phase: agentResponse.phaseTransition });
    }

    // Apply session updates
    if (agentResponse.sessionUpdates) {
      updateSession(session.id, agentResponse.sessionUpdates);
    }

    // Add agent message to session
    const agentMsg: ChatMessage = {
      role: 'agent',
      segments: agentResponse.segments,
      timestamp: new Date().toISOString(),
    };
    addMessage(session.id, agentMsg);

    // Get updated session
    session = getSession(session.id)!;

    return NextResponse.json({
      session_id: session.id,
      phase: session.phase,
      status: session.status,
      message: agentMsg,
      recipe_draft: session.recipe_draft,
    });
  } catch (err) {
    console.error('Chat API error:', err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : 'Internal error' },
      { status: 500 }
    );
  }
}
```

**Step 2: Run TypeScript check**

Run: `cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npx tsc --noEmit src/app/api/chat/route.ts`
Expected: No errors

**Step 3: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/src/app/api/chat/route.ts
git commit -m "feat: add POST /api/chat route with agent orchestration"
```

---

## Task 7: Chat Session Management API Routes

**Files:**
- Create: `kalla-web/src/app/api/chat/sessions/route.ts`
- Create: `kalla-web/src/app/api/chat/sessions/[id]/route.ts`

**Step 1: Write the sessions list/create route**

```typescript
// kalla-web/src/app/api/chat/sessions/route.ts
import { NextResponse } from 'next/server';
import { createSession } from '@/lib/session-store';

export async function POST() {
  const session = createSession();
  return NextResponse.json({
    session_id: session.id,
    phase: session.phase,
    status: session.status,
  });
}
```

**Step 2: Write the session detail route**

```typescript
// kalla-web/src/app/api/chat/sessions/[id]/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { getSession } from '@/lib/session-store';

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const session = getSession(id);
  if (!session) {
    return NextResponse.json({ error: 'Session not found' }, { status: 404 });
  }
  return NextResponse.json(session);
}
```

**Step 3: Run TypeScript check**

Run: `cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npx tsc --noEmit`
Expected: No errors

**Step 4: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/src/app/api/chat/sessions/
git commit -m "feat: add chat session management API routes"
```

---

## Task 8: Chat UI Components — Message Bubbles & Cards

**Files:**
- Create: `kalla-web/src/components/chat/ChatMessage.tsx`
- Create: `kalla-web/src/components/chat/MatchProposalCard.tsx`
- Create: `kalla-web/src/components/chat/RecipeCard.tsx`

**Step 1: Write the ChatMessage component**

```typescript
// kalla-web/src/components/chat/ChatMessage.tsx
'use client';

import type { ChatMessage as ChatMessageType, ChatSegment } from '@/lib/chat-types';
import { MatchProposalCard } from './MatchProposalCard';
import { cn } from '@/lib/utils';

interface ChatMessageProps {
  message: ChatMessageType;
  onCardAction?: (cardId: string, action: string, value?: unknown) => void;
}

export function ChatMessage({ message, onCardAction }: ChatMessageProps) {
  const isAgent = message.role === 'agent';

  return (
    <div className={cn('flex gap-3 px-4 py-3', isAgent ? '' : 'flex-row-reverse')}>
      <div
        className={cn(
          'h-8 w-8 rounded-full flex items-center justify-center text-sm font-medium shrink-0',
          isAgent ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'
        )}
      >
        {isAgent ? 'K' : 'U'}
      </div>
      <div className={cn('flex flex-col gap-2 max-w-[80%]', isAgent ? '' : 'items-end')}>
        {message.segments.map((segment, i) => (
          <SegmentRenderer
            key={i}
            segment={segment}
            isAgent={isAgent}
            onCardAction={onCardAction}
          />
        ))}
        <span className="text-xs text-muted-foreground">
          {new Date(message.timestamp).toLocaleTimeString()}
        </span>
      </div>
    </div>
  );
}

function SegmentRenderer({
  segment,
  isAgent,
  onCardAction,
}: {
  segment: ChatSegment;
  isAgent: boolean;
  onCardAction?: (cardId: string, action: string, value?: unknown) => void;
}) {
  if (segment.type === 'text' && segment.content) {
    return (
      <div
        className={cn(
          'rounded-lg px-4 py-2 text-sm whitespace-pre-wrap',
          isAgent
            ? 'bg-muted text-foreground'
            : 'bg-primary text-primary-foreground'
        )}
      >
        {segment.content}
      </div>
    );
  }

  if (segment.type === 'card' && segment.card_type === 'match_proposal') {
    return (
      <MatchProposalCard
        cardId={segment.card_id!}
        data={segment.data!}
        onAction={onCardAction}
      />
    );
  }

  return null;
}
```

**Step 2: Write the MatchProposalCard component**

```typescript
// kalla-web/src/components/chat/MatchProposalCard.tsx
'use client';

import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Check, X, HelpCircle } from 'lucide-react';

interface MatchProposalCardProps {
  cardId: string;
  data: Record<string, unknown>;
  onAction?: (cardId: string, action: string, value?: unknown) => void;
}

export function MatchProposalCard({ cardId, data, onAction }: MatchProposalCardProps) {
  const [responded, setResponded] = useState(false);
  const [response, setResponse] = useState<string | null>(null);

  const leftRow = (data.left || {}) as Record<string, string>;
  const rightRow = (data.right || {}) as Record<string, string>;
  const reasoning = data.reasoning as string || '';

  const handleAction = (action: string) => {
    setResponded(true);
    setResponse(action);
    onAction?.(cardId, action, action === 'confirm' ? { left: leftRow, right: rightRow } : undefined);
  };

  return (
    <Card className="w-full max-w-lg">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center gap-2">
          Match Proposal
          {responded && (
            <Badge variant={response === 'confirm' ? 'default' : 'destructive'}>
              {response === 'confirm' ? 'Confirmed' : 'Rejected'}
            </Badge>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="grid grid-cols-2 gap-3 text-xs">
          <div>
            <p className="font-medium text-muted-foreground mb-1">Left Source</p>
            {Object.entries(leftRow).slice(0, 5).map(([k, v]) => (
              <p key={k}><span className="font-medium">{k}:</span> {v}</p>
            ))}
          </div>
          <div>
            <p className="font-medium text-muted-foreground mb-1">Right Source</p>
            {Object.entries(rightRow).slice(0, 5).map(([k, v]) => (
              <p key={k}><span className="font-medium">{k}:</span> {v}</p>
            ))}
          </div>
        </div>
        {reasoning && (
          <p className="text-xs text-muted-foreground italic">{reasoning}</p>
        )}
        {!responded && (
          <div className="flex gap-2">
            <Button size="sm" onClick={() => handleAction('confirm')}>
              <Check className="mr-1 h-3 w-3" /> Yes, match
            </Button>
            <Button size="sm" variant="destructive" onClick={() => handleAction('reject')}>
              <X className="mr-1 h-3 w-3" /> No
            </Button>
            <Button size="sm" variant="outline" onClick={() => handleAction('unsure')}>
              <HelpCircle className="mr-1 h-3 w-3" /> Not sure
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
```

**Step 3: Write the RecipeCard component**

```typescript
// kalla-web/src/components/chat/RecipeCard.tsx
'use client';

import { useState } from 'react';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ChevronDown, ChevronUp, FileText } from 'lucide-react';

interface RecipeCardProps {
  recipe: Record<string, unknown> | null;
}

export function RecipeCard({ recipe }: RecipeCardProps) {
  const [expanded, setExpanded] = useState(false);
  const [showJson, setShowJson] = useState(false);

  if (!recipe) {
    return (
      <div className="fixed bottom-0 left-0 right-0 border-t bg-background/95 backdrop-blur px-4 py-2">
        <div className="container mx-auto flex items-center gap-2 text-sm text-muted-foreground">
          <FileText className="h-4 w-4" />
          Recipe: No rules defined yet
        </div>
      </div>
    );
  }

  const matchRules = (recipe.match_rules as Array<Record<string, unknown>>) || [];
  const sources = recipe.sources as Record<string, Record<string, unknown>> | undefined;

  return (
    <div className="fixed bottom-0 left-0 right-0 border-t bg-background/95 backdrop-blur">
      <div className="container mx-auto">
        <button
          onClick={() => setExpanded(!expanded)}
          className="w-full flex items-center justify-between px-4 py-2 text-sm hover:bg-muted/50 transition-colors"
        >
          <div className="flex items-center gap-2">
            <FileText className="h-4 w-4" />
            <span className="font-medium">
              Recipe: {matchRules.length} rule{matchRules.length !== 1 ? 's' : ''} defined
            </span>
            {sources && (
              <span className="text-muted-foreground">
                ({(sources.left as Record<string, unknown>)?.alias} ↔ {(sources.right as Record<string, unknown>)?.alias})
              </span>
            )}
          </div>
          {expanded ? <ChevronDown className="h-4 w-4" /> : <ChevronUp className="h-4 w-4" />}
        </button>

        {expanded && (
          <Card className="mx-4 mb-4 border-t-0 rounded-t-none">
            <CardContent className="pt-4 space-y-3">
              {matchRules.map((rule, i) => {
                const conditions = (rule.conditions as Array<Record<string, unknown>>) || [];
                return (
                  <div key={i} className="p-3 rounded-lg border text-sm">
                    <div className="flex items-center justify-between mb-1">
                      <span className="font-medium">{rule.name as string}</span>
                      <Badge variant="outline">{rule.pattern as string}</Badge>
                    </div>
                    <ul className="text-muted-foreground space-y-0.5">
                      {conditions.map((c, j) => (
                        <li key={j}>
                          {c.left as string} {c.op as string} {c.right as string}
                          {c.threshold !== undefined && ` (±${c.threshold})`}
                        </li>
                      ))}
                    </ul>
                  </div>
                );
              })}

              <div className="flex gap-2">
                <Button size="sm" variant="outline" onClick={() => setShowJson(!showJson)}>
                  {showJson ? 'Hide JSON' : 'Show JSON'}
                </Button>
              </div>

              {showJson && (
                <pre className="text-xs bg-muted p-3 rounded-lg overflow-x-auto max-h-60">
                  {JSON.stringify(recipe, null, 2)}
                </pre>
              )}
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
```

**Step 4: Run TypeScript check**

Run: `cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npx tsc --noEmit`
Expected: No errors

**Step 5: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/src/components/chat/
git commit -m "feat: add chat UI components — messages, match proposals, recipe card"
```

---

## Task 9: Chat Page (Replace /reconcile)

**Files:**
- Modify: `kalla-web/src/app/reconcile/page.tsx` (full rewrite)

**Step 1: Write the chat page**

Replace the entire file with the conversational chat UI:

```typescript
'use client';

import { useState, useRef, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Loader2, Send, RotateCcw } from 'lucide-react';
import { ChatMessage } from '@/components/chat/ChatMessage';
import { RecipeCard } from '@/components/chat/RecipeCard';
import type { ChatMessage as ChatMessageType, CardResponse } from '@/lib/chat-types';

export default function ReconcilePage() {
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [messages, setMessages] = useState<ChatMessageType[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [phase, setPhase] = useState('greeting');
  const [recipeDraft, setRecipeDraft] = useState<Record<string, unknown> | null>(null);
  const [started, setStarted] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // Focus input after loading completes
  useEffect(() => {
    if (!loading) inputRef.current?.focus();
  }, [loading]);

  const sendMessage = async (text: string, cardResponse?: CardResponse) => {
    if (!text.trim() && !cardResponse) return;

    setLoading(true);

    // Add user message to UI immediately
    if (!cardResponse) {
      const userMsg: ChatMessageType = {
        role: 'user',
        segments: [{ type: 'text', content: text }],
        timestamp: new Date().toISOString(),
      };
      setMessages(prev => [...prev, userMsg]);
      setInput('');
    }

    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_id: sessionId,
          message: cardResponse ? undefined : text,
          card_response: cardResponse,
        }),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(errData.error || 'Request failed');
      }

      const data = await res.json();
      setSessionId(data.session_id);
      setPhase(data.phase);
      if (data.recipe_draft) setRecipeDraft(data.recipe_draft);

      // Add agent message
      if (data.message) {
        setMessages(prev => [...prev, data.message]);
      }
    } catch (err) {
      // Show error as agent message
      const errMsg: ChatMessageType = {
        role: 'agent',
        segments: [{ type: 'text', content: `Error: ${err instanceof Error ? err.message : 'Something went wrong'}` }],
        timestamp: new Date().toISOString(),
      };
      setMessages(prev => [...prev, errMsg]);
    } finally {
      setLoading(false);
    }
  };

  const handleCardAction = (cardId: string, action: string, value?: unknown) => {
    sendMessage('', { card_id: cardId, action, value });
  };

  const handleStart = () => {
    setStarted(true);
    sendMessage('Hello, I want to reconcile some data.');
  };

  const handleReset = () => {
    setSessionId(null);
    setMessages([]);
    setInput('');
    setPhase('greeting');
    setRecipeDraft(null);
    setStarted(false);
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (input.trim() && !loading) sendMessage(input);
  };

  if (!started) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh] space-y-6">
        <div className="text-center space-y-2">
          <h1 className="text-3xl font-bold tracking-tight">Recipe Builder</h1>
          <p className="text-muted-foreground max-w-md">
            Build reconciliation recipes by demonstrating matches with examples.
            The AI agent will guide you through the process.
          </p>
        </div>
        <Button size="lg" onClick={handleStart}>
          Start Conversation
        </Button>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-[calc(100vh-8rem)]">
      {/* Header */}
      <div className="flex items-center justify-between border-b px-4 py-2">
        <div className="flex items-center gap-2">
          <h1 className="text-lg font-semibold">Recipe Builder</h1>
          <span className="text-xs text-muted-foreground capitalize bg-muted px-2 py-0.5 rounded">
            {phase}
          </span>
        </div>
        <Button variant="ghost" size="sm" onClick={handleReset}>
          <RotateCcw className="h-4 w-4 mr-1" />
          Reset
        </Button>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto pb-32">
        {messages.map((msg, i) => (
          <ChatMessage key={i} message={msg} onCardAction={handleCardAction} />
        ))}
        {loading && (
          <div className="flex gap-3 px-4 py-3">
            <div className="h-8 w-8 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-sm font-medium">
              K
            </div>
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Thinking...
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="border-t px-4 py-3 bg-background">
        <form onSubmit={handleSubmit} className="flex gap-2 max-w-3xl mx-auto">
          <Input
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={loading ? 'Waiting for response...' : 'Type your message...'}
            disabled={loading}
            className="flex-1"
          />
          <Button type="submit" disabled={loading || !input.trim()}>
            <Send className="h-4 w-4" />
          </Button>
        </form>
      </div>

      {/* Recipe Card */}
      <RecipeCard recipe={recipeDraft} />
    </div>
  );
}
```

**Step 2: Run TypeScript check**

Run: `cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npx tsc --noEmit`
Expected: No errors

**Step 3: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/src/app/reconcile/page.tsx
git commit -m "feat: replace form-based reconcile page with chat UI"
```

---

## Task 10: Environment Configuration

**Files:**
- Modify: `kalla-web/next.config.ts`

**Step 1: Add env configuration to Next.js config**

The `ANTHROPIC_API_KEY` must be available in server-side API routes. Next.js automatically loads `.env` files, but we need to ensure the key is available. Add `serverExternalPackages` for `pg` (PostgreSQL client).

```typescript
import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: 'standalone',
  serverExternalPackages: ['pg'],
};

export default nextConfig;
```

**Step 2: Verify .env has ANTHROPIC_API_KEY**

The `.env` file already has `ANTHROPIC_API_KEY` set. Next.js will automatically pick it up for server-side code (API routes). No changes needed to `.env`.

**Step 3: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/next.config.ts
git commit -m "feat: configure Next.js for pg and server env"
```

---

## Task 11: Playwright Configuration

**Files:**
- Create: `kalla-web/playwright.config.ts`

**Step 1: Write Playwright config**

```typescript
import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false, // Run sequentially — tests share DB state
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: 'html',
  timeout: 120_000, // 2 minutes — agent conversations take time
  expect: {
    timeout: 30_000, // 30 seconds for assertions
  },
  use: {
    baseURL: 'http://localhost:3000',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: [
    {
      // Rust backend must be running (via docker-compose)
      command: 'echo "Rust backend expected on port 3001"',
      port: 3001,
      reuseExistingServer: true,
    },
    {
      command: 'npm run dev',
      port: 3000,
      reuseExistingServer: !process.env.CI,
      env: {
        ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY || '',
        NEXT_PUBLIC_API_URL: 'http://localhost:3001',
      },
    },
  ],
});
```

**Step 2: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/playwright.config.ts
git commit -m "feat: add Playwright config for integration tests"
```

---

## Task 12: Integration Test — Scenario 1: Full Conversation Flow

**Files:**
- Create: `kalla-web/e2e/scenario-1-full-flow.spec.ts`

**Step 1: Write the full conversation flow test**

This test walks through the complete happy path: greeting → source selection → sample loading → match demonstration → rule inference → recipe validation → execution.

```typescript
import { test, expect } from '@playwright/test';

test.describe('Scenario 1: Full Conversation Flow — Invoice to Payment Reconciliation', () => {
  test('completes full agentic recipe building flow', async ({ page }) => {
    // Navigate to the reconcile page
    await page.goto('/reconcile');
    await expect(page.getByText('Recipe Builder')).toBeVisible();

    // Click "Start Conversation"
    await page.getByRole('button', { name: 'Start Conversation' }).click();

    // Wait for the agent's greeting message
    await expect(page.locator('[class*="bg-muted"]').first()).toBeVisible({ timeout: 60_000 });

    // Verify agent greeted and mentioned data sources
    const firstAgentMessage = page.locator('[class*="bg-muted"]').first();
    await expect(firstAgentMessage).toContainText(/source|data|invoices|payments/i, { timeout: 60_000 });

    // User states intent: reconcile invoices with payments
    const input = page.getByPlaceholder('Type your message...');
    await input.fill('I want to reconcile invoices with payments');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();

    // Wait for agent response about sources
    await expect(page.locator('[class*="bg-muted"]').nth(1)).toBeVisible({ timeout: 60_000 });

    // Phase should progress — agent should ask about sampling or show data
    // User provides sample criteria
    await input.fill('Load a sample of the invoices and payments — all records are fine for now');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();

    // Wait for agent to show sample data or ask about matches
    await expect(page.locator('[class*="bg-muted"]').nth(2)).toBeVisible({ timeout: 60_000 });

    // User confirms a match: tell the agent about a known matching pair
    await input.fill('INV-2024-001 matches PAY-2024-001 — same customer Acme Corporation, same amount $15,000');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();

    // Wait for agent to acknowledge and potentially propose more matches
    await expect(page.locator('[class*="bg-muted"]').nth(3)).toBeVisible({ timeout: 60_000 });

    // Provide another example
    await input.fill('INV-2024-002 also matches PAY-2024-002, both are $7,500.50 from TechStart Inc');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();

    await expect(page.locator('[class*="bg-muted"]').nth(4)).toBeVisible({ timeout: 60_000 });

    // Ask agent to build the recipe based on examples
    await input.fill('I think those examples are enough. Can you build a recipe from these patterns?');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();

    // Wait for agent to propose rules or build recipe
    await expect(page.locator('[class*="bg-muted"]').nth(5)).toBeVisible({ timeout: 60_000 });

    // Verify the conversation progressed — at least 6 agent messages
    const agentMessages = page.locator('[class*="bg-muted"]');
    const count = await agentMessages.count();
    expect(count).toBeGreaterThanOrEqual(6);

    // Verify the phase indicator shows progress
    const phaseIndicator = page.locator('text=/greeting|intent|sampling|demonstration|inference|validation|execution/i');
    await expect(phaseIndicator).toBeVisible();
  });
});
```

**Step 2: Run the test (requires Docker services running)**

Run:
```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && ANTHROPIC_API_KEY=$(grep ANTHROPIC_API_KEY ../.env | cut -d= -f2) npx playwright test e2e/scenario-1-full-flow.spec.ts --reporter=list
```
Expected: Test passes (requires Rust backend on 3001 + PostgreSQL running)

**Step 3: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/e2e/scenario-1-full-flow.spec.ts
git commit -m "test: add e2e scenario 1 — full conversation flow"
```

---

## Task 13: Integration Test — Scenario 2: Source Discovery & Preview

**Files:**
- Create: `kalla-web/e2e/scenario-2-source-discovery.spec.ts`

**Step 1: Write the source discovery test**

This test validates the agent correctly discovers and presents available data sources, shows previews, and handles source selection.

```typescript
import { test, expect } from '@playwright/test';

test.describe('Scenario 2: Source Discovery & Data Preview', () => {
  test('agent lists sources and shows data previews', async ({ page }) => {
    await page.goto('/reconcile');

    // Start the conversation
    await page.getByRole('button', { name: 'Start Conversation' }).click();

    // Wait for greeting — agent should mention available sources
    const firstMessage = page.locator('[class*="bg-muted"]').first();
    await expect(firstMessage).toBeVisible({ timeout: 60_000 });

    // Ask the agent to show all available sources
    const input = page.getByPlaceholder('Type your message...');
    await input.fill('What data sources do I have available?');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();

    // Agent should list sources — expect to see invoices and payments mentioned
    const sourceResponse = page.locator('[class*="bg-muted"]').nth(1);
    await expect(sourceResponse).toBeVisible({ timeout: 60_000 });
    await expect(sourceResponse).toContainText(/invoices|payments/i, { timeout: 10_000 });

    // Ask for a preview of invoices
    await input.fill('Show me a preview of the invoices source');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();

    // Agent should show column info or sample data
    const previewResponse = page.locator('[class*="bg-muted"]').nth(2);
    await expect(previewResponse).toBeVisible({ timeout: 60_000 });
    // Should mention column names from the invoices table
    await expect(previewResponse).toContainText(/invoice_id|customer|amount/i, { timeout: 10_000 });

    // Ask for payments preview
    await input.fill('Now show me the payments source');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();

    const paymentsPreview = page.locator('[class*="bg-muted"]').nth(3);
    await expect(paymentsPreview).toBeVisible({ timeout: 60_000 });
    await expect(paymentsPreview).toContainText(/payment_id|payer|amount|paid_amount/i, { timeout: 10_000 });
  });

  test('agent handles non-existent source gracefully', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();

    await expect(page.locator('[class*="bg-muted"]').first()).toBeVisible({ timeout: 60_000 });

    const input = page.getByPlaceholder('Type your message...');
    await input.fill('Show me the source called "nonexistent_table"');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();

    // Agent should respond with an error or explain the source doesn't exist
    const response = page.locator('[class*="bg-muted"]').nth(1);
    await expect(response).toBeVisible({ timeout: 60_000 });
    // Should not crash — should explain the source is not found
    await expect(response).toContainText(/not found|doesn't exist|error|available/i, { timeout: 10_000 });
  });
});
```

**Step 2: Run the test**

Run:
```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && ANTHROPIC_API_KEY=$(grep ANTHROPIC_API_KEY ../.env | cut -d= -f2) npx playwright test e2e/scenario-2-source-discovery.spec.ts --reporter=list
```
Expected: Both tests pass

**Step 3: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/e2e/scenario-2-source-discovery.spec.ts
git commit -m "test: add e2e scenario 2 — source discovery and preview"
```

---

## Task 14: Integration Test — Scenario 3: Session Persistence & Reset

**Files:**
- Create: `kalla-web/e2e/scenario-3-session-management.spec.ts`

**Step 1: Write the session management test**

This test validates chat session creation, message persistence across interactions, and the reset functionality.

```typescript
import { test, expect } from '@playwright/test';

test.describe('Scenario 3: Session Management & Reset', () => {
  test('maintains conversation context across multiple messages', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();

    // Wait for greeting
    await expect(page.locator('[class*="bg-muted"]').first()).toBeVisible({ timeout: 60_000 });

    const input = page.getByPlaceholder('Type your message...');

    // First message — establish context
    await input.fill('I want to work with invoices and payments');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(1)).toBeVisible({ timeout: 60_000 });

    // Second message — reference previous context
    await input.fill('Tell me more about the invoices source — what columns does it have?');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(2)).toBeVisible({ timeout: 60_000 });

    // Verify conversation history is visible — should have user messages and agent messages
    const allMessages = page.locator('[class*="rounded-lg"][class*="px-4"][class*="py-2"]');
    const messageCount = await allMessages.count();
    // At minimum: greeting + user1 + agent1 + user2 + agent2 = 5 message bubbles
    expect(messageCount).toBeGreaterThanOrEqual(5);

    // Third message — agent should remember we're talking about invoices
    await input.fill('What about the customer_name column? Is it useful for matching?');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(3)).toBeVisible({ timeout: 60_000 });

    // Agent should reference invoices context from earlier messages
    const contextResponse = page.locator('[class*="bg-muted"]').nth(3);
    // Agent should show understanding of the conversation context
    await expect(contextResponse).toBeVisible();
  });

  test('reset clears conversation and starts fresh', async ({ page }) => {
    await page.goto('/reconcile');
    await page.getByRole('button', { name: 'Start Conversation' }).click();

    // Wait for greeting
    await expect(page.locator('[class*="bg-muted"]').first()).toBeVisible({ timeout: 60_000 });

    // Send a message
    const input = page.getByPlaceholder('Type your message...');
    await input.fill('Show me invoices');
    await page.getByRole('button').filter({ has: page.locator('svg') }).last().click();
    await expect(page.locator('[class*="bg-muted"]').nth(1)).toBeVisible({ timeout: 60_000 });

    // Verify messages exist
    const messagesBeforeReset = page.locator('[class*="bg-muted"]');
    const countBefore = await messagesBeforeReset.count();
    expect(countBefore).toBeGreaterThanOrEqual(2);

    // Click reset
    await page.getByRole('button', { name: 'Reset' }).click();

    // Should return to the start screen
    await expect(page.getByRole('button', { name: 'Start Conversation' })).toBeVisible();

    // No messages should be visible
    await expect(page.locator('[class*="bg-muted"]')).toHaveCount(0);

    // Start again — should get a fresh greeting
    await page.getByRole('button', { name: 'Start Conversation' }).click();
    await expect(page.locator('[class*="bg-muted"]').first()).toBeVisible({ timeout: 60_000 });

    // Fresh greeting — only one agent message
    const messagesAfterReset = page.locator('[class*="bg-muted"]');
    const countAfter = await messagesAfterReset.count();
    expect(countAfter).toBe(1);
  });

  test('chat API returns proper session structure', async ({ request }) => {
    // Test the API directly to verify session management
    const response = await request.post('/api/chat', {
      data: {
        message: 'Hello, what sources do I have?',
      },
    });

    expect(response.ok()).toBeTruthy();
    const body = await response.json();

    // Verify response structure
    expect(body).toHaveProperty('session_id');
    expect(body).toHaveProperty('phase');
    expect(body).toHaveProperty('status');
    expect(body).toHaveProperty('message');

    // Verify message structure
    expect(body.message).toHaveProperty('role', 'agent');
    expect(body.message).toHaveProperty('segments');
    expect(body.message).toHaveProperty('timestamp');
    expect(body.message.segments.length).toBeGreaterThan(0);

    // Send follow-up with session_id
    const followUp = await request.post('/api/chat', {
      data: {
        session_id: body.session_id,
        message: 'Tell me about the invoices source',
      },
    });

    expect(followUp.ok()).toBeTruthy();
    const followUpBody = await followUp.json();

    // Same session ID should be returned
    expect(followUpBody.session_id).toBe(body.session_id);
  });
});
```

**Step 2: Run the test**

Run:
```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && ANTHROPIC_API_KEY=$(grep ANTHROPIC_API_KEY ../.env | cut -d= -f2) npx playwright test e2e/scenario-3-session-management.spec.ts --reporter=list
```
Expected: All tests pass

**Step 3: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/e2e/scenario-3-session-management.spec.ts
git commit -m "test: add e2e scenario 3 — session management and reset"
```

---

## Task 15: Add `pg` Dependency & Fix TypeScript Issues

**Files:**
- Modify: `kalla-web/package.json`

The session store uses `pg` for PostgreSQL persistence. Since this runs server-side only (API routes), we need the `pg` package.

**Step 1: Install pg**

Run:
```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npm install pg && npm install -D @types/pg
```

**Step 2: Run full TypeScript check**

Run: `cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && npx tsc --noEmit`
Expected: No errors

**Step 3: Commit**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add kalla-web/package.json kalla-web/package-lock.json
git commit -m "feat: add pg for server-side PostgreSQL access"
```

---

## Task 16: Run All Integration Tests

**Step 1: Start the backend services**

Run:
```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && docker compose up -d postgres server
```
Expected: PostgreSQL and Rust server start successfully

**Step 2: Wait for services to be ready**

Run:
```bash
curl -s http://localhost:3001/health
```
Expected: Returns "OK"

**Step 3: Run all Playwright tests**

Run:
```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/kalla-web && ANTHROPIC_API_KEY=$(grep ANTHROPIC_API_KEY ../.env | cut -d= -f2) npx playwright test --reporter=list
```
Expected: All 6 tests across 3 scenario files pass

**Step 4: Commit all remaining files**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla && git add -A
git commit -m "feat: complete agentic recipe builder with Playwright integration tests"
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Install dependencies | package.json |
| 2 | Chat types & constants | src/lib/chat-types.ts |
| 3 | Session store | src/lib/session-store.ts |
| 4 | Agent tool executor | src/lib/agent-tools.ts |
| 5 | Anthropic orchestrator | src/lib/agent.ts |
| 6 | Chat API route | src/app/api/chat/route.ts |
| 7 | Session management APIs | src/app/api/chat/sessions/ |
| 8 | Chat UI components | src/components/chat/*.tsx |
| 9 | Chat page (replace /reconcile) | src/app/reconcile/page.tsx |
| 10 | Env configuration | next.config.ts |
| 11 | Playwright config | playwright.config.ts |
| 12 | E2E test: Full flow | e2e/scenario-1-full-flow.spec.ts |
| 13 | E2E test: Source discovery | e2e/scenario-2-source-discovery.spec.ts |
| 14 | E2E test: Session management | e2e/scenario-3-session-management.spec.ts |
| 15 | pg dependency | package.json |
| 16 | Run all tests | — |
