import Anthropic from '@anthropic-ai/sdk';
import type {
  ChatSession,
  ChatPhase,
  ChatSegment,
  AgentTool,
  PhaseConfig,
  ContextInjection,
  ColumnInfo,
  SourceInfo,
  FilterCondition,
} from './chat-types';
import { PHASES, PHASE_ORDER } from './chat-types';
import { executeTool } from './agent-tools';

// ---------------------------------------------------------------------------
// Anthropic client (lazy singleton per-request)
// ---------------------------------------------------------------------------

function getClient(): Anthropic {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error('ANTHROPIC_API_KEY is not set');
  const baseURL = process.env.ANTHROPIC_BASE_URL || process.env.LLM_API_URL;
  return new Anthropic({ apiKey, ...(baseURL ? { baseURL } : {}) });
}

const MODEL = process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-20250514';

// ---------------------------------------------------------------------------
// Tool Definitions for Claude tool_use
//
// Each entry maps to an agent tool. The input_schema follows JSON Schema and
// is typed as `Anthropic.Tool.InputSchema` which requires `type: 'object'`.
// ---------------------------------------------------------------------------

const TOOL_DEFINITIONS: Anthropic.Tool[] = [
  {
    name: 'list_sources',
    description:
      'List all registered data sources with their aliases, URIs, types, and connection status.',
    input_schema: {
      type: 'object' as const,
      properties: {},
      required: [],
    },
  },
  {
    name: 'get_source_preview',
    description:
      'Get schema info and sample rows from a data source. Provide either alias (for registered sources) or s3_uri (for uploaded files). Returns column names, data types, and a preview of rows.',
    input_schema: {
      type: 'object' as const,
      properties: {
        alias: { type: 'string', description: 'The alias of a registered data source to preview' },
        limit: { type: 'number', description: 'Max rows to return (default 10, max 100)' },
        s3_uri: {
          type: 'string',
          description: 'S3 URI of an uploaded file to preview (use this for user-uploaded CSV files)',
        },
      },
      required: [],
    },
  },
  {
    name: 'load_scoped',
    description:
      "Load a filtered subset of rows from a data source. Pass structured filter conditions that will be translated to the source's native query language.",
    input_schema: {
      type: 'object' as const,
      properties: {
        alias: { type: 'string', description: 'The alias of the data source' },
        conditions: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              column: { type: 'string', description: 'Column name to filter on' },
              op: {
                type: 'string',
                enum: ['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'between', 'in', 'like'],
                description: 'Filter operator',
              },
              value: {
                description:
                  'Filter value - string, number, array of strings, or [from, to] for between',
              },
            },
            required: ['column', 'op', 'value'],
          },
          description: 'Filter conditions to scope the data',
        },
        limit: { type: 'number', description: 'Max rows to load (default 200, max 1000)' },
      },
      required: ['alias', 'conditions'],
    },
  },
  {
    name: 'propose_match',
    description:
      'Propose a candidate match between a left-source row and a right-source row. Include reasoning about why they might match. The user will confirm or reject.',
    input_schema: {
      type: 'object' as const,
      properties: {
        left_row: {
          type: 'object',
          description: 'The left source row as key-value pairs',
        },
        right_row: {
          type: 'object',
          description: 'The right source row as key-value pairs',
        },
        reasoning: {
          type: 'string',
          description: 'Explanation of why these rows might match',
        },
      },
      required: ['left_row', 'right_row', 'reasoning'],
    },
  },
  {
    name: 'infer_rules',
    description:
      'Analyze confirmed match pairs to detect column-level matching rules. Returns candidate rules with confidence scores.',
    input_schema: {
      type: 'object' as const,
      properties: {
        left_columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Column names from the left source',
        },
        right_columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Column names from the right source',
        },
      },
      required: ['left_columns', 'right_columns'],
    },
  },
  {
    name: 'build_recipe',
    description:
      'Build a reconciliation recipe with SQL matching logic. Write a SELECT that joins left_src and right_src (use these as table aliases in your SQL). The SQL should express the matching conditions derived from infer_rules output.',
    input_schema: {
      type: 'object' as const,
      properties: {
        name: { type: 'string', description: 'Human-readable recipe name' },
        description: { type: 'string', description: 'What this recipe reconciles' },
        match_sql: {
          type: 'string',
          description: 'SQL SELECT joining left_src and right_src using matching conditions. Use left_src and right_src as table aliases.',
        },
        match_description: {
          type: 'string',
          description: 'Plain-language description of the matching logic',
        },
        left_alias: { type: 'string' },
        right_alias: { type: 'string' },
        left_uri: { type: 'string' },
        right_uri: { type: 'string' },
        left_pk: {
          type: 'array',
          items: { type: 'string' },
          description: 'Primary key column(s) of the left source',
        },
        right_pk: {
          type: 'array',
          items: { type: 'string' },
          description: 'Primary key column(s) of the right source',
        },
        left_schema: {
          type: 'array',
          items: { type: 'string' },
          description: 'Column names of the left source',
        },
        right_schema: {
          type: 'array',
          items: { type: 'string' },
          description: 'Column names of the right source',
        },
      },
      required: [
        'name',
        'match_sql',
        'match_description',
        'left_alias',
        'right_alias',
        'left_uri',
        'right_uri',
        'left_pk',
        'right_pk',
        'left_schema',
        'right_schema',
      ],
    },
  },
  {
    name: 'save_recipe',
    description:
      'Save a built recipe to the database. Call this after build_recipe to persist the recipe.',
    input_schema: {
      type: 'object' as const,
      properties: {
        recipe: {
          type: 'object',
          description: 'The recipe object returned by build_recipe',
        },
      },
      required: ['recipe'],
    },
  },
  {
    name: 'validate_recipe',
    description:
      'Validate a recipe structure locally. Checks that required fields (recipe_id, name, match_sql, sources) are present.',
    input_schema: {
      type: 'object' as const,
      properties: {
        recipe: {
          type: 'object',
          description: 'The recipe object to validate',
        },
      },
      required: ['recipe'],
    },
  },
  {
    name: 'run_sample',
    description:
      'Execute a saved recipe against sample data. Returns match statistics. Requires a saved recipe_id.',
    input_schema: {
      type: 'object' as const,
      properties: {
        recipe_id: {
          type: 'string',
          description: 'The recipe_id of a saved recipe',
        },
      },
      required: ['recipe_id'],
    },
  },
  {
    name: 'run_full',
    description:
      'Execute a saved recipe against the full datasets. Returns a run_id for tracking progress. Requires a saved recipe_id.',
    input_schema: {
      type: 'object' as const,
      properties: {
        recipe_id: {
          type: 'string',
          description: 'The recipe_id of a saved recipe',
        },
      },
      required: ['recipe_id'],
    },
  },
  {
    name: 'request_file_upload',
    description:
      'Ask the user to upload a CSV file. Use when you need a file from the user (sample data, source file for reconciliation).',
    input_schema: {
      type: 'object' as const,
      properties: {
        message: {
          type: 'string',
          description: 'Context message explaining what file is needed and why',
        },
      },
      required: ['message'],
    },
  },
];

// ---------------------------------------------------------------------------
// Prerequisite Checker
//
// Validates that all required session fields are populated before entering
// a phase. Throws with a descriptive error listing missing fields.
// ---------------------------------------------------------------------------

/**
 * Verify that all prerequisite session fields are populated.
 *
 * Fields are considered "missing" if they are:
 * - null or undefined (for object/array fields)
 * - false (for boolean fields like validation_approved)
 * - empty arrays (e.g., confirmed_pairs with length 0)
 *
 * Throws an error listing all missing fields if any prerequisites are unmet.
 */
export function checkPrerequisites(config: PhaseConfig, session: ChatSession): void {
  const missing: string[] = [];

  for (const field of config.prerequisites.sessionFields) {
    const value = session[field];

    // Boolean fields: check for `=== true` (specifically for validation_approved)
    if (typeof value === 'boolean') {
      if (value !== true) {
        missing.push(field);
      }
      continue;
    }

    // Array fields: check for null or empty
    if (Array.isArray(value)) {
      if (value.length === 0) {
        missing.push(field);
      }
      continue;
    }

    // All other fields: check for null/undefined
    if (value === null || value === undefined) {
      missing.push(field);
    }
  }

  if (missing.length > 0) {
    throw new Error(
      `Phase "${config.name}" prerequisites not met. Missing: ${missing.join(', ')}`,
    );
  }
}

// ---------------------------------------------------------------------------
// Context Injection Builder
//
// Produces a formatted string of contextual data to append to the system
// prompt. Each injection type maps to a labeled section.
// ---------------------------------------------------------------------------

const SAMPLE_ROW_LIMIT = 20;

function formatSourcesList(sources: SourceInfo[]): string {
  const lines = sources.map(
    (s) => `  - ${s.alias} (${s.source_type}) [${s.status}] ${s.uri}`,
  );
  return `\n\nAVAILABLE SOURCES:\n${lines.join('\n')}`;
}

function formatSchema(label: string, columns: ColumnInfo[]): string {
  const lines = columns.map(
    (c) => `  - ${c.name}: ${c.data_type}${c.nullable ? ' (nullable)' : ''}`,
  );
  return `\n\n${label}:\n${lines.join('\n')}`;
}

function formatSampleData(
  label: string,
  rows: Record<string, unknown>[],
): string {
  const total = rows.length;
  const truncated = rows.slice(0, SAMPLE_ROW_LIMIT);
  const header =
    total > SAMPLE_ROW_LIMIT
      ? `${label} (Showing ${SAMPLE_ROW_LIMIT} of ${total} rows):`
      : `${label} (${total} rows):`;
  return `\n\n${header}\n${JSON.stringify(truncated, null, 2)}`;
}

export function buildContextInjections(
  config: PhaseConfig,
  session: ChatSession,
): string {
  const parts: string[] = [];

  for (const injection of config.contextInjections) {
    switch (injection) {
      case 'sources_list': {
        if (session.sources_list && session.sources_list.length > 0) {
          parts.push(formatSourcesList(session.sources_list));
        }
        break;
      }
      case 'schema_left': {
        if (session.schema_left && session.schema_left.length > 0) {
          parts.push(formatSchema('LEFT SOURCE SCHEMA', session.schema_left));
        }
        break;
      }
      case 'schema_right': {
        if (session.schema_right && session.schema_right.length > 0) {
          parts.push(formatSchema('RIGHT SOURCE SCHEMA', session.schema_right));
        }
        break;
      }
      case 'sample_left': {
        if (session.sample_left && session.sample_left.length > 0) {
          parts.push(formatSampleData('LEFT SOURCE DATA', session.sample_left));
        }
        break;
      }
      case 'sample_right': {
        if (session.sample_right && session.sample_right.length > 0) {
          parts.push(
            formatSampleData('RIGHT SOURCE DATA', session.sample_right),
          );
        }
        break;
      }
      case 'confirmed_pairs': {
        if (session.confirmed_pairs && session.confirmed_pairs.length > 0) {
          parts.push(
            `\n\nCONFIRMED MATCH PAIRS: ${session.confirmed_pairs.length} pairs\n${JSON.stringify(session.confirmed_pairs, null, 2)}`,
          );
        }
        break;
      }
      case 'recipe_draft': {
        if (session.recipe_draft) {
          parts.push(
            `\n\nCURRENT RECIPE DRAFT:\n${JSON.stringify(session.recipe_draft, null, 2)}`,
          );
        }
        break;
      }
    }
  }

  return parts.join('');
}

// ---------------------------------------------------------------------------
// Phase-aware Tool Filtering
//
// Returns the full Anthropic.Tool definitions for tools allowed in the given
// phase, optionally excluding tools that have exhausted their retry budget.
// ---------------------------------------------------------------------------

export function getPhaseTools(
  phase: ChatPhase,
  exhaustedTools?: Set<string>,
): Anthropic.Tool[] {
  const allowed: AgentTool[] = PHASES[phase].tools;
  return TOOL_DEFINITIONS.filter((t) => {
    if (!(allowed as string[]).includes(t.name)) return false;
    if (exhaustedTools && exhaustedTools.has(t.name)) return false;
    return true;
  });
}

// ---------------------------------------------------------------------------
// System Prompt Builder
// ---------------------------------------------------------------------------

function buildSystemPrompt(session: ChatSession, config: PhaseConfig): string {
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
    `CURRENT PHASE: ${config.name}`,
    `Available tools in this phase: ${config.tools.join(', ')}`,
    '',
    `PHASE INSTRUCTIONS: ${config.instructions}`,
  ];

  // Context injections from declarative config
  const injections = buildContextInjections(config, session);
  if (injections) {
    lines.push(injections);
  }

  // File upload instructions
  lines.push(
    '',
    'FILE UPLOADS:',
    'When a user sends a message with attached files, the metadata appears as [Attached files: ...] in their message.',
    'Each attached file has: filename, columns, row_count, and s3_uri.',
    'To inspect an uploaded file, call get_source_preview with the s3_uri parameter (not alias).',
    'To ask the user for a file, use request_file_upload.',
    'Treat uploaded files as data sources — use their s3_uri wherever you would use a source alias.',
  );

  // Selected sources info
  if (session.left_source_alias || session.right_source_alias) {
    lines.push('', 'SELECTED SOURCES:');
    if (session.left_source_alias) lines.push(`- Left: ${session.left_source_alias}`);
    if (session.right_source_alias) lines.push(`- Right: ${session.right_source_alias}`);
  }

  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Agent Response
// ---------------------------------------------------------------------------

export interface AgentResponse {
  segments: ChatSegment[];
  phaseTransition?: ChatPhase;
  sessionUpdates?: Partial<ChatSession>;
}

// ---------------------------------------------------------------------------
// Phase Advancement
//
// Finds the next phase in PHASE_ORDER after the current one.
// Returns undefined if already at the last phase.
// ---------------------------------------------------------------------------

function getNextPhase(current: ChatPhase): ChatPhase | undefined {
  const idx = PHASE_ORDER.indexOf(current);
  if (idx < 0 || idx >= PHASE_ORDER.length - 1) return undefined;
  return PHASE_ORDER[idx + 1];
}

// ---------------------------------------------------------------------------
// runAgent -- the main orchestrator loop
//
// 1. Get phase config from PHASES declarative map
// 2. Check prerequisites
// 3. Build conversation history from session.messages
// 4. Call Claude with system prompt + phase tools
// 5. Handle tool_use blocks: execute tool, feed result back, call Claude again
// 6. After each tool success: check advancesWhen — if true, advance phase
// 7. Track retries per tool — remove exhausted tools from available set
// 8. Repeat until Claude returns a final text response
// ---------------------------------------------------------------------------

export async function runAgent(
  session: ChatSession,
  userMessage: string,
  onTextChunk?: (text: string) => void,
): Promise<AgentResponse> {
  const client = getClient();

  let currentPhase = session.phase;
  let config = PHASES[currentPhase];

  // Check prerequisites for the current phase
  checkPrerequisites(config, session);

  // Retry tracking: tool name -> number of failures
  const retryTracker = new Map<string, number>();
  const exhaustedTools = new Set<string>();

  // Build initial system prompt and tools
  let systemPrompt = buildSystemPrompt(session, config);
  let tools = getPhaseTools(currentPhase, exhaustedTools);

  // Build conversation history for Claude from session messages
  const conversationMessages: Anthropic.MessageParam[] = [];
  for (const msg of session.messages) {
    const textContent = msg.segments
      .filter((s) => s.type === 'text')
      .map((s) => s.content)
      .join('\n');
    if (textContent) {
      conversationMessages.push({
        role: msg.role === 'user' ? 'user' : 'assistant',
        content: textContent,
      });
    }
  }

  // Append the current user message
  conversationMessages.push({ role: 'user', content: userMessage });

  const segments: ChatSegment[] = [];
  const sessionUpdates: Partial<ChatSession> = {};
  let phaseTransition: ChatPhase | undefined;

  // Working copy of session for advancesWhen checks
  const workingSession = { ...session };

  // Tool-use loop: keep calling Claude until we get a final text response
  let currentMessages = conversationMessages;
  let continueLoop = true;

  try {
    while (continueLoop) {
      const response = await client.messages.create({
        model: MODEL,
        max_tokens: 4096,
        system: systemPrompt,
        tools,
        messages: currentMessages,
      });

      // Assume this is the final turn unless we encounter a tool_use block.
      continueLoop = false;

      // Collect all tool_use blocks from this response.
      const toolUseBlocks: Array<{ id: string; name: string; input: unknown }> = [];

      for (const block of response.content) {
        if (block.type === 'text') {
          segments.push({ type: 'text', content: block.text });
          if (onTextChunk) onTextChunk(block.text);
        } else if (block.type === 'tool_use') {
          toolUseBlocks.push({ id: block.id, name: block.name, input: block.input });
        }
      }

      // If there were tool_use blocks, execute them all and feed results back.
      if (toolUseBlocks.length > 0) {
        const toolResults: Anthropic.ToolResultBlockParam[] = [];

        for (const tu of toolUseBlocks) {
          try {
            const result = await executeTool(
              tu.name,
              tu.input as Record<string, unknown>,
              workingSession,
            );

            // --- Store tool results on sessionUpdates ---
            if (tu.name === 'list_sources') {
              sessionUpdates.sources_list = result as SourceInfo[];
              workingSession.sources_list = result as SourceInfo[];
            } else if (tu.name === 'get_source_preview') {
              const preview = result as {
                alias: string;
                columns: ColumnInfo[];
                rows: string[][];
              };
              const inputArgs = tu.input as Record<string, unknown>;
              const isUploadedFile = !!inputArgs.s3_uri;

              // First preview populates left, second populates right.
              // Also stores the source alias for left/right detection.
              const isLeft =
                !workingSession.schema_left ||
                (workingSession.left_source_alias &&
                  preview.alias === workingSession.left_source_alias);

              if (isLeft) {
                sessionUpdates.schema_left = preview.columns;
                sessionUpdates.left_source_alias = preview.alias;
                workingSession.schema_left = preview.columns;
                workingSession.left_source_alias = preview.alias;
              } else {
                sessionUpdates.schema_right = preview.columns;
                sessionUpdates.right_source_alias = preview.alias;
                workingSession.schema_right = preview.columns;
                workingSession.right_source_alias = preview.alias;
              }

              // For uploaded files: also populate sources_list and sample data
              // so phase advancement works without load_scoped
              if (isUploadedFile) {
                const virtualSource: SourceInfo = {
                  alias: preview.alias,
                  uri: inputArgs.s3_uri as string,
                  source_type: 'csv_upload',
                  status: 'ok',
                };
                const currentSources = workingSession.sources_list || [];
                const updatedSources = [...currentSources.filter(s => s.alias !== preview.alias), virtualSource];
                sessionUpdates.sources_list = updatedSources;
                workingSession.sources_list = updatedSources;

                // Convert preview rows to objects for sample data
                const asObjects = preview.rows.map((row) => {
                  const obj: Record<string, unknown> = {};
                  preview.columns.forEach((col, j) => {
                    obj[col.name] = row[j];
                  });
                  return obj;
                });

                if (isLeft) {
                  sessionUpdates.sample_left = asObjects;
                  workingSession.sample_left = asObjects;
                } else {
                  sessionUpdates.sample_right = asObjects;
                  workingSession.sample_right = asObjects;
                }
              }
            } else if (tu.name === 'load_scoped') {
              const preview = result as {
                alias: string;
                columns: ColumnInfo[];
                rows: string[][];
              };
              const conditions = (tu.input as Record<string, unknown>)
                .conditions as FilterCondition[];
              const asObjects = preview.rows.map((row) => {
                const obj: Record<string, unknown> = {};
                preview.columns.forEach((col, j) => {
                  obj[col.name] = row[j];
                });
                return obj;
              });

              // First load populates left, second populates right
              if (
                !workingSession.sample_left ||
                (workingSession.left_source_alias &&
                  preview.alias === workingSession.left_source_alias)
              ) {
                sessionUpdates.sample_left = asObjects;
                sessionUpdates.scope_left = conditions;
                workingSession.sample_left = asObjects;
                workingSession.scope_left = conditions;
              } else {
                sessionUpdates.sample_right = asObjects;
                sessionUpdates.scope_right = conditions;
                workingSession.sample_right = asObjects;
                workingSession.scope_right = conditions;
              }
            } else if (tu.name === 'build_recipe') {
              sessionUpdates.recipe_draft = result as Record<string, unknown>;
              workingSession.recipe_draft = result as Record<string, unknown>;
            } else if (tu.name === 'save_recipe') {
              // Recipe already stored in recipe_draft by build_recipe; no extra state needed
            } else if (tu.name === 'propose_match') {
              // Emit as match_proposal card segment
              segments.push({
                type: 'card',
                card_type: 'match_proposal',
                card_id: `match-${Date.now()}`,
                data: result as Record<string, unknown>,
              });
            } else if (tu.name === 'run_sample') {
              // Emit validation results as a card for user review
              segments.push({
                type: 'card',
                card_type: 'result_summary',
                card_id: `validation-${Date.now()}`,
                data: result as Record<string, unknown>,
              });
            } else if (tu.name === 'run_full') {
              sessionUpdates.status = 'running';
              workingSession.status = 'running';
              // Emit a live progress card so the user can track execution
              const runResult = result as Record<string, unknown>;
              if (runResult.run_id) {
                segments.push({
                  type: 'card',
                  card_type: 'progress',
                  card_id: `progress-${Date.now()}`,
                  data: runResult,
                });
              }
            } else if (tu.name === 'request_file_upload') {
              segments.push({
                type: 'card',
                card_type: 'upload_request',
                card_id: `upload-${Date.now()}`,
                data: result as Record<string, unknown>,
              });
            }

            toolResults.push({
              type: 'tool_result',
              tool_use_id: tu.id,
              content: JSON.stringify(result),
            });

            // --- Check for phase advancement after tool success ---
            if (config.advancesWhen(workingSession)) {
              const nextPhase = getNextPhase(currentPhase);
              if (nextPhase) {
                phaseTransition = nextPhase;
                currentPhase = nextPhase;
                config = PHASES[currentPhase];

                // A new phase gets fresh retry budgets — tools that were
                // exhausted in the previous phase should be available again.
                retryTracker.clear();
                exhaustedTools.clear();

                // Rebuild system prompt and tools for new phase
                systemPrompt = buildSystemPrompt(workingSession, config);
                tools = getPhaseTools(currentPhase, exhaustedTools);
              }
            }
          } catch (err) {
            const errorMsg = err instanceof Error ? err.message : 'Tool execution failed';
            toolResults.push({
              type: 'tool_result',
              tool_use_id: tu.id,
              content: JSON.stringify({ error: errorMsg }),
              is_error: true,
            });

            // --- Retry tracking ---
            const currentRetries = retryTracker.get(tu.name) ?? 0;
            const newRetries = currentRetries + 1;
            retryTracker.set(tu.name, newRetries);

            if (newRetries >= config.errorPolicy.maxRetriesPerTool) {
              exhaustedTools.add(tu.name);
              // Rebuild tools without exhausted ones
              tools = getPhaseTools(currentPhase, exhaustedTools);
            }
          }
        }

        // Feed the assistant response (with tool_use blocks) and the tool results
        // back into the conversation so Claude can produce a follow-up.
        currentMessages = [
          ...currentMessages,
          { role: 'assistant' as const, content: response.content },
          { role: 'user' as const, content: toolResults },
        ];

        continueLoop = true;
      }
    }
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : 'Agent failed';
    segments.push({
      type: 'text',
      content: `I encountered an issue connecting to the AI service: ${errorMsg}. Please check your API key configuration.`,
    });
  }

  return { segments, phaseTransition, sessionUpdates };
}
