import Anthropic from '@anthropic-ai/sdk';
import type { ChatSession, ChatPhase, ChatSegment, AgentTool } from './chat-types';
import { PHASE_TOOLS } from './chat-types';
import { executeTool } from './agent-tools';

// ---------------------------------------------------------------------------
// Anthropic client (lazy singleton per-request)
// ---------------------------------------------------------------------------

function getClient(): Anthropic {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error('ANTHROPIC_API_KEY is not set');
  return new Anthropic({ apiKey });
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
      'Get schema info and sample rows from a registered data source. Returns column names, data types, and a preview of rows.',
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
    description:
      'Load a filtered sample of rows from a data source for match demonstration. Use this to get a workable subset of data.',
    input_schema: {
      type: 'object' as const,
      properties: {
        alias: { type: 'string', description: 'The alias of the data source' },
        criteria: {
          type: 'string',
          description: 'Filter criteria description (e.g., date range, customer ID)',
        },
        limit: { type: 'number', description: 'Max rows to load (default 50)' },
      },
      required: ['alias'],
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
      'Build a complete MatchRecipe from the inferred rules and source configuration.',
    input_schema: {
      type: 'object' as const,
      properties: {
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
        rules: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              name: { type: 'string' },
              pattern: {
                type: 'string',
                enum: ['1:1', '1:N', 'M:1'],
              },
              conditions: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    left: { type: 'string' },
                    op: {
                      type: 'string',
                      enum: [
                        'eq',
                        'tolerance',
                        'gt',
                        'lt',
                        'gte',
                        'lte',
                        'contains',
                        'startswith',
                        'endswith',
                      ],
                    },
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
      required: [
        'left_alias',
        'right_alias',
        'left_uri',
        'right_uri',
        'left_pk',
        'right_pk',
        'rules',
      ],
    },
  },
  {
    name: 'validate_recipe',
    description:
      'Validate a recipe structure and field references against source schemas.',
    input_schema: {
      type: 'object' as const,
      properties: {
        recipe: {
          type: 'object',
          description: 'The complete MatchRecipe to validate',
        },
      },
      required: ['recipe'],
    },
  },
  {
    name: 'run_sample',
    description:
      'Execute the recipe against loaded sample data. Returns match statistics.',
    input_schema: {
      type: 'object' as const,
      properties: {
        recipe: {
          type: 'object',
          description: 'The MatchRecipe to run',
        },
      },
      required: ['recipe'],
    },
  },
  {
    name: 'run_full',
    description:
      'Execute the recipe against the full datasets. Returns a run_id for tracking progress.',
    input_schema: {
      type: 'object' as const,
      properties: {
        recipe: {
          type: 'object',
          description: 'The MatchRecipe to run on the full data',
        },
      },
      required: ['recipe'],
    },
  },
];

// ---------------------------------------------------------------------------
// System Prompt Builder
// ---------------------------------------------------------------------------

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
  const phaseInstructions: Record<ChatPhase, string> = {
    greeting:
      'PHASE INSTRUCTIONS: Greet the user. Use list_sources to see what data sources are available. Tell the user what sources they have and ask what they want to reconcile.',
    intent:
      'PHASE INSTRUCTIONS: The user has stated what they want to reconcile. Confirm the left and right sources. Use get_source_preview to understand the data structure if needed.',
    sampling:
      'PHASE INSTRUCTIONS: Ask the user for filter criteria to narrow down each source to a workable sample. Load samples using load_sample.',
    demonstration:
      'PHASE INSTRUCTIONS: Examine the loaded sample data. Propose candidate matches using propose_match. The user will confirm or reject. Build up a set of confirmed pairs.',
    inference:
      'PHASE INSTRUCTIONS: Analyze confirmed match pairs using infer_rules. Propose the matching rules to the user. Build the recipe using build_recipe once rules are agreed upon.',
    validation:
      'PHASE INSTRUCTIONS: Validate the recipe using validate_recipe. Run it on sample data using run_sample. Show results and let the user iterate if needed.',
    execution:
      'PHASE INSTRUCTIONS: The user has approved the recipe. Run it on the full dataset using run_full. Show the results summary.',
  };

  lines.push(phaseInstructions[session.phase]);

  // Context injection — selected sources
  if (session.left_source_alias || session.right_source_alias) {
    lines.push('', 'SELECTED SOURCES:');
    if (session.left_source_alias) lines.push(`- Left: ${session.left_source_alias}`);
    if (session.right_source_alias) lines.push(`- Right: ${session.right_source_alias}`);
  }

  // Context injection — confirmed pairs count
  if (session.confirmed_pairs.length > 0) {
    lines.push('', `CONFIRMED MATCH PAIRS: ${session.confirmed_pairs.length} pairs confirmed so far.`);
  }

  // Context injection — recipe draft
  if (session.recipe_draft) {
    lines.push('', 'CURRENT RECIPE DRAFT:', JSON.stringify(session.recipe_draft, null, 2));
  }

  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Phase-aware tool filtering
// ---------------------------------------------------------------------------

function getPhaseTools(phase: ChatPhase): Anthropic.Tool[] {
  const allowed: AgentTool[] = PHASE_TOOLS[phase];
  return TOOL_DEFINITIONS.filter((t) =>
    (allowed as string[]).includes(t.name),
  );
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
// runAgent — the main orchestrator loop
//
// 1. Build conversation history from session.messages
// 2. Call Claude with system prompt + phase tools
// 3. Handle tool_use blocks: execute tool, feed result back, call Claude again
// 4. Repeat until Claude returns a final text response
// 5. Detect phase transitions based on which tools were used
// ---------------------------------------------------------------------------

export async function runAgent(
  session: ChatSession,
  userMessage: string,
  onTextChunk?: (text: string) => void,
): Promise<AgentResponse> {
  const client = getClient();
  const systemPrompt = buildSystemPrompt(session);
  const tools = getPhaseTools(session.phase);

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

  // Tool-use loop: keep calling Claude until we get a final text response
  // (i.e., no more tool_use blocks in the response).
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

      // Collect all tool_use blocks from this response (there may be several).
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
              session,
            );

            // --- Phase transition detection ---
            if (tu.name === 'list_sources' && session.phase === 'greeting') {
              phaseTransition = 'intent';
            } else if (tu.name === 'load_sample' && session.phase === 'sampling') {
              phaseTransition = 'demonstration';
            } else if (tu.name === 'infer_rules') {
              phaseTransition = 'inference';
            } else if (tu.name === 'build_recipe') {
              phaseTransition = 'validation';
              sessionUpdates.recipe_draft = result as Record<string, unknown>;
            } else if (tu.name === 'validate_recipe') {
              // Stay in validation phase
              if (!phaseTransition) phaseTransition = 'validation';
            } else if (tu.name === 'run_full') {
              phaseTransition = 'execution';
              sessionUpdates.status = 'running';
            }

            // --- Store sample data on session ---
            if (tu.name === 'load_sample' || tu.name === 'get_source_preview') {
              const preview = result as {
                alias: string;
                rows: string[][];
                columns: Array<{ name: string }>;
              };
              const asObjects = preview.rows.map((row) => {
                const obj: Record<string, unknown> = {};
                preview.columns.forEach((col, j) => {
                  obj[col.name] = row[j];
                });
                return obj;
              });

              if (session.left_source_alias && preview.alias === session.left_source_alias) {
                sessionUpdates.sample_left = asObjects;
              } else if (
                session.right_source_alias &&
                preview.alias === session.right_source_alias
              ) {
                sessionUpdates.sample_right = asObjects;
              }
            }

            toolResults.push({
              type: 'tool_result',
              tool_use_id: tu.id,
              content: JSON.stringify(result),
            });
          } catch (err) {
            const errorMsg = err instanceof Error ? err.message : 'Tool execution failed';
            toolResults.push({
              type: 'tool_result',
              tool_use_id: tu.id,
              content: JSON.stringify({ error: errorMsg }),
              is_error: true,
            });
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
