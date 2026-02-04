# Agentic Layer State Machine Redesign

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the fragile, split-responsibility agentic orchestrator with a declarative state machine that guarantees phase invariants, provides structured error recovery, and preserves full context across turns.

**Architecture:** A declarative phase config defines each phase's prerequisites, tools, context injections, advance conditions, and error policy. A generic orchestrator loop reads the config and manages all transitions. The route becomes a thin pass-through. A new scoping phase with structured filter conditions replaces the fake `load_sample` criteria parameter, with per-connector translation to native query languages.

**Tech Stack:** TypeScript/Next.js (orchestrator), Rust/Axum (new scoped-load endpoint, connector abstraction), DataFusion

---

## Problem Statement

Analysis of two production transcripts (CSV and PostgreSQL) revealed 5 functional gaps in the agentic layer:

### Gap 1: Phase/Tool Mismatch
The route-level intent detection (`route.ts:52-74`) advances the phase from `intent` to `sampling` before the agent runs. The agent enters `sampling` without having previewed schemas — it was never in `intent` long enough to call `get_source_preview`. The agent then has `sampling` tools and instructions but lacks the context to use them.

### Gap 2: No Error Recovery Strategy
When a tool fails, the raw error is fed back to Claude with no recovery guidance. The agent retries the same failing tool indefinitely — the `while (continueLoop)` loop in `agent.ts:361` has no iteration limit. There is no retry budget, no circuit breaker, and no error-specific hints in the system prompt.

### Gap 3: Conversation History Loses Tool Context
The history reconstruction (`agent.ts:334-346`) strips all tool calls and tool results, keeping only text segments. On subsequent turns, Claude has no access to prior tool results — schemas, source metadata, sample data — only the text summary it wrote. Multi-turn tool chains break because structured data doesn't survive between turns.

### Gap 4: Phase Transition Timing Conflicts
Two independent systems manage phase transitions: the route (before the agent) and the agent (during tool execution). They can conflict. Phases only advance forward — there's no mechanism to detect that a phase's work wasn't completed or to re-enter a phase if prerequisites are missing.

### Gap 5: `load_sample` Criteria Is Fake
The `criteria` parameter is silently ignored (`agent-tools.ts:47` — prefixed with underscore). The tool always returns the first N rows regardless of what criteria the user provides. The sampling phase adds a user interaction step that has no effect on the data. Additionally, loading first-N rows from left and right sources independently yields samples that are unlikely to contain matching records since the sources are independently ordered.

---

## Design

### Phase Config Schema

Each phase is declared as a config object. The orchestrator reads these configs — no special-case logic.

```typescript
interface PhaseConfig {
  name: string;
  tools: AgentTool[];
  instructions: string;
  prerequisites: {
    sessionFields: (keyof ChatSession)[];  // must be non-null to enter this phase
  };
  contextInjections: ContextInjection[];   // structured data to inject into system prompt
  advancesWhen: (session: ChatSession) => boolean;  // gate-keeps advancement
  errorPolicy: {
    maxRetriesPerTool: number;
    onExhausted: 'inform_user' | 'skip_phase';
  };
}

type ContextInjection =
  | 'sources_list'       // full SourceInfo[] from list_sources
  | 'schema_left'        // column definitions from get_source_preview
  | 'schema_right'
  | 'sample_left'        // scoped data rows (up to 20 for prompt)
  | 'sample_right'
  | 'confirmed_pairs'    // full match pair data
  | 'recipe_draft';      // current recipe JSON
```

### Phase Definitions (7 Phases)

```
Phase           Prerequisites               Tools                           Context Injections          Advances When
─────           ─────────────               ─────                           ──────────────────          ─────────────
greeting        (none)                      list_sources                    (none)                      sources_list is populated
intent          sources_list                list_sources, get_source_       sources_list                schema_left AND schema_right
                                            preview                                                     populated
scoping         schema_left, schema_right   list_sources, get_source_       schemas                     sample_left AND sample_right
                                            preview, load_scoped                                        populated
demonstration   sample_left, sample_right   get_source_preview,             schemas, samples,           confirmed_pairs.length >= 3
                                            propose_match                   confirmed_pairs count
inference       confirmed_pairs (>= 1)      infer_rules, build_recipe,      schemas, confirmed_pairs    recipe_draft is populated
                                            propose_match
validation      recipe_draft                validate_recipe, run_sample,    recipe_draft, schemas       validation_approved === true
                                            get_source_preview
execution       recipe_draft,               run_full, validate_recipe       recipe_draft                run completes
                validation_approved
```

### Phase Details

#### greeting
```typescript
{
  name: 'greeting',
  tools: ['list_sources'],
  instructions: 'Greet the user. Use list_sources to see what data sources are available. Tell the user what sources they have and ask what they want to reconcile.',
  prerequisites: { sessionFields: [] },
  contextInjections: [],
  advancesWhen: (s) => s.sources_list !== null,
  errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
}
```

#### intent
```typescript
{
  name: 'intent',
  tools: ['list_sources', 'get_source_preview'],
  instructions: 'The user has stated what they want to reconcile. Confirm the left and right sources. Use get_source_preview on both sources to understand the data structure. You must preview both sources before proceeding.',
  prerequisites: { sessionFields: ['sources_list'] },
  contextInjections: ['sources_list'],
  advancesWhen: (s) => s.schema_left !== null && s.schema_right !== null,
  errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
}
```

#### scoping
```typescript
{
  name: 'scoping',
  tools: ['list_sources', 'get_source_preview', 'load_scoped'],
  instructions: `The user's sources have been confirmed and schemas are loaded. Ask the user what subset of data they want to reconcile (e.g., date range, customer segment, amount range). Use load_scoped to load filtered data from each source into the working set. Pass structured filter conditions — the backend handles translation to the source's native query language.`,
  prerequisites: { sessionFields: ['schema_left', 'schema_right'] },
  contextInjections: ['schema_left', 'schema_right'],
  advancesWhen: (s) => s.sample_left !== null && s.sample_right !== null && s.sample_left.length > 0 && s.sample_right.length > 0,
  errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
}
```

#### demonstration
```typescript
{
  name: 'demonstration',
  tools: ['get_source_preview', 'propose_match'],
  instructions: 'Examine the scoped data. Propose candidate matches using propose_match. Explain your reasoning for each match. The user will confirm or reject. Build up at least 3 confirmed pairs before moving on.',
  prerequisites: { sessionFields: ['sample_left', 'sample_right'] },
  contextInjections: ['schema_left', 'schema_right', 'sample_left', 'sample_right', 'confirmed_pairs'],
  advancesWhen: (s) => s.confirmed_pairs.length >= 3,
  errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
}
```

#### inference
```typescript
{
  name: 'inference',
  tools: ['infer_rules', 'build_recipe', 'propose_match'],
  instructions: 'Analyze the confirmed match pairs using infer_rules. Propose matching rules to the user in plain language. Once rules are agreed upon, build the recipe using build_recipe.',
  prerequisites: { sessionFields: ['confirmed_pairs'] },
  contextInjections: ['schema_left', 'schema_right', 'confirmed_pairs'],
  advancesWhen: (s) => s.recipe_draft !== null,
  errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
}
```

#### validation
```typescript
{
  name: 'validation',
  tools: ['validate_recipe', 'run_sample', 'get_source_preview'],
  instructions: 'Validate the recipe using validate_recipe. Run it on the scoped data using run_sample. Present the results to the user. Ask if they want to adjust the rules or approve.',
  prerequisites: { sessionFields: ['recipe_draft'] },
  contextInjections: ['recipe_draft', 'schema_left', 'schema_right'],
  advancesWhen: (s) => s.validation_approved === true,
  errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
}
```

#### execution
```typescript
{
  name: 'execution',
  tools: ['run_full', 'validate_recipe'],
  instructions: 'The user has approved the recipe. Run it on the full scoped dataset using run_full. Present the results summary.',
  prerequisites: { sessionFields: ['recipe_draft', 'validation_approved'] },
  contextInjections: ['recipe_draft'],
  advancesWhen: () => false, // terminal phase
  errorPolicy: { maxRetriesPerTool: 2, onExhausted: 'inform_user' },
}
```

### Orchestrator Loop

The orchestrator is a single function in `agent.ts`. The route (`route.ts`) becomes a thin pass-through — no intent detection, no phase management.

```
runAgent(session, userMessage):

  retryTracker = new Map<string, number>()    // tool_name → error count
  currentPhase = getPhaseConfig(session.phase)

  1. CHECK PREREQUISITES
     For each field in currentPhase.prerequisites.sessionFields:
       if session[field] is null → error: "Phase {name} entered without {field}"
     (This is a programming error, not a user error — should never happen
      if advancesWhen conditions are correct.)

  2. BUILD SYSTEM PROMPT
     - Base behavioral rules
     - currentPhase.instructions
     - For each injection in currentPhase.contextInjections:
         Format and append the session data (schemas, samples, pairs, etc.)

  3. BUILD CONVERSATION HISTORY
     - From session.messages: text segments only (same as today)
     - Append current user message

  4. GET PHASE TOOLS
     - Filter TOOL_DEFINITIONS to only currentPhase.tools
     - Remove any tools that have exhausted their retry budget

  5. CALL CLAUDE
     messages.create({ model, max_tokens, system, tools, messages })

  6. PROCESS RESPONSE
     For each content block:
       text → collect into segments
       tool_use → add to toolUseBlocks

  7. EXECUTE TOOLS (if any tool_use blocks)
     For each tool call:
       try:
         result = executeTool(name, args, session)
         Update session state (schemas, samples, etc.)
         Feed success result back to Claude
       catch:
         retryTracker[name] = (retryTracker[name] || 0) + 1
         count = retryTracker[name]
         max = currentPhase.errorPolicy.maxRetriesPerTool

         if count < max:
           Feed error + hint: "Failed: {error}. {max - count} retries left."
         else:
           Feed error + final: "Tool {name} failed {max} times. Do not retry."
           Remove tool from available set for rest of turn.

  8. CHECK PHASE ADVANCEMENT
     if currentPhase.advancesWhen(session):
       nextPhase = PHASE_ORDER[currentIndex + 1]
       session.phase = nextPhase.name
       Rebuild system prompt and tools for next phase
       (Agent continues in same turn with new phase context)

  9. LOOP
     If Claude returned tool_use blocks → go to step 5
     If Claude returned only text → exit loop

  10. RETURN
      { segments, sessionUpdates }
```

**Key properties:**
- The orchestrator is generic — no phase-specific if-statements
- Mid-turn phase advancement: if the agent completes a phase's work in one turn, it seamlessly continues with the next phase's tools and instructions
- Tool removal on retry exhaustion physically prevents the LLM from retrying
- Prerequisites are checked at entry — if something is wrong, it fails fast

### Session State Changes

New fields on `ChatSession`:

```typescript
interface ChatSession {
  // existing (unchanged)
  id: string;
  status: SessionStatus;
  phase: ChatPhase;
  left_source_alias: string | null;
  right_source_alias: string | null;
  recipe_draft: Record<string, unknown> | null;
  sample_left: Record<string, unknown>[] | null;
  sample_right: Record<string, unknown>[] | null;
  confirmed_pairs: MatchPair[];
  messages: ChatMessage[];
  created_at: string;
  updated_at: string;

  // new
  sources_list: SourceInfo[] | null;        // stored after list_sources
  schema_left: ColumnInfo[] | null;         // stored after get_source_preview (left)
  schema_right: ColumnInfo[] | null;        // stored after get_source_preview (right)
  scope_left: FilterCondition[] | null;     // user-defined scope for left source
  scope_right: FilterCondition[] | null;    // user-defined scope for right source
  validation_approved: boolean;             // set by card_response in validation phase

  // removed
  // sample_criteria_left — replaced by scope_left
  // sample_criteria_right — replaced by scope_right
}
```

### Context Injection Sizing

To avoid bloating the system prompt:

| Injection | Max size in prompt |
|-----------|--------------------|
| sources_list | Full list (typically < 10 sources) |
| schema_left/right | Full column definitions (typically < 30 columns) |
| sample_left/right | Up to 20 rows per source |
| confirmed_pairs | All pairs (typically 3-10) |
| recipe_draft | Full JSON |

If `sample_left` or `sample_right` exceed 20 rows, truncate to 20 with a note: "Showing 20 of {total} rows. Use get_source_preview for more."

### Two-Phase Filtering: Scoping + Recipe Execution

#### Concept

Filtering happens at two distinct layers:

1. **Scoping** (before DataFusion) — Defines the data universe. Filters are applied at the raw data source using the source's native query language. This determines what data gets loaded into DataFusion.

2. **Recipe execution** (inside DataFusion) — The reconciliation recipe runs SQL against the scoped data already in DataFusion.

The agent works with a **structured filter format** that is source-agnostic:

```typescript
interface FilterCondition {
  column: string;
  op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'between' | 'in' | 'like';
  value: string | number | string[] | [string, string];  // [from, to] for between
}
```

Each connector translates this into its native query language.

#### Connector Translation Examples

**PostgreSQL:**
```sql
-- Input: { column: "invoice_date", op: "between", value: ["2024-01-01", "2024-01-31"] }
SELECT * FROM invoices WHERE invoice_date BETWEEN '2024-01-01' AND '2024-01-31'
```

**CSV (no pre-filtering):**
```sql
-- Load full CSV into DataFusion, then:
SELECT * FROM invoices_csv WHERE invoice_date BETWEEN '2024-01-01' AND '2024-01-31'
```

**Future Elasticsearch:**
```json
{ "query": { "bool": { "filter": [{ "range": { "invoice_date": { "gte": "2024-01-01", "lte": "2024-01-31" }}}]}}}
```

**Future BigQuery:**
```sql
-- Same as PostgreSQL syntax
SELECT * FROM `project.dataset.invoices` WHERE invoice_date BETWEEN '2024-01-01' AND '2024-01-31'
```

#### Connector Interface Change (Rust)

```rust
/// Structured filter condition — source-agnostic
#[derive(Debug, Clone, Deserialize)]
pub struct FilterCondition {
    pub column: String,
    pub op: FilterOp,
    pub value: FilterValue,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    Eq, Neq, Gt, Gte, Lt, Lte, Between, In, Like,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FilterValue {
    String(String),
    Number(f64),
    StringArray(Vec<String>),
    Range([String; 2]),
}

/// Extended connector trait
#[async_trait]
pub trait SourceConnector {
    /// Register a scoped subset of data into the DataFusion context.
    /// The connector translates FilterConditions into its native query language.
    async fn register_scoped(
        &self,
        ctx: &SessionContext,
        alias: &str,
        table: &str,
        conditions: &[FilterCondition],
    ) -> Result<(), Box<dyn std::error::Error>>;
}
```

For the PostgreSQL connector, `register_scoped` builds a WHERE clause from conditions and loads the result. For CSV, it loads the full file and registers it — DataFusion applies the conditions later via SQL.

#### New Backend Endpoint

```
POST /api/sources/:alias/load-scoped
Body: { "conditions": [...], "limit": 50 }
Response: { "alias": "invoices", "columns": [...], "rows": [...], "total_rows": 150, "preview_rows": 50 }
```

This endpoint:
1. Looks up the source's connector type
2. Calls `register_scoped` with the conditions
3. Returns a preview of the loaded data

#### New Agent Tool: `load_scoped`

Replaces the old `load_sample`. The agent constructs structured filter conditions based on its understanding of the schemas and the user's scoping request.

```typescript
{
  name: 'load_scoped',
  description: 'Load a filtered subset of data from a source into the working set. Pass structured filter conditions — the backend translates them to the source\'s native query language. Use this to scope the data the user wants to reconcile.',
  input_schema: {
    type: 'object',
    properties: {
      alias: { type: 'string', description: 'Source alias' },
      conditions: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            column: { type: 'string' },
            op: { type: 'string', enum: ['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'between', 'in', 'like'] },
            value: { description: 'Filter value. String, number, array of strings, or [from, to] for between.' },
          },
          required: ['column', 'op', 'value'],
        },
        description: 'Filter conditions to scope the data',
      },
      limit: { type: 'number', description: 'Max rows to load (default 200)' },
    },
    required: ['alias', 'conditions'],
  },
}
```

### Route Changes

The chat route (`route.ts`) becomes minimal:

```typescript
export async function POST(request: NextRequest) {
  const body = await request.json();
  const { session_id, message, card_response } = body;

  // Get or create session
  let session = session_id ? getSession(session_id) : undefined;
  if (!session) session = createSession();

  // Build user text from message or card response
  const userText = buildUserText(message, card_response);

  // Handle card-specific session updates (match confirmation, validation approval)
  if (card_response) {
    handleCardResponse(session, card_response);
  }

  // Add user message to session
  addMessage(session.id, { role: 'user', segments: [{ type: 'text', content: userText }], timestamp: new Date().toISOString() });

  // Run the agent — ALL phase management happens inside runAgent
  const agentResponse = await runAgent(session, userText);

  // Apply session updates from agent
  if (agentResponse.sessionUpdates) {
    updateSession(session.id, agentResponse.sessionUpdates);
  }

  // Add agent message
  addMessage(session.id, { role: 'agent', segments: agentResponse.segments, timestamp: new Date().toISOString() });

  session = getSession(session.id)!;
  return NextResponse.json({
    session_id: session.id,
    phase: session.phase,
    status: session.status,
    message: { role: 'agent', segments: agentResponse.segments, timestamp: new Date().toISOString() },
    recipe_draft: session.recipe_draft,
  });
}
```

No intent detection. No phase transitions. No source alias detection. The orchestrator handles everything.

### Source Alias Detection

The old route-level `detectSourceAliases` is removed. Instead, the agent detects source aliases naturally during the `intent` phase by:

1. Examining the `sources_list` (injected into system prompt)
2. Matching the user's description to available aliases
3. Calling `get_source_preview` on the chosen sources

The orchestrator stores the aliases when `get_source_preview` returns successfully — it reads the `alias` field from the response and sets `left_source_alias` / `right_source_alias` on the session based on call order (first preview = left, second = right).

### Error Message Enhancement

Tool errors currently return opaque messages like "Failed to get preview for invoices: Internal Server Error." The Rust backend should return structured errors:

```json
{
  "error": "source_not_registered",
  "message": "Source 'invoices' is not registered with the query engine",
  "suggestion": "The source may need to be re-registered. Try listing sources first."
}
```

The orchestrator can then inject more helpful hints to Claude based on the error type.

---

## Files Changed

### TypeScript (kalla-web)

| File | Change |
|------|--------|
| `src/lib/chat-types.ts` | Add new session fields, update `ChatPhase` type (replace `sampling` with `scoping`), define `FilterCondition`, define `PhaseConfig`, export `PHASES` config |
| `src/lib/agent.ts` | Replace current orchestrator with generic state-machine loop |
| `src/lib/agent-tools.ts` | Replace `load_sample` with `load_scoped`, add new endpoint call |
| `src/app/api/chat/route.ts` | Remove intent detection, phase management — thin pass-through |
| `src/lib/intent-detection.ts` | Delete (no longer needed) |
| `src/__tests__/intent-detection.test.ts` | Delete |
| `src/__tests__/phase-config.test.ts` | New — test advancesWhen conditions, prerequisites |
| `src/__tests__/orchestrator.test.ts` | New — test error recovery, phase transitions, context injection |

### Rust (kalla-server)

| File | Change |
|------|--------|
| `kalla-server/src/main.rs` | Add `POST /api/sources/:alias/load-scoped` endpoint |
| `crates/kalla-connectors/src/lib.rs` | Define `SourceConnector` trait with `register_scoped` |
| `crates/kalla-connectors/src/postgres.rs` | Implement `register_scoped` for PostgreSQL |
| `crates/kalla-connectors/src/filter.rs` | New — `FilterCondition` types, SQL WHERE builder |
| `crates/kalla-core/src/engine.rs` | Add `register_scoped_csv` that loads full file then applies DataFusion SQL filter |

---

## What This Does NOT Change

- The Anthropic Claude API integration (still uses tool_use, same SDK)
- The frontend React components (chat UI, cards, etc.)
- The recipe schema and transpiler (kalla-recipe crate)
- The evidence store (kalla-evidence crate)
- The reconciliation execution engine (kalla-core)
- The database schema (chat_sessions table — just has more JSONB fields)
