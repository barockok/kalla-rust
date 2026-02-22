# MCP AI Architecture — Wizard-First Reconciliation Builder

**Date:** 2026-02-22
**Status:** Approved

## Context

The current kalla-web AI layer (`agent.ts`) is a monolithic chat-driven agent with a 7-phase state machine, 11 inline tools, and Claude as the orchestrator. The new UI redesign replaces this with a structured wizard where the user drives the flow and AI assists silently behind the scenes.

This document defines the architecture for migrating from the chat-driven agent to a stateless MCP tool server called by the wizard UI.

## Decisions

| Decision | Choice |
|---|---|
| UX model | Wizard-first — AI assists silently, user drives |
| AI transport | MCP server (stdio, stateless) |
| AI scope | Full lifecycle migration (all tools) |
| MCP language | Node/TypeScript |
| MCP client | Next.js API routes (single gateway) |
| API surface | Single `POST /api/ai` endpoint for all tools |
| State ownership | Next.js + PostgreSQL (MCP server is stateless) |
| LLM usage | Claude as reasoning engine inside tools, not orchestrator |
| Migration | Phased — Screen 2 first, then 3, then 5 |

## System Architecture

```
┌──────────────────────────────────────────────────────┐
│  Browser — Wizard UI (React)                         │
│  Screen 1 → Screen 2 → Screen 3 → Screen 4 → 5     │
└──────────────────────┬───────────────────────────────┘
                       │ REST/JSON
┌──────────────────────▼───────────────────────────────┐
│  kalla-web (Next.js)                                 │
│  ┌────────────────┐  ┌────────────────────────────┐  │
│  │ API Routes     │  │ Session Store              │  │
│  │ POST /api/ai   │  │ (PostgreSQL + in-memory)   │  │
│  │ /api/sources/* │  └────────────────────────────┘  │
│  │ /api/recipes/* │                                  │
│  │ /api/runs/*    │                                  │
│  └───────┬────────┘                                  │
│          │ MCP (stdio)                               │
│  ┌───────▼────────────────────────────────────────┐  │
│  │ MCP Client (singleton, spawns kalla-mcp)       │  │
│  └───────┬────────────────────────────────────────┘  │
└──────────┼───────────────────────────────────────────┘
           │
┌──────────▼───────────────────────────────────────────┐
│  kalla-mcp (Node/TS MCP Server — stateless)          │
│                                                       │
│  Tools:                                               │
│  ├─ detect_field_mappings(schemaA, schemaB)           │
│  ├─ parse_nl_filter(text, schemas, mappings)          │
│  ├─ propose_match(rowsA, rowsB, schemas)              │
│  ├─ infer_rules(samples, schemas, mappings)           │
│  ├─ build_recipe(rules, sources, primary_keys)        │
│  ├─ validate_recipe(recipe_sql, schemas)              │
│  └─ nl_to_sql(text, schemas, context)                 │
│                                                       │
│  Internal: Anthropic SDK → Claude for reasoning       │
└───────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────┐
│  kalla-runner (Rust)                                  │
│  POST /api/jobs — execute reconciliation              │
│  Callbacks → kalla-web /api/worker/*                  │
└───────────────────────────────────────────────────────┘
```

## Single AI Gateway

All AI interactions go through one endpoint. The wizard frontend never knows about MCP — it calls REST, Next.js translates to MCP.

```typescript
// POST /api/ai
// Request:  { tool: string, input: Record<string, unknown> }
// Response: { result: Record<string, unknown> }
```

Next.js route handler:

```typescript
// app/api/ai/route.ts
import { getMcpClient } from "@/lib/mcp-client";

export async function POST(req: Request) {
  const { tool, input } = await req.json();
  const client = await getMcpClient();
  const result = await client.callTool({ name: tool, arguments: input });
  return Response.json({ result: result.content });
}
```

Frontend helper:

```typescript
async function callAI<T>(tool: string, input: Record<string, unknown>): Promise<T> {
  const res = await fetch('/api/ai', {
    method: 'POST',
    body: JSON.stringify({ tool, input })
  });
  return (await res.json()).result as T;
}
```

## MCP Tool Definitions

### `detect_field_mappings`

Analyzes two source schemas to identify columns that represent the same real-world information despite having different names. This is the core AI capability for Screen 2's common filters.

```typescript
// Input
{
  schema_a: { alias: string, columns: ColumnInfo[] },
  schema_b: { alias: string, columns: ColumnInfo[] },
  sample_a?: Record<string, unknown>[],  // optional, improves detection
  sample_b?: Record<string, unknown>[]
}

// Output
{
  mappings: [
    {
      field_a: "transaction_date",
      field_b: "invoice_date",
      confidence: 0.92,
      reason: "Both are date columns with overlapping value ranges"
    }
  ],
  suggested_filters: [
    { type: "date_range", field_a: "transaction_date", field_b: "invoice_date" },
    { type: "amount_range", field_a: "amount", field_b: "total_amount" }
  ]
}
```

### `parse_nl_filter`

Translates natural language filter descriptions into structured filter conditions, using the current schema context and field mappings.

```typescript
// Input
{
  text: "Only show transactions above $500 from last month",
  schema_a: { alias: string, columns: ColumnInfo[] },
  schema_b: { alias: string, columns: ColumnInfo[] },
  current_mappings: FieldMapping[]
}

// Output
{
  filters: [
    { source: "left_src", column: "amount", op: "gt", value: 500 },
    { source: "left_src", column: "transaction_date", op: "between",
      value: ["2026-01-01", "2026-01-31"] }
  ],
  explanation: "Filtering left source for amounts > $500 in January 2026"
}
```

### `infer_rules`

Analyzes sample data from both sources with field mappings to detect matching patterns, primary keys, and SQL matching rules.

```typescript
// Input
{
  schema_a: { alias: string, columns: ColumnInfo[] },
  schema_b: { alias: string, columns: ColumnInfo[] },
  sample_a: Record<string, unknown>[],
  sample_b: Record<string, unknown>[],
  mappings: FieldMapping[]
}

// Output
{
  pattern: {
    type: "1:N",
    description: "One bank transaction matches multiple invoices",
    confidence: 0.88
  },
  primary_keys: {
    source_a: ["transaction_id"],
    source_b: ["invoice_id"]
  },
  rules: [
    {
      name: "Amount Sum Match",
      sql: "tolerance_match(l.amount, SUM(r.total_amount), 0.01)",
      description: "Sum of invoice amounts equals bank transaction",
      confidence: 0.91
    }
  ]
}
```

### `build_recipe`

Creates a complete SQL recipe from confirmed rules, sources, and primary keys.

```typescript
// Input
{
  rules: Rule[],
  sources: { alias_a: string, alias_b: string },
  primary_keys: { source_a: string[], source_b: string[] },
  pattern_type: "1:1" | "1:N" | "N:M"
}

// Output
{
  match_sql: string,   // Complete DataFusion SQL query
  explanation: string  // Human-readable description
}
```

### `validate_recipe`

Checks a recipe SQL for structural validity against the source schemas.

```typescript
// Input
{
  recipe_sql: string,
  schema_a: { alias: string, columns: ColumnInfo[] },
  schema_b: { alias: string, columns: ColumnInfo[] }
}

// Output
{
  valid: boolean,
  issues: string[],
  suggestions: string[]
}
```

### `nl_to_sql`

Converts a natural language matching rule into a DataFusion SQL expression.

```typescript
// Input
{
  text: "Invoice date must be within 7 days of bank transaction date",
  schema_a: { alias: string, columns: ColumnInfo[] },
  schema_b: { alias: string, columns: ColumnInfo[] },
  mappings: FieldMapping[]
}

// Output
{
  sql: "r.invoice_date BETWEEN l.transaction_date - INTERVAL '7 days' AND l.transaction_date",
  explanation: "Matches invoices dated within 7 days before the bank transaction"
}
```

## Wizard Screen → Tool Mapping

### Screen 1 — Source Selection
No AI. Pure CRUD via existing `/api/sources` routes.

### Screen 2 — Sample Data

| User Action | Endpoint | MCP Tool |
|---|---|---|
| Screen loads | `POST /api/ai` | `detect_field_mappings` |
| User changes mapping dropdown | None (local state) | — |
| User types NL override | `POST /api/ai` | `parse_nl_filter` |
| User clicks "Load Sample" | `POST /api/sources/[alias]/load-scoped` | — (no AI) |

### Screen 3 — AI Pattern Detection & Rules

| User Action | Endpoint | MCP Tool |
|---|---|---|
| Screen loads | `POST /api/ai` | `infer_rules` |
| User edits a rule | None (direct edit) | — |
| User types NL custom rule | `POST /api/ai` | `nl_to_sql` |
| User confirms rules | `POST /api/ai` | `build_recipe` |

### Screen 4 — Run Parameters
No AI. User selects runtime-filterable fields. Pure UI state.

### Screen 5 — Review & Save

| User Action | Endpoint | MCP Tool |
|---|---|---|
| Screen loads | `POST /api/ai` | `validate_recipe` |
| User clicks "Run Sample" | `POST /api/runs` | — (Rust runner) |
| User clicks "Save Recipe" | `POST /api/recipes` | — (existing) |

## Screen 2 Data Flow (Detail)

```
User completes Screen 1 (selected: Bank Statement + Invoice System)
  │
  ▼ Screen 2 loads
  │
  ├─ GET /api/sources/bank_statement/preview       (parallel)
  ├─ GET /api/sources/invoice_system/preview        (parallel)
  └─ POST /api/ai { tool: "detect_field_mappings" } (parallel)
  │
  ▼ Frontend renders
  │  Common Filters (AI-detected)
  │  ├─ Date Range: [transaction_date ▾] → [invoice_date ▾]
  │  ├─ Amount:     [amount ▾]           → [total_amount ▾]
  │  └─ NL input:   "Adjust filters in your own words..."
  │
  ▼ User adjusts dropdowns → local state only
  │
  ▼ User types NL override (debounced 500ms)
  │  POST /api/ai { tool: "parse_nl_filter" }
  │  → Returns filter conditions → merge into UI state
  │
  ▼ User clicks "Load Sample"
  │  Frontend translates common filters to per-source conditions:
  │    bank_statement:  [{ column: "transaction_date", ... }, { column: "amount", ... }]
  │    invoice_system:  [{ column: "invoice_date", ... }, { column: "total_amount", ... }]
  │
  ├─ POST /api/sources/bank_statement/load-scoped   (parallel)
  └─ POST /api/sources/invoice_system/load-scoped    (parallel)
  │
  ▼ Frontend renders Sample Preview tables
  │  User clicks "Continue →" to Screen 3
```

## MCP Server Internals

### Package structure

```
kalla-mcp/
├── package.json
├── tsconfig.json
├── src/
│   ├── index.ts              ← Server entry (stdio transport)
│   ├── tools/
│   │   ├── detect-field-mappings.ts
│   │   ├── parse-nl-filter.ts
│   │   ├── infer-rules.ts
│   │   ├── build-recipe.ts
│   │   ├── validate-recipe.ts
│   │   └── nl-to-sql.ts
│   ├── llm/
│   │   ├── client.ts         ← Anthropic SDK wrapper
│   │   └── prompts.ts        ← System prompts per tool
│   └── types/
│       ├── schemas.ts        ← ColumnInfo, FieldMapping, FilterCondition
│       └── tool-io.ts        ← Zod schemas for input/output validation
```

### Each tool = one file, one function

```typescript
export const detectFieldMappings = {
  name: "detect_field_mappings",
  description: "Analyze two source schemas and detect semantically equivalent columns",
  inputSchema: { /* JSON Schema */ },
  handler: async (input: DetectFieldMappingsInput): Promise<DetectFieldMappingsOutput> => {
    // 1. Build prompt with both schemas + optional sample rows
    // 2. Call Claude via shared LLM client
    // 3. Validate output with Zod
    // 4. Return typed result
  }
}
```

### Shared LLM client

Claude is called for pure reasoning inside each tool — not as an orchestrator, not with tool_use. Each tool has its own system prompt optimized for its task.

```typescript
export async function callClaude<T>(
  systemPrompt: string,
  userMessage: string,
  outputSchema: z.ZodSchema<T>
): Promise<T> {
  const response = await anthropic.messages.create({
    model: "claude-sonnet-4-20250514",
    system: systemPrompt,
    messages: [{ role: "user", content: userMessage }],
    max_tokens: 4096
  });
  // Parse, validate against Zod schema, return typed result
  // On validation failure: retry once with correction prompt
}
```

### No state — ever

The MCP server has:
- No database connection
- No session store
- No middleware
- No knowledge of wizard steps or user context

Every tool receives everything it needs in its input. Testable in isolation with mock inputs.

### Server registration

```typescript
const server = new McpServer({ name: "kalla-mcp", version: "0.1.0" });

for (const tool of allTools) {
  server.tool(tool.name, tool.description, tool.inputSchema, tool.handler);
}

const transport = new StdioServerTransport();
await server.connect(transport);
```

### Next.js MCP client

Singleton that spawns kalla-mcp as a child process and keeps it alive:

```typescript
let client: Client | null = null;

export async function getMcpClient(): Promise<Client> {
  if (client) return client;
  const transport = new StdioClientTransport({
    command: "node",
    args: ["../kalla-mcp/dist/index.js"],
    env: { ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY }
  });
  client = new Client({ name: "kalla-web", version: "0.1.0" });
  await client.connect(transport);
  return client;
}
```

## Frontend Changes

### Add

- Wizard step container with shared state (React context or Zustand)
- Each screen as a step component
- `useAI` hook for calling `/api/ai`
- Common filter mapping UI with dropdown overrides
- NL input components (debounced, with loading states)

### Deprecate (phased)

- Chat-as-primary-UI for recipe creation
- `ChatMessage`, `MatchProposalCard`, `UploadRequestCard` components
- Phase state machine in the frontend
- `agent.ts` inline tool implementations

## Migration Path

| Phase | Scope | Outcome |
|---|---|---|
| Phase 1 | Build `kalla-mcp` with `detect_field_mappings` + `parse_nl_filter` | MCP server exists, Screen 2 tools work |
| Phase 2 | Build wizard UI for Screen 2, call `/api/ai` | Sample data screen functional |
| Phase 3 | Add `infer_rules`, `build_recipe`, `nl_to_sql` to MCP | Screen 3 tools work |
| Phase 4 | Add `validate_recipe` to MCP | Screen 5 tools work |
| Phase 5 | Deprecate `agent.ts`, remove chat-driven flow | Clean codebase |

The chat interface can coexist with the wizard during migration. Both can call `/api/ai`.
