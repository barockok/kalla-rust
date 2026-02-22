# Phase 1: kalla-mcp + AI Gateway Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build the `kalla-mcp` MCP server with `detect_field_mappings` and `parse_nl_filter` tools, plus the `POST /api/ai` gateway in kalla-web.

**Architecture:** Stateless Node/TS MCP server (`kalla-mcp/`) communicates with kalla-web via stdio transport. Each tool calls Claude for reasoning, validates output with Zod, returns structured JSON. kalla-web exposes a single `POST /api/ai` endpoint that proxies tool calls to the MCP server.

**Tech Stack:** TypeScript, `@modelcontextprotocol/sdk`, `@anthropic-ai/sdk`, Zod, Vitest (for kalla-mcp tests), Next.js App Router (for gateway route)

---

### Task 1: Scaffold kalla-mcp Package

**Files:**
- Create: `kalla-mcp/package.json`
- Create: `kalla-mcp/tsconfig.json`
- Create: `kalla-mcp/src/index.ts` (empty placeholder)

**Step 1: Create package.json**

```json
{
  "name": "kalla-mcp",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "dist/index.js",
  "scripts": {
    "build": "tsc",
    "dev": "tsc --watch",
    "test": "vitest run",
    "test:watch": "vitest"
  },
  "dependencies": {
    "@anthropic-ai/sdk": "^0.72.1",
    "@modelcontextprotocol/sdk": "^1.12.1",
    "zod": "^3.24.0"
  },
  "devDependencies": {
    "@types/node": "^22.0.0",
    "typescript": "^5.7.0",
    "vitest": "^3.0.0"
  }
}
```

**Step 2: Create tsconfig.json**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "Node16",
    "moduleResolution": "Node16",
    "outDir": "dist",
    "rootDir": "src",
    "strict": true,
    "esModuleInterop": true,
    "declaration": true,
    "skipLibCheck": true
  },
  "include": ["src"]
}
```

**Step 3: Create placeholder entry**

```typescript
// kalla-mcp/src/index.ts
console.log("kalla-mcp placeholder");
```

**Step 4: Install dependencies**

Run: `cd kalla-mcp && npm install`
Expected: `node_modules/` created, `package-lock.json` generated

**Step 5: Verify build**

Run: `cd kalla-mcp && npx tsc --noEmit`
Expected: No errors

**Step 6: Commit**

```bash
git add kalla-mcp/
git commit -m "feat(kalla-mcp): scaffold MCP server package"
```

---

### Task 2: Define Shared Types and Zod Schemas

**Files:**
- Create: `kalla-mcp/src/types/schemas.ts`
- Create: `kalla-mcp/src/types/tool-io.ts`

**Step 1: Write types/schemas.ts**

These mirror the types from `kalla-web/src/lib/chat-types.ts` that kalla-mcp needs.

```typescript
// kalla-mcp/src/types/schemas.ts
import { z } from "zod";

export const ColumnInfoSchema = z.object({
  name: z.string(),
  data_type: z.string(),
  nullable: z.boolean().optional(),
});
export type ColumnInfo = z.infer<typeof ColumnInfoSchema>;

export const SourceSchemaSchema = z.object({
  alias: z.string(),
  columns: z.array(ColumnInfoSchema),
});
export type SourceSchema = z.infer<typeof SourceSchemaSchema>;

export const FieldMappingSchema = z.object({
  field_a: z.string(),
  field_b: z.string(),
  confidence: z.number().min(0).max(1),
  reason: z.string(),
});
export type FieldMapping = z.infer<typeof FieldMappingSchema>;

export const FilterOpSchema = z.enum([
  "eq", "neq", "gt", "gte", "lt", "lte", "between", "in", "like",
]);
export type FilterOp = z.infer<typeof FilterOpSchema>;

export const FilterConditionSchema = z.object({
  column: z.string(),
  op: FilterOpSchema,
  value: z.union([z.string(), z.number(), z.array(z.string()), z.tuple([z.string(), z.string()])]),
});
export type FilterCondition = z.infer<typeof FilterConditionSchema>;

export const SuggestedFilterSchema = z.object({
  type: z.enum(["date_range", "amount_range", "select"]),
  field_a: z.string(),
  field_b: z.string(),
});
export type SuggestedFilter = z.infer<typeof SuggestedFilterSchema>;
```

**Step 2: Write types/tool-io.ts**

```typescript
// kalla-mcp/src/types/tool-io.ts
import { z } from "zod";
import {
  SourceSchemaSchema,
  FieldMappingSchema,
  SuggestedFilterSchema,
  FilterConditionSchema,
} from "./schemas.js";

// ── detect_field_mappings ─────────────────────────────
export const DetectFieldMappingsInputSchema = z.object({
  schema_a: SourceSchemaSchema,
  schema_b: SourceSchemaSchema,
  sample_a: z.array(z.record(z.unknown())).optional(),
  sample_b: z.array(z.record(z.unknown())).optional(),
});
export type DetectFieldMappingsInput = z.infer<typeof DetectFieldMappingsInputSchema>;

export const DetectFieldMappingsOutputSchema = z.object({
  mappings: z.array(FieldMappingSchema),
  suggested_filters: z.array(SuggestedFilterSchema),
});
export type DetectFieldMappingsOutput = z.infer<typeof DetectFieldMappingsOutputSchema>;

// ── parse_nl_filter ───────────────────────────────────
export const ParseNlFilterInputSchema = z.object({
  text: z.string(),
  schema_a: SourceSchemaSchema,
  schema_b: SourceSchemaSchema,
  current_mappings: z.array(FieldMappingSchema),
});
export type ParseNlFilterInput = z.infer<typeof ParseNlFilterInputSchema>;

export const SourceFilterSchema = z.object({
  source: z.string(),
  column: z.string(),
  op: z.string(),
  value: z.union([z.string(), z.number(), z.array(z.string()), z.tuple([z.string(), z.string()])]),
});

export const ParseNlFilterOutputSchema = z.object({
  filters: z.array(SourceFilterSchema),
  explanation: z.string(),
});
export type ParseNlFilterOutput = z.infer<typeof ParseNlFilterOutputSchema>;
```

**Step 3: Verify types compile**

Run: `cd kalla-mcp && npx tsc --noEmit`
Expected: No errors

**Step 4: Commit**

```bash
git add kalla-mcp/src/types/
git commit -m "feat(kalla-mcp): add shared types and Zod schemas"
```

---

### Task 3: Build LLM Client Wrapper

**Files:**
- Create: `kalla-mcp/src/llm/client.ts`
- Create: `kalla-mcp/src/llm/prompts.ts`
- Test: `kalla-mcp/src/llm/__tests__/client.test.ts`

**Step 1: Write the test**

```typescript
// kalla-mcp/src/llm/__tests__/client.test.ts
import { describe, it, expect, vi, beforeEach } from "vitest";
import { z } from "zod";

// We'll test parseJsonResponse (the pure parsing logic) without calling Claude
describe("parseJsonResponse", () => {
  it("should parse valid JSON from text block", async () => {
    const { parseJsonResponse } = await import("../client.js");
    const schema = z.object({ name: z.string(), value: z.number() });
    const result = parseJsonResponse('{"name": "test", "value": 42}', schema);
    expect(result).toEqual({ name: "test", value: 42 });
  });

  it("should extract JSON from markdown code block", async () => {
    const { parseJsonResponse } = await import("../client.js");
    const schema = z.object({ items: z.array(z.string()) });
    const text = 'Here is the result:\n```json\n{"items": ["a", "b"]}\n```';
    const result = parseJsonResponse(text, schema);
    expect(result).toEqual({ items: ["a", "b"] });
  });

  it("should throw on invalid JSON", async () => {
    const { parseJsonResponse } = await import("../client.js");
    const schema = z.object({ name: z.string() });
    expect(() => parseJsonResponse("not json at all", schema)).toThrow();
  });

  it("should throw on schema validation failure", async () => {
    const { parseJsonResponse } = await import("../client.js");
    const schema = z.object({ name: z.string(), required_field: z.number() });
    expect(() => parseJsonResponse('{"name": "test"}', schema)).toThrow();
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-mcp && npx vitest run src/llm/__tests__/client.test.ts`
Expected: FAIL — `parseJsonResponse` not found

**Step 3: Write llm/client.ts**

```typescript
// kalla-mcp/src/llm/client.ts
import Anthropic from "@anthropic-ai/sdk";
import { z } from "zod";

let anthropicClient: Anthropic | null = null;

function getClient(): Anthropic {
  if (anthropicClient) return anthropicClient;
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error("ANTHROPIC_API_KEY is not set");
  const baseURL = process.env.ANTHROPIC_BASE_URL || undefined;
  anthropicClient = new Anthropic({ apiKey, ...(baseURL ? { baseURL } : {}) });
  return anthropicClient;
}

const MODEL = process.env.ANTHROPIC_MODEL || "claude-sonnet-4-20250514";

/**
 * Extract and validate JSON from Claude's text response.
 * Handles both raw JSON and markdown-fenced JSON blocks.
 */
export function parseJsonResponse<T>(text: string, schema: z.ZodSchema<T>): T {
  // Try to extract JSON from markdown code block first
  const fenced = text.match(/```(?:json)?\s*\n?([\s\S]*?)\n?```/);
  const jsonStr = fenced ? fenced[1].trim() : text.trim();

  let parsed: unknown;
  try {
    parsed = JSON.parse(jsonStr);
  } catch {
    throw new Error(`Failed to parse JSON from LLM response: ${jsonStr.slice(0, 200)}`);
  }

  const result = schema.safeParse(parsed);
  if (!result.success) {
    throw new Error(`LLM response failed schema validation: ${result.error.message}`);
  }
  return result.data;
}

/**
 * Call Claude with a system prompt and user message, parse response against Zod schema.
 * Retries once with a correction prompt on parse/validation failure.
 */
export async function callClaude<T>(
  systemPrompt: string,
  userMessage: string,
  outputSchema: z.ZodSchema<T>,
): Promise<T> {
  const client = getClient();

  const response = await client.messages.create({
    model: MODEL,
    max_tokens: 4096,
    system: systemPrompt,
    messages: [{ role: "user", content: userMessage }],
  });

  const textBlock = response.content.find((b) => b.type === "text");
  if (!textBlock || textBlock.type !== "text") {
    throw new Error("No text block in Claude response");
  }

  try {
    return parseJsonResponse(textBlock.text, outputSchema);
  } catch (firstError) {
    // Retry once with correction prompt
    const retryResponse = await client.messages.create({
      model: MODEL,
      max_tokens: 4096,
      system: systemPrompt,
      messages: [
        { role: "user", content: userMessage },
        { role: "assistant", content: textBlock.text },
        {
          role: "user",
          content: `Your response had a formatting issue: ${firstError instanceof Error ? firstError.message : String(firstError)}\n\nPlease return ONLY valid JSON matching the required schema. No explanation, just JSON.`,
        },
      ],
    });

    const retryText = retryResponse.content.find((b) => b.type === "text");
    if (!retryText || retryText.type !== "text") {
      throw new Error("No text block in retry response");
    }
    return parseJsonResponse(retryText.text, outputSchema);
  }
}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-mcp && npx vitest run src/llm/__tests__/client.test.ts`
Expected: All 4 tests PASS

**Step 5: Write llm/prompts.ts**

```typescript
// kalla-mcp/src/llm/prompts.ts

export const DETECT_FIELD_MAPPINGS_SYSTEM = `You are a data schema analyst. You receive two table schemas (with optional sample rows) and identify columns that represent the same real-world information despite having different names.

Rules:
- Compare column names, data types, and sample values
- A match means the columns hold semantically equivalent data
- Confidence 0.0-1.0 based on name similarity + type match + value overlap
- Only suggest matches above 0.5 confidence
- Each column can map to at most one column in the other schema
- Suggest filter types based on matched column data types:
  - date/timestamp columns → "date_range"
  - numeric/decimal columns → "amount_range"
  - string with low cardinality → "select"
  - string with high cardinality → skip

Return ONLY valid JSON. No explanation outside the JSON structure.

Required JSON shape:
{
  "mappings": [
    { "field_a": "col_from_source_a", "field_b": "col_from_source_b", "confidence": 0.9, "reason": "..." }
  ],
  "suggested_filters": [
    { "type": "date_range|amount_range|select", "field_a": "...", "field_b": "..." }
  ]
}`;

export const PARSE_NL_FILTER_SYSTEM = `You translate natural language filter descriptions into structured filter conditions. You receive schema context and field mappings so you know which columns exist and how they relate across sources.

Rules:
- Use the mapped column names when the user refers to a concept (e.g., "date" → use the actual column name per source)
- If a filter applies to a mapped pair, create conditions for BOTH sources using their respective column names
- Operators: eq, neq, gt, gte, lt, lte, between, in, like
- For date ranges use "between" with ISO date strings
- For amounts use numeric values (not strings)
- The "source" field should be the source alias

Return ONLY valid JSON. No explanation outside the JSON structure.

Required JSON shape:
{
  "filters": [
    { "source": "source_alias", "column": "column_name", "op": "operator", "value": "..." }
  ],
  "explanation": "Brief human-readable summary of what was parsed"
}`;
```

**Step 6: Commit**

```bash
git add kalla-mcp/src/llm/
git commit -m "feat(kalla-mcp): add LLM client wrapper with retry and prompts"
```

---

### Task 4: Implement detect_field_mappings Tool

**Files:**
- Create: `kalla-mcp/src/tools/detect-field-mappings.ts`
- Test: `kalla-mcp/src/tools/__tests__/detect-field-mappings.test.ts`

**Step 1: Write the test**

```typescript
// kalla-mcp/src/tools/__tests__/detect-field-mappings.test.ts
import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock the LLM client so tests don't call Claude
vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { detectFieldMappings } from "../detect-field-mappings.js";
import { callClaude } from "../../llm/client.js";
import type { DetectFieldMappingsInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("detect_field_mappings", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  const baseInput: DetectFieldMappingsInput = {
    schema_a: {
      alias: "bank_statement",
      columns: [
        { name: "transaction_id", data_type: "varchar" },
        { name: "transaction_date", data_type: "date" },
        { name: "amount", data_type: "decimal" },
        { name: "description", data_type: "varchar" },
      ],
    },
    schema_b: {
      alias: "invoice_system",
      columns: [
        { name: "invoice_id", data_type: "varchar" },
        { name: "invoice_date", data_type: "date" },
        { name: "total_amount", data_type: "decimal" },
        { name: "vendor_name", data_type: "varchar" },
      ],
    },
  };

  it("should call Claude and return parsed mappings", async () => {
    mockCallClaude.mockResolvedValueOnce({
      mappings: [
        { field_a: "transaction_date", field_b: "invoice_date", confidence: 0.92, reason: "Both date columns" },
        { field_a: "amount", field_b: "total_amount", confidence: 0.87, reason: "Both numeric amount columns" },
      ],
      suggested_filters: [
        { type: "date_range", field_a: "transaction_date", field_b: "invoice_date" },
        { type: "amount_range", field_a: "amount", field_b: "total_amount" },
      ],
    });

    const result = await detectFieldMappings.handler(baseInput);

    expect(result.mappings).toHaveLength(2);
    expect(result.mappings[0].field_a).toBe("transaction_date");
    expect(result.mappings[0].field_b).toBe("invoice_date");
    expect(result.suggested_filters).toHaveLength(2);
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should include sample rows in prompt when provided", async () => {
    mockCallClaude.mockResolvedValueOnce({
      mappings: [],
      suggested_filters: [],
    });

    const inputWithSamples = {
      ...baseInput,
      sample_a: [{ transaction_date: "2026-01-15", amount: 1500.00 }],
      sample_b: [{ invoice_date: "2026-01-14", total_amount: 1500.00 }],
    };

    await detectFieldMappings.handler(inputWithSamples);

    const callArgs = mockCallClaude.mock.calls[0];
    const userMessage = callArgs[1]; // second arg is userMessage
    expect(userMessage).toContain("Sample rows");
    expect(userMessage).toContain("2026-01-15");
  });

  it("should have correct tool metadata", () => {
    expect(detectFieldMappings.name).toBe("detect_field_mappings");
    expect(detectFieldMappings.description).toBeTruthy();
    expect(detectFieldMappings.inputSchema).toBeDefined();
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-mcp && npx vitest run src/tools/__tests__/detect-field-mappings.test.ts`
Expected: FAIL — module not found

**Step 3: Write the tool implementation**

```typescript
// kalla-mcp/src/tools/detect-field-mappings.ts
import { callClaude } from "../llm/client.js";
import { DETECT_FIELD_MAPPINGS_SYSTEM } from "../llm/prompts.js";
import {
  DetectFieldMappingsInputSchema,
  DetectFieldMappingsOutputSchema,
  type DetectFieldMappingsInput,
  type DetectFieldMappingsOutput,
} from "../types/tool-io.js";

function formatSampleRows(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return "(empty)";
  const keys = Object.keys(rows[0]);
  const header = keys.join(" | ");
  const body = rows
    .slice(0, 5)
    .map((r) => keys.map((k) => String(r[k] ?? "null")).join(" | "))
    .join("\n");
  return `${header}\n${body}`;
}

function buildUserMessage(input: DetectFieldMappingsInput): string {
  let msg = `Source A: "${input.schema_a.alias}"\nColumns:\n`;
  msg += input.schema_a.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  if (input.sample_a && input.sample_a.length > 0) {
    msg += `\n\nSample rows (Source A):\n${formatSampleRows(input.sample_a)}`;
  }

  msg += `\n\nSource B: "${input.schema_b.alias}"\nColumns:\n`;
  msg += input.schema_b.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  if (input.sample_b && input.sample_b.length > 0) {
    msg += `\n\nSample rows (Source B):\n${formatSampleRows(input.sample_b)}`;
  }

  msg += "\n\nIdentify all column pairs that represent the same information.";
  return msg;
}

export const detectFieldMappings = {
  name: "detect_field_mappings" as const,
  description:
    "Analyze two source schemas (with optional sample rows) and detect columns that represent the same real-world information despite having different names. Returns field mappings with confidence scores and suggested filter types.",
  inputSchema: {
    type: "object" as const,
    properties: {
      schema_a: {
        type: "object",
        description: "First source schema with alias and columns",
        properties: {
          alias: { type: "string" },
          columns: {
            type: "array",
            items: {
              type: "object",
              properties: {
                name: { type: "string" },
                data_type: { type: "string" },
                nullable: { type: "boolean" },
              },
              required: ["name", "data_type"],
            },
          },
        },
        required: ["alias", "columns"],
      },
      schema_b: {
        type: "object",
        description: "Second source schema with alias and columns",
        properties: {
          alias: { type: "string" },
          columns: {
            type: "array",
            items: {
              type: "object",
              properties: {
                name: { type: "string" },
                data_type: { type: "string" },
                nullable: { type: "boolean" },
              },
              required: ["name", "data_type"],
            },
          },
        },
        required: ["alias", "columns"],
      },
      sample_a: {
        type: "array",
        description: "Optional sample rows from source A for better detection",
        items: { type: "object" },
      },
      sample_b: {
        type: "array",
        description: "Optional sample rows from source B for better detection",
        items: { type: "object" },
      },
    },
    required: ["schema_a", "schema_b"],
  },
  handler: async (input: DetectFieldMappingsInput): Promise<DetectFieldMappingsOutput> => {
    const parsed = DetectFieldMappingsInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(DETECT_FIELD_MAPPINGS_SYSTEM, userMessage, DetectFieldMappingsOutputSchema);
  },
};
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-mcp && npx vitest run src/tools/__tests__/detect-field-mappings.test.ts`
Expected: All 3 tests PASS

**Step 5: Commit**

```bash
git add kalla-mcp/src/tools/detect-field-mappings.ts kalla-mcp/src/tools/__tests__/
git commit -m "feat(kalla-mcp): implement detect_field_mappings tool"
```

---

### Task 5: Implement parse_nl_filter Tool

**Files:**
- Create: `kalla-mcp/src/tools/parse-nl-filter.ts`
- Test: `kalla-mcp/src/tools/__tests__/parse-nl-filter.test.ts`

**Step 1: Write the test**

```typescript
// kalla-mcp/src/tools/__tests__/parse-nl-filter.test.ts
import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { parseNlFilter } from "../parse-nl-filter.js";
import { callClaude } from "../../llm/client.js";
import type { ParseNlFilterInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("parse_nl_filter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  const baseInput: ParseNlFilterInput = {
    text: "Only transactions above $500 from last month",
    schema_a: {
      alias: "bank_statement",
      columns: [
        { name: "transaction_date", data_type: "date" },
        { name: "amount", data_type: "decimal" },
      ],
    },
    schema_b: {
      alias: "invoice_system",
      columns: [
        { name: "invoice_date", data_type: "date" },
        { name: "total_amount", data_type: "decimal" },
      ],
    },
    current_mappings: [
      { field_a: "transaction_date", field_b: "invoice_date", confidence: 0.92, reason: "date columns" },
      { field_a: "amount", field_b: "total_amount", confidence: 0.87, reason: "amount columns" },
    ],
  };

  it("should call Claude and return parsed filter conditions", async () => {
    mockCallClaude.mockResolvedValueOnce({
      filters: [
        { source: "bank_statement", column: "amount", op: "gt", value: 500 },
        { source: "invoice_system", column: "total_amount", op: "gt", value: 500 },
      ],
      explanation: "Filtering both sources for amounts > $500",
    });

    const result = await parseNlFilter.handler(baseInput);

    expect(result.filters).toHaveLength(2);
    expect(result.filters[0].source).toBe("bank_statement");
    expect(result.filters[0].column).toBe("amount");
    expect(result.explanation).toBeTruthy();
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should include current mappings in the prompt", async () => {
    mockCallClaude.mockResolvedValueOnce({
      filters: [],
      explanation: "No filters parsed",
    });

    await parseNlFilter.handler(baseInput);

    const userMessage = mockCallClaude.mock.calls[0][1];
    expect(userMessage).toContain("transaction_date");
    expect(userMessage).toContain("invoice_date");
    expect(userMessage).toContain("Current field mappings");
  });

  it("should have correct tool metadata", () => {
    expect(parseNlFilter.name).toBe("parse_nl_filter");
    expect(parseNlFilter.inputSchema).toBeDefined();
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-mcp && npx vitest run src/tools/__tests__/parse-nl-filter.test.ts`
Expected: FAIL — module not found

**Step 3: Write the tool implementation**

```typescript
// kalla-mcp/src/tools/parse-nl-filter.ts
import { callClaude } from "../llm/client.js";
import { PARSE_NL_FILTER_SYSTEM } from "../llm/prompts.js";
import {
  ParseNlFilterInputSchema,
  ParseNlFilterOutputSchema,
  type ParseNlFilterInput,
  type ParseNlFilterOutput,
} from "../types/tool-io.js";

function buildUserMessage(input: ParseNlFilterInput): string {
  let msg = `User instruction: "${input.text}"\n\n`;

  msg += `Source A: "${input.schema_a.alias}"\nColumns:\n`;
  msg += input.schema_a.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  msg += `\n\nSource B: "${input.schema_b.alias}"\nColumns:\n`;
  msg += input.schema_b.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  if (input.current_mappings.length > 0) {
    msg += "\n\nCurrent field mappings (source A → source B):\n";
    msg += input.current_mappings
      .map((m) => `  - ${m.field_a} → ${m.field_b} (confidence: ${m.confidence})`)
      .join("\n");
  }

  msg += "\n\nTranslate the user instruction into filter conditions.";
  return msg;
}

export const parseNlFilter = {
  name: "parse_nl_filter" as const,
  description:
    "Translate a natural language filter description into structured filter conditions, using schema context and field mappings to resolve column references.",
  inputSchema: {
    type: "object" as const,
    properties: {
      text: { type: "string", description: "Natural language filter description from user" },
      schema_a: {
        type: "object",
        description: "First source schema",
        properties: {
          alias: { type: "string" },
          columns: {
            type: "array",
            items: {
              type: "object",
              properties: { name: { type: "string" }, data_type: { type: "string" } },
              required: ["name", "data_type"],
            },
          },
        },
        required: ["alias", "columns"],
      },
      schema_b: {
        type: "object",
        description: "Second source schema",
        properties: {
          alias: { type: "string" },
          columns: {
            type: "array",
            items: {
              type: "object",
              properties: { name: { type: "string" }, data_type: { type: "string" } },
              required: ["name", "data_type"],
            },
          },
        },
        required: ["alias", "columns"],
      },
      current_mappings: {
        type: "array",
        description: "Current field mappings between sources",
        items: {
          type: "object",
          properties: {
            field_a: { type: "string" },
            field_b: { type: "string" },
            confidence: { type: "number" },
            reason: { type: "string" },
          },
          required: ["field_a", "field_b", "confidence", "reason"],
        },
      },
    },
    required: ["text", "schema_a", "schema_b", "current_mappings"],
  },
  handler: async (input: ParseNlFilterInput): Promise<ParseNlFilterOutput> => {
    const parsed = ParseNlFilterInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(PARSE_NL_FILTER_SYSTEM, userMessage, ParseNlFilterOutputSchema);
  },
};
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-mcp && npx vitest run src/tools/__tests__/parse-nl-filter.test.ts`
Expected: All 3 tests PASS

**Step 5: Commit**

```bash
git add kalla-mcp/src/tools/parse-nl-filter.ts kalla-mcp/src/tools/__tests__/parse-nl-filter.test.ts
git commit -m "feat(kalla-mcp): implement parse_nl_filter tool"
```

---

### Task 6: Wire Up MCP Server Entry Point

**Files:**
- Modify: `kalla-mcp/src/index.ts`
- Test: `kalla-mcp/src/tools/__tests__/server.test.ts`

**Step 1: Write integration test**

```typescript
// kalla-mcp/src/tools/__tests__/server.test.ts
import { describe, it, expect, vi } from "vitest";

// Mock callClaude globally for server tests
vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn().mockResolvedValue({
    mappings: [
      { field_a: "date_col", field_b: "dt_col", confidence: 0.9, reason: "date match" },
    ],
    suggested_filters: [{ type: "date_range", field_a: "date_col", field_b: "dt_col" }],
  }),
  parseJsonResponse: vi.fn(),
}));

import { createServer } from "../server.js";

describe("MCP server", () => {
  it("should register all tools", () => {
    const server = createServer();
    // Server created without error means tools registered
    expect(server).toBeDefined();
  });
});
```

**Step 2: Create server.ts (extracted from index.ts for testability)**

```typescript
// kalla-mcp/src/server.ts
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { detectFieldMappings } from "./tools/detect-field-mappings.js";
import { parseNlFilter } from "./tools/parse-nl-filter.js";

const ALL_TOOLS = [detectFieldMappings, parseNlFilter];

export function createServer(): McpServer {
  const server = new McpServer({
    name: "kalla-mcp",
    version: "0.1.0",
  });

  for (const tool of ALL_TOOLS) {
    server.tool(
      tool.name,
      tool.description,
      tool.inputSchema,
      async (args: Record<string, unknown>) => {
        try {
          const result = await tool.handler(args as never);
          return { content: [{ type: "text" as const, text: JSON.stringify(result) }] };
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          return { content: [{ type: "text" as const, text: JSON.stringify({ error: message }) }], isError: true };
        }
      },
    );
  }

  return server;
}
```

**Step 3: Rewrite index.ts as thin entry point**

```typescript
// kalla-mcp/src/index.ts
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createServer } from "./server.js";

const server = createServer();
const transport = new StdioServerTransport();
await server.connect(transport);
```

**Step 4: Run all tests**

Run: `cd kalla-mcp && npx vitest run`
Expected: All tests PASS (client tests + detect + parse + server)

**Step 5: Verify build**

Run: `cd kalla-mcp && npm run build`
Expected: `dist/` directory created, no errors

**Step 6: Commit**

```bash
git add kalla-mcp/src/index.ts kalla-mcp/src/server.ts kalla-mcp/src/tools/__tests__/server.test.ts
git commit -m "feat(kalla-mcp): wire up MCP server with stdio transport"
```

---

### Task 7: Add MCP Client to kalla-web

**Files:**
- Create: `kalla-web/src/lib/mcp-client.ts`

**Step 1: Install MCP SDK in kalla-web**

Run: `cd kalla-web && npm install @modelcontextprotocol/sdk`

**Step 2: Write mcp-client.ts**

```typescript
// kalla-web/src/lib/mcp-client.ts
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import path from "path";

let client: Client | null = null;
let connecting: Promise<Client> | null = null;

export async function getMcpClient(): Promise<Client> {
  if (client) return client;

  // Prevent concurrent connection attempts
  if (connecting) return connecting;

  connecting = (async () => {
    const mcpServerPath = path.resolve(
      process.cwd(),
      process.env.MCP_SERVER_PATH || "../kalla-mcp/dist/index.js",
    );

    const transport = new StdioClientTransport({
      command: "node",
      args: [mcpServerPath],
      env: {
        ...process.env,
        ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY || "",
        ANTHROPIC_MODEL: process.env.ANTHROPIC_MODEL || "",
        ANTHROPIC_BASE_URL: process.env.ANTHROPIC_BASE_URL || "",
      } as Record<string, string>,
    });

    const c = new Client({ name: "kalla-web", version: "0.1.0" });
    await c.connect(transport);
    client = c;
    connecting = null;
    return c;
  })();

  return connecting;
}

export async function callMcpTool(
  toolName: string,
  input: Record<string, unknown>,
): Promise<unknown> {
  const c = await getMcpClient();
  const result = await c.callTool({ name: toolName, arguments: input });

  // MCP returns content array; extract text content and parse JSON
  const textContent = result.content.find(
    (c): c is { type: "text"; text: string } => c.type === "text",
  );

  if (!textContent) {
    throw new Error(`No text content in MCP tool response for ${toolName}`);
  }

  const parsed = JSON.parse(textContent.text);

  if (result.isError) {
    throw new Error(parsed.error || `MCP tool ${toolName} failed`);
  }

  return parsed;
}
```

**Step 3: Commit**

```bash
git add kalla-web/src/lib/mcp-client.ts kalla-web/package.json kalla-web/package-lock.json
git commit -m "feat(kalla-web): add MCP client for kalla-mcp connection"
```

---

### Task 8: Add POST /api/ai Gateway Route

**Files:**
- Create: `kalla-web/src/app/api/ai/route.ts`

**Step 1: Write the gateway route**

```typescript
// kalla-web/src/app/api/ai/route.ts
import { NextResponse } from "next/server";
import { callMcpTool } from "@/lib/mcp-client";

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { tool, input } = body;

    if (!tool || typeof tool !== "string") {
      return NextResponse.json({ error: "Missing or invalid 'tool' field" }, { status: 400 });
    }

    if (!input || typeof input !== "object") {
      return NextResponse.json({ error: "Missing or invalid 'input' field" }, { status: 400 });
    }

    const result = await callMcpTool(tool, input);
    return NextResponse.json({ result });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[/api/ai] Error:`, message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
```

**Step 2: Commit**

```bash
git add kalla-web/src/app/api/ai/route.ts
git commit -m "feat(kalla-web): add POST /api/ai gateway route"
```

---

### Task 9: End-to-End Verification

**Step 1: Build kalla-mcp**

Run: `cd kalla-mcp && npm run build`
Expected: Clean build, `dist/` populated

**Step 2: Run kalla-mcp tests**

Run: `cd kalla-mcp && npx vitest run`
Expected: All tests PASS

**Step 3: Smoke test the MCP server manually**

Run: `cd kalla-mcp && echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1.0"}}}' | ANTHROPIC_API_KEY=test node dist/index.js`
Expected: JSON-RPC response with server capabilities (will error on tool calls without real API key, but initialize should succeed)

**Step 4: Start kalla-web dev server and test gateway**

Run: `cd kalla-web && npm run dev`

In another terminal, test the gateway with curl (requires kalla-mcp to be built and ANTHROPIC_API_KEY set):

```bash
curl -X POST http://localhost:3000/api/ai \
  -H "Content-Type: application/json" \
  -d '{
    "tool": "detect_field_mappings",
    "input": {
      "schema_a": {
        "alias": "bank",
        "columns": [
          {"name": "transaction_date", "data_type": "date"},
          {"name": "amount", "data_type": "decimal"}
        ]
      },
      "schema_b": {
        "alias": "invoices",
        "columns": [
          {"name": "invoice_date", "data_type": "date"},
          {"name": "total_amount", "data_type": "decimal"}
        ]
      }
    }
  }'
```

Expected: JSON response with `{ "result": { "mappings": [...], "suggested_filters": [...] } }`

**Step 5: Final commit**

```bash
git add -A
git commit -m "feat: complete Phase 1 — kalla-mcp with detect_field_mappings + parse_nl_filter + /api/ai gateway"
```
