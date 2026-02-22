# Phase 3: Screen 3 — AI Pattern Detection & Rules — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add three MCP tools (infer_rules, build_recipe, nl_to_sql) and build wizard Screen 3 UI for AI pattern detection, rule review, and custom rule creation.

**Architecture:** Three new MCP tools follow existing kalla-mcp patterns (one file per tool, Zod I/O, shared callClaude). Wizard state gets new fields/actions for patterns, rules, and recipe SQL. Screen 3 UI has PatternCard, PrimaryKeysCard, RuleCard, and AddCustomRule components.

**Tech Stack:** TypeScript, MCP SDK, Zod, Anthropic SDK, React 19, Next.js 16, Tailwind CSS v4, shadcn/ui, Lucide icons

---

### Task 1: Add Zod schemas and types for new MCP tools

**Files:**
- Modify: `kalla-mcp/src/types/schemas.ts`
- Modify: `kalla-mcp/src/types/tool-io.ts`

**Step 1: Add new schemas to `kalla-mcp/src/types/schemas.ts`**

Append after the `SuggestedFilter` block:

```typescript
export const PatternTypeSchema = z.enum(["1:1", "1:N", "N:M"]);
export type PatternType = z.infer<typeof PatternTypeSchema>;

export const DetectedPatternSchema = z.object({
  type: PatternTypeSchema,
  description: z.string(),
  confidence: z.number().min(0).max(1),
});
export type DetectedPattern = z.infer<typeof DetectedPatternSchema>;

export const PrimaryKeysSchema = z.object({
  source_a: z.array(z.string()),
  source_b: z.array(z.string()),
});
export type PrimaryKeys = z.infer<typeof PrimaryKeysSchema>;

export const InferredRuleSchema = z.object({
  name: z.string(),
  sql: z.string(),
  description: z.string(),
  confidence: z.number().min(0).max(1),
  evidence: z.array(z.record(z.unknown())).optional().default([]),
});
export type InferredRule = z.infer<typeof InferredRuleSchema>;
```

**Step 2: Add I/O schemas to `kalla-mcp/src/types/tool-io.ts`**

Add imports at the top:

```typescript
import {
  SourceSchemaSchema,
  FieldMappingSchema,
  SuggestedFilterSchema,
  DetectedPatternSchema,
  PrimaryKeysSchema,
  InferredRuleSchema,
  PatternTypeSchema,
} from "./schemas.js";
```

Append after the `ParseNlFilterOutput` block:

```typescript
// ── infer_rules ───────────────────────────────────────
export const InferRulesInputSchema = z.object({
  schema_a: SourceSchemaSchema,
  schema_b: SourceSchemaSchema,
  sample_a: z.array(z.record(z.unknown())),
  sample_b: z.array(z.record(z.unknown())),
  mappings: z.array(FieldMappingSchema),
});
export type InferRulesInput = z.infer<typeof InferRulesInputSchema>;

export const InferRulesOutputSchema = z.object({
  pattern: DetectedPatternSchema,
  primary_keys: PrimaryKeysSchema,
  rules: z.array(InferredRuleSchema),
});
export type InferRulesOutput = z.infer<typeof InferRulesOutputSchema>;

// ── build_recipe ──────────────────────────────────────
export const BuildRecipeInputSchema = z.object({
  rules: z.array(z.object({
    name: z.string(),
    sql: z.string(),
    description: z.string(),
  })),
  sources: z.object({
    alias_a: z.string(),
    alias_b: z.string(),
  }),
  primary_keys: PrimaryKeysSchema,
  pattern_type: PatternTypeSchema,
});
export type BuildRecipeInput = z.infer<typeof BuildRecipeInputSchema>;

export const BuildRecipeOutputSchema = z.object({
  match_sql: z.string(),
  explanation: z.string(),
});
export type BuildRecipeOutput = z.infer<typeof BuildRecipeOutputSchema>;

// ── nl_to_sql ─────────────────────────────────────────
export const NlToSqlInputSchema = z.object({
  text: z.string(),
  schema_a: SourceSchemaSchema,
  schema_b: SourceSchemaSchema,
  mappings: z.array(FieldMappingSchema),
});
export type NlToSqlInput = z.infer<typeof NlToSqlInputSchema>;

export const NlToSqlOutputSchema = z.object({
  name: z.string(),
  sql: z.string(),
  description: z.string(),
  confidence: z.number().min(0).max(1),
});
export type NlToSqlOutput = z.infer<typeof NlToSqlOutputSchema>;
```

**Step 3: Verify build**

Run: `cd kalla-mcp && npm run build`
Expected: Clean compile

**Step 4: Commit**

```bash
git add kalla-mcp/src/types/schemas.ts kalla-mcp/src/types/tool-io.ts
git commit -m "feat(kalla-mcp): add Zod schemas for infer_rules, build_recipe, nl_to_sql"
```

---

### Task 2: Add system prompts for the three new tools

**Files:**
- Modify: `kalla-mcp/src/llm/prompts.ts`

**Step 1: Append three new prompts**

```typescript
export const INFER_RULES_SYSTEM = `You are a data reconciliation expert. You analyze sample data from two sources to detect matching patterns, identify primary keys, and generate DataFusion SQL matching rules.

Context:
- You receive two schemas with sample rows and known field mappings
- You determine the relationship pattern (1:1, 1:N, or N:M)
- You identify primary key columns for joining records
- You generate DataFusion SQL expressions for each matching rule

DataFusion SQL notes:
- Use "l." prefix for left source columns, "r." for right source columns
- Tolerance matching: ABS(l.amount - r.amount) <= 0.01
- Date range: r.date BETWEEN l.date - INTERVAL '7 days' AND l.date + INTERVAL '7 days'
- String matching: l.name = r.name or LOWER(l.name) = LOWER(r.name)
- Aggregation for 1:N: SUM(r.amount) with GROUP BY on left PK

Rules:
- Confidence 0.0-1.0 for pattern type and each rule
- Include 2-3 evidence rows that demonstrate the rule
- Evidence rows should have columns from both sources showing the match
- Generate practical, specific rules (not generic ones)

Return ONLY valid JSON:
{
  "pattern": { "type": "1:1|1:N|N:M", "description": "...", "confidence": 0.9 },
  "primary_keys": { "source_a": ["col"], "source_b": ["col"] },
  "rules": [
    {
      "name": "Rule Name",
      "sql": "DataFusion SQL expression",
      "description": "Human-readable explanation",
      "confidence": 0.9,
      "evidence": [{"left_col": "val", "right_col": "val"}]
    }
  ]
}`;

export const BUILD_RECIPE_SYSTEM = `You are a DataFusion SQL expert. You assemble matching rules into a complete DataFusion SQL query for reconciliation.

Context:
- You receive accepted matching rules with SQL expressions
- You receive source aliases, primary keys, and pattern type
- You produce a single complete SQL query

DataFusion SQL requirements:
- Use source aliases as table names: FROM left_alias l JOIN right_alias r
- For 1:1 patterns: simple JOIN with matching conditions
- For 1:N patterns: use GROUP BY on left PK, aggregate right-side values
- For N:M patterns: CROSS JOIN with WHERE conditions
- Include all accepted rule SQL expressions as JOIN/WHERE conditions
- Output columns: all primary keys from both sources, matched status

Return ONLY valid JSON:
{
  "match_sql": "SELECT ... FROM ... JOIN ... ON ... WHERE ...",
  "explanation": "Human-readable description of what this query does"
}`;

export const NL_TO_SQL_SYSTEM = `You convert a natural language matching rule into a DataFusion SQL expression for data reconciliation.

Context:
- You receive schema information and field mappings
- The user describes a matching condition in plain language
- You produce a single SQL expression (not a full query)

DataFusion SQL notes:
- Use "l." prefix for left source columns, "r." for right source columns
- Tolerance: ABS(l.amount - r.amount) <= threshold
- Date range: r.date BETWEEN l.date - INTERVAL 'N days' AND l.date + INTERVAL 'N days'
- String: LOWER(l.name) = LOWER(r.name)
- Numeric: l.amount = r.amount

Return ONLY valid JSON:
{
  "name": "Short rule name",
  "sql": "DataFusion SQL expression using l. and r. prefixes",
  "description": "Human-readable explanation",
  "confidence": 0.85
}`;
```

**Step 2: Verify build**

Run: `cd kalla-mcp && npm run build`
Expected: Clean compile

**Step 3: Commit**

```bash
git add kalla-mcp/src/llm/prompts.ts
git commit -m "feat(kalla-mcp): add system prompts for infer_rules, build_recipe, nl_to_sql"
```

---

### Task 3: Implement infer_rules tool with tests

**Files:**
- Create: `kalla-mcp/src/tools/infer-rules.ts`
- Create: `kalla-mcp/src/tools/__tests__/infer-rules.test.ts`

**Step 1: Write the test**

```typescript
// kalla-mcp/src/tools/__tests__/infer-rules.test.ts
import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { inferRules } from "../infer-rules.js";
import { callClaude } from "../../llm/client.js";
import type { InferRulesInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("infer_rules", () => {
  beforeEach(() => vi.clearAllMocks());

  const baseInput: InferRulesInput = {
    schema_a: {
      alias: "bank_statement",
      columns: [
        { name: "transaction_id", data_type: "varchar" },
        { name: "transaction_date", data_type: "date" },
        { name: "amount", data_type: "decimal" },
      ],
    },
    schema_b: {
      alias: "invoice_system",
      columns: [
        { name: "invoice_id", data_type: "varchar" },
        { name: "invoice_date", data_type: "date" },
        { name: "total_amount", data_type: "decimal" },
      ],
    },
    sample_a: [
      { transaction_id: "TXN-001", transaction_date: "2026-01-15", amount: 1500.0 },
    ],
    sample_b: [
      { invoice_id: "INV-1001", invoice_date: "2026-01-14", total_amount: 1500.0 },
    ],
    mappings: [
      { field_a: "transaction_date", field_b: "invoice_date", confidence: 0.92, reason: "Both date columns" },
      { field_a: "amount", field_b: "total_amount", confidence: 0.87, reason: "Both amount columns" },
    ],
  };

  it("should call Claude and return pattern, primary keys, and rules", async () => {
    mockCallClaude.mockResolvedValueOnce({
      pattern: { type: "1:N", description: "One bank txn matches multiple invoices", confidence: 0.88 },
      primary_keys: { source_a: ["transaction_id"], source_b: ["invoice_id"] },
      rules: [
        {
          name: "Amount Sum Match",
          sql: "ABS(l.amount - SUM(r.total_amount)) <= 0.01",
          description: "Sum of invoice amounts equals bank transaction",
          confidence: 0.91,
          evidence: [{ transaction_id: "TXN-001", amount: 1500, total_amount: 1500 }],
        },
      ],
    });

    const result = await inferRules.handler(baseInput);

    expect(result.pattern.type).toBe("1:N");
    expect(result.primary_keys.source_a).toEqual(["transaction_id"]);
    expect(result.rules).toHaveLength(1);
    expect(result.rules[0].sql).toContain("SUM");
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should include sample data and mappings in prompt", async () => {
    mockCallClaude.mockResolvedValueOnce({
      pattern: { type: "1:1", description: "Direct match", confidence: 0.9 },
      primary_keys: { source_a: ["transaction_id"], source_b: ["invoice_id"] },
      rules: [],
    });

    await inferRules.handler(baseInput);

    const userMessage = mockCallClaude.mock.calls[0][1];
    expect(userMessage).toContain("TXN-001");
    expect(userMessage).toContain("transaction_date → invoice_date");
  });

  it("should have correct tool metadata", () => {
    expect(inferRules.name).toBe("infer_rules");
    expect(inferRules.description).toBeTruthy();
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-mcp && npx vitest run src/tools/__tests__/infer-rules.test.ts`
Expected: FAIL — cannot find `../infer-rules.js`

**Step 3: Implement the tool**

```typescript
// kalla-mcp/src/tools/infer-rules.ts
import { callClaude } from "../llm/client.js";
import { INFER_RULES_SYSTEM } from "../llm/prompts.js";
import {
  InferRulesInputSchema,
  InferRulesOutputSchema,
  type InferRulesInput,
  type InferRulesOutput,
} from "../types/tool-io.js";

function formatSampleRows(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return "(empty)";
  const keys = Object.keys(rows[0]);
  const header = keys.join(" | ");
  const body = rows
    .slice(0, 10)
    .map((r) => keys.map((k) => String(r[k] ?? "null")).join(" | "))
    .join("\n");
  return `${header}\n${body}`;
}

function buildUserMessage(input: InferRulesInput): string {
  let msg = `Source A: "${input.schema_a.alias}"\nColumns:\n`;
  msg += input.schema_a.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");
  msg += `\n\nSample rows (Source A):\n${formatSampleRows(input.sample_a)}`;

  msg += `\n\nSource B: "${input.schema_b.alias}"\nColumns:\n`;
  msg += input.schema_b.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");
  msg += `\n\nSample rows (Source B):\n${formatSampleRows(input.sample_b)}`;

  if (input.mappings.length > 0) {
    msg += "\n\nKnown field mappings (A → B):\n";
    msg += input.mappings
      .map((m) => `  - ${m.field_a} → ${m.field_b} (confidence: ${m.confidence})`)
      .join("\n");
  }

  msg += "\n\nAnalyze the data and identify: matching pattern, primary keys, and matching rules with DataFusion SQL.";
  return msg;
}

export const inferRules = {
  name: "infer_rules" as const,
  description:
    "Analyze sample data from both sources to detect the matching pattern (1:1, 1:N, N:M), identify primary keys, and generate DataFusion SQL matching rules with evidence.",
  inputSchema: {
    type: "object" as const,
    properties: {
      schema_a: { type: "object", description: "Left source schema", properties: { alias: { type: "string" }, columns: { type: "array", items: { type: "object", properties: { name: { type: "string" }, data_type: { type: "string" } }, required: ["name", "data_type"] } } }, required: ["alias", "columns"] },
      schema_b: { type: "object", description: "Right source schema", properties: { alias: { type: "string" }, columns: { type: "array", items: { type: "object", properties: { name: { type: "string" }, data_type: { type: "string" } }, required: ["name", "data_type"] } } }, required: ["alias", "columns"] },
      sample_a: { type: "array", description: "Sample rows from source A", items: { type: "object" } },
      sample_b: { type: "array", description: "Sample rows from source B", items: { type: "object" } },
      mappings: { type: "array", description: "Known field mappings", items: { type: "object", properties: { field_a: { type: "string" }, field_b: { type: "string" }, confidence: { type: "number" }, reason: { type: "string" } }, required: ["field_a", "field_b", "confidence", "reason"] } },
    },
    required: ["schema_a", "schema_b", "sample_a", "sample_b", "mappings"],
  },
  handler: async (input: InferRulesInput): Promise<InferRulesOutput> => {
    const parsed = InferRulesInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(INFER_RULES_SYSTEM, userMessage, InferRulesOutputSchema);
  },
};
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-mcp && npx vitest run src/tools/__tests__/infer-rules.test.ts`
Expected: 3 tests PASS

**Step 5: Commit**

```bash
git add kalla-mcp/src/tools/infer-rules.ts kalla-mcp/src/tools/__tests__/infer-rules.test.ts
git commit -m "feat(kalla-mcp): add infer_rules tool with tests"
```

---

### Task 4: Implement build_recipe and nl_to_sql tools with tests

**Files:**
- Create: `kalla-mcp/src/tools/build-recipe.ts`
- Create: `kalla-mcp/src/tools/nl-to-sql.ts`
- Create: `kalla-mcp/src/tools/__tests__/build-recipe.test.ts`
- Create: `kalla-mcp/src/tools/__tests__/nl-to-sql.test.ts`

**Step 1: Write build_recipe test**

```typescript
// kalla-mcp/src/tools/__tests__/build-recipe.test.ts
import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { buildRecipe } from "../build-recipe.js";
import { callClaude } from "../../llm/client.js";
import type { BuildRecipeInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("build_recipe", () => {
  beforeEach(() => vi.clearAllMocks());

  const baseInput: BuildRecipeInput = {
    rules: [
      { name: "Amount Sum Match", sql: "ABS(l.amount - SUM(r.total_amount)) <= 0.01", description: "Sum match" },
    ],
    sources: { alias_a: "bank_statement", alias_b: "invoice_system" },
    primary_keys: { source_a: ["transaction_id"], source_b: ["invoice_id"] },
    pattern_type: "1:N",
  };

  it("should call Claude and return match SQL", async () => {
    mockCallClaude.mockResolvedValueOnce({
      match_sql: "SELECT l.transaction_id, r.invoice_id FROM bank_statement l JOIN invoice_system r ON ...",
      explanation: "Joins bank transactions to invoices with amount sum matching",
    });

    const result = await buildRecipe.handler(baseInput);

    expect(result.match_sql).toContain("SELECT");
    expect(result.explanation).toBeTruthy();
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should have correct tool metadata", () => {
    expect(buildRecipe.name).toBe("build_recipe");
  });
});
```

**Step 2: Write nl_to_sql test**

```typescript
// kalla-mcp/src/tools/__tests__/nl-to-sql.test.ts
import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { nlToSql } from "../nl-to-sql.js";
import { callClaude } from "../../llm/client.js";
import type { NlToSqlInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("nl_to_sql", () => {
  beforeEach(() => vi.clearAllMocks());

  const baseInput: NlToSqlInput = {
    text: "Invoice date must be within 7 days of bank transaction date",
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
    mappings: [
      { field_a: "transaction_date", field_b: "invoice_date", confidence: 0.92, reason: "Both date columns" },
    ],
  };

  it("should convert NL to DataFusion SQL expression", async () => {
    mockCallClaude.mockResolvedValueOnce({
      name: "Date Range Match",
      sql: "r.invoice_date BETWEEN l.transaction_date - INTERVAL '7 days' AND l.transaction_date + INTERVAL '7 days'",
      description: "Invoice date within 7 days of transaction",
      confidence: 0.88,
    });

    const result = await nlToSql.handler(baseInput);

    expect(result.sql).toContain("INTERVAL");
    expect(result.name).toBe("Date Range Match");
    expect(result.confidence).toBeGreaterThan(0);
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should include mappings in prompt context", async () => {
    mockCallClaude.mockResolvedValueOnce({
      name: "Test", sql: "l.a = r.b", description: "test", confidence: 0.5,
    });

    await nlToSql.handler(baseInput);

    const userMessage = mockCallClaude.mock.calls[0][1];
    expect(userMessage).toContain("transaction_date → invoice_date");
  });

  it("should have correct tool metadata", () => {
    expect(nlToSql.name).toBe("nl_to_sql");
  });
});
```

**Step 3: Implement build_recipe**

```typescript
// kalla-mcp/src/tools/build-recipe.ts
import { callClaude } from "../llm/client.js";
import { BUILD_RECIPE_SYSTEM } from "../llm/prompts.js";
import {
  BuildRecipeInputSchema,
  BuildRecipeOutputSchema,
  type BuildRecipeInput,
  type BuildRecipeOutput,
} from "../types/tool-io.js";

function buildUserMessage(input: BuildRecipeInput): string {
  let msg = `Pattern type: ${input.pattern_type}\n`;
  msg += `Source A: "${input.sources.alias_a}" — Primary keys: [${input.primary_keys.source_a.join(", ")}]\n`;
  msg += `Source B: "${input.sources.alias_b}" — Primary keys: [${input.primary_keys.source_b.join(", ")}]\n\n`;
  msg += "Accepted matching rules:\n";
  input.rules.forEach((r, i) => {
    msg += `\n${i + 1}. ${r.name}\n   SQL: ${r.sql}\n   Description: ${r.description}\n`;
  });
  msg += "\nAssemble these rules into a complete DataFusion SQL query.";
  return msg;
}

export const buildRecipe = {
  name: "build_recipe" as const,
  description:
    "Assemble accepted matching rules into a complete DataFusion SQL reconciliation query.",
  inputSchema: {
    type: "object" as const,
    properties: {
      rules: { type: "array", description: "Accepted matching rules", items: { type: "object", properties: { name: { type: "string" }, sql: { type: "string" }, description: { type: "string" } }, required: ["name", "sql", "description"] } },
      sources: { type: "object", properties: { alias_a: { type: "string" }, alias_b: { type: "string" } }, required: ["alias_a", "alias_b"] },
      primary_keys: { type: "object", properties: { source_a: { type: "array", items: { type: "string" } }, source_b: { type: "array", items: { type: "string" } } }, required: ["source_a", "source_b"] },
      pattern_type: { type: "string", enum: ["1:1", "1:N", "N:M"] },
    },
    required: ["rules", "sources", "primary_keys", "pattern_type"],
  },
  handler: async (input: BuildRecipeInput): Promise<BuildRecipeOutput> => {
    const parsed = BuildRecipeInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(BUILD_RECIPE_SYSTEM, userMessage, BuildRecipeOutputSchema);
  },
};
```

**Step 4: Implement nl_to_sql**

```typescript
// kalla-mcp/src/tools/nl-to-sql.ts
import { callClaude } from "../llm/client.js";
import { NL_TO_SQL_SYSTEM } from "../llm/prompts.js";
import {
  NlToSqlInputSchema,
  NlToSqlOutputSchema,
  type NlToSqlInput,
  type NlToSqlOutput,
} from "../types/tool-io.js";

function buildUserMessage(input: NlToSqlInput): string {
  let msg = `User rule description: "${input.text}"\n\n`;

  msg += `Source A: "${input.schema_a.alias}"\nColumns:\n`;
  msg += input.schema_a.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  msg += `\n\nSource B: "${input.schema_b.alias}"\nColumns:\n`;
  msg += input.schema_b.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  if (input.mappings.length > 0) {
    msg += "\n\nField mappings (A → B):\n";
    msg += input.mappings
      .map((m) => `  - ${m.field_a} → ${m.field_b} (confidence: ${m.confidence})`)
      .join("\n");
  }

  msg += "\n\nConvert this into a DataFusion SQL expression.";
  return msg;
}

export const nlToSql = {
  name: "nl_to_sql" as const,
  description:
    "Convert a natural language matching rule description into a DataFusion SQL expression, using schema context and field mappings.",
  inputSchema: {
    type: "object" as const,
    properties: {
      text: { type: "string", description: "Natural language rule description" },
      schema_a: { type: "object", description: "Left source schema", properties: { alias: { type: "string" }, columns: { type: "array", items: { type: "object", properties: { name: { type: "string" }, data_type: { type: "string" } }, required: ["name", "data_type"] } } }, required: ["alias", "columns"] },
      schema_b: { type: "object", description: "Right source schema", properties: { alias: { type: "string" }, columns: { type: "array", items: { type: "object", properties: { name: { type: "string" }, data_type: { type: "string" } }, required: ["name", "data_type"] } } }, required: ["alias", "columns"] },
      mappings: { type: "array", description: "Field mappings", items: { type: "object", properties: { field_a: { type: "string" }, field_b: { type: "string" }, confidence: { type: "number" }, reason: { type: "string" } }, required: ["field_a", "field_b", "confidence", "reason"] } },
    },
    required: ["text", "schema_a", "schema_b", "mappings"],
  },
  handler: async (input: NlToSqlInput): Promise<NlToSqlOutput> => {
    const parsed = NlToSqlInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(NL_TO_SQL_SYSTEM, userMessage, NlToSqlOutputSchema);
  },
};
```

**Step 5: Run all tests**

Run: `cd kalla-mcp && npx vitest run`
Expected: All tests pass (existing + 7 new)

**Step 6: Commit**

```bash
git add kalla-mcp/src/tools/build-recipe.ts kalla-mcp/src/tools/nl-to-sql.ts kalla-mcp/src/tools/__tests__/build-recipe.test.ts kalla-mcp/src/tools/__tests__/nl-to-sql.test.ts
git commit -m "feat(kalla-mcp): add build_recipe and nl_to_sql tools with tests"
```

---

### Task 5: Register new tools in MCP server

**Files:**
- Modify: `kalla-mcp/src/server.ts`

**Step 1: Add imports and registrations**

Add to imports:

```typescript
import { inferRules } from "./tools/infer-rules.js";
import { buildRecipe } from "./tools/build-recipe.js";
import { nlToSql } from "./tools/nl-to-sql.js";
import {
  DetectFieldMappingsInputSchema,
  ParseNlFilterInputSchema,
  InferRulesInputSchema,
  BuildRecipeInputSchema,
  NlToSqlInputSchema,
} from "./types/tool-io.js";
```

Add three `server.tool()` blocks after the existing two, following the same pattern:

```typescript
  server.tool(
    inferRules.name,
    inferRules.description,
    InferRulesInputSchema.shape,
    async (args) => {
      try {
        const result = await inferRules.handler(args);
        return { content: [{ type: "text" as const, text: JSON.stringify(result) }] };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { content: [{ type: "text" as const, text: JSON.stringify({ error: message }) }], isError: true };
      }
    },
  );

  server.tool(
    buildRecipe.name,
    buildRecipe.description,
    BuildRecipeInputSchema.shape,
    async (args) => {
      try {
        const result = await buildRecipe.handler(args);
        return { content: [{ type: "text" as const, text: JSON.stringify(result) }] };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { content: [{ type: "text" as const, text: JSON.stringify({ error: message }) }], isError: true };
      }
    },
  );

  server.tool(
    nlToSql.name,
    nlToSql.description,
    NlToSqlInputSchema.shape,
    async (args) => {
      try {
        const result = await nlToSql.handler(args);
        return { content: [{ type: "text" as const, text: JSON.stringify(result) }] };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { content: [{ type: "text" as const, text: JSON.stringify({ error: message }) }], isError: true };
      }
    },
  );
```

**Step 2: Build and run all tests**

Run: `cd kalla-mcp && npm run build && npx vitest run`
Expected: Clean build, all tests pass

**Step 3: Commit**

```bash
git add kalla-mcp/src/server.ts
git commit -m "feat(kalla-mcp): register infer_rules, build_recipe, nl_to_sql in MCP server"
```

---

### Task 6: Add Screen 3 types and actions to wizard state

**Files:**
- Modify: `kalla-web/src/lib/wizard-types.ts`
- Modify: `kalla-web/src/components/wizard/wizard-context.tsx`

**Step 1: Add types to wizard-types.ts**

Append after `SampleData` interface:

```typescript
export type PatternType = "1:1" | "1:N" | "N:M";

export interface DetectedPattern {
  type: PatternType;
  description: string;
  confidence: number;
}

export interface PrimaryKeys {
  source_a: string[];
  source_b: string[];
}

export interface InferredRule {
  id: string;
  name: string;
  sql: string;
  description: string;
  confidence: number;
  evidence: Record<string, unknown>[];
}

export type RuleStatus = "pending" | "accepted" | "rejected";

export interface RuleWithStatus extends InferredRule {
  status: RuleStatus;
}
```

Add new fields to `WizardState` (after `sampleRight`):

```typescript
  detectedPattern: DetectedPattern | null;
  primaryKeys: PrimaryKeys | null;
  inferredRules: RuleWithStatus[];
  builtRecipeSql: string | null;
```

Add to `INITIAL_WIZARD_STATE`:

```typescript
  detectedPattern: null,
  primaryKeys: null,
  inferredRules: [],
  builtRecipeSql: null,
```

Add new action types to `WizardAction`:

```typescript
  | { type: "SET_INFERRED_RULES"; pattern: DetectedPattern; primaryKeys: PrimaryKeys; rules: RuleWithStatus[] }
  | { type: "ACCEPT_RULE"; id: string }
  | { type: "REJECT_RULE"; id: string }
  | { type: "ADD_CUSTOM_RULE"; rule: RuleWithStatus }
  | { type: "SET_RECIPE_SQL"; sql: string }
```

**Step 2: Add reducer cases to wizard-context.tsx**

Add after the `SET_ERROR` case:

```typescript
    case "SET_INFERRED_RULES":
      return {
        ...state,
        detectedPattern: action.pattern,
        primaryKeys: action.primaryKeys,
        inferredRules: action.rules,
      };
    case "ACCEPT_RULE":
      return {
        ...state,
        inferredRules: state.inferredRules.map((r) =>
          r.id === action.id ? { ...r, status: "accepted" as const } : r,
        ),
      };
    case "REJECT_RULE":
      return {
        ...state,
        inferredRules: state.inferredRules.map((r) =>
          r.id === action.id ? { ...r, status: "rejected" as const } : r,
        ),
      };
    case "ADD_CUSTOM_RULE":
      return {
        ...state,
        inferredRules: [...state.inferredRules, action.rule],
      };
    case "SET_RECIPE_SQL":
      return { ...state, builtRecipeSql: action.sql };
```

**Step 3: Verify**

Run: `cd kalla-web && npx tsc --noEmit && npx jest --passWithNoTests`
Expected: Clean compile, all tests pass

**Step 4: Commit**

```bash
git add kalla-web/src/lib/wizard-types.ts kalla-web/src/components/wizard/wizard-context.tsx
git commit -m "feat(kalla-web): add Screen 3 types and actions to wizard state"
```

---

### Task 7: Build Screen 3 UI — AIRules parent + PatternCard + PrimaryKeysCard

**Files:**
- Create: `kalla-web/src/components/wizard/steps/AIRules.tsx`
- Modify: `kalla-web/src/app/recipes/new/page.tsx`

**Step 1: Create AIRules.tsx**

This component calls `infer_rules` on mount, then renders PatternCard, PrimaryKeysCard, rule cards, and AddCustomRule. Match the Pencil design at node `UZqO7`.

```typescript
"use client";

import { useEffect } from "react";
import { useWizard } from "@/components/wizard/wizard-context";
import { callAI } from "@/lib/ai-client";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  ArrowLeft,
  ArrowRight,
  Loader2,
  Sparkles,
  Landmark,
  FileText,
  Key,
  CheckCircle2,
  X,
} from "lucide-react";
import type { RuleWithStatus } from "@/lib/wizard-types";

function rowsToRecords(
  columns: { name: string }[],
  rows: string[][],
): Record<string, unknown>[] {
  return rows.map((row) => {
    const obj: Record<string, unknown> = {};
    columns.forEach((col, i) => { obj[col.name] = row[i]; });
    return obj;
  });
}

/* ── PatternCard ──────────────────────────────── */
function PatternCard() {
  const { state } = useWizard();
  const p = state.detectedPattern;
  if (!p) return null;

  const aliasA = state.leftSource?.alias ?? "Source A";
  const aliasB = state.rightSource?.alias ?? "Source B";

  return (
    <div className="rounded-xl border-[1.5px] border-border p-6">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold">Detected Pattern</h3>
        <Badge variant="outline" className="text-xs">
          {Math.round(p.confidence * 100)}% confident
        </Badge>
      </div>
      <div className="mt-4 flex items-center justify-center gap-6 py-4">
        <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-3 py-1.5 text-sm font-medium">
          <Landmark className="h-3.5 w-3.5" />
          {aliasA}
        </span>
        <Badge className="bg-foreground text-background font-mono text-sm px-3 py-1">
          {p.type}
        </Badge>
        <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-3 py-1.5 text-sm font-medium">
          <FileText className="h-3.5 w-3.5" />
          {aliasB}
        </span>
      </div>
      <p className="text-[13px] leading-relaxed text-muted-foreground">{p.description}</p>
    </div>
  );
}

/* ── PrimaryKeysCard ──────────────────────────── */
function PrimaryKeysCard() {
  const { state } = useWizard();
  const pk = state.primaryKeys;
  if (!pk) return null;

  return (
    <div className="rounded-xl border-[1.5px] border-border p-6">
      <div className="flex items-center gap-2">
        <Key className="h-4 w-4 text-muted-foreground" />
        <h3 className="text-sm font-semibold">Primary Keys & Join Fields</h3>
      </div>
      <p className="mt-2 text-[13px] leading-relaxed text-muted-foreground">
        AI identified the following fields as primary keys for joining records across sources.
      </p>
      <div className="mt-3 flex items-center justify-center gap-6 rounded-lg bg-muted py-3">
        <span className="rounded bg-background px-3 py-1 text-sm font-mono">
          {pk.source_a.join(", ")}
        </span>
        <ArrowRight className="h-4 w-4 text-muted-foreground" />
        <span className="rounded bg-background px-3 py-1 text-sm font-mono">
          {pk.source_b.join(", ")}
        </span>
      </div>
    </div>
  );
}

/* ── RuleCard ─────────────────────────────────── */
function RuleCard({ rule }: { rule: RuleWithStatus }) {
  const { dispatch } = useWizard();

  if (rule.status === "rejected") return null;

  return (
    <div className="rounded-xl border-[1.5px] border-border p-5">
      <div className="flex items-center justify-between">
        <h4 className="text-sm font-semibold">{rule.name}</h4>
        <Badge variant="outline" className="text-xs">
          {Math.round(rule.confidence * 100)}% match
        </Badge>
      </div>
      <p className="mt-2 text-[13px] leading-relaxed text-muted-foreground">
        {rule.description}
      </p>
      <div className="mt-3 rounded-lg bg-muted px-3.5 py-2.5">
        <code className="text-xs font-mono text-foreground whitespace-pre-wrap break-all">
          {rule.sql}
        </code>
      </div>
      {rule.evidence.length > 0 && (
        <div className="mt-3">
          <p className="text-xs font-medium text-muted-foreground mb-1">Sample Evidence</p>
          <div className="overflow-x-auto rounded border text-xs">
            <table className="w-full">
              <thead>
                <tr className="border-b bg-muted/50">
                  {Object.keys(rule.evidence[0]).map((k) => (
                    <th key={k} className="px-2 py-1 text-left font-medium">{k}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rule.evidence.slice(0, 3).map((row, i) => (
                  <tr key={i} className="border-b last:border-0">
                    {Object.values(row).map((v, j) => (
                      <td key={j} className="px-2 py-1">{String(v)}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      <div className="mt-3 flex items-center justify-end gap-2">
        {rule.status === "pending" && (
          <>
            <Button
              size="sm"
              variant="outline"
              className="text-destructive border-destructive/30 hover:bg-destructive/10"
              onClick={() => dispatch({ type: "REJECT_RULE", id: rule.id })}
            >
              <X className="mr-1 h-3.5 w-3.5" />
              Reject
            </Button>
            <Button
              size="sm"
              className="bg-green-600 hover:bg-green-700 text-white"
              onClick={() => dispatch({ type: "ACCEPT_RULE", id: rule.id })}
            >
              <CheckCircle2 className="mr-1 h-3.5 w-3.5" />
              Accept
            </Button>
          </>
        )}
        {rule.status === "accepted" && (
          <Badge className="bg-green-100 text-green-700 border-green-200">Accepted</Badge>
        )}
      </div>
    </div>
  );
}

/* ── AddCustomRule ─────────────────────────────── */
function AddCustomRule() {
  const { state, dispatch } = useWizard();
  const [text, setText] = __import_useState("");
  const [submitting, setSubmitting] = __import_useState(false);

  async function handleSubmit() {
    if (!text.trim()) return;
    setSubmitting(true);
    try {
      const result = await callAI<{
        name: string; sql: string; description: string; confidence: number;
      }>("nl_to_sql", {
        text,
        schema_a: { alias: state.leftSource!.alias, columns: state.schemaLeft! },
        schema_b: { alias: state.rightSource!.alias, columns: state.schemaRight! },
        mappings: state.fieldMappings,
      });
      dispatch({
        type: "ADD_CUSTOM_RULE",
        rule: {
          id: `custom-${Date.now()}`,
          name: result.name,
          sql: result.sql,
          description: result.description,
          confidence: result.confidence,
          evidence: [],
          status: "accepted",
        },
      });
      setText("");
    } catch {
      dispatch({ type: "SET_ERROR", key: "nlToSql", error: "Failed to convert rule" });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-2">
      <h4 className="text-sm font-medium">Add Custom Rule</h4>
      <p className="text-xs text-muted-foreground">
        Describe a matching rule in plain language. AI will convert it to SQL.
      </p>
      <div className="flex items-center gap-2 rounded-lg border-[1.5px] border-input px-3.5 py-2.5">
        <Sparkles className="h-4 w-4 text-muted-foreground shrink-0" />
        <input
          type="text"
          className="flex-1 bg-transparent text-sm placeholder:text-muted-foreground focus:outline-none"
          placeholder="e.g. Invoice date must be within 7 days of bank transaction date"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSubmit()}
          disabled={submitting}
        />
        {submitting && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
      </div>
    </div>
  );
}

/* ── AIRules (parent) ─────────────────────────── */
export function AIRules() {
  const { state, dispatch } = useWizard();
  const isLoading = state.loading.inferRules;

  useEffect(() => {
    if (state.detectedPattern || !state.sampleLeft || !state.sampleRight) return;

    dispatch({ type: "SET_LOADING", key: "inferRules", value: true });

    const samplesA = rowsToRecords(state.schemaLeft!, state.sampleLeft.rows);
    const samplesB = rowsToRecords(state.schemaRight!, state.sampleRight.rows);

    callAI<{
      pattern: { type: string; description: string; confidence: number };
      primary_keys: { source_a: string[]; source_b: string[] };
      rules: { name: string; sql: string; description: string; confidence: number; evidence: Record<string, unknown>[] }[];
    }>("infer_rules", {
      schema_a: { alias: state.leftSource!.alias, columns: state.schemaLeft! },
      schema_b: { alias: state.rightSource!.alias, columns: state.schemaRight! },
      sample_a: samplesA,
      sample_b: samplesB,
      mappings: state.fieldMappings,
    })
      .then((result) => {
        dispatch({
          type: "SET_INFERRED_RULES",
          pattern: result.pattern as any,
          primaryKeys: result.primary_keys,
          rules: result.rules.map((r, i) => ({
            ...r,
            id: `rule-${i}`,
            evidence: r.evidence ?? [],
            status: "pending" as const,
          })),
        });
      })
      .catch((err) => {
        dispatch({
          type: "SET_ERROR",
          key: "inferRules",
          error: err instanceof Error ? err.message : "Failed to infer rules",
        });
      })
      .finally(() => {
        dispatch({ type: "SET_LOADING", key: "inferRules", value: false });
      });
  }, []);

  const acceptedRules = state.inferredRules.filter((r) => r.status === "accepted");
  const canContinue = acceptedRules.length > 0;

  async function handleContinue() {
    dispatch({ type: "SET_LOADING", key: "buildRecipe", value: true });
    try {
      const result = await callAI<{ match_sql: string; explanation: string }>(
        "build_recipe",
        {
          rules: acceptedRules.map((r) => ({
            name: r.name,
            sql: r.sql,
            description: r.description,
          })),
          sources: {
            alias_a: state.leftSource!.alias,
            alias_b: state.rightSource!.alias,
          },
          primary_keys: state.primaryKeys!,
          pattern_type: state.detectedPattern!.type,
        },
      );
      dispatch({ type: "SET_RECIPE_SQL", sql: result.match_sql });
      dispatch({ type: "SET_STEP", step: 4 });
    } catch (err) {
      dispatch({
        type: "SET_ERROR",
        key: "buildRecipe",
        error: err instanceof Error ? err.message : "Failed to build recipe",
      });
    } finally {
      dispatch({ type: "SET_LOADING", key: "buildRecipe", value: false });
    }
  }

  return (
    <div className="flex flex-col gap-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex flex-col gap-1">
          <h1 className="text-[22px] font-semibold">AI Pattern Detection & Rules</h1>
          <p className="text-sm text-muted-foreground">
            AI analyzes your sample data to detect matching patterns.
          </p>
        </div>
        <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-1 text-xs font-medium">
          <Sparkles className="h-3 w-3" />
          AI-powered
        </span>
      </div>

      {isLoading ? (
        <div className="flex flex-col items-center justify-center gap-3 py-16">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          <p className="text-sm text-muted-foreground">Analyzing sample data...</p>
        </div>
      ) : state.errors.inferRules ? (
        <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4">
          <p className="text-sm text-destructive">{state.errors.inferRules}</p>
        </div>
      ) : (
        <>
          <PatternCard />
          <PrimaryKeysCard />

          {/* Rules section */}
          <h2 className="text-base font-semibold">AI-Suggested Rules</h2>
          <div className="flex flex-col gap-4">
            {state.inferredRules.map((rule) => (
              <RuleCard key={rule.id} rule={rule} />
            ))}
          </div>

          <AddCustomRule />
        </>
      )}

      {/* Footer */}
      <div className="flex justify-between border-t pt-6">
        <Button
          variant="outline"
          onClick={() => dispatch({ type: "SET_STEP", step: 2 })}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <Button
          disabled={!canContinue || state.loading.buildRecipe}
          onClick={handleContinue}
        >
          {state.loading.buildRecipe && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          Continue
          <ArrowRight className="ml-2 h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
```

**Important:** Replace `__import_useState` with the actual React `useState` import — add `useState` to the existing React import at the top. This is a plan placeholder.

**Step 2: Add Screen 3 route to page.tsx**

In `kalla-web/src/app/recipes/new/page.tsx`, add import and render case:

```typescript
import { AIRules } from "@/components/wizard/steps/AIRules";
```

Add after the step 2 block:

```typescript
      {state.step === 3 && (
        <div>
          <div className="mt-6">
            <AIRules />
          </div>
        </div>
      )}
```

**Step 3: Verify**

Run: `cd kalla-web && npx tsc --noEmit`
Expected: Clean compile

**Step 4: Commit**

```bash
git add kalla-web/src/components/wizard/steps/AIRules.tsx kalla-web/src/app/recipes/new/page.tsx
git commit -m "feat(kalla-web): add Screen 3 AI Rules UI with pattern, keys, rules, and NL input"
```

---

### Task 8: End-to-end verification

**Step 1: Run all kalla-mcp tests**

Run: `cd kalla-mcp && npx vitest run`
Expected: All tests pass (existing 10 + 7 new = 17)

**Step 2: Run all kalla-web tests**

Run: `cd kalla-web && npx jest`
Expected: All tests pass

**Step 3: Type check**

Run: `cd kalla-web && npx tsc --noEmit`
Expected: Clean

**Step 4: Build kalla-mcp**

Run: `cd kalla-mcp && npm run build`
Expected: Clean build

**Step 5: Build kalla-web**

Run: `cd kalla-web && npx next build`
Expected: Clean build
