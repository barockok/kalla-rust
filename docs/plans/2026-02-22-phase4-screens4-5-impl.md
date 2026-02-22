# Phase 4: Screens 4 & 5 + preview_match MCP Tool — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build wizard Screens 4 (Run Parameters) and 5 (Review & Save), plus the `preview_match` MCP tool for AI-simulated match preview.

**Architecture:** New MCP tool follows existing kalla-mcp pattern (Zod schemas → system prompt → callClaude → server registration). Frontend screens follow wizard pattern (step components in `steps/`, state via useReducer, `callAI` for backend communication). Screen 4 selects runtime filter fields. Screen 5 shows recipe summary + AI match preview + Save Recipe button.

**Tech Stack:** TypeScript, Zod, MCP SDK, React 18 (Next.js App Router), Tailwind CSS, shadcn/ui, Lucide icons

---

## Task 1: Add `preview_match` Zod I/O schemas

**Files:**
- Modify: `kalla-mcp/src/types/tool-io.ts` (append after nl_to_sql section)

**Step 1: Add schemas to tool-io.ts**

Append after the `NlToSqlOutput` type (line ~103):

```typescript
// ── preview_match ────────────────────────────────────
export const PreviewMatchInputSchema = z.object({
  match_sql: z.string(),
  sample_a: z.array(z.record(z.unknown())),
  sample_b: z.array(z.record(z.unknown())),
  schema_a: SourceSchemaSchema,
  schema_b: SourceSchemaSchema,
  primary_keys: PrimaryKeysSchema,
  rules: z.array(z.object({
    name: z.string(),
    sql: z.string(),
    description: z.string(),
  })),
});
export type PreviewMatchInput = z.infer<typeof PreviewMatchInputSchema>;

export const MatchPreviewRowSchema = z.object({
  left_row: z.record(z.unknown()),
  right_rows: z.array(z.record(z.unknown())),
  status: z.enum(["matched", "unmatched", "partial"]),
});

export const MatchPreviewSummarySchema = z.object({
  total_left: z.number(),
  total_right: z.number(),
  matched: z.number(),
  unmatched: z.number(),
});

export const PreviewMatchOutputSchema = z.object({
  matches: z.array(MatchPreviewRowSchema),
  summary: MatchPreviewSummarySchema,
});
export type PreviewMatchOutput = z.infer<typeof PreviewMatchOutputSchema>;
```

**Step 2: Verify build**

Run: `cd kalla-mcp && npm run build`
Expected: Clean compilation

**Step 3: Commit**

```bash
git add kalla-mcp/src/types/tool-io.ts
git commit -m "feat(kalla-mcp): add preview_match Zod I/O schemas"
```

---

## Task 2: Add `preview_match` system prompt

**Files:**
- Modify: `kalla-mcp/src/llm/prompts.ts` (append after NL_TO_SQL_SYSTEM)

**Step 1: Add prompt constant**

Append after `NL_TO_SQL_SYSTEM` (line ~124):

```typescript
export const PREVIEW_MATCH_SYSTEM = `You are a data reconciliation engine. You simulate running a match SQL query against sample data to preview matching results.

Context:
- You receive a match SQL query, sample data from both sources, schemas, primary keys, and matching rules
- You mentally execute the query logic against the sample rows
- For each left-source row, determine which right-source rows it matches based on the rules
- Apply ALL rules: amount tolerance, date range, string matching, etc.

Execution rules:
- Process every left-source row
- For each left row, check every right row against ALL matching rules
- A row is "matched" if at least one right row satisfies all applicable rules
- A row is "partial" if it matches some but not all rules with any right row
- A row is "unmatched" if no right row satisfies the rules
- Include the actual left_row and right_rows data (not just keys)
- right_rows should be an array (empty for unmatched, 1+ for matched/partial)

Return ONLY valid JSON:
{
  "matches": [
    {
      "left_row": { "col": "val", ... },
      "right_rows": [{ "col": "val", ... }],
      "status": "matched|unmatched|partial"
    }
  ],
  "summary": {
    "total_left": 10,
    "total_right": 15,
    "matched": 7,
    "unmatched": 3
  }
}`;
```

**Step 2: Verify build**

Run: `cd kalla-mcp && npm run build`
Expected: Clean compilation

**Step 3: Commit**

```bash
git add kalla-mcp/src/llm/prompts.ts
git commit -m "feat(kalla-mcp): add preview_match system prompt"
```

---

## Task 3: Implement `preview_match` tool + tests

**Files:**
- Create: `kalla-mcp/src/tools/preview-match.ts`
- Create: `kalla-mcp/src/tools/__tests__/preview-match.test.ts`

**Step 1: Write the test file**

Create `kalla-mcp/src/tools/__tests__/preview-match.test.ts`:

```typescript
import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { previewMatch } from "../preview-match.js";
import { callClaude } from "../../llm/client.js";
import type { PreviewMatchInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("preview_match", () => {
  beforeEach(() => vi.clearAllMocks());

  const baseInput: PreviewMatchInput = {
    match_sql: "SELECT l.*, r.* FROM bank l JOIN invoices r ON l.amount = r.total",
    sample_a: [
      { transaction_id: "TXN-001", amount: 1500.0, date: "2026-01-15" },
      { transaction_id: "TXN-002", amount: 200.0, date: "2026-01-16" },
    ],
    sample_b: [
      { invoice_id: "INV-1001", total: 1500.0, inv_date: "2026-01-14" },
      { invoice_id: "INV-1002", total: 750.0, inv_date: "2026-01-15" },
    ],
    schema_a: {
      alias: "bank_statement",
      columns: [
        { name: "transaction_id", data_type: "varchar" },
        { name: "amount", data_type: "decimal" },
        { name: "date", data_type: "date" },
      ],
    },
    schema_b: {
      alias: "invoice_system",
      columns: [
        { name: "invoice_id", data_type: "varchar" },
        { name: "total", data_type: "decimal" },
        { name: "inv_date", data_type: "date" },
      ],
    },
    primary_keys: { source_a: ["transaction_id"], source_b: ["invoice_id"] },
    rules: [
      { name: "Amount Match", sql: "ABS(l.amount - r.total) <= 0.01", description: "Exact amount match" },
    ],
  };

  it("should call Claude and return matches with summary", async () => {
    mockCallClaude.mockResolvedValueOnce({
      matches: [
        {
          left_row: { transaction_id: "TXN-001", amount: 1500.0 },
          right_rows: [{ invoice_id: "INV-1001", total: 1500.0 }],
          status: "matched",
        },
        {
          left_row: { transaction_id: "TXN-002", amount: 200.0 },
          right_rows: [],
          status: "unmatched",
        },
      ],
      summary: { total_left: 2, total_right: 2, matched: 1, unmatched: 1 },
    });

    const result = await previewMatch.handler(baseInput);

    expect(result.matches).toHaveLength(2);
    expect(result.matches[0].status).toBe("matched");
    expect(result.matches[1].status).toBe("unmatched");
    expect(result.summary.matched).toBe(1);
    expect(result.summary.unmatched).toBe(1);
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should include match SQL and rules in prompt", async () => {
    mockCallClaude.mockResolvedValueOnce({
      matches: [],
      summary: { total_left: 2, total_right: 2, matched: 0, unmatched: 2 },
    });

    await previewMatch.handler(baseInput);

    const userMessage = mockCallClaude.mock.calls[0][1];
    expect(userMessage).toContain("SELECT l.*, r.*");
    expect(userMessage).toContain("Amount Match");
    expect(userMessage).toContain("TXN-001");
  });

  it("should have correct tool metadata", () => {
    expect(previewMatch.name).toBe("preview_match");
    expect(previewMatch.description).toBeTruthy();
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-mcp && npx vitest run src/tools/__tests__/preview-match.test.ts`
Expected: FAIL — cannot find `../preview-match.js`

**Step 3: Implement the tool**

Create `kalla-mcp/src/tools/preview-match.ts`:

```typescript
import { callClaude } from "../llm/client.js";
import { PREVIEW_MATCH_SYSTEM } from "../llm/prompts.js";
import {
  PreviewMatchInputSchema,
  PreviewMatchOutputSchema,
  type PreviewMatchInput,
  type PreviewMatchOutput,
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

function buildUserMessage(input: PreviewMatchInput): string {
  let msg = `Match SQL:\n${input.match_sql}\n`;

  msg += `\nSource A: "${input.schema_a.alias}"\nColumns:\n`;
  msg += input.schema_a.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");
  msg += `\nPrimary keys: ${input.primary_keys.source_a.join(", ")}`;
  msg += `\n\nSample rows (Source A):\n${formatSampleRows(input.sample_a)}`;

  msg += `\n\nSource B: "${input.schema_b.alias}"\nColumns:\n`;
  msg += input.schema_b.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");
  msg += `\nPrimary keys: ${input.primary_keys.source_b.join(", ")}`;
  msg += `\n\nSample rows (Source B):\n${formatSampleRows(input.sample_b)}`;

  msg += "\n\nMatching rules:\n";
  msg += input.rules
    .map((r) => `  - ${r.name}: ${r.sql}\n    ${r.description}`)
    .join("\n");

  msg += "\n\nSimulate the match SQL against the sample data above. For each left-source row, determine if it matches any right-source rows based on the rules.";
  return msg;
}

export const previewMatch = {
  name: "preview_match" as const,
  description:
    "Simulate running a match SQL query against sample data to preview matching results. Returns matched/unmatched rows with summary statistics.",
  inputSchema: {
    type: "object" as const,
    properties: {
      match_sql: { type: "string", description: "The complete match SQL query" },
      sample_a: { type: "array", description: "Sample rows from source A", items: { type: "object" } },
      sample_b: { type: "array", description: "Sample rows from source B", items: { type: "object" } },
      schema_a: {
        type: "object", description: "Left source schema",
        properties: { alias: { type: "string" }, columns: { type: "array", items: { type: "object", properties: { name: { type: "string" }, data_type: { type: "string" } }, required: ["name", "data_type"] } } },
        required: ["alias", "columns"],
      },
      schema_b: {
        type: "object", description: "Right source schema",
        properties: { alias: { type: "string" }, columns: { type: "array", items: { type: "object", properties: { name: { type: "string" }, data_type: { type: "string" } }, required: ["name", "data_type"] } } },
        required: ["alias", "columns"],
      },
      primary_keys: {
        type: "object", description: "Primary key columns",
        properties: { source_a: { type: "array", items: { type: "string" } }, source_b: { type: "array", items: { type: "string" } } },
        required: ["source_a", "source_b"],
      },
      rules: {
        type: "array", description: "Matching rules", items: {
          type: "object", properties: { name: { type: "string" }, sql: { type: "string" }, description: { type: "string" } },
          required: ["name", "sql", "description"],
        },
      },
    },
    required: ["match_sql", "sample_a", "sample_b", "schema_a", "schema_b", "primary_keys", "rules"],
  },
  handler: async (input: PreviewMatchInput): Promise<PreviewMatchOutput> => {
    const parsed = PreviewMatchInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(PREVIEW_MATCH_SYSTEM, userMessage, PreviewMatchOutputSchema);
  },
};
```

**Step 4: Run tests**

Run: `cd kalla-mcp && npx vitest run src/tools/__tests__/preview-match.test.ts`
Expected: 3 tests PASS

**Step 5: Run all tests**

Run: `cd kalla-mcp && npx vitest run`
Expected: All 21 tests pass (18 existing + 3 new)

**Step 6: Commit**

```bash
git add kalla-mcp/src/tools/preview-match.ts kalla-mcp/src/tools/__tests__/preview-match.test.ts
git commit -m "feat(kalla-mcp): add preview_match tool with tests"
```

---

## Task 4: Register `preview_match` in MCP server

**Files:**
- Modify: `kalla-mcp/src/server.ts`

**Step 1: Add import and registration**

In `server.ts`, add import at top with other tool imports:

```typescript
import { previewMatch } from "./tools/preview-match.js";
```

Add import for the input schema:

```typescript
import {
  DetectFieldMappingsInputSchema,
  ParseNlFilterInputSchema,
  InferRulesInputSchema,
  BuildRecipeInputSchema,
  NlToSqlInputSchema,
  PreviewMatchInputSchema,
} from "./types/tool-io.js";
```

Add registration block after the last `server.tool(...)` block (before `return server;`):

```typescript
  server.tool(
    previewMatch.name,
    previewMatch.description,
    PreviewMatchInputSchema.shape,
    async (args) => {
      try {
        const result = await previewMatch.handler(args);
        return { content: [{ type: "text" as const, text: JSON.stringify(result) }] };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { content: [{ type: "text" as const, text: JSON.stringify({ error: message }) }], isError: true };
      }
    },
  );
```

**Step 2: Build and verify**

Run: `cd kalla-mcp && npm run build`
Expected: Clean compilation

**Step 3: Commit**

```bash
git add kalla-mcp/src/server.ts
git commit -m "feat(kalla-mcp): register preview_match in MCP server"
```

---

## Task 5: Add wizard state for Screens 4 & 5

**Files:**
- Modify: `kalla-web/src/lib/wizard-types.ts`
- Modify: `kalla-web/src/components/wizard/wizard-context.tsx`

**Step 1: Add types to wizard-types.ts**

After the `RuleWithStatus` interface (line ~74), add:

```typescript
export interface MatchPreviewRow {
  left_row: Record<string, unknown>;
  right_rows: Record<string, unknown>[];
  status: "matched" | "unmatched" | "partial";
}

export interface MatchPreviewSummary {
  total_left: number;
  total_right: number;
  matched: number;
  unmatched: number;
}

export interface MatchPreviewResult {
  matches: MatchPreviewRow[];
  summary: MatchPreviewSummary;
}
```

**Step 2: Add state fields to WizardState**

After `builtRecipeSql: string | null;` (line ~96), add:

```typescript
  runtimeFieldsLeft: string[];
  runtimeFieldsRight: string[];
  recipeName: string;
  matchPreviewResult: MatchPreviewResult | null;
```

**Step 3: Add defaults to INITIAL_WIZARD_STATE**

After `builtRecipeSql: null,` (line ~121), add:

```typescript
  runtimeFieldsLeft: [],
  runtimeFieldsRight: [],
  recipeName: "",
  matchPreviewResult: null,
```

**Step 4: Add new actions to WizardAction**

After `| { type: "SET_RECIPE_SQL"; sql: string }` (line ~144), add:

```typescript
  | { type: "TOGGLE_RUNTIME_FIELD"; side: "left" | "right"; field: string }
  | { type: "SET_RECIPE_NAME"; name: string }
  | { type: "SET_MATCH_PREVIEW"; result: MatchPreviewResult };
```

Update the import of `MatchPreviewResult` in the union — it's in the same file so no import needed.

**Step 5: Add reducer cases to wizard-context.tsx**

In `wizardReducer`, before the `default:` case, add:

```typescript
    case "TOGGLE_RUNTIME_FIELD": {
      const key = action.side === "left" ? "runtimeFieldsLeft" : "runtimeFieldsRight";
      const current = state[key];
      const next = current.includes(action.field)
        ? current.filter((f) => f !== action.field)
        : [...current, action.field];
      return { ...state, [key]: next };
    }
    case "SET_RECIPE_NAME":
      return { ...state, recipeName: action.name };
    case "SET_MATCH_PREVIEW":
      return { ...state, matchPreviewResult: action.result };
```

**Step 6: Verify build**

Run: `cd kalla-web && npm run build`
Expected: Clean compilation

**Step 7: Commit**

```bash
git add kalla-web/src/lib/wizard-types.ts kalla-web/src/components/wizard/wizard-context.tsx
git commit -m "feat(kalla-web): add wizard state for Screens 4 & 5"
```

---

## Task 6: Build Screen 4 UI — RunParameters

**Files:**
- Create: `kalla-web/src/components/wizard/steps/RunParameters.tsx`

**Step 1: Create RunParameters.tsx**

Create `kalla-web/src/components/wizard/steps/RunParameters.tsx`:

```typescript
"use client";

import { useEffect } from "react";
import { useWizard } from "@/components/wizard/wizard-context";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  ArrowLeft,
  ArrowRight,
  Calendar,
  DollarSign,
  Type,
  SlidersHorizontal,
} from "lucide-react";
import type { ColumnInfo } from "@/lib/chat-types";

function filterIcon(dataType: string) {
  const t = dataType.toLowerCase();
  if (t.includes("date") || t.includes("timestamp")) return <Calendar className="h-3.5 w-3.5 text-muted-foreground" />;
  if (t.includes("decimal") || t.includes("numeric") || t.includes("float") || t.includes("int") || t.includes("money"))
    return <DollarSign className="h-3.5 w-3.5 text-muted-foreground" />;
  return <Type className="h-3.5 w-3.5 text-muted-foreground" />;
}

function sampleValue(
  fieldName: string,
  schema: ColumnInfo[],
  previewRows: string[][] | null,
): string {
  if (!previewRows || previewRows.length === 0) return "—";
  const idx = schema.findIndex((c) => c.name === fieldName);
  if (idx === -1) return "—";
  return previewRows[0][idx] ?? "—";
}

/* ── RuntimeFieldCard ────────────────────── */
function RuntimeFieldCard({
  side,
  sourceName,
  schema,
  previewRows,
  selectedFields,
}: {
  side: "left" | "right";
  sourceName: string;
  schema: ColumnInfo[];
  previewRows: string[][] | null;
  selectedFields: string[];
}) {
  const { dispatch } = useWizard();

  return (
    <div className="rounded-xl border-[1.5px] border-border">
      <div className="flex items-center justify-between px-6 py-4 border-b">
        <div className="flex items-center gap-2">
          <SlidersHorizontal className="h-4 w-4 text-muted-foreground" />
          <h3 className="text-sm font-semibold">{sourceName}</h3>
        </div>
        <Badge variant="outline" className="text-xs">
          {selectedFields.length} field{selectedFields.length !== 1 ? "s" : ""} selected
        </Badge>
      </div>
      <p className="px-6 pt-3 text-[13px] text-muted-foreground">
        Select fields that users can filter on when running this recipe.
      </p>
      <div className="overflow-x-auto px-3 pb-4 pt-3">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left">
              <th className="px-3 py-2 w-10"></th>
              <th className="px-3 py-2 font-medium text-muted-foreground">Field Name</th>
              <th className="px-3 py-2 font-medium text-muted-foreground">Type</th>
              <th className="px-3 py-2 font-medium text-muted-foreground">Sample Value</th>
              <th className="px-3 py-2 font-medium text-muted-foreground w-10">Filter</th>
            </tr>
          </thead>
          <tbody>
            {schema.map((col) => (
              <tr key={col.name} className="border-b last:border-0 hover:bg-muted/30">
                <td className="px-3 py-2">
                  <input
                    type="checkbox"
                    checked={selectedFields.includes(col.name)}
                    onChange={() => dispatch({ type: "TOGGLE_RUNTIME_FIELD", side, field: col.name })}
                    className="h-4 w-4 rounded border-gray-300"
                  />
                </td>
                <td className="px-3 py-2 font-mono text-[13px]">{col.name}</td>
                <td className="px-3 py-2 text-muted-foreground text-[13px]">{col.data_type}</td>
                <td className="px-3 py-2 text-[13px] truncate max-w-[200px]">
                  {sampleValue(col.name, schema, previewRows)}
                </td>
                <td className="px-3 py-2 text-center">{filterIcon(col.data_type)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ── RunParameters (parent) ──────────────── */
export function RunParameters() {
  const { state, dispatch } = useWizard();

  // Pre-check fields from suggestedFilters on mount
  useEffect(() => {
    if (state.runtimeFieldsLeft.length > 0 || state.runtimeFieldsRight.length > 0) return;
    for (const sf of state.suggestedFilters) {
      if (sf.field_a) dispatch({ type: "TOGGLE_RUNTIME_FIELD", side: "left", field: sf.field_a });
      if (sf.field_b) dispatch({ type: "TOGGLE_RUNTIME_FIELD", side: "right", field: sf.field_b });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="flex flex-col gap-6">
      {/* Header */}
      <div className="flex flex-col gap-1">
        <h1 className="text-[22px] font-semibold">Run Parameters</h1>
        <p className="text-sm text-muted-foreground">
          Select which fields become runtime filters when executing this recipe.
        </p>
      </div>

      {/* Source A field grid */}
      {state.schemaLeft && (
        <RuntimeFieldCard
          side="left"
          sourceName={state.leftSource?.alias ?? "Source A"}
          schema={state.schemaLeft}
          previewRows={state.previewLeft}
          selectedFields={state.runtimeFieldsLeft}
        />
      )}

      {/* Source B field grid */}
      {state.schemaRight && (
        <RuntimeFieldCard
          side="right"
          sourceName={state.rightSource?.alias ?? "Source B"}
          schema={state.schemaRight}
          previewRows={state.previewRight}
          selectedFields={state.runtimeFieldsRight}
        />
      )}

      {/* Footer */}
      <div className="flex justify-between border-t pt-6">
        <Button
          variant="outline"
          onClick={() => dispatch({ type: "SET_STEP", step: 3 })}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <Button onClick={() => dispatch({ type: "SET_STEP", step: 5 })}>
          Continue
          <ArrowRight className="ml-2 h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
```

**Step 2: Verify build**

Run: `cd kalla-web && npm run build`
Expected: Clean compilation

**Step 3: Commit**

```bash
git add kalla-web/src/components/wizard/steps/RunParameters.tsx
git commit -m "feat(kalla-web): add Screen 4 RunParameters UI"
```

---

## Task 7: Build Screen 5 UI — ReviewSave

**Files:**
- Create: `kalla-web/src/components/wizard/steps/ReviewSave.tsx`

**Step 1: Create ReviewSave.tsx**

Create `kalla-web/src/components/wizard/steps/ReviewSave.tsx`:

```typescript
"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useWizard } from "@/components/wizard/wizard-context";
import { callAI } from "@/lib/ai-client";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  ArrowLeft,
  ArrowRight,
  Loader2,
  FlaskConical,
  Landmark,
  FileText,
  Save,
  Play,
} from "lucide-react";

function rowsToRecords(
  columns: { name: string }[],
  rows: string[][],
): Record<string, unknown>[] {
  return rows.map((row) => {
    const obj: Record<string, unknown> = {};
    columns.forEach((col, i) => {
      obj[col.name] = row[i];
    });
    return obj;
  });
}

/* ── RecipeSummary ───────────────────────── */
function RecipeSummary() {
  const { state, dispatch } = useWizard();

  const aliasA = state.leftSource?.alias ?? "Source A";
  const aliasB = state.rightSource?.alias ?? "Source B";
  const acceptedRules = state.inferredRules.filter((r) => r.status === "accepted");

  return (
    <div className="rounded-xl border-[1.5px] border-border p-6">
      <h3 className="text-sm font-semibold">Recipe Summary</h3>

      {/* Recipe Name */}
      <div className="mt-4">
        <label className="text-[13px] font-medium text-muted-foreground">Recipe Name</label>
        <input
          type="text"
          value={state.recipeName}
          onChange={(e) => dispatch({ type: "SET_RECIPE_NAME", name: e.target.value })}
          placeholder="e.g. Bank-to-Invoice Monthly Recon"
          className="mt-1 w-full rounded-lg border-[1.5px] border-input bg-transparent px-3.5 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
        />
      </div>

      {/* Pattern visual */}
      <div className="mt-4 flex items-center justify-center gap-6 rounded-lg bg-muted py-4">
        <span className="inline-flex items-center gap-1.5 rounded-full bg-background px-3 py-1.5 text-sm font-medium">
          <Landmark className="h-3.5 w-3.5" />
          {aliasA}
        </span>
        <Badge className="bg-foreground text-background font-mono text-sm px-3 py-1">
          {state.detectedPattern?.type ?? "—"}
        </Badge>
        <span className="inline-flex items-center gap-1.5 rounded-full bg-background px-3 py-1.5 text-sm font-medium">
          <FileText className="h-3.5 w-3.5" />
          {aliasB}
        </span>
      </div>

      {/* Matching rules */}
      {acceptedRules.length > 0 && (
        <div className="mt-4">
          <p className="text-[13px] font-medium text-muted-foreground">Matching Rules</p>
          <ul className="mt-2 space-y-1.5">
            {acceptedRules.map((rule) => (
              <li key={rule.id} className="text-[13px] flex items-start gap-2">
                <ArrowRight className="mt-0.5 h-3 w-3 text-muted-foreground shrink-0" />
                <span>{rule.description}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

/* ── SampleMatchPreview ──────────────────── */
function SampleMatchPreview() {
  const { state, dispatch } = useWizard();
  const isLoading = state.loading.previewMatch;
  const preview = state.matchPreviewResult;

  useEffect(() => {
    if (preview || !state.builtRecipeSql || !state.sampleLeft || !state.sampleRight) return;

    dispatch({ type: "SET_LOADING", key: "previewMatch", value: true });

    const samplesA = rowsToRecords(state.schemaLeft!, state.sampleLeft.rows);
    const samplesB = rowsToRecords(state.schemaRight!, state.sampleRight.rows);

    const acceptedRules = state.inferredRules
      .filter((r) => r.status === "accepted")
      .map((r) => ({ name: r.name, sql: r.sql, description: r.description }));

    callAI<{
      matches: {
        left_row: Record<string, unknown>;
        right_rows: Record<string, unknown>[];
        status: "matched" | "unmatched" | "partial";
      }[];
      summary: { total_left: number; total_right: number; matched: number; unmatched: number };
    }>("preview_match", {
      match_sql: state.builtRecipeSql,
      sample_a: samplesA,
      sample_b: samplesB,
      schema_a: {
        alias: state.leftSource!.alias,
        columns: state.schemaLeft!,
      },
      schema_b: {
        alias: state.rightSource!.alias,
        columns: state.schemaRight!,
      },
      primary_keys: state.primaryKeys!,
      rules: acceptedRules,
    })
      .then((result) => {
        dispatch({ type: "SET_MATCH_PREVIEW", result });
      })
      .catch((err) => {
        dispatch({
          type: "SET_ERROR",
          key: "previewMatch",
          error: err instanceof Error ? err.message : "Failed to preview matches",
        });
      })
      .finally(() => {
        dispatch({ type: "SET_LOADING", key: "previewMatch", value: false });
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-12 rounded-xl border-[1.5px] border-border">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <p className="text-sm text-muted-foreground">Running sample match preview...</p>
      </div>
    );
  }

  if (state.errors.previewMatch) {
    return (
      <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4">
        <p className="text-sm text-destructive">{state.errors.previewMatch}</p>
      </div>
    );
  }

  if (!preview) return null;

  // Derive display columns from left-source PK + a value column
  const leftPk = state.primaryKeys?.source_a[0] ?? "id";

  return (
    <div className="rounded-xl border-[1.5px] border-border">
      <div className="flex items-center justify-between px-6 py-4 border-b">
        <div className="flex items-center gap-2">
          <FlaskConical className="h-4 w-4 text-muted-foreground" />
          <h3 className="text-sm font-semibold">Sample Match Preview</h3>
        </div>
        <Badge className="bg-green-100 text-green-700 border-green-200 text-xs">
          {preview.summary.matched}/{preview.summary.total_left} matched
        </Badge>
      </div>
      <p className="px-6 pt-3 text-[13px] text-muted-foreground">
        Results from running your recipe against the {preview.summary.total_left} sample transactions.
      </p>
      <div className="overflow-x-auto px-3 pb-4 pt-3">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left">
              <th className="px-3 py-2 font-medium text-muted-foreground">Left Record</th>
              <th className="px-3 py-2 font-medium text-muted-foreground">Matched Right Records</th>
              <th className="px-3 py-2 font-medium text-muted-foreground w-24">Status</th>
            </tr>
          </thead>
          <tbody>
            {preview.matches.map((match, i) => (
              <tr key={i} className="border-b last:border-0">
                <td className="px-3 py-2 font-mono text-[13px]">
                  {String(match.left_row[leftPk] ?? JSON.stringify(match.left_row).slice(0, 40))}
                </td>
                <td className="px-3 py-2 text-[13px]">
                  {match.right_rows.length > 0
                    ? match.right_rows.map((r, j) => (
                        <span key={j} className="mr-2 inline-block rounded bg-muted px-1.5 py-0.5 text-xs font-mono">
                          {String(Object.values(r)[0] ?? "—")}
                        </span>
                      ))
                    : <span className="text-muted-foreground">—</span>}
                </td>
                <td className="px-3 py-2">
                  {match.status === "matched" && (
                    <Badge className="bg-green-100 text-green-700 border-green-200 text-xs">Matched</Badge>
                  )}
                  {match.status === "unmatched" && (
                    <Badge variant="destructive" className="text-xs">Unmatched</Badge>
                  )}
                  {match.status === "partial" && (
                    <Badge variant="outline" className="text-xs text-amber-600 border-amber-300">Partial</Badge>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ── ReviewSave (parent) ─────────────────── */
export function ReviewSave() {
  const { state, dispatch } = useWizard();
  const router = useRouter();
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  async function handleSave() {
    if (!state.recipeName.trim()) {
      setSaveError("Recipe name is required");
      return;
    }
    setSaving(true);
    setSaveError(null);

    try {
      const payload = {
        recipe_id: crypto.randomUUID(),
        name: state.recipeName,
        description: "",
        match_sql: state.builtRecipeSql,
        match_description: state.detectedPattern?.description ?? "",
        sources: {
          left: {
            alias: state.leftSource!.alias,
            type: state.leftSource!.source_type,
            uri: state.leftSource!.uri,
            primary_key: state.primaryKeys!.source_a,
          },
          right: {
            alias: state.rightSource!.alias,
            type: state.rightSource!.source_type,
            uri: state.rightSource!.uri,
            primary_key: state.primaryKeys!.source_b,
          },
        },
      };

      const res = await fetch("/api/recipes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || `Save failed: ${res.status}`);
      }

      router.push("/recipes");
    } catch (err) {
      setSaveError(err instanceof Error ? err.message : "Failed to save recipe");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="flex flex-col gap-6">
      {/* Header */}
      <div className="flex flex-col gap-1">
        <h1 className="text-[22px] font-semibold">Review & Save</h1>
        <p className="text-sm text-muted-foreground">
          Confirm your recipe configuration and save.
        </p>
      </div>

      <RecipeSummary />
      <SampleMatchPreview />

      {/* Save error */}
      {saveError && (
        <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4">
          <p className="text-sm text-destructive">{saveError}</p>
        </div>
      )}

      {/* Footer */}
      <div className="flex justify-between border-t pt-6">
        <Button
          variant="outline"
          onClick={() => dispatch({ type: "SET_STEP", step: 4 })}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <div className="flex items-center gap-3">
          <Button variant="outline" disabled className="opacity-50">
            <Play className="mr-2 h-4 w-4" />
            Save & Run Now
          </Button>
          <Button onClick={handleSave} disabled={saving}>
            {saving ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Save className="mr-2 h-4 w-4" />
            )}
            Save Recipe
          </Button>
        </div>
      </div>
    </div>
  );
}
```

**Step 2: Verify build**

Run: `cd kalla-web && npm run build`
Expected: Clean compilation

**Step 3: Commit**

```bash
git add kalla-web/src/components/wizard/steps/ReviewSave.tsx
git commit -m "feat(kalla-web): add Screen 5 ReviewSave UI"
```

---

## Task 8: Wire Screens 4 & 5 into wizard page routing

**Files:**
- Modify: `kalla-web/src/app/recipes/new/page.tsx`

**Step 1: Add imports and step rendering**

Add imports for the two new components:

```typescript
import { RunParameters } from "@/components/wizard/steps/RunParameters";
import { ReviewSave } from "@/components/wizard/steps/ReviewSave";
```

After the `{state.step === 3 && <AIRules />}` block, add:

```typescript
      {state.step === 4 && (
        <div>
          <div className="mt-6">
            <RunParameters />
          </div>
        </div>
      )}
      {state.step === 5 && (
        <div>
          <div className="mt-6">
            <ReviewSave />
          </div>
        </div>
      )}
```

**Step 2: Verify build**

Run: `cd kalla-web && npm run build`
Expected: Clean compilation

**Step 3: Commit**

```bash
git add kalla-web/src/app/recipes/new/page.tsx
git commit -m "feat(kalla-web): wire Screens 4 & 5 into wizard routing"
```

---

## Task 9: End-to-end verification

**Step 1: Run all MCP tests**

Run: `cd kalla-mcp && npx vitest run`
Expected: All 21 tests pass

**Step 2: Build kalla-mcp**

Run: `cd kalla-mcp && npm run build`
Expected: Clean compilation

**Step 3: Build kalla-web**

Run: `cd kalla-web && npm run build`
Expected: Clean compilation

**Step 4: Verify no lint issues**

Run: `cd kalla-web && npx next lint`
Expected: No errors
