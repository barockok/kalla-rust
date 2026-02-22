# Phase 3: Screen 3 — AI Pattern Detection & Rules

**Date:** 2026-02-22
**Status:** Approved

## Goal

Add three MCP tools (`infer_rules`, `build_recipe`, `nl_to_sql`) to kalla-mcp and build the wizard Screen 3 UI that lets users review AI-detected matching patterns, accept/reject rules, and add custom rules via natural language.

## Decisions

| Decision | Choice |
|---|---|
| SQL dialect | DataFusion SQL — generated directly by AI, no translation layer |
| Rule editing | No inline edit. Accept keeps, Reject removes. Users add/redefine via "Add Custom Rule" NL input |
| Scope | Screen 3 only. Screen 2 cleanup (remove common filters) deferred |

## MCP Tools

### `infer_rules`

Called when Screen 3 loads. Analyzes sample data to detect matching patterns.

```typescript
// Input
{
  schema_a: SourceSchema,
  schema_b: SourceSchema,
  sample_a: Record<string, unknown>[],
  sample_b: Record<string, unknown>[],
  mappings: FieldMapping[]
}

// Output
{
  pattern: {
    type: "1:1" | "1:N" | "N:M",
    description: string,
    confidence: number
  },
  primary_keys: {
    source_a: string[],
    source_b: string[]
  },
  rules: [
    {
      name: string,
      sql: string,           // DataFusion SQL expression
      description: string,
      confidence: number,
      evidence: Record<string, unknown>[]
    }
  ]
}
```

### `build_recipe`

Called when user clicks "Continue" on Screen 3. Assembles accepted rules into a complete DataFusion SQL query.

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
  match_sql: string,
  explanation: string
}
```

### `nl_to_sql`

Called from "Add Custom Rule" NL input. Converts natural language to DataFusion SQL.

```typescript
// Input
{
  text: string,
  schema_a: SourceSchema,
  schema_b: SourceSchema,
  mappings: FieldMapping[]
}

// Output
{
  sql: string,
  explanation: string
}
```

All tools follow existing kalla-mcp patterns: one file per tool, Zod I/O schemas, shared `callClaude()`, system prompts in `prompts.ts`.

## Wizard State Additions

### New types

```typescript
interface DetectedPattern {
  type: "1:1" | "1:N" | "N:M";
  description: string;
  confidence: number;
}

interface PrimaryKeys {
  source_a: string[];
  source_b: string[];
}

interface InferredRule {
  id: string;
  name: string;
  sql: string;
  description: string;
  confidence: number;
  evidence: Record<string, unknown>[];
}

type RuleStatus = "pending" | "accepted" | "rejected";

interface RuleWithStatus extends InferredRule {
  status: RuleStatus;
}
```

### New state fields

- `detectedPattern: DetectedPattern | null`
- `primaryKeys: PrimaryKeys | null`
- `inferredRules: RuleWithStatus[]`
- `builtRecipeSql: string | null`

### New actions

- `SET_INFERRED_RULES` — stores pattern, primary keys, and rules from `infer_rules`
- `ACCEPT_RULE` / `REJECT_RULE` — toggle rule status by id
- `ADD_CUSTOM_RULE` — append new rule from `nl_to_sql` result
- `SET_RECIPE_SQL` — store output from `build_recipe`

## Screen 3 UI Components

### AIRules.tsx (parent)

Wrapper component for Screen 3. Calls `infer_rules` on mount. Contains all sub-components and footer navigation.

### PatternCard

Displays detected pattern type with visual badges:
`[Bank Statement]` → `[1:N]` → `[Invoice System]`
Plus explanation text underneath.

### PrimaryKeysCard

Shows AI-detected join keys as a visual: `[transaction_id]` ↔ `[invoice_number]`.
Evidence table showing sample matching rows. Description text.

### RuleCard (per rule)

Each card shows:
- Header: rule name + confidence badge
- Description text
- SQL code block (muted background, monospace)
- Evidence rows table
- Accept (green) / Reject (remove) buttons

### AddCustomRule

"Add Custom Rule" section:
- Label + description text
- NL text input with sparkles icon
- On submit: calls `nl_to_sql` → appends result as new accepted RuleWithStatus

### Footer

Back button (→ step 2) + "Continue →" button.
Continue calls `build_recipe` with accepted rules, stores SQL, advances to step 4.

## Data Flow

```
User completes Screen 2 (has sample data loaded)
  │
  ▼ Screen 3 loads
  │
  └─ POST /api/ai { tool: "infer_rules" }
     (schemas + sample data + field mappings)
  │
  ▼ Frontend renders
  │  Pattern Card (1:N detected)
  │  Primary Keys Card (transaction_id ↔ invoice_number)
  │  Rule Cards (Amount Sum Match, Date Range Match, etc.)
  │  Add Custom Rule input
  │
  ▼ User accepts/rejects rules → local state only
  │
  ▼ User types custom rule in NL input
  │  POST /api/ai { tool: "nl_to_sql" }
  │  → Returns SQL + explanation → new RuleCard appears
  │
  ▼ User clicks "Continue"
  │  POST /api/ai { tool: "build_recipe" }
  │  (accepted rules + sources + primary keys + pattern)
  │  → Returns complete match_sql
  │
  ▼ Advance to Screen 4
```
