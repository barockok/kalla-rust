# Phase 4: Screens 4 & 5 — Run Parameters + Review & Save

**Date:** 2026-02-22
**Status:** Approved

## Goal

Build wizard Screens 4 (Run Parameters) and 5 (Review & Save), plus a new `preview_match` MCP tool for AI-simulated match preview.

## Decisions

| Decision | Choice |
|---|---|
| Match preview | AI simulation via MCP tool (no backend needed) |
| Save scope | "Save Recipe" calls POST /api/recipes. "Save & Run Now" disabled for now |
| Screen 4 purpose | Select which fields become runtime filters (not setting filter values) |
| Pre-selection | AI-suggested filters from step 2 pre-check relevant fields |

## MCP Tool: `preview_match`

AI simulates running match SQL against sample data.

```typescript
// Input
{
  match_sql: string,
  sample_a: Record<string, unknown>[],
  sample_b: Record<string, unknown>[],
  schema_a: SourceSchema,
  schema_b: SourceSchema,
  primary_keys: { source_a: string[], source_b: string[] },
  rules: { name: string, sql: string, description: string }[]
}

// Output
{
  matches: {
    left_row: Record<string, unknown>,
    right_rows: Record<string, unknown>[],
    status: "matched" | "unmatched" | "partial"
  }[],
  summary: {
    total_left: number,
    total_right: number,
    matched: number,
    unmatched: number
  }
}
```

Follows existing kalla-mcp pattern: Zod schemas, system prompt, callClaude(), registered in server.ts.

## Wizard State Additions

### New types

```typescript
interface MatchPreviewRow {
  left_row: Record<string, unknown>;
  right_rows: Record<string, unknown>[];
  status: "matched" | "unmatched" | "partial";
}

interface MatchPreviewSummary {
  total_left: number;
  total_right: number;
  matched: number;
  unmatched: number;
}

interface MatchPreviewResult {
  matches: MatchPreviewRow[];
  summary: MatchPreviewSummary;
}
```

### New state fields

- `runtimeFieldsLeft: string[]`
- `runtimeFieldsRight: string[]`
- `recipeName: string`
- `matchPreviewResult: MatchPreviewResult | null`

### New actions

- `TOGGLE_RUNTIME_FIELD` — toggle a field name in left or right runtime fields
- `SET_RECIPE_NAME` — update recipe name
- `SET_MATCH_PREVIEW` — store preview_match result

## Screen 4 UI — Run Parameters

### RunParameters.tsx (parent)

Header with title and description. Contains two RuntimeFieldCard components (one per source) and footer navigation.

### RuntimeFieldCard (per source)

- Header: source icon + name + badge ("N fields selected")
- Description text
- Field grid table:
  - Header row: blank, Field Name, Type, Sample Value, Filter
  - Data rows from schemaLeft/schemaRight with:
    - Checkbox (toggle field selection)
    - Field name (text)
    - Data type (muted text)
    - Sample value from first preview row
    - Filter icon (calendar for dates, dollar-sign for decimals, text icon for strings)
- Pre-checks fields that appear in suggestedFilters on mount

### Footer

Back button (→ step 3) + Continue button (→ step 5).

## Screen 5 UI — Review & Save

### ReviewSave.tsx (parent)

Contains RecipeSummary card, SampleMatchPreview, and footer.

### RecipeSummary

- Recipe Name: editable input field (default: "Bank-to-Invoice Monthly Recon")
- Summary grid: two columns showing source names, pattern type, run schedule
- Pattern visual: `[Source A]` → `[1:N]` → `[Source B]` with icons
- Matching Rules: bullet list of accepted rule descriptions from step 3

### SampleMatchPreview

- Calls `preview_match` MCP tool on mount
- Header: flask icon + "Sample Match Preview" + green badge "N/M matched"
- Description: "Results from running your recipe against the N sample transactions."
- Results table:
  - Columns: Bank Transaction, Amount, Matched Invoices, Status
  - Status badges: green "Matched", red "Unmatched"
  - Unmatched rows show "—" for matched invoices

### Footer

- Back button (→ step 4)
- "Save & Run Now" button (disabled, greyed out)
- "Save Recipe" button (primary) — calls POST /api/recipes

### Save Payload

```typescript
{
  recipe_id: crypto.randomUUID(),
  name: state.recipeName,
  match_sql: state.builtRecipeSql,
  match_description: state.detectedPattern?.description ?? "",
  sources: {
    left: {
      alias: state.leftSource.alias,
      type: state.leftSource.source_type,
      uri: state.leftSource.uri,
      primary_key: state.primaryKeys.source_a,
    },
    right: {
      alias: state.rightSource.alias,
      type: state.rightSource.source_type,
      uri: state.rightSource.uri,
      primary_key: state.primaryKeys.source_b,
    },
  }
}
```

On success → redirect to `/recipes` dashboard.

## Data Flow

```
Screen 3 completes (builtRecipeSql in state)
  │
  ▼ Screen 4 loads
  │  Renders field grids from schemaLeft/schemaRight
  │  Pre-checks suggestedFilter fields
  │  User toggles checkboxes → local state only
  │
  ▼ User clicks "Continue"
  │  Advances to step 5
  │
  ▼ Screen 5 loads
  │  Renders RecipeSummary from wizard state
  │  POST /api/ai { tool: "preview_match" }
  │  (match_sql + sample data + schemas + rules)
  │  → AI returns match results
  │  → SampleMatchPreview table rendered
  │
  ▼ User edits recipe name, reviews
  │
  ▼ User clicks "Save Recipe"
  │  POST /api/recipes { recipe payload }
  │  → Redirect to /recipes
```
