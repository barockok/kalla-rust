# Screen 2 V2 — NL Filter + Mixed Load/Upload

**Date:** 2026-02-23
**Status:** Approved
**Design Reference:** `Kalla-ui-design.pen` → "Screen 2 – Collapsed Sources" + "Screen 2 – Expanded Sources (Edit)"

## Overview

Full rewrite of Screen 2 (Sample Data) with new component tree and state slice. Old `SampleData.tsx` + `FilterCard.tsx` remain untouched until swap-over. The new design introduces:

- Collapsible source configuration (expanded ↔ collapsed bar)
- Mixed data loading: DB query or CSV upload (via presigned S3/MinIO URL)
- NL-only Smart Filter generating read-only chips with scope badges
- Enhanced preview tables with field selector + value preview popovers

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Implementation strategy | Full rewrite (Option B) | Clean slate, no risk of breaking existing flow |
| CSV processing | Backend via presigned upload | Uniform source abstraction, no client-side parsing |
| Chip scope assignment | AI decides automatically | `parse_nl_filter` already has both schemas, can match columns |
| Data load trigger | Auto-load on configuration | Zero friction — DB auto-queries, CSV auto-registers on upload |
| Filter reactivity | Auto-refresh on chip change | Debounced 500ms, both source types use same `load-scoped` path |

## New State Schema

Added to `WizardState` in `wizard-types.ts`:

```typescript
// Per-source configuration
sourceConfigLeft: SourceConfig | null;
sourceConfigRight: SourceConfig | null;

// NL filter chips (new UI model, replaces commonFilters for v2)
filterChips: FilterChip[];

// Collapse/expand toggle
sourcesExpanded: boolean;
```

New types:

```typescript
type SourceConfig = {
  mode: "db" | "csv";
  loaded: boolean;
  originalAlias: string;   // alias from Screen 1
  activeAlias: string;     // same as original for DB; disposable alias for CSV
  csvFileName?: string;    // display name for uploaded file
  csvFileSize?: number;    // bytes
  csvRowCount?: number;    // rows detected
  csvColCount?: number;    // columns mapped
};

type FilterChip = {
  id: string;
  label: string;
  icon: string;              // lucide icon name
  scope: "both" | "left" | "right";
  type: string;              // "date_range" | "amount_range" | "text_match"
  field_a?: string;          // left source column
  field_b?: string;          // right source column
  value: [string, string] | string | null;
};
```

New action types:

```
SET_SOURCE_CONFIG       — { side: "left"|"right", config: SourceConfig }
SET_FILTER_CHIPS        — { chips: FilterChip[] }
REMOVE_FILTER_CHIP      — { chipId: string }
TOGGLE_SOURCES_EXPANDED — (no payload)
```

Old fields (`commonFilters`, `sourceFiltersLeft/Right`, `nlFilterText`) remain for backward compatibility until old Screen 2 is removed.

## Component Architecture

```
SampleDataV2.tsx                     (orchestrator)
├── CollapsedSourcesBar.tsx          (summary: name · mode · rows ✓ | Edit button)
├── ExpandedSourceCards.tsx           (single bordered box, center divider)
│   └── SourceCardContent.tsx         (per-source: header + tabs + body)
│       ├── LoadFromSourceBody.tsx    (description + Load Sample btn + connection info)
│       └── UploadCsvBody.tsx         (description + dropzone/replace + uploaded indicator)
├── SmartFilter.tsx                   (sparkle header + NL input bar + submit)
│   └── FilterChip.tsx                (scope badge [colored] + icon + label + X delete)
├── SamplePreviewV2.tsx               (header + side-by-side tables)
│   ├── FieldSelectorPopover.tsx      (search + column list + type badges + checkmarks)
│   └── ValuePreviewPopover.tsx       (field name + sample values + distinct count)
└── Footer (Back / Continue)
```

## Data Flow

### 1. Source Configuration (Expanded State)

```
User opens Screen 2 → sourcesExpanded = true

DB path:
  Click "Load Sample" → POST /api/sources/[alias]/load-scoped
  → SET_SAMPLE + SET_SOURCE_CONFIG { mode: "db", loaded: true, activeAlias: originalAlias }

CSV path:
  Drop file → POST /api/upload/presign { filename, contentType }
  ← { uploadUrl, downloadUrl }
  → PUT file to uploadUrl (direct S3/MinIO)
  → POST /api/sources/register-disposable { downloadUrl, originalAlias }
  ← { alias: "tmp_invoice_csv_abc123", rowCount, colCount }
  → POST /api/sources/[disposableAlias]/load-scoped
  → SET_SAMPLE + SET_SOURCE_CONFIG { mode: "csv", loaded: true, activeAlias: disposableAlias, csvFileName, ... }
```

### 2. Auto-Collapse

```
useEffect watches sourceConfigLeft?.loaded && sourceConfigRight?.loaded
  → both true (first time) → dispatch TOGGLE_SOURCES_EXPANDED (→ false)
  → CollapsedSourcesBar renders, ExpandedSourceCards hides
```

### 3. NL Filter → Chips

```
User types NL text → clicks submit (arrow button)
  → callAI("parse_nl_filter", { text, schemaLeft, schemaRight, ... })
  ← { filters: [{ label, icon, scope, type, field_a, field_b, value }], explanation }
  → dispatch SET_FILTER_CHIPS (replaces all chips)
  → Chips render as read-only pills with colored scope badges
```

### 4. Auto-Refresh Preview

```
useEffect watches filterChips (debounced 500ms)
  → For each source: POST /api/sources/[activeAlias]/load-scoped with chip filters
  → SET_SAMPLE for both sides
  → Preview tables re-render
```

### 5. Continue to Step 3

```
canContinue = sampleLeft !== null && sampleRight !== null
  → dispatch SET_STEP(3)
  → Existing AIRules screen receives samples as before
```

## CSV Upload — Presigned URL Flow

```
Frontend                          Backend                         S3/MinIO
   │                                │                                │
   │─POST /api/upload/presign──────→│                                │
   │  { filename, contentType }     │─generatePresignedUrl()────────→│
   │                                │←─{ uploadUrl, downloadUrl }────│
   │←─{ uploadUrl, downloadUrl }────│                                │
   │                                │                                │
   │─PUT file to uploadUrl─────────────────────────────────────────→│
   │←─200 OK───────────────────────────────────────────────────────│
   │                                │                                │
   │─POST /api/sources/register─────→│                                │
   │  { downloadUrl, schema... }    │  creates disposable source     │
   │←─{ alias, rowCount, colCount }─│                                │
```

## Backend Work Required

Two new endpoints (or MCP tools):

1. **Presigned URL generation**
   - `POST /api/upload/presign` → `{ uploadUrl, downloadUrl }`
   - Generates S3/MinIO presigned PUT URL with TTL

2. **Disposable source registration**
   - `POST /api/sources/register-disposable` → `{ alias, rowCount, colCount }`
   - Creates a temporary source entry pointing to the S3/MinIO download URL
   - Source has a TTL (auto-cleanup)
   - Must support `load-scoped` queries like any other source

## UI Specifications (from Pencil Design)

### Collapsed Sources Bar
- Single bordered box (rounded-[10px], border-[1.5px])
- Two pill chips: icon + "Source Name · Mode · N rows" + green checkmark
- "Edit" button (pencil-line icon) on far right
- `justify-between` layout

### Filter Chips
- Pill shape (rounded-full), muted background
- Left: lucide icon (calendar, dollar-sign, type)
- Scope badge: colored pill — blue (#3B82F6) for "Both", orange (#F97316) for "Left", violet (#8B5CF6) for "Right"
- Label text (12px, medium weight)
- X close icon on right
- Font size: 11px for badge text, 11px for chip text

### Source Cards (Expanded)
- Single outer box with center divider (left border on right card)
- Each side: header (icon + name, border-bottom) → tabs (active has muted bg + primary border-bottom 2px) → body
- DB body: description + outlined Load Sample button + green dot connection info
- CSV body: description + uploaded file indicator (green bg) + "Replace file" compact row
- Equal height via `items-stretch`

### Smart Filter Card
- Bordered card (rounded-xl, border-[1.5px])
- Sparkle icon + "Smart Filter" header
- Description text (muted, 13px)
- NL input row: input field with sparkle icon placeholder + submit arrow button (primary fill)
- Chips row below input (gap-2, wrapping)

### Sample Preview
- Header: rows-3 icon + "Sample Preview" + badge "Showing X + Y rows (CSV)"
- Side-by-side tables (layout: none for popover overlay)
- Field Selector Popover: 220px wide, search input, column list with type badges, check on selected
- Value Preview Popover: 200px wide, field name + type badge, sample values list, distinct count

## New Dependencies

None. CSV upload handled server-side. No `papaparse` needed.

## Testing Strategy

- Unit tests for new reducer cases (SET_SOURCE_CONFIG, SET_FILTER_CHIPS, REMOVE_FILTER_CHIP, TOGGLE_SOURCES_EXPANDED)
- Unit tests for CSV upload flow (presign → upload → register → load)
- Component tests for collapse/expand toggle behavior
- E2E: DB source → load → filter → preview → continue
- E2E: CSV upload → register → load → filter → preview → continue
