# Screen 2 V2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the redesigned Screen 2 (Sample Data) with collapsible source configuration, mixed DB/CSV loading via presigned S3 uploads, NL-powered smart filter chips, and enhanced preview tables with popovers.

**Architecture:** Full rewrite alongside existing code (Option B). New components live in `kalla-web/src/components/wizard/steps/v2/`. New types and reducer cases are added to existing `wizard-types.ts` and `wizard-context.tsx`. Old Screen 2 components remain untouched until final swap-over in the last task.

**Tech Stack:** React 19, Next.js App Router, Tailwind CSS, shadcn/ui (card, badge, tabs, popover, button, input, table), lucide-react icons, existing `callAI()` + `uploadFile()` + `/api/sources/[alias]/load-scoped` + `/api/uploads/presign` infrastructure.

**Design Reference:** `Kalla-ui-design.pen` → "Screen 2 – Collapsed Sources" + "Screen 2 – Expanded Sources (Edit)"

**Approved Design Doc:** `docs/plans/2026-02-23-screen2-v2-design.md`

---

## Task 1: Add V2 Types to wizard-types.ts

**Files:**
- Modify: `kalla-web/src/lib/wizard-types.ts:40-174`
- Test: `kalla-web/src/__tests__/wizard-types-v2.test.ts`

**Step 1: Write the failing test**

Create `kalla-web/src/__tests__/wizard-types-v2.test.ts`:

```typescript
import type { SourceConfig, FilterChip, WizardState, WizardAction } from "@/lib/wizard-types";
import { INITIAL_WIZARD_STATE } from "@/lib/wizard-types";

describe("V2 wizard types", () => {
  test("SourceConfig type is usable", () => {
    const config: SourceConfig = {
      mode: "db",
      loaded: true,
      originalAlias: "invoices",
      activeAlias: "invoices",
    };
    expect(config.mode).toBe("db");
    expect(config.loaded).toBe(true);
  });

  test("SourceConfig csv mode has optional fields", () => {
    const config: SourceConfig = {
      mode: "csv",
      loaded: true,
      originalAlias: "payments",
      activeAlias: "tmp_payments_abc123",
      csvFileName: "payments.csv",
      csvFileSize: 1024,
      csvRowCount: 50,
      csvColCount: 8,
    };
    expect(config.csvFileName).toBe("payments.csv");
  });

  test("FilterChip type is usable", () => {
    const chip: FilterChip = {
      id: "chip-1",
      label: "Last 30 days",
      icon: "calendar",
      scope: "both",
      type: "date_range",
      field_a: "date",
      field_b: "txn_date",
      value: ["2026-01-01", "2026-01-31"],
    };
    expect(chip.scope).toBe("both");
  });

  test("INITIAL_WIZARD_STATE has v2 fields", () => {
    expect(INITIAL_WIZARD_STATE.sourceConfigLeft).toBeNull();
    expect(INITIAL_WIZARD_STATE.sourceConfigRight).toBeNull();
    expect(INITIAL_WIZARD_STATE.filterChips).toEqual([]);
    expect(INITIAL_WIZARD_STATE.sourcesExpanded).toBe(true);
  });

  test("WizardAction union includes v2 actions", () => {
    const a1: WizardAction = { type: "SET_SOURCE_CONFIG", side: "left", config: { mode: "db", loaded: false, originalAlias: "a", activeAlias: "a" } };
    const a2: WizardAction = { type: "SET_FILTER_CHIPS", chips: [] };
    const a3: WizardAction = { type: "REMOVE_FILTER_CHIP", chipId: "c1" };
    const a4: WizardAction = { type: "TOGGLE_SOURCES_EXPANDED" };
    expect(a1.type).toBe("SET_SOURCE_CONFIG");
    expect(a2.type).toBe("SET_FILTER_CHIPS");
    expect(a3.type).toBe("REMOVE_FILTER_CHIP");
    expect(a4.type).toBe("TOGGLE_SOURCES_EXPANDED");
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npx jest --testPathPattern=wizard-types-v2 --no-coverage 2>&1 | head -30`
Expected: FAIL — `SourceConfig` and `FilterChip` are not exported, `INITIAL_WIZARD_STATE` missing v2 fields.

**Step 3: Add SourceConfig and FilterChip types**

In `kalla-web/src/lib/wizard-types.ts`, after `CommonFilter` interface (line 40), add:

```typescript
export interface SourceConfig {
  mode: "db" | "csv";
  loaded: boolean;
  originalAlias: string;
  activeAlias: string;
  csvFileName?: string;
  csvFileSize?: number;
  csvRowCount?: number;
  csvColCount?: number;
}

export interface FilterChip {
  id: string;
  label: string;
  icon: string;
  scope: "both" | "left" | "right";
  type: string;
  field_a?: string;
  field_b?: string;
  value: [string, string] | string | null;
}
```

**Step 4: Add v2 fields to WizardState**

In `WizardState` interface (after line 121, before `loading`), add:

```typescript
  sourceConfigLeft: SourceConfig | null;
  sourceConfigRight: SourceConfig | null;
  filterChips: FilterChip[];
  sourcesExpanded: boolean;
```

In `INITIAL_WIZARD_STATE` (before `loading`), add:

```typescript
  sourceConfigLeft: null,
  sourceConfigRight: null,
  filterChips: [],
  sourcesExpanded: true,
```

**Step 5: Add v2 action types to WizardAction union**

At the end of the `WizardAction` type union (before the semicolon on line 173), add:

```typescript
  | { type: "SET_SOURCE_CONFIG"; side: "left" | "right"; config: SourceConfig }
  | { type: "SET_FILTER_CHIPS"; chips: FilterChip[] }
  | { type: "REMOVE_FILTER_CHIP"; chipId: string }
  | { type: "TOGGLE_SOURCES_EXPANDED" }
```

**Step 6: Run test to verify it passes**

Run: `cd kalla-web && npx jest --testPathPattern=wizard-types-v2 --no-coverage`
Expected: PASS

**Step 7: Commit**

```bash
git add kalla-web/src/lib/wizard-types.ts kalla-web/src/__tests__/wizard-types-v2.test.ts
git commit -m "feat(wizard): add Screen 2 v2 types — SourceConfig, FilterChip, new actions"
```

---

## Task 2: Add V2 Reducer Cases to wizard-context.tsx

**Files:**
- Modify: `kalla-web/src/components/wizard/wizard-context.tsx:10-107`
- Test: `kalla-web/src/__tests__/wizard-reducer-v2.test.ts`

**Step 1: Write the failing test**

Create `kalla-web/src/__tests__/wizard-reducer-v2.test.ts`:

```typescript
import { INITIAL_WIZARD_STATE, type WizardState, type WizardAction, type SourceConfig, type FilterChip } from "@/lib/wizard-types";

// We need to import the reducer. It's not exported directly, so we test via dispatch.
// Actually, the reducer IS used via the provider. Let's import and test it directly.
// We'll need to export it or test through the hook. Let's test through the hook.

// Better: extract reducer tests by importing wizardReducer.
// The reducer is not exported. We need to export it.
// For now, we'll test through the context hook using renderHook.

import { renderHook, act } from "@testing-library/react";
import { WizardProvider, useWizard } from "@/components/wizard/wizard-context";
import type { ReactNode } from "react";

function wrapper({ children }: { children: ReactNode }) {
  return <WizardProvider>{children}</WizardProvider>;
}

describe("V2 reducer cases", () => {
  test("SET_SOURCE_CONFIG sets left config", () => {
    const { result } = renderHook(() => useWizard(), { wrapper });
    const config: SourceConfig = {
      mode: "db",
      loaded: true,
      originalAlias: "invoices",
      activeAlias: "invoices",
    };
    act(() => result.current.dispatch({ type: "SET_SOURCE_CONFIG", side: "left", config }));
    expect(result.current.state.sourceConfigLeft).toEqual(config);
    expect(result.current.state.sourceConfigRight).toBeNull();
  });

  test("SET_SOURCE_CONFIG sets right config", () => {
    const { result } = renderHook(() => useWizard(), { wrapper });
    const config: SourceConfig = {
      mode: "csv",
      loaded: true,
      originalAlias: "payments",
      activeAlias: "tmp_pay_abc",
      csvFileName: "payments.csv",
      csvFileSize: 2048,
      csvRowCount: 100,
      csvColCount: 5,
    };
    act(() => result.current.dispatch({ type: "SET_SOURCE_CONFIG", side: "right", config }));
    expect(result.current.state.sourceConfigRight).toEqual(config);
  });

  test("SET_FILTER_CHIPS replaces all chips", () => {
    const { result } = renderHook(() => useWizard(), { wrapper });
    const chips: FilterChip[] = [
      { id: "c1", label: "Last 30 days", icon: "calendar", scope: "both", type: "date_range", value: ["2026-01-01", "2026-01-31"] },
      { id: "c2", label: "Amount > 100", icon: "dollar-sign", scope: "left", type: "amount_range", field_a: "amount", value: "100" },
    ];
    act(() => result.current.dispatch({ type: "SET_FILTER_CHIPS", chips }));
    expect(result.current.state.filterChips).toHaveLength(2);
    expect(result.current.state.filterChips[0].label).toBe("Last 30 days");
  });

  test("REMOVE_FILTER_CHIP removes by id", () => {
    const { result } = renderHook(() => useWizard(), { wrapper });
    const chips: FilterChip[] = [
      { id: "c1", label: "Chip 1", icon: "calendar", scope: "both", type: "date_range", value: null },
      { id: "c2", label: "Chip 2", icon: "type", scope: "right", type: "text_match", value: "test" },
    ];
    act(() => result.current.dispatch({ type: "SET_FILTER_CHIPS", chips }));
    act(() => result.current.dispatch({ type: "REMOVE_FILTER_CHIP", chipId: "c1" }));
    expect(result.current.state.filterChips).toHaveLength(1);
    expect(result.current.state.filterChips[0].id).toBe("c2");
  });

  test("TOGGLE_SOURCES_EXPANDED flips boolean", () => {
    const { result } = renderHook(() => useWizard(), { wrapper });
    expect(result.current.state.sourcesExpanded).toBe(true);
    act(() => result.current.dispatch({ type: "TOGGLE_SOURCES_EXPANDED" }));
    expect(result.current.state.sourcesExpanded).toBe(false);
    act(() => result.current.dispatch({ type: "TOGGLE_SOURCES_EXPANDED" }));
    expect(result.current.state.sourcesExpanded).toBe(true);
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npx jest --testPathPattern=wizard-reducer-v2 --no-coverage 2>&1 | head -30`
Expected: FAIL — reducer doesn't handle v2 action types.

**Step 3: Add reducer cases**

In `kalla-web/src/components/wizard/wizard-context.tsx`, in the `wizardReducer` switch statement, before the `default` case (line 104), add:

```typescript
    case "SET_SOURCE_CONFIG":
      return action.side === "left"
        ? { ...state, sourceConfigLeft: action.config }
        : { ...state, sourceConfigRight: action.config };
    case "SET_FILTER_CHIPS":
      return { ...state, filterChips: action.chips };
    case "REMOVE_FILTER_CHIP":
      return { ...state, filterChips: state.filterChips.filter((c) => c.id !== action.chipId) };
    case "TOGGLE_SOURCES_EXPANDED":
      return { ...state, sourcesExpanded: !state.sourcesExpanded };
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npx jest --testPathPattern=wizard-reducer-v2 --no-coverage`
Expected: PASS

**Step 5: Run all existing tests to check no regressions**

Run: `cd kalla-web && npx jest --no-coverage 2>&1 | tail -20`
Expected: All tests pass.

**Step 6: Commit**

```bash
git add kalla-web/src/components/wizard/wizard-context.tsx kalla-web/src/__tests__/wizard-reducer-v2.test.ts
git commit -m "feat(wizard): add Screen 2 v2 reducer cases"
```

---

## Task 3: Disposable Source Registration Endpoint

The CSV upload flow needs an endpoint to register an uploaded S3 file as a disposable data source that `load-scoped` can query. The existing upload infrastructure handles presigning and S3 upload; this endpoint bridges from S3 object → queryable source.

**Files:**
- Create: `kalla-web/src/app/api/sources/register-csv/route.ts`
- Test: `kalla-web/src/__tests__/register-csv-route.test.ts`

**Step 1: Write the failing test**

Create `kalla-web/src/__tests__/register-csv-route.test.ts`:

```typescript
/**
 * Integration-style test for POST /api/sources/register-csv.
 * We mock the S3 getObject and db pool so it runs without infra.
 */

// Mock s3-client before importing route
jest.mock("@/lib/s3-client", () => ({
  getObject: jest.fn(),
  UPLOADS_BUCKET: "test-bucket",
}));

jest.mock("@/lib/db", () => ({
  __esModule: true,
  default: {
    query: jest.fn(),
  },
}));

import { POST } from "@/app/api/sources/register-csv/route";
import { getObject } from "@/lib/s3-client";
import pool from "@/lib/db";

const mockGetObject = getObject as jest.Mock;
const mockQuery = pool.query as jest.Mock;

function makeRequest(body: Record<string, unknown>) {
  return new Request("http://localhost/api/sources/register-csv", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("POST /api/sources/register-csv", () => {
  beforeEach(() => jest.clearAllMocks());

  test("returns 400 if s3_uri missing", async () => {
    const res = await POST(makeRequest({}));
    expect(res.status).toBe(400);
  });

  test("returns 400 if original_alias missing", async () => {
    const res = await POST(makeRequest({ s3_uri: "s3://bucket/key" }));
    expect(res.status).toBe(400);
  });

  test("registers CSV source and returns alias + metadata", async () => {
    // Mock S3 returns CSV content
    const csvContent = "id,name,amount\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n";
    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue(encoder.encode(csvContent));
        controller.close();
      },
    });
    mockGetObject.mockResolvedValue(stream);

    // Mock DB insert succeeds
    mockQuery.mockResolvedValue({ rows: [{ alias: "csv_payments_abc123" }] });

    const res = await POST(
      makeRequest({
        s3_uri: "s3://test-bucket/session/upload/payments.csv",
        original_alias: "payments",
      }),
    );

    expect(res.status).toBe(200);
    const data = await res.json();
    expect(data.alias).toBeDefined();
    expect(data.row_count).toBe(3);
    expect(data.col_count).toBe(3);
    expect(data.columns).toEqual(["id", "name", "amount"]);
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npx jest --testPathPattern=register-csv-route --no-coverage 2>&1 | head -30`
Expected: FAIL — route file doesn't exist.

**Step 3: Implement the register-csv route**

Create `kalla-web/src/app/api/sources/register-csv/route.ts`:

```typescript
import { NextResponse } from "next/server";
import { getObject, UPLOADS_BUCKET } from "@/lib/s3-client";
import { parse } from "csv-parse/sync";
import pool from "@/lib/db";
import { v4 as uuidv4 } from "uuid";

export async function POST(request: Request) {
  let body: { s3_uri?: string; original_alias?: string };
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const { s3_uri, original_alias } = body;

  if (!s3_uri) {
    return NextResponse.json({ error: "Missing required field: s3_uri" }, { status: 400 });
  }
  if (!original_alias) {
    return NextResponse.json({ error: "Missing required field: original_alias" }, { status: 400 });
  }

  // Extract key from s3_uri
  const prefix = `s3://${UPLOADS_BUCKET}/`;
  if (!s3_uri.startsWith(prefix)) {
    return NextResponse.json({ error: `Invalid s3_uri: must start with ${prefix}` }, { status: 400 });
  }
  const key = s3_uri.slice(prefix.length);

  try {
    // Read CSV from S3
    const stream = await getObject(key);
    if (!stream) {
      return NextResponse.json({ error: "File not found in S3" }, { status: 404 });
    }

    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];
    for (;;) {
      const { value, done } = await reader.read();
      if (value) chunks.push(value);
      if (done) break;
    }
    const totalLength = chunks.reduce((acc, c) => acc + c.length, 0);
    const buffer = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      buffer.set(chunk, offset);
      offset += chunk.length;
    }
    const csvText = new TextDecoder().decode(buffer);

    // Parse CSV to get metadata
    const records: Record<string, string>[] = parse(csvText, {
      columns: true,
      skip_empty_lines: true,
    });

    const columns = records.length > 0 ? Object.keys(records[0]) : [];
    const row_count = records.length;
    const col_count = columns.length;

    // Generate disposable alias
    const shortId = uuidv4().slice(0, 8);
    const alias = `csv_${original_alias}_${shortId}`;

    // Register as a source in DB (disposable, with s3_uri as the URI)
    await pool.query(
      `INSERT INTO sources (alias, uri, source_type, status)
       VALUES ($1, $2, 'csv', 'active')
       ON CONFLICT (alias) DO UPDATE SET uri = $2, status = 'active'`,
      [alias, s3_uri],
    );

    return NextResponse.json({ alias, columns, row_count, col_count });
  } catch (err) {
    console.error("Register CSV error:", err);
    return NextResponse.json({ error: "Failed to register CSV source" }, { status: 500 });
  }
}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npx jest --testPathPattern=register-csv-route --no-coverage`
Expected: PASS

**Step 5: Commit**

```bash
git add kalla-web/src/app/api/sources/register-csv/route.ts kalla-web/src/__tests__/register-csv-route.test.ts
git commit -m "feat(api): add POST /api/sources/register-csv for disposable CSV sources"
```

---

## Task 4: CollapsedSourcesBar Component

**Files:**
- Create: `kalla-web/src/components/wizard/steps/v2/CollapsedSourcesBar.tsx`
- Test: `kalla-web/src/__tests__/CollapsedSourcesBar.test.tsx`

**Step 1: Write the failing test**

Create `kalla-web/src/__tests__/CollapsedSourcesBar.test.tsx`:

```tsx
import { render, screen, fireEvent } from "@testing-library/react";
import { CollapsedSourcesBar } from "@/components/wizard/steps/v2/CollapsedSourcesBar";
import type { SourceConfig } from "@/lib/wizard-types";

const dbConfig: SourceConfig = {
  mode: "db",
  loaded: true,
  originalAlias: "invoices",
  activeAlias: "invoices",
};

const csvConfig: SourceConfig = {
  mode: "csv",
  loaded: true,
  originalAlias: "payments",
  activeAlias: "csv_payments_abc",
  csvFileName: "payments.csv",
  csvRowCount: 150,
  csvColCount: 8,
};

describe("CollapsedSourcesBar", () => {
  test("renders both source pills with names and modes", () => {
    render(<CollapsedSourcesBar left={dbConfig} right={csvConfig} onEdit={() => {}} />);
    expect(screen.getByText(/invoices/i)).toBeInTheDocument();
    expect(screen.getByText(/DB/i)).toBeInTheDocument();
    expect(screen.getByText(/payments/i)).toBeInTheDocument();
    expect(screen.getByText(/CSV/i)).toBeInTheDocument();
  });

  test("shows CSV row count when available", () => {
    render(<CollapsedSourcesBar left={dbConfig} right={csvConfig} onEdit={() => {}} />);
    expect(screen.getByText(/150 rows/i)).toBeInTheDocument();
  });

  test("calls onEdit when Edit button is clicked", () => {
    const onEdit = jest.fn();
    render(<CollapsedSourcesBar left={dbConfig} right={csvConfig} onEdit={onEdit} />);
    fireEvent.click(screen.getByRole("button", { name: /edit/i }));
    expect(onEdit).toHaveBeenCalledTimes(1);
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npx jest --testPathPattern=CollapsedSourcesBar --no-coverage 2>&1 | head -20`
Expected: FAIL — module not found.

**Step 3: Implement CollapsedSourcesBar**

Create directory: `kalla-web/src/components/wizard/steps/v2/`

Create `kalla-web/src/components/wizard/steps/v2/CollapsedSourcesBar.tsx`:

```tsx
"use client";

import { Button } from "@/components/ui/button";
import { Landmark, FileText, CheckCircle2, PencilLine } from "lucide-react";
import type { SourceConfig } from "@/lib/wizard-types";

interface Props {
  left: SourceConfig;
  right: SourceConfig;
  onEdit: () => void;
}

function SourcePill({ config }: { config: SourceConfig }) {
  const Icon = config.mode === "db" ? Landmark : FileText;
  const modeLabel = config.mode === "db" ? "DB" : "CSV";
  const rowsLabel = config.csvRowCount ? `${config.csvRowCount} rows` : null;

  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border bg-background px-3 py-1.5 text-xs font-medium">
      <Icon className="h-3.5 w-3.5 text-muted-foreground" />
      <span>{config.originalAlias}</span>
      <span className="text-muted-foreground">·</span>
      <span className="text-muted-foreground">{modeLabel}</span>
      {rowsLabel && (
        <>
          <span className="text-muted-foreground">·</span>
          <span className="text-muted-foreground">{rowsLabel}</span>
        </>
      )}
      <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />
    </span>
  );
}

export function CollapsedSourcesBar({ left, right, onEdit }: Props) {
  return (
    <div className="flex items-center justify-between rounded-[10px] border-[1.5px] px-4 py-3">
      <div className="flex items-center gap-3">
        <SourcePill config={left} />
        <SourcePill config={right} />
      </div>
      <Button variant="ghost" size="sm" onClick={onEdit} aria-label="Edit sources">
        <PencilLine className="mr-1.5 h-3.5 w-3.5" />
        Edit
      </Button>
    </div>
  );
}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npx jest --testPathPattern=CollapsedSourcesBar --no-coverage`
Expected: PASS

**Step 5: Commit**

```bash
git add kalla-web/src/components/wizard/steps/v2/CollapsedSourcesBar.tsx kalla-web/src/__tests__/CollapsedSourcesBar.test.tsx
git commit -m "feat(wizard): add CollapsedSourcesBar component for Screen 2 v2"
```

---

## Task 5: ExpandedSourceCards Component (with Tabs)

**Files:**
- Create: `kalla-web/src/components/wizard/steps/v2/ExpandedSourceCards.tsx`
- Test: `kalla-web/src/__tests__/ExpandedSourceCards.test.tsx`

**Step 1: Write the failing test**

Create `kalla-web/src/__tests__/ExpandedSourceCards.test.tsx`:

```tsx
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { ExpandedSourceCards } from "@/components/wizard/steps/v2/ExpandedSourceCards";

// Mock fetch for load-scoped and upload flows
global.fetch = jest.fn();

const props = {
  leftAlias: "invoices",
  rightAlias: "payments",
  leftLoaded: false,
  rightLoaded: false,
  onSourceLoaded: jest.fn(),
};

describe("ExpandedSourceCards", () => {
  beforeEach(() => jest.clearAllMocks());

  test("renders two source cards with headers", () => {
    render(<ExpandedSourceCards {...props} />);
    expect(screen.getByText("invoices")).toBeInTheDocument();
    expect(screen.getByText("payments")).toBeInTheDocument();
  });

  test("each card has Load from Source and Upload CSV tabs", () => {
    render(<ExpandedSourceCards {...props} />);
    const loadTabs = screen.getAllByText("Load from Source");
    const csvTabs = screen.getAllByText("Upload CSV");
    expect(loadTabs).toHaveLength(2);
    expect(csvTabs).toHaveLength(2);
  });

  test("clicking Load Sample triggers fetch to load-scoped", async () => {
    (global.fetch as jest.Mock).mockResolvedValue({
      ok: true,
      json: async () => ({
        alias: "invoices",
        columns: [{ name: "id", data_type: "integer", nullable: false }],
        rows: [["1"]],
        total_rows: 1,
        preview_rows: 1,
      }),
    });

    render(<ExpandedSourceCards {...props} />);
    const loadButtons = screen.getAllByRole("button", { name: /load sample/i });
    fireEvent.click(loadButtons[0]);

    await waitFor(() => {
      expect(global.fetch).toHaveBeenCalledWith(
        "/api/sources/invoices/load-scoped",
        expect.objectContaining({ method: "POST" }),
      );
    });
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npx jest --testPathPattern=ExpandedSourceCards --no-coverage 2>&1 | head -20`
Expected: FAIL — module not found.

**Step 3: Implement ExpandedSourceCards**

Create `kalla-web/src/components/wizard/steps/v2/ExpandedSourceCards.tsx`:

```tsx
"use client";

import { useState, useCallback, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Landmark,
  FileText,
  Upload,
  Database,
  CheckCircle2,
  Loader2,
  FileUp,
  RefreshCw,
} from "lucide-react";
import type { SourceConfig, SampleData } from "@/lib/wizard-types";
import type { ColumnInfo } from "@/lib/chat-types";

interface Props {
  leftAlias: string;
  rightAlias: string;
  leftLoaded: boolean;
  rightLoaded: boolean;
  onSourceLoaded: (
    side: "left" | "right",
    config: SourceConfig,
    sample: SampleData,
  ) => void;
}

type TabMode = "db" | "csv";

interface SourceCardProps {
  alias: string;
  side: "left" | "right";
  loaded: boolean;
  onLoaded: Props["onSourceLoaded"];
}

function SourceCard({ alias, side, loaded, onLoaded }: SourceCardProps) {
  const [activeTab, setActiveTab] = useState<TabMode>("db");
  const [loading, setLoading] = useState(false);
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvUploaded, setCsvUploaded] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleLoadFromSource = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/sources/${alias}/load-scoped`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ conditions: [], limit: 200 }),
      });
      if (!res.ok) throw new Error("Load failed");
      const data = await res.json();
      const sample: SampleData = {
        columns: data.columns,
        rows: data.rows,
        totalRows: data.total_rows,
      };
      onLoaded(side, {
        mode: "db",
        loaded: true,
        originalAlias: alias,
        activeAlias: alias,
      }, sample);
    } finally {
      setLoading(false);
    }
  }, [alias, side, onLoaded]);

  const handleCsvUpload = useCallback(async (file: File) => {
    setCsvFile(file);
    setLoading(true);
    try {
      // 1. Presign
      const presignRes = await fetch("/api/uploads/presign", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ filename: file.name, session_id: "wizard" }),
      });
      if (!presignRes.ok) throw new Error("Presign failed");
      const { presigned_url, s3_uri } = await presignRes.json();

      // 2. Upload to S3
      const uploadRes = await fetch(presigned_url, {
        method: "PUT",
        headers: { "Content-Type": "text/csv" },
        body: file,
      });
      if (!uploadRes.ok) throw new Error("S3 upload failed");

      // 3. Register as disposable source
      const regRes = await fetch("/api/sources/register-csv", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ s3_uri, original_alias: alias }),
      });
      if (!regRes.ok) throw new Error("Register failed");
      const { alias: csvAlias, row_count, col_count, columns } = await regRes.json();

      // 4. Load scoped from the new disposable source
      const loadRes = await fetch(`/api/sources/${csvAlias}/load-scoped`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ conditions: [], limit: 200 }),
      });
      if (!loadRes.ok) throw new Error("Load CSV source failed");
      const loadData = await loadRes.json();

      const sample: SampleData = {
        columns: loadData.columns,
        rows: loadData.rows,
        totalRows: loadData.total_rows,
      };

      setCsvUploaded(true);
      onLoaded(side, {
        mode: "csv",
        loaded: true,
        originalAlias: alias,
        activeAlias: csvAlias,
        csvFileName: file.name,
        csvFileSize: file.size,
        csvRowCount: row_count,
        csvColCount: col_count,
      }, sample);
    } finally {
      setLoading(false);
    }
  }, [alias, side, onLoaded]);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      const file = e.dataTransfer.files[0];
      if (file?.name.endsWith(".csv")) handleCsvUpload(file);
    },
    [handleCsvUpload],
  );

  const handleFileSelect = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) handleCsvUpload(file);
    },
    [handleCsvUpload],
  );

  const Icon = side === "left" ? Landmark : FileText;

  return (
    <div className="flex flex-1 flex-col">
      {/* Header */}
      <div className="flex items-center gap-2 border-b px-4 py-3">
        <Icon className="h-4 w-4 text-muted-foreground" />
        <span className="text-sm font-semibold">{alias}</span>
      </div>

      {/* Tabs */}
      <div className="flex border-b">
        <button
          className={`flex-1 px-4 py-2 text-xs font-medium ${
            activeTab === "db"
              ? "border-b-2 border-primary bg-muted/50"
              : "text-muted-foreground"
          }`}
          onClick={() => setActiveTab("db")}
        >
          Load from Source
        </button>
        <button
          className={`flex-1 px-4 py-2 text-xs font-medium ${
            activeTab === "csv"
              ? "border-b-2 border-primary bg-muted/50"
              : "text-muted-foreground"
          }`}
          onClick={() => setActiveTab("csv")}
        >
          Upload CSV
        </button>
      </div>

      {/* Body */}
      <div className="flex flex-1 flex-col justify-center px-4 py-4">
        {activeTab === "db" ? (
          <div className="flex flex-col items-center gap-3 text-center">
            <p className="text-xs text-muted-foreground">
              Pull sample data directly from the connected source.
            </p>
            {loaded && (
              <span className="inline-flex items-center gap-1 text-xs text-green-600">
                <CheckCircle2 className="h-3.5 w-3.5" /> Loaded
              </span>
            )}
            <Button
              variant="outline"
              size="sm"
              onClick={handleLoadFromSource}
              disabled={loading}
              aria-label="Load Sample"
            >
              {loading ? (
                <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
              ) : (
                <Database className="mr-1.5 h-3.5 w-3.5" />
              )}
              Load Sample
            </Button>
            <span className="inline-flex items-center gap-1 text-[10px] text-muted-foreground">
              <span className="h-1.5 w-1.5 rounded-full bg-green-500" />
              Connected
            </span>
          </div>
        ) : (
          <div
            className="flex flex-col items-center gap-3 text-center"
            onDragOver={(e) => e.preventDefault()}
            onDrop={handleDrop}
          >
            <p className="text-xs text-muted-foreground">
              Upload a CSV file to use as sample data for this source.
            </p>
            {csvUploaded && csvFile ? (
              <div className="flex flex-col items-center gap-2">
                <span className="inline-flex items-center gap-1.5 rounded-md bg-green-50 px-3 py-1.5 text-xs font-medium text-green-700">
                  <CheckCircle2 className="h-3.5 w-3.5" />
                  {csvFile.name}
                </span>
                <button
                  className="inline-flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground"
                  onClick={() => fileInputRef.current?.click()}
                >
                  <RefreshCw className="h-3 w-3" />
                  Replace file
                </button>
              </div>
            ) : (
              <Button
                variant="outline"
                size="sm"
                onClick={() => fileInputRef.current?.click()}
                disabled={loading}
              >
                {loading ? (
                  <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
                ) : (
                  <FileUp className="mr-1.5 h-3.5 w-3.5" />
                )}
                Choose CSV
              </Button>
            )}
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              className="hidden"
              onChange={handleFileSelect}
            />
          </div>
        )}
      </div>
    </div>
  );
}

export function ExpandedSourceCards({
  leftAlias,
  rightAlias,
  leftLoaded,
  rightLoaded,
  onSourceLoaded,
}: Props) {
  return (
    <div className="flex overflow-hidden rounded-xl border-[1.5px]">
      <SourceCard
        alias={leftAlias}
        side="left"
        loaded={leftLoaded}
        onLoaded={onSourceLoaded}
      />
      <div className="border-l" />
      <SourceCard
        alias={rightAlias}
        side="right"
        loaded={rightLoaded}
        onLoaded={onSourceLoaded}
      />
    </div>
  );
}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npx jest --testPathPattern=ExpandedSourceCards --no-coverage`
Expected: PASS

**Step 5: Commit**

```bash
git add kalla-web/src/components/wizard/steps/v2/ExpandedSourceCards.tsx kalla-web/src/__tests__/ExpandedSourceCards.test.tsx
git commit -m "feat(wizard): add ExpandedSourceCards with DB load + CSV upload tabs"
```

---

## Task 6: SmartFilter + FilterChip Components

**Files:**
- Create: `kalla-web/src/components/wizard/steps/v2/SmartFilter.tsx`
- Create: `kalla-web/src/components/wizard/steps/v2/FilterChipPill.tsx`
- Test: `kalla-web/src/__tests__/SmartFilter.test.tsx`

**Step 1: Write the failing test**

Create `kalla-web/src/__tests__/SmartFilter.test.tsx`:

```tsx
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { SmartFilter } from "@/components/wizard/steps/v2/SmartFilter";
import type { FilterChip } from "@/lib/wizard-types";

const chips: FilterChip[] = [
  { id: "c1", label: "Last 30 days", icon: "calendar", scope: "both", type: "date_range", value: ["2026-01-01", "2026-01-31"] },
  { id: "c2", label: "Amount > 100", icon: "dollar-sign", scope: "left", type: "amount_range", field_a: "amount", value: "100" },
];

describe("SmartFilter", () => {
  test("renders header with sparkle icon", () => {
    render(<SmartFilter chips={[]} onSubmit={() => {}} onRemoveChip={() => {}} loading={false} />);
    expect(screen.getByText("Smart Filter")).toBeInTheDocument();
  });

  test("renders NL input and submit button", () => {
    render(<SmartFilter chips={[]} onSubmit={() => {}} onRemoveChip={() => {}} loading={false} />);
    expect(screen.getByPlaceholderText(/describe/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /submit/i })).toBeInTheDocument();
  });

  test("renders chips with scope badges", () => {
    render(<SmartFilter chips={chips} onSubmit={() => {}} onRemoveChip={() => {}} loading={false} />);
    expect(screen.getByText("Last 30 days")).toBeInTheDocument();
    expect(screen.getByText("Amount > 100")).toBeInTheDocument();
    expect(screen.getByText("Both")).toBeInTheDocument();
    expect(screen.getByText("Left")).toBeInTheDocument();
  });

  test("calls onSubmit with input text", () => {
    const onSubmit = jest.fn();
    render(<SmartFilter chips={[]} onSubmit={onSubmit} onRemoveChip={() => {}} loading={false} />);
    const input = screen.getByPlaceholderText(/describe/i);
    fireEvent.change(input, { target: { value: "last 30 days" } });
    fireEvent.click(screen.getByRole("button", { name: /submit/i }));
    expect(onSubmit).toHaveBeenCalledWith("last 30 days");
  });

  test("calls onRemoveChip when X clicked", () => {
    const onRemove = jest.fn();
    render(<SmartFilter chips={chips} onSubmit={() => {}} onRemoveChip={onRemove} loading={false} />);
    const removeButtons = screen.getAllByRole("button", { name: /remove/i });
    fireEvent.click(removeButtons[0]);
    expect(onRemove).toHaveBeenCalledWith("c1");
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npx jest --testPathPattern=SmartFilter --no-coverage 2>&1 | head -20`
Expected: FAIL — module not found.

**Step 3: Implement FilterChipPill**

Create `kalla-web/src/components/wizard/steps/v2/FilterChipPill.tsx`:

```tsx
"use client";

import { X, Calendar, DollarSign, Type } from "lucide-react";
import type { FilterChip } from "@/lib/wizard-types";

const ICON_MAP: Record<string, typeof Calendar> = {
  calendar: Calendar,
  "dollar-sign": DollarSign,
  type: Type,
};

const SCOPE_COLORS: Record<FilterChip["scope"], string> = {
  both: "bg-blue-500 text-white",
  left: "bg-orange-500 text-white",
  right: "bg-violet-500 text-white",
};

const SCOPE_LABELS: Record<FilterChip["scope"], string> = {
  both: "Both",
  left: "Left",
  right: "Right",
};

interface Props {
  chip: FilterChip;
  onRemove: (id: string) => void;
}

export function FilterChipPill({ chip, onRemove }: Props) {
  const Icon = ICON_MAP[chip.icon] ?? Type;

  return (
    <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-1 text-[11px] font-medium">
      <Icon className="h-3 w-3 text-muted-foreground" />
      <span className={`rounded-full px-1.5 py-0.5 text-[10px] font-semibold ${SCOPE_COLORS[chip.scope]}`}>
        {SCOPE_LABELS[chip.scope]}
      </span>
      <span>{chip.label}</span>
      <button
        onClick={() => onRemove(chip.id)}
        className="ml-0.5 text-muted-foreground hover:text-foreground"
        aria-label={`Remove ${chip.label}`}
      >
        <X className="h-3 w-3" />
      </button>
    </span>
  );
}
```

**Step 4: Implement SmartFilter**

Create `kalla-web/src/components/wizard/steps/v2/SmartFilter.tsx`:

```tsx
"use client";

import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Sparkles, ArrowRight, Loader2 } from "lucide-react";
import type { FilterChip } from "@/lib/wizard-types";
import { FilterChipPill } from "./FilterChipPill";

interface Props {
  chips: FilterChip[];
  onSubmit: (text: string) => void;
  onRemoveChip: (chipId: string) => void;
  loading: boolean;
}

export function SmartFilter({ chips, onSubmit, onRemoveChip, loading }: Props) {
  const [text, setText] = useState("");

  const handleSubmit = () => {
    if (!text.trim()) return;
    onSubmit(text.trim());
  };

  return (
    <div className="rounded-xl border-[1.5px] p-4">
      {/* Header */}
      <div className="flex items-center gap-2">
        <Sparkles className="h-4 w-4 text-primary" />
        <span className="text-sm font-semibold">Smart Filter</span>
      </div>

      {/* Description */}
      <p className="mt-1 text-[13px] text-muted-foreground">
        Describe your filter criteria in plain English. AI will parse it into structured filters applied to both sources.
      </p>

      {/* Input row */}
      <div className="mt-3 flex gap-2">
        <div className="relative flex-1">
          <Sparkles className="absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            className="pl-9 text-sm"
            placeholder="Describe filters, e.g. 'last 30 days, amount > $100'"
            value={text}
            onChange={(e) => setText(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSubmit()}
          />
        </div>
        <Button
          size="sm"
          onClick={handleSubmit}
          disabled={loading || !text.trim()}
          aria-label="Submit filter"
        >
          {loading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <ArrowRight className="h-4 w-4" />
          )}
        </Button>
      </div>

      {/* Chips */}
      {chips.length > 0 && (
        <div className="mt-3 flex flex-wrap gap-2">
          {chips.map((chip) => (
            <FilterChipPill key={chip.id} chip={chip} onRemove={onRemoveChip} />
          ))}
        </div>
      )}
    </div>
  );
}
```

**Step 5: Run test to verify it passes**

Run: `cd kalla-web && npx jest --testPathPattern=SmartFilter --no-coverage`
Expected: PASS

**Step 6: Commit**

```bash
git add kalla-web/src/components/wizard/steps/v2/SmartFilter.tsx kalla-web/src/components/wizard/steps/v2/FilterChipPill.tsx kalla-web/src/__tests__/SmartFilter.test.tsx
git commit -m "feat(wizard): add SmartFilter with NL input + FilterChipPill components"
```

---

## Task 7: SamplePreviewV2 with Popovers

**Files:**
- Create: `kalla-web/src/components/wizard/steps/v2/SamplePreviewV2.tsx`
- Create: `kalla-web/src/components/wizard/steps/v2/FieldSelectorPopover.tsx`
- Create: `kalla-web/src/components/wizard/steps/v2/ValuePreviewPopover.tsx`
- Test: `kalla-web/src/__tests__/SamplePreviewV2.test.tsx`

**Step 1: Write the failing test**

Create `kalla-web/src/__tests__/SamplePreviewV2.test.tsx`:

```tsx
import { render, screen, fireEvent } from "@testing-library/react";
import { SamplePreviewV2 } from "@/components/wizard/steps/v2/SamplePreviewV2";
import type { SampleData } from "@/lib/wizard-types";

const sampleLeft: SampleData = {
  columns: [
    { name: "id", data_type: "integer", nullable: false },
    { name: "date", data_type: "date", nullable: false },
    { name: "amount", data_type: "numeric", nullable: true },
    { name: "description", data_type: "text", nullable: true },
  ],
  rows: [
    ["1", "2026-01-01", "100.00", "Payment A"],
    ["2", "2026-01-02", "250.50", "Payment B"],
  ],
  totalRows: 2,
};

const sampleRight: SampleData = {
  columns: [
    { name: "txn_id", data_type: "integer", nullable: false },
    { name: "txn_date", data_type: "date", nullable: false },
    { name: "value", data_type: "numeric", nullable: true },
  ],
  rows: [
    ["101", "2026-01-01", "100.00"],
    ["102", "2026-01-03", "300.00"],
  ],
  totalRows: 2,
};

describe("SamplePreviewV2", () => {
  test("renders header with row count badge", () => {
    render(
      <SamplePreviewV2
        sampleLeft={sampleLeft}
        sampleRight={sampleRight}
        leftAlias="invoices"
        rightAlias="payments"
      />,
    );
    expect(screen.getByText("Sample Preview")).toBeInTheDocument();
    expect(screen.getByText(/2 \+ 2 rows/i)).toBeInTheDocument();
  });

  test("renders side-by-side tables with source names", () => {
    render(
      <SamplePreviewV2
        sampleLeft={sampleLeft}
        sampleRight={sampleRight}
        leftAlias="invoices"
        rightAlias="payments"
      />,
    );
    expect(screen.getByText("invoices")).toBeInTheDocument();
    expect(screen.getByText("payments")).toBeInTheDocument();
  });

  test("shows first 3 columns by default", () => {
    render(
      <SamplePreviewV2
        sampleLeft={sampleLeft}
        sampleRight={sampleRight}
        leftAlias="invoices"
        rightAlias="payments"
      />,
    );
    // Left table has 4 cols, should show 3
    expect(screen.getByText("id")).toBeInTheDocument();
    expect(screen.getByText("date")).toBeInTheDocument();
    expect(screen.getByText("amount")).toBeInTheDocument();
    // "description" should NOT be visible by default
    expect(screen.queryByText("description")).not.toBeInTheDocument();
  });

  test("returns null when no data", () => {
    const { container } = render(
      <SamplePreviewV2
        sampleLeft={null}
        sampleRight={null}
        leftAlias="invoices"
        rightAlias="payments"
      />,
    );
    expect(container.innerHTML).toBe("");
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npx jest --testPathPattern=SamplePreviewV2 --no-coverage 2>&1 | head -20`
Expected: FAIL — module not found.

**Step 3: Implement FieldSelectorPopover**

Create `kalla-web/src/components/wizard/steps/v2/FieldSelectorPopover.tsx`:

```tsx
"use client";

import { useState } from "react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Settings, Check } from "lucide-react";
import type { ColumnInfo } from "@/lib/chat-types";

interface Props {
  columns: ColumnInfo[];
  selected: string[];
  onToggle: (colName: string) => void;
}

export function FieldSelectorPopover({ columns, selected, onToggle }: Props) {
  const [search, setSearch] = useState("");
  const filtered = columns.filter((c) =>
    c.name.toLowerCase().includes(search.toLowerCase()),
  );

  return (
    <Popover>
      <PopoverTrigger asChild>
        <button className="text-muted-foreground hover:text-foreground">
          <Settings className="h-3.5 w-3.5" />
        </button>
      </PopoverTrigger>
      <PopoverContent className="w-[220px] p-0" align="end">
        <div className="border-b p-2">
          <Input
            className="h-7 text-xs"
            placeholder="Search columns..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
        <div className="max-h-[200px] overflow-y-auto p-1">
          {filtered.map((col) => (
            <button
              key={col.name}
              className="flex w-full items-center gap-2 rounded px-2 py-1 text-xs hover:bg-muted"
              onClick={() => onToggle(col.name)}
            >
              <span className="flex-1 text-left">{col.name}</span>
              <Badge variant="outline" className="text-[9px] font-normal">
                {col.data_type}
              </Badge>
              {selected.includes(col.name) && (
                <Check className="h-3 w-3 text-primary" />
              )}
            </button>
          ))}
        </div>
      </PopoverContent>
    </Popover>
  );
}
```

**Step 4: Implement ValuePreviewPopover**

Create `kalla-web/src/components/wizard/steps/v2/ValuePreviewPopover.tsx`:

```tsx
"use client";

import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Badge } from "@/components/ui/badge";
import type { ColumnInfo } from "@/lib/chat-types";

interface Props {
  column: ColumnInfo;
  values: string[];
  children: React.ReactNode;
}

export function ValuePreviewPopover({ column, values, children }: Props) {
  const unique = [...new Set(values.filter(Boolean))];
  const displayValues = unique.slice(0, 8);

  return (
    <Popover>
      <PopoverTrigger asChild>{children}</PopoverTrigger>
      <PopoverContent className="w-[200px] p-3" align="start">
        <div className="mb-2 flex items-center gap-2">
          <span className="text-xs font-semibold">{column.name}</span>
          <Badge variant="outline" className="text-[9px] font-normal">
            {column.data_type}
          </Badge>
        </div>
        <div className="space-y-0.5">
          {displayValues.map((val, i) => (
            <div key={i} className="truncate text-xs text-muted-foreground">
              {val}
            </div>
          ))}
        </div>
        <div className="mt-2 text-[10px] text-muted-foreground">
          {unique.length} distinct value{unique.length !== 1 ? "s" : ""}
        </div>
      </PopoverContent>
    </Popover>
  );
}
```

**Step 5: Implement SamplePreviewV2**

Create `kalla-web/src/components/wizard/steps/v2/SamplePreviewV2.tsx`:

```tsx
"use client";

import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Rows3, Landmark, FileText } from "lucide-react";
import type { SampleData } from "@/lib/wizard-types";
import { FieldSelectorPopover } from "./FieldSelectorPopover";
import { ValuePreviewPopover } from "./ValuePreviewPopover";

interface SourceTableProps {
  title: string;
  icon: typeof Landmark;
  data: SampleData;
}

function SourceTableV2({ title, icon: Icon, data }: SourceTableProps) {
  const [selectedCols, setSelectedCols] = useState<string[]>(
    data.columns.slice(0, 3).map((c) => c.name),
  );

  const displayCols = data.columns.filter((c) => selectedCols.includes(c.name));
  const colIndices = displayCols.map((c) =>
    data.columns.findIndex((col) => col.name === c.name),
  );

  const toggleCol = (colName: string) => {
    setSelectedCols((prev) =>
      prev.includes(colName)
        ? prev.filter((n) => n !== colName)
        : [...prev, colName],
    );
  };

  return (
    <div className="relative flex-1 overflow-hidden rounded-xl border-[1.5px]">
      <div className="flex items-center justify-between border-b bg-background px-4 py-2.5">
        <div className="flex items-center gap-2">
          <Icon className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-semibold">{title}</span>
          <Badge variant="secondary" className="text-[10px]">
            {data.rows.length} rows
          </Badge>
        </div>
        <FieldSelectorPopover
          columns={data.columns}
          selected={selectedCols}
          onToggle={toggleCol}
        />
      </div>
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              {displayCols.map((col) => (
                <TableHead key={col.name} className="text-xs font-medium">
                  <ValuePreviewPopover
                    column={col}
                    values={data.rows.map((r) => r[data.columns.findIndex((c) => c.name === col.name)] ?? "")}
                  >
                    <button className="hover:underline">{col.name}</button>
                  </ValuePreviewPopover>
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.rows.map((row, rowIdx) => (
              <TableRow key={rowIdx}>
                {colIndices.map((colIdx) => (
                  <TableCell key={colIdx} className="text-xs">
                    {row[colIdx] ?? ""}
                  </TableCell>
                ))}
              </TableRow>
            ))}
            {data.rows.length === 0 && (
              <TableRow>
                <TableCell
                  colSpan={displayCols.length}
                  className="py-8 text-center text-xs text-muted-foreground"
                >
                  No data loaded
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

interface Props {
  sampleLeft: SampleData | null;
  sampleRight: SampleData | null;
  leftAlias: string;
  rightAlias: string;
}

export function SamplePreviewV2({ sampleLeft, sampleRight, leftAlias, rightAlias }: Props) {
  if (!sampleLeft && !sampleRight) return null;

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Rows3 className="h-4 w-4" />
          <span className="text-[15px] font-semibold">Sample Preview</span>
        </div>
        {(sampleLeft || sampleRight) && (
          <Badge variant="secondary" className="text-[10px]">
            Showing {sampleLeft?.rows.length ?? 0} + {sampleRight?.rows.length ?? 0} rows
          </Badge>
        )}
      </div>
      <div className="flex gap-4">
        {sampleLeft && (
          <SourceTableV2 title={leftAlias} icon={Landmark} data={sampleLeft} />
        )}
        {sampleRight && (
          <SourceTableV2 title={rightAlias} icon={FileText} data={sampleRight} />
        )}
      </div>
    </div>
  );
}
```

**Step 6: Run test to verify it passes**

Run: `cd kalla-web && npx jest --testPathPattern=SamplePreviewV2 --no-coverage`
Expected: PASS

**Step 7: Commit**

```bash
git add kalla-web/src/components/wizard/steps/v2/SamplePreviewV2.tsx kalla-web/src/components/wizard/steps/v2/FieldSelectorPopover.tsx kalla-web/src/components/wizard/steps/v2/ValuePreviewPopover.tsx kalla-web/src/__tests__/SamplePreviewV2.test.tsx
git commit -m "feat(wizard): add SamplePreviewV2 with field selector + value preview popovers"
```

---

## Task 8: SampleDataV2 Orchestrator

This is the main Screen 2 v2 component that wires together all sub-components with the wizard state.

**Files:**
- Create: `kalla-web/src/components/wizard/steps/v2/SampleDataV2.tsx`
- Test: `kalla-web/src/__tests__/SampleDataV2.test.tsx`

**Step 1: Write the failing test**

Create `kalla-web/src/__tests__/SampleDataV2.test.tsx`:

```tsx
import { render, screen } from "@testing-library/react";
import { SampleDataV2 } from "@/components/wizard/steps/v2/SampleDataV2";
import { WizardProvider } from "@/components/wizard/wizard-context";
import type { ReactNode } from "react";

// Mock the child components to isolate orchestrator logic
jest.mock("@/components/wizard/steps/v2/ExpandedSourceCards", () => ({
  ExpandedSourceCards: (props: Record<string, unknown>) => (
    <div data-testid="expanded-cards">{JSON.stringify(props)}</div>
  ),
}));
jest.mock("@/components/wizard/steps/v2/CollapsedSourcesBar", () => ({
  CollapsedSourcesBar: () => <div data-testid="collapsed-bar" />,
}));
jest.mock("@/components/wizard/steps/v2/SmartFilter", () => ({
  SmartFilter: () => <div data-testid="smart-filter" />,
}));
jest.mock("@/components/wizard/steps/v2/SamplePreviewV2", () => ({
  SamplePreviewV2: () => <div data-testid="sample-preview" />,
}));

// Mock fetch and callAI
global.fetch = jest.fn();
jest.mock("@/lib/ai-client", () => ({
  callAI: jest.fn(),
}));

function wrapper({ children }: { children: ReactNode }) {
  return <WizardProvider>{children}</WizardProvider>;
}

describe("SampleDataV2", () => {
  test("renders expanded cards when sourcesExpanded is true (default)", () => {
    render(<SampleDataV2 />, { wrapper });
    expect(screen.getByTestId("expanded-cards")).toBeInTheDocument();
    expect(screen.queryByTestId("collapsed-bar")).not.toBeInTheDocument();
  });

  test("renders Smart Filter section", () => {
    render(<SampleDataV2 />, { wrapper });
    expect(screen.getByTestId("smart-filter")).toBeInTheDocument();
  });

  test("renders Back and Continue buttons", () => {
    render(<SampleDataV2 />, { wrapper });
    expect(screen.getByText("Back")).toBeInTheDocument();
    expect(screen.getByText("Continue")).toBeInTheDocument();
  });

  test("Continue button is disabled when no samples loaded", () => {
    render(<SampleDataV2 />, { wrapper });
    const continueBtn = screen.getByText("Continue").closest("button");
    expect(continueBtn).toBeDisabled();
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npx jest --testPathPattern=SampleDataV2 --no-coverage 2>&1 | head -20`
Expected: FAIL — module not found.

**Step 3: Implement SampleDataV2**

Create `kalla-web/src/components/wizard/steps/v2/SampleDataV2.tsx`:

```tsx
"use client";

import { useCallback, useEffect, useRef } from "react";
import { useWizard } from "@/components/wizard/wizard-context";
import { Button } from "@/components/ui/button";
import { ArrowLeft, ArrowRight } from "lucide-react";
import { callAI } from "@/lib/ai-client";
import type { SourceConfig, SampleData, FilterChip } from "@/lib/wizard-types";
import { CollapsedSourcesBar } from "./CollapsedSourcesBar";
import { ExpandedSourceCards } from "./ExpandedSourceCards";
import { SmartFilter } from "./SmartFilter";
import { SamplePreviewV2 } from "./SamplePreviewV2";

export function SampleDataV2() {
  const { state, dispatch } = useWizard();
  const autoCollapsedRef = useRef(false);

  const leftAlias = state.leftSource?.alias ?? "Source A";
  const rightAlias = state.rightSource?.alias ?? "Source B";
  const canContinue = state.sampleLeft !== null && state.sampleRight !== null;

  // Handle source loaded callback from ExpandedSourceCards
  const handleSourceLoaded = useCallback(
    (side: "left" | "right", config: SourceConfig, sample: SampleData) => {
      dispatch({ type: "SET_SOURCE_CONFIG", side, config });
      dispatch({ type: "SET_SAMPLE", side, data: sample });
    },
    [dispatch],
  );

  // Auto-collapse when both sources loaded (first time only)
  useEffect(() => {
    if (
      !autoCollapsedRef.current &&
      state.sourceConfigLeft?.loaded &&
      state.sourceConfigRight?.loaded
    ) {
      autoCollapsedRef.current = true;
      dispatch({ type: "TOGGLE_SOURCES_EXPANDED" });
    }
  }, [state.sourceConfigLeft?.loaded, state.sourceConfigRight?.loaded, dispatch]);

  // NL Filter submit
  const handleFilterSubmit = useCallback(
    async (text: string) => {
      if (!state.schemaLeft || !state.schemaRight) return;

      dispatch({ type: "SET_LOADING", key: "parseNlFilter", value: true });
      dispatch({ type: "SET_ERROR", key: "parseNlFilter", error: null });

      try {
        const result = await callAI<{
          filters: Array<{
            label: string;
            icon: string;
            scope: "both" | "left" | "right";
            type: string;
            field_a?: string;
            field_b?: string;
            value: [string, string] | string | null;
          }>;
          explanation: string;
        }>("parse_nl_filter", {
          text,
          schema_a: { alias: leftAlias, columns: state.schemaLeft },
          schema_b: { alias: rightAlias, columns: state.schemaRight },
          current_mappings: state.fieldMappings,
        });

        const chips: FilterChip[] = result.filters.map((f, i) => ({
          id: `chip-${Date.now()}-${i}`,
          ...f,
        }));

        dispatch({ type: "SET_FILTER_CHIPS", chips });
      } catch (err) {
        dispatch({
          type: "SET_ERROR",
          key: "parseNlFilter",
          error: err instanceof Error ? err.message : "Filter parsing failed",
        });
      } finally {
        dispatch({ type: "SET_LOADING", key: "parseNlFilter", value: false });
      }
    },
    [state.schemaLeft, state.schemaRight, state.fieldMappings, leftAlias, rightAlias, dispatch],
  );

  // Remove chip
  const handleRemoveChip = useCallback(
    (chipId: string) => {
      dispatch({ type: "REMOVE_FILTER_CHIP", chipId });
    },
    [dispatch],
  );

  // Auto-refresh preview when chips change (debounced)
  useEffect(() => {
    if (state.filterChips.length === 0) return;

    const timer = setTimeout(async () => {
      // Build conditions from chips for each source
      const buildConditions = (side: "left" | "right") =>
        state.filterChips
          .filter((c) => c.scope === "both" || c.scope === side)
          .map((c) => {
            const field = side === "left" ? c.field_a : c.field_b;
            if (!field) return null;
            if (c.type === "date_range" && Array.isArray(c.value)) {
              return { column: field, op: "between", value: c.value };
            }
            if (c.type === "amount_range" && c.value) {
              return { column: field, op: "gte", value: c.value };
            }
            if (c.type === "text_match" && c.value) {
              return { column: field, op: "like", value: `%${c.value}%` };
            }
            return null;
          })
          .filter(Boolean);

      const loadSide = async (side: "left" | "right") => {
        const config = side === "left" ? state.sourceConfigLeft : state.sourceConfigRight;
        if (!config?.loaded) return;
        const conditions = buildConditions(side);
        try {
          const res = await fetch(`/api/sources/${config.activeAlias}/load-scoped`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ conditions, limit: 200 }),
          });
          if (!res.ok) return;
          const data = await res.json();
          dispatch({
            type: "SET_SAMPLE",
            side,
            data: { columns: data.columns, rows: data.rows, totalRows: data.total_rows },
          });
        } catch {
          // Silently fail filter refresh — user still has previous data
        }
      };

      await Promise.all([loadSide("left"), loadSide("right")]);
    }, 500);

    return () => clearTimeout(timer);
  }, [state.filterChips, state.sourceConfigLeft, state.sourceConfigRight, dispatch]);

  return (
    <div className="flex flex-col gap-6">
      {/* Source Configuration */}
      {state.sourcesExpanded ? (
        <ExpandedSourceCards
          leftAlias={leftAlias}
          rightAlias={rightAlias}
          leftLoaded={state.sourceConfigLeft?.loaded ?? false}
          rightLoaded={state.sourceConfigRight?.loaded ?? false}
          onSourceLoaded={handleSourceLoaded}
        />
      ) : (
        state.sourceConfigLeft &&
        state.sourceConfigRight && (
          <CollapsedSourcesBar
            left={state.sourceConfigLeft}
            right={state.sourceConfigRight}
            onEdit={() => dispatch({ type: "TOGGLE_SOURCES_EXPANDED" })}
          />
        )
      )}

      {/* Smart Filter */}
      <SmartFilter
        chips={state.filterChips}
        onSubmit={handleFilterSubmit}
        onRemoveChip={handleRemoveChip}
        loading={!!state.loading.parseNlFilter}
      />

      {/* Sample Preview */}
      <SamplePreviewV2
        sampleLeft={state.sampleLeft}
        sampleRight={state.sampleRight}
        leftAlias={leftAlias}
        rightAlias={rightAlias}
      />

      {/* Footer */}
      <div className="flex justify-between border-t pt-6">
        <Button
          variant="outline"
          onClick={() => dispatch({ type: "SET_STEP", step: 1 })}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <Button
          disabled={!canContinue}
          onClick={() => dispatch({ type: "SET_STEP", step: 3 })}
        >
          Continue
          <ArrowRight className="ml-2 h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npx jest --testPathPattern=SampleDataV2 --no-coverage`
Expected: PASS

**Step 5: Commit**

```bash
git add kalla-web/src/components/wizard/steps/v2/SampleDataV2.tsx kalla-web/src/__tests__/SampleDataV2.test.tsx
git commit -m "feat(wizard): add SampleDataV2 orchestrator wiring all v2 sub-components"
```

---

## Task 9: V2 Barrel Export

Create index file for clean imports.

**Files:**
- Create: `kalla-web/src/components/wizard/steps/v2/index.ts`

**Step 1: Create barrel export**

Create `kalla-web/src/components/wizard/steps/v2/index.ts`:

```typescript
export { SampleDataV2 } from "./SampleDataV2";
export { CollapsedSourcesBar } from "./CollapsedSourcesBar";
export { ExpandedSourceCards } from "./ExpandedSourceCards";
export { SmartFilter } from "./SmartFilter";
export { FilterChipPill } from "./FilterChipPill";
export { SamplePreviewV2 } from "./SamplePreviewV2";
export { FieldSelectorPopover } from "./FieldSelectorPopover";
export { ValuePreviewPopover } from "./ValuePreviewPopover";
```

**Step 2: Commit**

```bash
git add kalla-web/src/components/wizard/steps/v2/index.ts
git commit -m "feat(wizard): add v2 barrel export"
```

---

## Task 10: Swap Screen 2 Route to V2

**Files:**
- Modify: `kalla-web/src/app/recipes/new/page.tsx:6,33`
- Test: manual verification

**Step 1: Update import in page.tsx**

In `kalla-web/src/app/recipes/new/page.tsx`:

Change line 6 from:
```typescript
import { SampleData } from "@/components/wizard/steps/SampleData";
```
to:
```typescript
import { SampleDataV2 } from "@/components/wizard/steps/v2";
```

**Step 2: Update component usage**

Change line 33 from:
```tsx
            <SampleData />
```
to:
```tsx
            <SampleDataV2 />
```

**Step 3: Run all tests**

Run: `cd kalla-web && npx jest --no-coverage 2>&1 | tail -20`
Expected: All tests pass.

**Step 4: Run dev server and verify manually**

Run: `cd kalla-web && npm run dev`
- Navigate to `/recipes/new`
- Complete Step 1 (select two sources)
- Verify Step 2 shows expanded source cards
- Load both sources → verify auto-collapse
- Type NL filter → verify chips appear
- Verify preview tables with popovers
- Click Continue → verify navigation to Step 3

**Step 5: Commit**

```bash
git add kalla-web/src/app/recipes/new/page.tsx
git commit -m "feat(wizard): swap Screen 2 route to SampleDataV2"
```

---

## Task 11: Cleanup — Remove Old SampleData Import (Optional)

The old `SampleData.tsx` and `FilterCard.tsx` are no longer imported by `page.tsx`. They can remain in the codebase for reference or be removed in a follow-up.

**Decision:** Keep old files for now. They are dead code but serve as reference. Remove in a separate PR after v2 is validated in production.

---

## Summary

| Task | Component | Tests | Commit Message |
|------|-----------|-------|----------------|
| 1 | wizard-types.ts (v2 types) | wizard-types-v2.test.ts | `feat(wizard): add Screen 2 v2 types` |
| 2 | wizard-context.tsx (v2 reducer) | wizard-reducer-v2.test.ts | `feat(wizard): add Screen 2 v2 reducer cases` |
| 3 | /api/sources/register-csv | register-csv-route.test.ts | `feat(api): add POST /api/sources/register-csv` |
| 4 | CollapsedSourcesBar | CollapsedSourcesBar.test.tsx | `feat(wizard): add CollapsedSourcesBar` |
| 5 | ExpandedSourceCards | ExpandedSourceCards.test.tsx | `feat(wizard): add ExpandedSourceCards` |
| 6 | SmartFilter + FilterChipPill | SmartFilter.test.tsx | `feat(wizard): add SmartFilter + FilterChipPill` |
| 7 | SamplePreviewV2 + Popovers | SamplePreviewV2.test.tsx | `feat(wizard): add SamplePreviewV2 with popovers` |
| 8 | SampleDataV2 (orchestrator) | SampleDataV2.test.tsx | `feat(wizard): add SampleDataV2 orchestrator` |
| 9 | Barrel export | — | `feat(wizard): add v2 barrel export` |
| 10 | Route swap | manual | `feat(wizard): swap Screen 2 route to SampleDataV2` |
