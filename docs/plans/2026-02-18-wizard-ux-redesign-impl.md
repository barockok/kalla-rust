# Wizard UX Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the chat-based reconciliation UI with a 6-step wizard, new Recipes home, enhanced Runs/Exceptions/Data Sources pages — all built with shadcn/ui on the existing Tailwind CSS v4 stack.

**Architecture:** Clean rebuild of all pages in `kalla-web/src/app/`. Sidebar replaces top nav. AI agent tools (`agent-tools.ts`) called directly from wizard step handlers (server actions) instead of through the chat streaming interface. Existing API routes (`/api/sources`, `/api/recipes`, `/api/runs`, `/api/uploads`, `/api/worker`) are preserved. New API routes added for exceptions and wizard actions.

**Tech Stack:** Next.js 16, React 19, TypeScript, Tailwind CSS v4 (OKLCH), shadcn/ui, TanStack React Query, Lucide icons, existing agent-tools.ts functions.

**Worktree:** `.worktrees/wizard-ux` on branch `feat/wizard-ux-redesign`

**Design doc:** `docs/plans/2026-02-18-wizard-ux-redesign.md`

**Prototype reference:** `design-ux/kalla-reconcile.jsx` on `feat/ux-refinement` branch (for visual/behavioral reference)

---

## Task 1: Update Color System & Add Green Accent

**Files:**
- Modify: `kalla-web/src/app/globals.css`

**Context:** The prototype uses `#3ECF8E` as the primary green accent. The current theme uses neutral gray OKLCH values for `--primary`. We need to shift primary to green while keeping the OKLCH structure that shadcn/ui expects.

**Step 1: Update CSS variables for light theme**

In `globals.css`, update `:root` section:
- `--primary`: change from `oklch(0.205 0 0)` (near-black) to `oklch(0.72 0.17 163)` (approx `#3ECF8E` green)
- `--primary-foreground`: change to `oklch(0.16 0.03 163)` (dark green for text on green bg)
- Add `--success` custom property: `oklch(0.72 0.17 163)` (same green, for explicit success states)
- Add `--warning`: `oklch(0.75 0.15 75)` (amber/orange for unmatched counts)
- `--accent`: keep as-is (gray) for subtle hover states
- `--sidebar`: `oklch(0.985 0 0)` (already correct — very light)
- `--sidebar-primary`: update to match new `--primary` green

**Step 2: Update CSS variables for dark theme**

In `.dark` section:
- `--primary`: `oklch(0.72 0.17 163)` (green stays same in dark)
- `--primary-foreground`: `oklch(0.16 0.03 163)`
- Add `--success` and `--warning` matching light theme values
- `--sidebar`: keep `oklch(0.205 0 0)`
- `--sidebar-primary`: update to match green

**Step 3: Add Tailwind theme inline mappings**

In the `@theme inline {}` block, add:
```css
--color-success: var(--success);
--color-warning: var(--warning);
```

**Step 4: Verify build compiles**

Run: `cd kalla-web && npm run build`
Expected: Build succeeds (no runtime check needed yet, just CSS compilation)

**Step 5: Commit**

```bash
git add kalla-web/src/app/globals.css
git commit -m "style: update color system with green accent for wizard UX"
```

---

## Task 2: Add Required shadcn/ui Components

**Files:**
- Create: `kalla-web/src/components/ui/separator.tsx`
- Create: `kalla-web/src/components/ui/tooltip.tsx`
- Create: `kalla-web/src/components/ui/progress.tsx`
- Create: `kalla-web/src/components/ui/dropdown-menu.tsx`
- Create: `kalla-web/src/components/ui/popover.tsx`
- Create: `kalla-web/src/components/ui/calendar.tsx` (date picker support)
- Modify: `kalla-web/package.json` (new Radix dependencies)

**Step 1: Install Radix primitives**

```bash
cd kalla-web
npm install @radix-ui/react-separator @radix-ui/react-tooltip @radix-ui/react-progress @radix-ui/react-dropdown-menu @radix-ui/react-popover
npm install react-day-picker date-fns
```

**Step 2: Add shadcn/ui Separator**

Create `src/components/ui/separator.tsx` — standard shadcn/ui Separator using `@radix-ui/react-separator`. Horizontal/vertical variants. Follows the same pattern as existing shadcn components in the project.

**Step 3: Add shadcn/ui Tooltip**

Create `src/components/ui/tooltip.tsx` — standard shadcn/ui Tooltip with TooltipProvider, TooltipTrigger, TooltipContent.

**Step 4: Add shadcn/ui Progress**

Create `src/components/ui/progress.tsx` — standard shadcn/ui Progress bar. Used for stage-based progress in run detail.

**Step 5: Add shadcn/ui DropdownMenu**

Create `src/components/ui/dropdown-menu.tsx` — standard shadcn/ui DropdownMenu. Used for exception status dropdowns, recipe card actions.

**Step 6: Add shadcn/ui Popover + Calendar (DatePicker)**

Create `src/components/ui/popover.tsx` and `src/components/ui/calendar.tsx`. Calendar wraps `react-day-picker`. Used for date range filters in wizard Step 3 and run configuration.

**Step 7: Verify build**

Run: `cd kalla-web && npm run build`
Expected: Build succeeds

**Step 8: Commit**

```bash
git add kalla-web/src/components/ui/ kalla-web/package.json kalla-web/package-lock.json
git commit -m "feat: add shadcn/ui components (separator, tooltip, progress, dropdown, popover, calendar)"
```

---

## Task 3: Build Sidebar Layout

**Files:**
- Create: `kalla-web/src/components/sidebar.tsx`
- Create: `kalla-web/src/components/theme-toggle.tsx`
- Modify: `kalla-web/src/app/layout.tsx`
- Delete: `kalla-web/src/components/navigation.tsx`

**Context:** Replace the top navbar with a fixed left sidebar matching the prototype: logo at top, nav items with count badges, theme toggle + user info at bottom.

**Step 1: Create ThemeToggle component**

Create `src/components/theme-toggle.tsx`:
- "use client" component
- Toggles `dark` class on `<html>` element
- Uses `localStorage` to persist preference
- Renders Sun/Moon icon from lucide-react
- Shows current mode label ("Light mode" / "Dark mode")

**Step 2: Create Sidebar component**

Create `src/components/sidebar.tsx`:
- "use client" component
- Fixed left sidebar, `w-56` (224px), full height
- Uses `usePathname()` for active state
- Logo: green circle with "K" + "Kalla" text + "Reconcile Engine" subtitle
- Section label: "WORKSPACE"
- Nav items with icons + count badges:
  - Recipes (`/`) — BookOpen icon — badge from React Query
  - Data Sources (`/sources`) — Database icon — badge
  - Runs (`/runs`) — Play icon — badge
  - Exceptions — AlertCircle icon — badge (links to latest run's exceptions or `/exceptions`)
- Bottom section: ThemeToggle + user avatar placeholder ("PM" circle + "Product Manager · Acme Fintech")
- Active item: green text + green background tint
- Inactive: `text-muted-foreground`

Each nav item structure:
```tsx
<Link href={item.href} className={cn(
  "flex items-center justify-between px-3 py-2 rounded-lg text-sm font-medium transition-colors",
  isActive ? "bg-primary/10 text-primary" : "text-muted-foreground hover:text-foreground hover:bg-muted"
)}>
  <div className="flex items-center gap-3">
    <Icon className="h-4 w-4" />
    <span>{item.label}</span>
  </div>
  {item.count !== undefined && (
    <Badge variant="secondary" className="text-xs">{item.count}</Badge>
  )}
</Link>
```

**Step 3: Update root layout**

Modify `src/app/layout.tsx`:
- Remove `<Navigation />` import
- Replace the `<main className="container mx-auto py-6 px-4">` with sidebar layout:
```tsx
<div className="min-h-screen bg-background flex">
  <Sidebar />
  <main className="flex-1 overflow-auto">
    {children}
  </main>
</div>
```
- Add `<TooltipProvider>` wrapper inside Providers for tooltip support

**Step 4: Delete old navigation**

Delete `src/components/navigation.tsx`

**Step 5: Verify build**

Run: `cd kalla-web && npm run build`
Expected: Build succeeds

**Step 6: Commit**

```bash
git add -A kalla-web/src/
git commit -m "feat: replace top navbar with sidebar layout"
```

---

## Task 4: Define Wizard Types & State

**Files:**
- Create: `kalla-web/src/lib/wizard-types.ts`
- Create: `kalla-web/src/lib/wizard-store.ts`

**Context:** The wizard needs shared state across 6 steps. Define the types and a React context/store for wizard state. The wizard holds recipe configuration that progressively fills in as the user advances through steps.

**Step 1: Create wizard-types.ts**

```typescript
export type ReconciliationType = 'payment' | 'settlement' | 'invoice' | 'custom';

export type WizardStep = 1 | 2 | 3 | 4 | 5 | 6;

export interface WizardSource {
  alias: string;
  type: string;
  label: string;        // user-given display name (e.g., "Stripe Payments")
  uri?: string;
  s3Uri?: string;       // for uploaded files
  columns?: ColumnInfo[];
}

export interface ScopeFilter {
  dateFrom?: string;
  dateTo?: string;
  status?: string[];
  amountMin?: number;
  amountMax?: number;
  refPattern?: string;
}

export type MatchVerdict = 'confirmed' | 'rejected' | 'pending';

export interface MatchProposal {
  id: string;
  leftRow: Record<string, string>;
  rightRow: Record<string, string>;
  reasoning: string;
  verdict: MatchVerdict;
  note?: string;
  discrepancies?: Array<{ field: string; leftVal: string; rightVal: string }>;
}

export interface MatchRule {
  id: string;
  leftCol: string;
  rightCol: string;
  op: 'eq' | 'tolerance' | 'contains' | 'date_range';
  tolerance?: number;
  confidence: number;
  reasoning: string;
  fromVerdict?: string;  // which verdict shaped this rule
}

export interface VerdictInsight {
  id: string;
  type: 'confirmed' | 'proposed';
  message: string;
  accepted?: boolean;
}

export interface RecipeParameter {
  name: string;
  type: 'date_range' | 'select' | 'number' | 'text';
  label: string;
  required: boolean;
  options?: string[];       // for select type
  defaultValue?: unknown;
}

export interface WizardState {
  step: WizardStep;
  recipeName: string;
  recipeType: ReconciliationType | null;
  description: string;
  sourceA: WizardSource | null;
  sourceB: WizardSource | null;
  scopeA: ScopeFilter;
  scopeB: ScopeFilter;
  sampleA: Record<string, unknown>[];
  sampleB: Record<string, unknown>[];
  proposals: MatchProposal[];
  rules: MatchRule[];
  verdictInsights: VerdictInsight[];
  parameters: RecipeParameter[];
  savedRecipeId: string | null;
}
```

Also re-export `ColumnInfo` from `chat-types.ts` (it's still needed).

**Step 2: Create wizard-store.ts**

React context + provider for wizard state:
```typescript
// WizardContext with state + dispatch actions
// Actions: setStep, setRecipeName, setRecipeType, setSourceA, setSourceB,
//          setScopeA, setScopeB, setSampleData, addProposal, updateVerdict,
//          setRules, setInsights, reset
// useWizard() hook for consuming components
```

Use `useReducer` for predictable state transitions. Wrap in a context provider.

**Step 3: Commit**

```bash
git add kalla-web/src/lib/wizard-types.ts kalla-web/src/lib/wizard-store.ts
git commit -m "feat: add wizard types and state management"
```

---

## Task 5: Build Wizard Stepper Component

**Files:**
- Create: `kalla-web/src/components/wizard/stepper.tsx`
- Create: `kalla-web/src/components/wizard/wizard-layout.tsx`

**Context:** Horizontal stepper at top of wizard pages. 6 steps with labels, numbered circles, green checkmarks for completed steps, active step highlighted. Below the stepper: content area with Back/Continue navigation.

**Step 1: Create Stepper component**

`src/components/wizard/stepper.tsx`:
- Props: `currentStep: number`, `steps: { label: string }[]`
- Renders horizontal row of step indicators
- Each step: circle (numbered or checkmark if completed) + label below
- Completed: green circle with Check icon, green label
- Active: green outlined circle with number, bold label
- Future: gray circle with number, muted label
- Connected by lines between circles (green for completed segments, gray for future)

Use Tailwind classes, no inline styles. Example structure:
```tsx
<div className="flex items-center justify-between">
  {steps.map((step, i) => (
    <div key={i} className="flex items-center">
      <div className={cn(
        "w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium",
        i < currentStep ? "bg-primary text-primary-foreground" :
        i === currentStep ? "border-2 border-primary text-primary" :
        "border border-muted-foreground/30 text-muted-foreground"
      )}>
        {i < currentStep ? <Check className="h-4 w-4" /> : i + 1}
      </div>
      {/* connector line between steps */}
    </div>
  ))}
</div>
```

**Step 2: Create WizardLayout component**

`src/components/wizard/wizard-layout.tsx`:
- Props: `children`, `onBack`, `onContinue`, `canContinue`, `isFirstStep`, `isLastStep`, `continueLabel?`
- Renders: Stepper at top → content area (children) → bottom bar with Back/Continue buttons
- Continue button: primary (green), disabled when `!canContinue`
- Back button: outline variant, hidden on first step
- Last step: Continue label changes to "Save Recipe"

**Step 3: Verify build**

Run: `cd kalla-web && npm run build`

**Step 4: Commit**

```bash
git add kalla-web/src/components/wizard/
git commit -m "feat: add wizard stepper and layout components"
```

---

## Task 6: Build Recipes Home Page

**Files:**
- Rewrite: `kalla-web/src/app/page.tsx`
- Create: `kalla-web/src/components/recipe-card.tsx`

**Context:** The landing page becomes the Recipes home — a grid of recipe cards with match rate, trend, unmatched count, source visualization, and action buttons. Replaces the current dashboard with feature cards.

**Step 1: Create RecipeCard component**

`src/components/recipe-card.tsx`:
- Props: recipe data (name, status, type, sourceA label, sourceB label, matchRate, matchRateTrend, unmatchedCount, lastRunAt, schedule?)
- Card layout:
  - Top: recipe name (bold) + status Badge (ACTIVE green / DRAFT gray) + schedule badge (teal pill, optional)
  - Middle: source flow visualization — SourceA icon → dashed arrow (via CSS border-dashed) → SourceB icon with labels underneath
  - Stats row: match rate % (green if >95, red if <90) + trend arrow (TrendingUp/TrendingDown icon, green/red) + unmatched count in orange
  - Bottom: "Last run: 2h ago" muted text + Run Button (primary, small) + Schedule Button (outline, small)
- Uses shadcn Card, Badge, Button

Source icon helper: render a small colored square with 2-letter abbreviation based on source type (PG for postgres, CS for csv, ES for elasticsearch).

**Step 2: Create empty state component**

Inline in `page.tsx` or a small component:
- Centered BookOpen icon (muted)
- "No recipes yet" heading
- "A recipe defines how to match records between two data sources." subtitle
- "Create your first recipe" Button (primary, links to `/recipes/new`)
- Optional: 2-3 template cards below (Payment Matching, Settlement, Invoice)

**Step 3: Rewrite page.tsx**

`src/app/page.tsx`:
- "use client"
- Fetch recipes via React Query: `useQuery({ queryKey: ['recipes'], queryFn: listRecipes })`
- Fetch recent runs for stats: `useQuery({ queryKey: ['runs'], queryFn: listRuns })`
- Merge run stats into recipe data (match rate, trend, last run time, unmatched count)
- Page header: "Recipes" title + `{count} recipes` subtitle + "+ New Recipe" Button linking to `/recipes/new`
- Search input (filters recipe cards by name, client-side)
- Grid: `grid grid-cols-1 md:grid-cols-2 gap-6` of RecipeCard components
- Empty state when no recipes
- Padding: `p-8` (no container, sidebar handles left margin)

**Step 4: Delete old reconcile page route**

Delete `src/app/reconcile/page.tsx` (the chat UI page)

**Step 5: Verify build**

Run: `cd kalla-web && npm run build`

**Step 6: Commit**

```bash
git add -A kalla-web/src/
git commit -m "feat: build recipes home page with recipe cards"
```

---

## Task 7: Build Wizard Step 1 — Setup

**Files:**
- Create: `kalla-web/src/app/recipes/new/page.tsx`
- Create: `kalla-web/src/components/wizard/step-setup.tsx`

**Context:** First wizard step. User enters recipe name, selects reconciliation type (4 cards), picks Source A and Source B from registered sources, and optionally adds a description.

**Step 1: Create the wizard page**

`src/app/recipes/new/page.tsx`:
- "use client"
- Wraps content in `<WizardProvider>`
- Renders WizardLayout with step-specific content
- Manages step navigation (state or URL params)
- Conditionally renders Step1/Step2/.../Step6 based on current step
- Fetches sources on mount via React Query for Step 1's source grid

**Step 2: Create StepSetup component**

`src/components/wizard/step-setup.tsx`:
- Recipe name Input (required, red asterisk label)
- Reconciliation type: 4 clickable Card components in a 2x2 grid
  - Payment Reconciliation: "Match payments against bank statements" — green border when selected
  - Settlement: "Reconcile settlement batches"
  - Invoice: "Match invoices to payments"
  - Custom: "Define custom matching rules"
- Data source grid: fetch from API, render as cards in 2x2 grid
  - Each card: source icon + name + meta (host, table count)
  - Click once → assigned as Source A (green "SOURCE A ✓" badge), card highlighted
  - Click a different card → assigned as Source B (blue "SOURCE B ✓" badge)
  - Already-assigned source: dimmed with badge showing assignment
  - Click assigned source again → unassign
- Description: Textarea, labeled "Description (optional)", muted styling
- `canContinue`: recipeName is non-empty AND recipeType is selected AND both sources assigned

**Step 3: Verify build**

Run: `cd kalla-web && npm run build`

**Step 4: Commit**

```bash
git add kalla-web/src/app/recipes/new/ kalla-web/src/components/wizard/step-setup.tsx
git commit -m "feat: wizard step 1 — recipe setup with type and source selection"
```

---

## Task 8: Build Wizard Step 2 — Sources

**Files:**
- Create: `kalla-web/src/components/wizard/step-sources.tsx`

**Context:** Confirms Source A and Source B selection from Step 1. Lets user add display labels. Shows source preview (columns).

**Step 1: Create StepSources component**

`src/components/wizard/step-sources.tsx`:
- Two-column layout (`grid grid-cols-2 gap-8`)
- Column A: "Source A" header + "(Primary — your records)" hint in muted text
  - Shows selected source name + type badge
  - "Display Label" Input — pre-filled from source alias, editable (e.g., "Stripe Payments")
  - Column preview: fetched via `getSourcePreview(alias)`, rendered as a list of column names with data types
- Column B: same layout for Source B with "(Comparison — external data)" hint
- Sources not selected show greyed text with strikethrough
- `canContinue`: both sources have labels AND both previews loaded successfully

**Step 2: Verify build and commit**

```bash
git add kalla-web/src/components/wizard/step-sources.tsx
git commit -m "feat: wizard step 2 — source confirmation with labels and column preview"
```

---

## Task 9: Build Wizard Step 3 — Scope & Preview

**Files:**
- Create: `kalla-web/src/components/wizard/step-scope.tsx`
- Create: `kalla-web/src/components/wizard/date-range-picker.tsx`
- Create: `kalla-web/src/components/wizard/data-table-preview.tsx`

**Context:** Structured filter controls per source, fetch sample data, display side-by-side data tables.

**Step 1: Create DateRangePicker component**

`src/components/wizard/date-range-picker.tsx`:
- Uses Popover + Calendar from shadcn/ui
- Displays "From" and "To" date inputs
- Returns `{ from: string, to: string }` in ISO format

**Step 2: Create DataTablePreview component**

`src/components/wizard/data-table-preview.tsx`:
- Props: `columns: string[]`, `rows: Record<string, unknown>[]`, `title: string`
- Renders shadcn Table with column headers and rows
- Monospace font for data values (`font-mono text-xs`)
- Max 10 rows displayed, "and N more..." footer if truncated
- Empty state: "No data fetched yet"

**Step 3: Create StepScope component**

`src/components/wizard/step-scope.tsx`:
- Two-column layout — filter panel per source side
- Each side has:
  - DateRangePicker
  - Status Select (multi-select or dropdown, sourced from column metadata)
  - Amount range: two number Inputs (Min / Max)
  - Reference pattern: Input (text search)
- "Fetch Sample Data" Button (primary) — calls `loadScoped()` for registered sources or uses already-loaded data for CSV uploads
- After fetch: side-by-side DataTablePreview components
- Loading state: Loader2 spinner during fetch
- `canContinue`: both sides have sample data loaded (sampleA.length > 0 AND sampleB.length > 0)

**Step 4: Wire API calls**

When "Fetch Sample Data" is clicked:
- For each source, build filter conditions from the structured inputs
- Call `loadScoped(alias, conditions)` from `agent-tools.ts`
- Store results in wizard state via `setSampleData()`

**Step 5: Verify build and commit**

```bash
git add kalla-web/src/components/wizard/step-scope.tsx kalla-web/src/components/wizard/date-range-picker.tsx kalla-web/src/components/wizard/data-table-preview.tsx
git commit -m "feat: wizard step 3 — scope filters and data preview"
```

---

## Task 10: Build Wizard Step 4 — Confirm Matches

**Files:**
- Create: `kalla-web/src/components/wizard/step-confirm.tsx`
- Create: `kalla-web/src/components/wizard/match-card.tsx`
- Create: `kalla-web/src/app/api/wizard/propose-matches/route.ts`

**Context:** AI silently proposes match pairs from sample data. User confirms/rejects each with optional notes. This is the core interaction of the wizard.

**Step 1: Create API route for match proposals**

`src/app/api/wizard/propose-matches/route.ts`:
- POST endpoint
- Accepts: `{ sampleLeft, sampleRight, leftColumns, rightColumns }`
- Uses `proposeMatch()` from `agent-tools.ts` — but needs AI to pick likely pairs
- Implementation: call the Anthropic SDK directly (similar to current `agent.ts`) with a focused prompt:
  - System: "You are a data matching assistant. Given two sets of records, propose likely matches."
  - Pass sample data as context
  - Request tool_use calls to `propose_match` for each pair
  - Parse responses into `MatchProposal[]`
- Returns: `{ proposals: MatchProposal[] }`

**Step 2: Create MatchCard component**

`src/components/wizard/match-card.tsx`:
- Props: `proposal: MatchProposal`, `sourceALabel`, `sourceBLabel`, `onConfirm`, `onReject`, `onNote`
- Card layout:
  - Left half: Source A record with blue header bar showing sourceALabel
  - Right half: Source B record with green header bar showing sourceBLabel
  - Between them: status icon (CheckCircle green / AlertTriangle orange / XCircle red)
  - Each field rendered as `key: value` pairs, monospace for values
  - Discrepancies: value rendered in orange with "(differs by $X)" annotation
- Bottom: three buttons
  - Note (outline, MessageSquare icon) — toggles note textarea
  - Confirm (green bg, Check icon + "Confirm" text)
  - Reject (red bg, X icon + "Reject" text)
- When rejected: note textarea auto-expands with border highlight
- Confirmed: card gets green left border + check badge
- Rejected: card gets red left border + X badge
- Pending: neutral border

**Step 3: Create StepConfirm component**

`src/components/wizard/step-confirm.tsx`:
- On mount: call `/api/wizard/propose-matches` with sample data from wizard state
- Loading state: skeleton cards
- Summary bar at top: 4 badges — PROPOSED (total) / CONFIRMED (green) / REJECTED (red) / PENDING (gray)
- Renders MatchCard for each proposal
- Updates wizard state on confirm/reject/note via dispatch
- `canContinue`: all proposals have verdict !== 'pending'

**Step 4: Verify build and commit**

```bash
git add kalla-web/src/components/wizard/step-confirm.tsx kalla-web/src/components/wizard/match-card.tsx kalla-web/src/app/api/wizard/propose-matches/route.ts
git commit -m "feat: wizard step 4 — AI-proposed match confirmation with cards"
```

---

## Task 11: Build Wizard Step 5 — Match Rules

**Files:**
- Create: `kalla-web/src/components/wizard/step-rules.tsx`
- Create: `kalla-web/src/components/wizard/verdict-insights.tsx`
- Create: `kalla-web/src/components/wizard/rule-card.tsx`
- Create: `kalla-web/src/app/api/wizard/infer-rules/route.ts`

**Context:** AI infers match rules from confirmed/rejected pairs. Shows "Your Verdicts Shaped These Rules" feedback. User can accept/dismiss proposed adjustments.

**Step 1: Create API route for rule inference**

`src/app/api/wizard/infer-rules/route.ts`:
- POST endpoint
- Accepts: `{ confirmedPairs, rejectedPairs, leftColumns, rightColumns, recipeType }`
- Calls `inferRules()` from `agent-tools.ts` for base rules
- For recipe type presets: merge type-specific defaults (payment → amount ±$0.01 + ref exact + currency exact + date same-day)
- Generates `VerdictInsight[]` from verdicts:
  - For each confirmed pair with discrepancy: "You confirmed the $X difference — rule set to ± tolerance"
  - For each rejected pair with note: quote note, propose rule adjustment
- Returns: `{ rules: MatchRule[], insights: VerdictInsight[] }`

**Step 2: Create VerdictInsights component**

`src/components/wizard/verdict-insights.tsx`:
- Props: `insights: VerdictInsight[]`, `onAccept(id)`, `onDismiss(id)`
- Callout block with green left border: "Your Verdicts Shaped These Rules"
- List of insights:
  - Confirmed type: green CheckCircle icon + message text
  - Proposed type: amber AlertCircle icon + message + Accept/Dismiss buttons

**Step 3: Create RuleCard component**

`src/components/wizard/rule-card.tsx`:
- Props: `rule: MatchRule`
- Shows: left column → op label → right column, confidence %, reasoning
- Editable tolerance (number Input) if op is 'tolerance'
- "from sample #N" reference if `fromVerdict` is set

**Step 4: Create StepRules component**

`src/components/wizard/step-rules.tsx`:
- On mount: call `/api/wizard/infer-rules` with wizard state
- Recipe type info banner: "Rules pre-configured for {type} — Customize below as needed" (green Alert)
- VerdictInsights component
- Separator
- Rule cards list
- `canContinue`: at least 1 rule exists

**Step 5: Verify build and commit**

```bash
git add kalla-web/src/components/wizard/step-rules.tsx kalla-web/src/components/wizard/verdict-insights.tsx kalla-web/src/components/wizard/rule-card.tsx kalla-web/src/app/api/wizard/infer-rules/route.ts
git commit -m "feat: wizard step 5 — AI-inferred rules with verdict feedback"
```

---

## Task 12: Build Wizard Step 6 — Review & Save

**Files:**
- Create: `kalla-web/src/components/wizard/step-review.tsx`

**Context:** Final review of recipe before saving. Shows complete summary, then saves via API.

**Step 1: Create StepReview component**

`src/components/wizard/step-review.tsx`:
- Card: Recipe summary
  - Name (bold, large) + type Badge
  - Two source cards side-by-side: icon + label + field count
  - Validation stats: "4 confirmed / 1 rejected from sample"
  - Rules table (shadcn Table): Field Mapping | Operation | Tolerance | Confidence
- Two action buttons:
  - "Save Recipe" (primary) — calls `buildRecipe()` then `saveRecipe()` from agent-tools, stores recipe_id
  - "Save & Run Now" (primary) — saves, then redirects to `/runs/new?recipe={id}`
- After save: success Alert with link to recipe + option to run

**Step 2: Wire save logic**

On "Save Recipe":
1. Build recipe object via `buildRecipe()` with wizard state data
2. Call `saveRecipe()` API
3. Update wizard state with `savedRecipeId`
4. Show success state

On "Save & Run Now":
1. Same as above
2. Redirect to `/runs/new?recipe={savedRecipeId}` via `router.push()`

**Step 3: Verify build and commit**

```bash
git add kalla-web/src/components/wizard/step-review.tsx
git commit -m "feat: wizard step 6 — review and save recipe"
```

---

## Task 13: Build Run Configuration Page

**Files:**
- Create: `kalla-web/src/app/runs/new/page.tsx`

**Context:** Dedicated page for configuring and starting a run. Reads recipe's parameter schema and renders dynamic form. Not a modal — user can navigate away since runs take hours.

**Step 1: Create run config page**

`src/app/runs/new/page.tsx`:
- "use client"
- Reads `recipe` query param
- Fetches recipe via `useQuery({ queryKey: ['recipe', id], queryFn: () => getRecipe(id) })`
- Renders:
  - Header: "Run Configuration" + recipe name
  - Source visualization card: Source A → arrow → Source B with labels
  - Dynamic parameter form:
    - For each `RecipeParameter` in recipe config:
      - `date_range` → DateRangePicker
      - `select` → Select dropdown
      - `number` → Input type number
      - `text` → Input type text
    - Required fields marked with red asterisk
    - Default values pre-filled
  - "Start Reconciliation" Button (primary, large)
- On start:
  - Call `runFull(recipeId)` from agent-tools (or `POST /api/runs` with recipe + params)
  - Get back `{ run_id }`
  - Redirect to `/runs/{run_id}`

**Note:** The `RecipeParameter[]` schema needs to be saved as part of the recipe when building it in Step 6. Extend the `Recipe` type in `recipe-types.ts` to include `parameters?: RecipeParameter[]`. For the initial implementation, default parameters can be `[{ name: "date_range", type: "date_range", label: "Period", required: true }]` if none are explicitly defined.

**Step 2: Verify build and commit**

```bash
git add kalla-web/src/app/runs/new/page.tsx kalla-web/src/lib/recipe-types.ts
git commit -m "feat: run configuration page with dynamic recipe parameters"
```

---

## Task 14: Enhance Run Detail Page

**Files:**
- Modify: `kalla-web/src/app/runs/[id]/page.tsx`
- Create: `kalla-web/src/components/run-progress.tsx`

**Context:** Currently shows basic run metadata. Needs stage-based progress for active runs, completion stats, and "View Exceptions" CTA.

**Step 1: Create RunProgress component**

`src/components/run-progress.tsx`:
- Props: `runId: string`
- Polls `GET /api/runs/{id}` every 2 seconds while status is "Running"
- Shows 4 stages vertically:
  1. "Connecting to sources..." — spinner or checkmark
  2. "Fetching records..." — shows record count when available
  3. "Matching records..." — shows matched count
  4. "Generating results..." — final stage
- Current stage: animated, with Loader2 spinner
- Completed stages: green checkmark
- Future stages: gray, dimmed
- Uses Progress bar component for overall progress (optional)

**Step 2: Rewrite run detail page**

`src/app/runs/[id]/page.tsx`:
- Header: "Run {id}" + recipe name + status Badge
- When Running: RunProgress component
- When Completed: 4 stat cards in a row:
  - Rate: match percentage, large green text
  - Matched: count, green
  - Unmatched: count, orange
  - Time: duration string
- Primary CTA: "View Exceptions" Button linking to `/runs/{id}/exceptions` (only when completed and unmatched > 0)
- Secondary: "Back to Runs" link
- When Failed: red Alert with error message

**Step 3: Verify build and commit**

```bash
git add kalla-web/src/app/runs/\\[id\\]/page.tsx kalla-web/src/components/run-progress.tsx
git commit -m "feat: enhanced run detail with stage-based progress and completion stats"
```

---

## Task 15: Build Exceptions View + API

**Files:**
- Create: `kalla-web/src/app/runs/[id]/exceptions/page.tsx`
- Create: `kalla-web/src/components/exception-table.tsx`
- Create: `kalla-web/src/components/manual-match-modal.tsx`
- Create: `kalla-web/src/app/api/runs/[id]/exceptions/route.ts`

**Context:** New page showing unmatched records from a run. Users can review, add notes, change status, and manually match records. This is where finance users spend most daily time.

**Step 1: Create exceptions API route**

`src/app/api/runs/[id]/exceptions/route.ts`:
- GET: reads unmatched records from the Parquet evidence files (via Rust backend or Postgres)
  - For initial implementation: read from `output_paths.unmatched_left` Parquet file via the existing worker complete data
  - Return: `{ exceptions: Exception[], totalCount: number }`
  - Exception shape: `{ id, amount, date, ref, reason, status: 'unreviewed'|'investigated'|'resolved', note?: string }`
- PATCH (future): update exception status/note

**Note:** The full Parquet-reading implementation may require a new Rust endpoint. For initial implementation, mock the data structure and use the unmatched counts from the run metadata. Create the frontend with proper types and mock data, with a TODO for backend integration.

**Step 2: Create ExceptionTable component**

`src/components/exception-table.tsx`:
- Props: `exceptions: Exception[]`, `onStatusChange`, `onNoteChange`, `onMatch`
- Renders shadcn Table:
  - Columns: ID (monospace, green link), Amount (monospace), Date, Ref, Reason (truncated with tooltip), Status (Select dropdown), Note (Input), Match (Button)
  - Status dropdown: Unreviewed (orange) / Investigated (blue) / Resolved (green) — uses shadcn Select
  - Note: inline Input, expands on focus
  - Match button: opens ManualMatchModal

**Step 3: Create ManualMatchModal component**

`src/components/manual-match-modal.tsx`:
- Props: `exceptionId`, `exceptionData`, `onMatch`, open/close state
- Uses shadcn Dialog
- Content:
  - "Match {ID} against a record from the comparison source"
  - Search Input: "Search by ID, amount, or reference..."
  - Candidate list: 2-3 rows from comparison source (initially mocked)
  - Each candidate: ID + amount + date, with a "Select" button
- On select: calls `onMatch(exceptionId, candidateId)`

**Step 4: Create exceptions page**

`src/app/runs/[id]/exceptions/page.tsx`:
- Header: "Exceptions" + recipe name ↔ source subtitle + run ID + "200 unmatched"
- 4 summary cards (clickable as filters):
  - Total Exceptions — number, bold
  - Unreviewed — orange number
  - Investigated — blue number
  - Resolved — green number
  - Active filter: card gets highlighted border
- ExceptionTable component
- Header buttons: "← Back to Runs" (outline) + "Export CSV" (outline with Download icon)
- Export CSV: generates CSV from current filtered exceptions, triggers browser download

**Step 5: Verify build and commit**

```bash
git add kalla-web/src/app/runs/\\[id\\]/exceptions/ kalla-web/src/components/exception-table.tsx kalla-web/src/components/manual-match-modal.tsx kalla-web/src/app/api/runs/\\[id\\]/exceptions/
git commit -m "feat: exceptions view with status management and manual match modal"
```

---

## Task 16: Redesign Runs List Page

**Files:**
- Rewrite: `kalla-web/src/app/runs/page.tsx`

**Context:** Enhance the existing runs page with summary cards and better table design matching the prototype.

**Step 1: Rewrite runs page**

`src/app/runs/page.tsx`:
- 4 summary cards at top (grid grid-cols-4):
  - Total Runs: count
  - Avg Match Rate: percentage with color
  - Active Now: count (with green dot if > 0)
  - Failed: count (red if > 0)
- Runs table (shadcn Table):
  - Columns: Run ID (monospace, truncated), Recipe Name, Status Badge, Match Rate %, Matched Count, Unmatched Count, Duration, Started At, View button
  - Active runs: show "● 74%" with orange dot + percentage inline
  - Status badges: green (Completed), blue (Running), red (Failed)
  - Click row or eye icon → navigate to `/runs/{id}`
- Refresh button in header
- Empty state: "No runs yet" with CTA to create recipe

**Step 2: Verify build and commit**

```bash
git add kalla-web/src/app/runs/page.tsx
git commit -m "feat: redesign runs page with summary cards and enhanced table"
```

---

## Task 17: Redesign Data Sources Page

**Files:**
- Rewrite: `kalla-web/src/app/sources/page.tsx`

**Context:** Simplify to a flat table matching the prototype. Keep the existing add-source functionality.

**Step 1: Rewrite sources page**

`src/app/sources/page.tsx`:
- Header: "Data Sources" + "Add Source" Button (primary)
- Table (shadcn Table):
  - Columns: Name, Type (badge), Host, Database, Status (badge — green "Connected" / red "Error")
  - Click "Add Source" → opens Dialog with the existing Tabs (File Upload / Connection String) form
- Keep existing `registerSource()` and `listSources()` API integration
- Remove the preview/primary-key inline expansion (simplify for now — those features can be accessed from the wizard)

**Step 2: Verify build and commit**

```bash
git add kalla-web/src/app/sources/page.tsx
git commit -m "feat: redesign data sources page as flat table"
```

---

## Task 18: Cleanup — Remove Chat Code

**Files:**
- Delete: `kalla-web/src/app/reconcile/` (entire directory)
- Delete: `kalla-web/src/components/chat/` (entire directory — ChatMessage, MatchProposalCard, UploadRequestCard, FileMessageCard, FileUploadPill, MarkdownRenderer)
- Delete: `kalla-web/src/app/api/chat/` (entire directory — chat route, sessions routes)
- Delete: `kalla-web/src/components/SourcePreview.tsx` (functionality moved to wizard)
- Delete: `kalla-web/src/components/PrimaryKeyConfirmation.tsx` (not needed in wizard flow)
- Delete: `kalla-web/src/components/FieldPreview.tsx` (not needed in wizard flow)
- Delete: `kalla-web/src/components/ResultSummary.tsx` (replaced by run detail stats)
- Delete: `kalla-web/src/components/LiveProgressIndicator.tsx` (replaced by RunProgress)
- Keep: `kalla-web/src/lib/chat-types.ts` — rename to `kalla-web/src/lib/source-types.ts`, keep only SourceInfo, ColumnInfo, SourcePreview, FilterCondition types (used by wizard). Remove chat-specific types (ChatPhase, ChatMessage, ChatSession, CardType, etc.)
- Keep: `kalla-web/src/lib/agent-tools.ts` — the wizard API routes import from here
- Keep: `kalla-web/src/lib/agent.ts` — the AI orchestration is used by wizard propose-matches and infer-rules routes
- Keep: `kalla-web/src/lib/session-store.ts` — may still be needed for wizard session persistence; evaluate during cleanup

**Step 1: Delete files**

```bash
rm -rf kalla-web/src/app/reconcile
rm -rf kalla-web/src/components/chat
rm -rf kalla-web/src/app/api/chat
rm kalla-web/src/components/SourcePreview.tsx
rm kalla-web/src/components/PrimaryKeyConfirmation.tsx
rm kalla-web/src/components/FieldPreview.tsx
rm kalla-web/src/components/ResultSummary.tsx
rm kalla-web/src/components/LiveProgressIndicator.tsx
```

**Step 2: Refactor chat-types.ts → source-types.ts**

Rename file and keep only:
- `FilterOp`, `FilterCondition`
- `SourceInfo`, `ColumnInfo`, `SourcePreview`
- `FileAttachment` (still used for CSV uploads in wizard)

Update all imports across the codebase:
- `agent-tools.ts`: change import from `chat-types` to `source-types`
- Any wizard components that import these types

**Step 3: Verify build passes**

Run: `cd kalla-web && npm run build`
Fix any broken imports or missing references.

**Step 4: Run tests**

Run: `cd kalla-web && npm test`
Fix or update any tests that reference deleted components/routes.

**Step 5: Commit**

```bash
git add -A kalla-web/src/
git commit -m "chore: remove chat UI code, refactor types for wizard architecture"
```

---

## Task 19: Final Verification & Polish

**Files:** Various

**Step 1: Full build verification**

```bash
cd kalla-web && npm run build
```

Fix any TypeScript errors or build failures.

**Step 2: Test all routes**

Manually verify (or write test) that these routes resolve:
- `/` — Recipes home
- `/recipes/new` — Wizard (all 6 steps navigate correctly)
- `/sources` — Data Sources table
- `/runs` — Runs list
- `/runs/new?recipe=X` — Run configuration
- `/runs/X` — Run detail
- `/runs/X/exceptions` — Exceptions view

**Step 3: Theme verification**

- Light mode: all pages render with white backgrounds, green accent, readable text
- Dark mode: toggle works, all pages render correctly with dark surfaces

**Step 4: Run lint**

```bash
cd kalla-web && npm run lint
```

Fix any lint errors.

**Step 5: Commit any fixes**

```bash
git add -A kalla-web/src/
git commit -m "fix: polish and resolve build/lint issues after UX rebuild"
```

---

## Execution Dependencies

```
Task 1 (colors) → Task 2 (components) → Task 3 (sidebar) → Task 4 (types)
Task 4 → Task 5 (stepper) → Tasks 6-12 (pages, can be partially parallel)
Task 6 (recipes home) — independent once sidebar exists
Task 7-12 (wizard steps) — sequential, each step builds on prior
Task 13 (run config) — after Task 12
Task 14 (run detail) — independent once sidebar exists
Task 15 (exceptions) — after Task 14
Task 16 (runs list) — independent once sidebar exists
Task 17 (data sources) — independent once sidebar exists
Task 18 (cleanup) — LAST, after all new code is in place
Task 19 (verification) — LAST
```

**Parallelizable after Task 5:** Tasks 6, 14, 16, 17 can all be built independently since they don't depend on each other.

---

## Key Files Reference

| File | Purpose | Keep/Delete/Modify |
|------|---------|-------------------|
| `src/app/layout.tsx` | Root layout | Modify (sidebar) |
| `src/app/globals.css` | Theme | Modify (green accent) |
| `src/app/page.tsx` | Dashboard → Recipes home | Rewrite |
| `src/app/reconcile/page.tsx` | Chat UI | Delete |
| `src/app/sources/page.tsx` | Data Sources | Rewrite |
| `src/app/recipes/page.tsx` | Old recipe browser | Rewrite as redirect or remove |
| `src/app/runs/page.tsx` | Runs list | Rewrite |
| `src/app/runs/[id]/page.tsx` | Run detail | Enhance |
| `src/components/navigation.tsx` | Top nav | Delete |
| `src/components/chat/*` | Chat components | Delete |
| `src/components/ui/*` | shadcn primitives | Keep + extend |
| `src/components/providers.tsx` | Query provider | Keep |
| `src/lib/agent-tools.ts` | Tool implementations | Keep (called by wizard APIs) |
| `src/lib/agent.ts` | AI orchestration | Keep (used by wizard APIs) |
| `src/lib/api.ts` | Client API helpers | Keep + extend |
| `src/lib/chat-types.ts` | Types | Refactor → source-types.ts |
| `src/lib/recipe-types.ts` | Recipe types | Extend (add parameters) |
| `src/lib/wizard-types.ts` | Wizard types | Create |
| `src/lib/wizard-store.ts` | Wizard state | Create |
