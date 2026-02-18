# Wizard UX Redesign — Chat to Structured Wizard

**Date:** 2026-02-18
**Status:** Approved
**Approach:** Clean rebuild (Approach B)
**Source material:** `design-ux/` on `feat/ux-refinement` branch (prototype JSX, UX review, 2 rounds persona feedback)

---

## Summary

Replace the current chat-based conversational UI with a structured 6-step wizard for recipe creation, plus redesigned Recipes home, Runs, Exceptions, and Data Sources pages. AI engine stays but operates silently behind wizard steps. Built with shadcn/ui components on the existing Tailwind CSS v4 stack.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| UX paradigm | Wizard replaces chat | Prototype validated with personas; wizard matches finance-user mental model |
| AI role | Silent behind-the-scenes | AI proposes matches (Step 4) and infers rules (Step 5) without a chat interface |
| Component library | shadcn/ui | Already in use; extend with new components (Sidebar, Stepper, etc.) |
| Screen scope | Full prototype | All screens: Recipes home, 6-step wizard, Runs, Run detail, Exceptions, Data Sources |
| Run execution | Dedicated page, not modal | Runs take hours; modal traps user. Dedicated page allows navigate-away-and-return |
| Run parameters | Dynamic per recipe | Recipe schema defines available arguments; run config page renders dynamically |
| Round 2 feedback | Deferred | Collaboration, audit trail, pagination etc. are future iterations |

---

## Navigation & Layout

**Replace** top horizontal navbar **with** fixed left sidebar (~220px):

- Logo: "Kalla / Reconcile Engine"
- Nav items with count badges: Recipes, Data Sources, Runs, Exceptions
- Bottom: Light/Dark theme toggle + user avatar/role

**Color system:**
- Light theme default (white/off-white backgrounds, dark text)
- Green accent: `#3ECF8E` — carried through both themes
- Update existing OKLCH CSS variables to match prototype palette
- Dark theme preserved as toggle

**New shadcn/ui components:** Sidebar (or custom), Tooltip, Separator, DropdownMenu, Popover

---

## Screen 1: Recipes Home

**Route:** `/` (replaces current dashboard)

- Page header: "Recipes" + subtitle + "+ New Recipe" green button
- Search bar to filter recipes
- 2-column recipe card grid:
  - Name + status badge (ACTIVE/DRAFT) + schedule badge (teal pill)
  - Last run timestamp
  - Source flow: Source A icon → dashed arrow → Source B icon with user labels
  - Match rate (green/red) with trend arrow + unmatched count (orange)
  - Schedule + Run buttons
- Empty state: icon, "No recipes yet", CTA, template cards

**Data:** `GET /api/recipes` + `GET /api/runs` for stats

---

## Screen 2: 6-Step Recipe Wizard

**Route:** `/recipes/new`

### Step 1: Setup

- Recipe name (required)
- Reconciliation type: 4 selectable cards (Payment / Settlement / Invoice / Custom) — green border on selection; type pre-configures default rules for Step 5
- Data source grid: registered sources as cards, click to assign Source A / Source B
- Description: optional, demoted to secondary

### Step 2: Sources

- Two columns: Source A ("Primary — your records") / Source B ("Comparison — external data")
- Pre-filled from Step 1
- Display label inputs (user-given names carried throughout wizard)
- Already-assigned sources greyed with strikethrough

**Data:** `GET /api/sources`, `POST /api/sources/[alias]/preview`

### Step 3: Scope & Preview

- Structured filter controls per source: date range picker, status dropdown, amount range, ref search
- "Fetch Sample Data" button
- After fetch: side-by-side data tables with sample records

**Data:** `POST /api/sources/[alias]/load-scoped`

### Step 4: Confirm Matches

- AI silently calls `propose_match` to generate candidate pairs
- Vertical match cards: Source A record (blue header) vs Source B record (green header)
- Status icon between records (checkmark/warning/X)
- Discrepancies highlighted in orange
- Per-card: Note / Confirm (green) / Reject (red) text-labeled buttons
- Reject auto-expands correction note field
- Summary bar: PROPOSED / CONFIRMED / REJECTED / PENDING
- Gate: all matches must be confirmed or rejected to continue

### Step 5: Match Rules

- "Your Verdicts Shaped These Rules" callout with specific citations from Step 4
  - Green checkmarks for confirmed insights
  - Accept/Dismiss for proposed adjustments from rejections
- Rule cards: field mapping + tolerance + confidence
- Pre-configured from recipe type + refined by verdicts
- AI silently calls `infer_rules` to generate rules

### Step 6: Review & Save

- Full summary: name, type badge, source cards with field counts
- Validation stats (confirmed / rejected)
- Rules table
- "Save Recipe" + "Run Now" buttons

**Data:** `save_recipe` + optionally redirect to run config

**Navigation:** Back/Continue buttons at bottom; Back preserves state.

---

## Screen 3: Run Configuration

**Route:** `/runs/new?recipe=<id>`

- Recipe name + source visualization
- Dynamic form fields rendered from recipe parameter schema:
  ```
  parameters: [
    { name: "date_range", type: "date_range", label: "Period", required: true },
    { name: "status_filter", type: "select", label: "Status", options: [...] },
    { name: "min_amount", type: "number", label: "Minimum Amount" }
  ]
  ```
- "Start Reconciliation" button → creates run → redirects to `/runs/[id]`

**Data:** `GET /api/recipes/[id]` for parameter schema, calls `run_full` agent tool

---

## Screen 4: Run Detail

**Route:** `/runs/[id]` (existing, enhanced)

- Stage-based progress: Connecting → Fetching (live record count) → Matching → Complete
- Completion stats: Rate / Matched / Unmatched / Time (4 cards)
- "View Exceptions" primary CTA when complete
- User can navigate away and return; progress persists via worker webhook polling

**Data:** `GET /api/runs/[id]`, worker webhooks (`/api/worker/progress`, `/api/worker/complete`)

---

## Screen 5: Exceptions View

**Route:** `/runs/[id]/exceptions` (new)

- Header: recipe name + run ID + unmatched count
- 4 summary cards (clickable as filters): Total / Unreviewed / Investigated / Resolved
- Table: ID, Amount, Date, Ref, Reason, Status dropdown, Note input, Match button
- "Export CSV" + "Back to Runs" in header
- Manual Match modal: search comparison source by ID/amount/ref, select candidate

**New API:**
- `GET /api/runs/[id]/exceptions`
- `PATCH /api/runs/[id]/exceptions/[eid]` (update status/note)
- `POST /api/runs/[id]/exceptions/[eid]/match` (manual match)

---

## Screen 6: Runs View

**Route:** `/runs` (existing, redesigned)

- 4 summary cards: Total, Avg match rate, Active now, Failed
- Run history table: Run ID, Recipe, Status (live progress for active), match rate, matched/unmatched, duration, timestamp

**Data:** `GET /api/runs`

---

## Screen 7: Data Sources

**Route:** `/sources` (existing, redesigned)

- Table: Name, Type, Host, Database, Status badge
- "Add Source" button

**Data:** `GET /api/sources`

---

## AI Integration (Behind the Scenes)

The existing agent engine (`src/lib/agent.ts`, `agent-tools.ts`) is repurposed:

| Wizard Step | Agent Tool Called | Purpose |
|-------------|-----------------|---------|
| Step 1 | — | No AI needed |
| Step 2 | `get_source_preview` | Preview source schemas |
| Step 3 | `load_scoped` | Fetch filtered sample data |
| Step 4 | `propose_match` | Generate candidate match pairs from samples |
| Step 5 | `infer_rules` | Infer SQL rules from confirmed/rejected pairs |
| Step 6 | `save_recipe` | Persist recipe to database |
| Run Config | `run_full` | Dispatch reconciliation to Rust scheduler |

Tools are called as direct function invocations from wizard step handlers (server actions or API routes), not through the chat/streaming interface.

---

## What Gets Deleted

- `src/app/page.tsx` — current dashboard (replaced by Recipes home)
- `src/app/reconcile/` — entire chat-based flow
- `src/components/chat/` — all chat components (ChatMessage, MatchProposalCard as chat cards, etc.)
- `src/components/navigation.tsx` — top nav (replaced by sidebar)
- Chat-specific API: `POST /api/chat`, `/api/chat/sessions` routes
- `src/lib/chat-types.ts` — chat phase/message types

## What Gets Kept

- All `src/lib/` agent logic (`agent.ts`, `agent-tools.ts`) — repurposed for direct calls
- `src/lib/recipe-types.ts`, `session-store.ts`, `worker-client.ts`, `s3-client.ts`, `upload-client.ts`, `db.ts`, `api.ts`, `utils.ts`
- All existing API routes except chat: `/api/sources`, `/api/recipes`, `/api/runs`, `/api/uploads`, `/api/worker`
- All shadcn/ui primitives in `src/components/ui/`
- `src/components/providers.tsx`
- Tailwind + globals.css structure (updated colors)
- TanStack React Query setup

## What Gets Added

- Sidebar component
- Wizard stepper component
- Recipe card component
- Match card component (wizard version, not chat version)
- Run configuration form with dynamic parameter rendering
- Exceptions table + Manual Match modal
- Verdict-to-rule callout component
- New API routes for exceptions
- Recipe parameter schema type definitions
