# Fix Transcript Issues Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix the bugs causing the agentic chat to fail when a user asks to reconcile CSV sources — CSV source previews return errors, and the wrong source aliases are stored on the session.

**Architecture:** Two independent bugs need fixing. (1) The Rust backend does not re-register CSV/Parquet file sources with the DataFusion SessionContext on startup — only PostgreSQL sources have lazy registration. (2) The Next.js chat route hardcodes `invoices` and `payments` as alias names during intent detection, ignoring when the user explicitly says "invoices csv" / "payments csv" (the actual aliases are `invoices_csv` / `payments_csv`).

**Tech Stack:** Rust/Axum (kalla-server), TypeScript/Next.js (kalla-web), DataFusion, PostgreSQL

---

## Bug Analysis (from transcript.txt)

**Turn 1 (user):** "Hello, I want to reconcile some data."
**Turn 1 (agent):** Lists 4 sources correctly (invoices, payments, invoices_csv, payments_csv). Phase transitions greeting→intent. All good.

**Turn 2 (user):** "reconcile the invoices csv and payment csv"
**Turn 2 (agent):** Tries `get_source_preview("invoices_csv")` — fails. Tries `list_sources` — works. Tries `get_source_preview` again — fails. Falls back to asking filtering questions.

**Root cause 1 — CSV sources not registered with DataFusion after server restart:**
- `register_source()` (main.rs:629-690) registers CSV files with DataFusion **and** stores them in the DB.
- On server startup (main.rs:372-381), sources are loaded from DB into `state.sources`, but **NOT re-registered** with the DataFusion SessionContext.
- `register_source_with_engine()` (main.rs:99-147) only handles PostgreSQL — line 120 explicitly rejects non-postgres sources.
- Result: `get_source_preview` calls `engine.context().table(&alias)` which fails with "Source not found" because the CSV table was never registered in the current SessionContext.

**Root cause 2 — Intent detection sets wrong aliases:**
- route.ts:51-64 does naive word matching: `words.includes('invoices')` → sets alias to `'invoices'` (the DB table, not the CSV).
- User said "invoices csv" — the word "invoices" matches, so alias is set to `invoices` instead of `invoices_csv`.
- Even if CSV registration is fixed, the agent would query the wrong source.

---

### Task 1: Fix CSV/Parquet source re-registration on startup

**Files:**
- Modify: `kalla-server/src/main.rs:99-147` (extend `register_source_with_engine`)
- Modify: `kalla-server/src/main.rs:360-416` (add startup registration loop)
- Test: `kalla-server/tests/reconciliation_test.rs`

**Step 1: Write the failing test**

Add a test to `kalla-server/tests/reconciliation_test.rs` that confirms CSV source preview works:

```rust
#[tokio::test]
async fn test_csv_source_preview() {
    let client = reqwest::Client::new();
    let api_url = std::env::var("API_URL").unwrap_or_else(|_| "http://localhost:3001".to_string());

    // The init.sql seeds invoices_csv and payments_csv.
    // After a server restart they should still be queryable.
    let res = client
        .get(format!("{}/api/sources/invoices_csv/preview?limit=5", api_url))
        .send()
        .await
        .expect("request failed");

    assert_eq!(res.status(), 200, "CSV source preview should succeed");

    let body: serde_json::Value = res.json().await.unwrap();
    assert!(body["rows"].as_array().unwrap().len() > 0, "Should return rows");
    assert!(body["columns"].as_array().unwrap().len() > 0, "Should return columns");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --package kalla-server test_csv_source_preview -- --nocapture`
Expected: FAIL — 404 "Source not found" because CSV is not registered with DataFusion after startup.

**Step 3: Extend `register_source_with_engine` to handle CSV and Parquet**

In `kalla-server/src/main.rs`, modify the `register_source_with_engine` function. Replace the early-exit for non-postgres sources (lines 119-122) with file:// handling:

```rust
async fn register_source_with_engine(
    state: &Arc<AppState>,
    source_alias: &str,
) -> Result<(), String> {
    // Check if table is already registered
    {
        let engine = state.engine.read().await;
        if engine.context().table_exist(source_alias).map_err(|e| e.to_string())? {
            return Ok(());
        }
    }

    // Look up the source by alias
    let sources = state.sources.read().await;
    let source = sources
        .iter()
        .find(|s| s.alias == source_alias)
        .ok_or_else(|| format!("Source '{}' not found", source_alias))?;

    let uri = source.uri.clone();
    drop(sources);

    if uri.starts_with("postgres://") {
        let (conn_string, table_name) = parse_postgres_uri(&uri)?;
        let engine = state.engine.write().await;
        if engine.context().table_exist(source_alias).map_err(|e| e.to_string())? {
            return Ok(());
        }
        let connector = PostgresConnector::new(&conn_string)
            .await
            .map_err(|e| format!("Failed to connect to database: {}", e))?;
        connector
            .register_table(engine.context(), source_alias, &table_name, None)
            .await
            .map_err(|e| format!("Failed to register table: {}", e))?;
    } else if uri.starts_with("file://") {
        let path = uri.strip_prefix("file://").unwrap();
        let engine = state.engine.write().await;
        if engine.context().table_exist(source_alias).map_err(|e| e.to_string())? {
            return Ok(());
        }
        if path.ends_with(".csv") {
            engine
                .register_csv(source_alias, path)
                .await
                .map_err(|e| format!("Failed to register CSV: {}", e))?;
        } else if path.ends_with(".parquet") {
            engine
                .register_parquet(source_alias, path)
                .await
                .map_err(|e| format!("Failed to register parquet: {}", e))?;
        } else {
            return Err(format!("Unsupported file format for '{}'", source_alias));
        }
    } else {
        return Err(format!("Unsupported URI scheme for '{}'", source_alias));
    }

    Ok(())
}
```

**Step 4: Add lazy registration call in `get_source_preview`**

In `get_source_preview` (main.rs:498), add a call to `register_source_with_engine` before trying to access the table. Insert this before `let engine = state.engine.read().await;`:

```rust
async fn get_source_preview(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
    Query(params): Query<PreviewParams>,
) -> Result<Json<SourcePreviewResponse>, (StatusCode, String)> {
    use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};

    let limit = params.limit.unwrap_or(10).min(100);

    // Ensure the source is registered with the DataFusion engine (lazy registration)
    register_source_with_engine(&state, &alias)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("Source not found: {}", e)))?;

    let engine = state.engine.read().await;
    // ... rest unchanged
```

**Step 5: Run test to verify it passes**

Run: `cargo test --package kalla-server test_csv_source_preview -- --nocapture`
Expected: PASS

**Step 6: Commit**

```bash
git add kalla-server/src/main.rs kalla-server/tests/reconciliation_test.rs
git commit -m "fix: re-register CSV/Parquet sources with DataFusion on demand"
```

---

### Task 2: Fix intent detection to use correct source aliases

**Files:**
- Modify: `kalla-web/src/app/api/chat/route.ts:50-64`
- Test: `kalla-web/e2e/scenario-1-full-flow.spec.ts` (existing E2E covers this flow)

**Step 1: Write a failing unit test**

Create `kalla-web/src/__tests__/intent-detection.test.ts`:

```typescript
import { describe, it, expect } from 'vitest';
import { detectSourceAliases } from '@/lib/intent-detection';

describe('detectSourceAliases', () => {
  const availableSources = [
    { alias: 'invoices', source_type: 'postgres' },
    { alias: 'payments', source_type: 'postgres' },
    { alias: 'invoices_csv', source_type: 'csv' },
    { alias: 'payments_csv', source_type: 'csv' },
  ];

  it('detects CSV aliases when user says "invoices csv and payments csv"', () => {
    const result = detectSourceAliases('reconcile the invoices csv and payment csv', availableSources);
    expect(result.left).toBe('invoices_csv');
    expect(result.right).toBe('payments_csv');
  });

  it('detects DB aliases when user says "invoices and payments"', () => {
    const result = detectSourceAliases('reconcile invoices and payments', availableSources);
    expect(result.left).toBe('invoices');
    expect(result.right).toBe('payments');
  });

  it('returns null when no match', () => {
    const result = detectSourceAliases('hello world', availableSources);
    expect(result.left).toBeNull();
    expect(result.right).toBeNull();
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npx vitest run src/__tests__/intent-detection.test.ts`
Expected: FAIL — module `@/lib/intent-detection` does not exist.

**Step 3: Extract intent detection into its own module**

Create `kalla-web/src/lib/intent-detection.ts`:

```typescript
interface SourceStub {
  alias: string;
  source_type: string;
}

/**
 * Detect which source aliases the user is referring to from free-text input.
 * Matches the longest alias first so "invoices_csv" wins over "invoices".
 */
export function detectSourceAliases(
  text: string,
  availableSources: SourceStub[],
): { left: string | null; right: string | null } {
  const normalised = text.toLowerCase().replace(/_/g, ' ');

  // Sort aliases longest-first so "invoices_csv" matches before "invoices"
  const sorted = [...availableSources].sort(
    (a, b) => b.alias.length - a.alias.length,
  );

  const matched: string[] = [];
  for (const source of sorted) {
    const aliasWords = source.alias.replace(/_/g, ' ');
    if (normalised.includes(aliasWords) && !matched.includes(source.alias)) {
      matched.push(source.alias);
    }
  }

  return {
    left: matched[0] ?? null,
    right: matched[1] ?? null,
  };
}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npx vitest run src/__tests__/intent-detection.test.ts`
Expected: PASS

**Step 5: Commit**

```bash
git add kalla-web/src/lib/intent-detection.ts kalla-web/src/__tests__/intent-detection.test.ts
git commit -m "feat: add intent detection module with longest-alias-first matching"
```

---

### Task 3: Wire intent detection into the chat route

**Files:**
- Modify: `kalla-web/src/app/api/chat/route.ts:50-64`
- Modify: `kalla-web/src/lib/agent-tools.ts` (import listSources for alias lookup)

**Step 1: Write the failing test**

Add a test case to `kalla-web/src/__tests__/intent-detection.test.ts`:

```typescript
it('detects "payment csv" as "payments_csv" via partial match', () => {
  const result = detectSourceAliases('reconcile the invoices csv and payment csv', availableSources);
  expect(result.right).toBe('payments_csv');
});
```

Note: The user typed "payment csv" (singular) but the alias is "payments_csv". The current `normalised.includes(aliasWords)` approach won't match this. We need to handle this.

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npx vitest run src/__tests__/intent-detection.test.ts`
Expected: FAIL — "payment csv" does not include "payments csv".

**Step 3: Improve matching to handle singular/plural**

Update `kalla-web/src/lib/intent-detection.ts` to try stemmed matching:

```typescript
interface SourceStub {
  alias: string;
  source_type: string;
}

function normalise(text: string): string {
  return text.toLowerCase().replace(/_/g, ' ').trim();
}

/**
 * Detect which source aliases the user is referring to from free-text input.
 * Matches longest alias first. Handles singular/plural by also checking
 * with trailing 's' stripped from both alias and text tokens.
 */
export function detectSourceAliases(
  text: string,
  availableSources: SourceStub[],
): { left: string | null; right: string | null } {
  const normalised = normalise(text);

  // Sort aliases longest-first so "invoices_csv" matches before "invoices"
  const sorted = [...availableSources].sort(
    (a, b) => b.alias.length - a.alias.length,
  );

  const matched: string[] = [];
  let remaining = normalised;

  for (const source of sorted) {
    const aliasWords = normalise(source.alias);

    // Direct substring match
    if (remaining.includes(aliasWords)) {
      matched.push(source.alias);
      remaining = remaining.replace(aliasWords, '');
      continue;
    }

    // Try singularised alias: "payments csv" → "payment csv"
    const singularAlias = aliasWords.replace(/s\b/g, '');
    if (singularAlias !== aliasWords && remaining.includes(singularAlias)) {
      matched.push(source.alias);
      remaining = remaining.replace(singularAlias, '');
      continue;
    }
  }

  return {
    left: matched[0] ?? null,
    right: matched[1] ?? null,
  };
}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npx vitest run src/__tests__/intent-detection.test.ts`
Expected: PASS

**Step 5: Update chat route to use the new module**

Replace the hardcoded intent detection block in `kalla-web/src/app/api/chat/route.ts` (lines 50-64):

```typescript
import { detectSourceAliases } from '@/lib/intent-detection';
import { listSources } from '@/lib/agent-tools';

// ... inside POST handler, replace lines 50-64 with:

    // Detect intent from user message for phase transitions
    if (session.phase === 'intent' && !session.left_source_alias) {
      try {
        const sources = await listSources();
        const detected = detectSourceAliases(
          userText,
          sources.map((s) => ({ alias: s.alias, source_type: s.source_type })),
        );
        if (detected.left) {
          updateSession(session.id, { left_source_alias: detected.left });
        }
        if (detected.right) {
          updateSession(session.id, { right_source_alias: detected.right });
        }
        session = getSession(session.id)!;
        if (session.left_source_alias && session.right_source_alias) {
          updateSession(session.id, { phase: 'sampling' });
          session = getSession(session.id)!;
        }
      } catch {
        // If source listing fails, let the agent handle it via tool calls
      }
    }
```

**Step 6: Run tests to verify everything passes**

Run: `cd kalla-web && npx vitest run`
Expected: PASS

**Step 7: Commit**

```bash
git add kalla-web/src/app/api/chat/route.ts kalla-web/src/lib/intent-detection.ts kalla-web/src/__tests__/intent-detection.test.ts
git commit -m "fix: use dynamic alias detection so CSV sources are matched correctly"
```

---

### Task 4: Verify full flow end-to-end

**Files:**
- Read: `kalla-web/e2e/scenario-1-full-flow.spec.ts`

**Step 1: Start the full stack**

Run: `docker compose up -d`

**Step 2: Run the E2E test**

Run: `cd kalla-web && npx playwright test e2e/scenario-1-full-flow.spec.ts`
Expected: PASS — the chat should now successfully preview CSV sources and proceed through the flow.

**Step 3: Manual smoke test**

1. Open http://localhost:3000/reconcile
2. Type "Hello, I want to reconcile some data"
3. Verify agent lists all 4 sources
4. Type "reconcile the invoices csv and payment csv"
5. Verify agent successfully shows schema/preview for both CSV sources
6. Verify phase transitions to sampling

**Step 4: Commit (if any test adjustments needed)**

```bash
git add -A
git commit -m "test: verify CSV source flow in E2E"
```
