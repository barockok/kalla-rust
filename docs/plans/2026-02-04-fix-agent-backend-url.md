# Fix Agent Backend URL for Docker Environment

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix the agent's inability to reach the Rust backend when the web container runs inside Docker, so that `list_sources`, `get_source_preview`, and other backend-calling tools work correctly.

**Architecture:** Add a server-side env var (`SERVER_API_URL`) that defaults to `http://localhost:3001` for local dev but can be overridden to `http://server:3001` in Docker. The `NEXT_PUBLIC_API_URL` stays unchanged for browser-side calls. Only one line changes in `agent-tools.ts`, and one line is added to `docker-compose.yml`.

**Tech Stack:** Next.js, Docker Compose

---

### Task 1: Add SERVER_API_URL to agent-tools.ts

The root cause is `agent-tools.ts:11` which uses `NEXT_PUBLIC_API_URL` for server-side fetch calls. Inside Docker, `localhost:3001` resolves to the web container itself, not the Rust backend.

**Files:**
- Modify: `/Users/barock/Library/Mobile Documents/com~apple~CloudDocs/Code/kalla/kalla-web/src/lib/agent-tools.ts:11`

**Step 1: Change the RUST_API constant**

Replace line 11:

```typescript
const RUST_API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3001';
```

with:

```typescript
const RUST_API = process.env.SERVER_API_URL || process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3001';
```

This adds a server-side-only env var (`SERVER_API_URL`) with higher priority. Local dev continues to work unchanged (falls through to `NEXT_PUBLIC_API_URL` or the hardcoded default). Docker sets `SERVER_API_URL` to the internal service hostname.

**Step 2: Commit**

```bash
git add kalla-web/src/lib/agent-tools.ts
git commit -m "fix: use SERVER_API_URL for server-side backend calls in agent tools"
```

---

### Task 2: Add SERVER_API_URL to docker-compose web service

**Files:**
- Modify: `/Users/barock/code/kalla/docker-compose.yml`

**Step 1: Add SERVER_API_URL to the web service environment**

In the `web` service's `environment` block, add:

```yaml
      SERVER_API_URL: http://server:3001
```

The full environment block should look like:

```yaml
    environment:
      NEXT_PUBLIC_API_URL: http://localhost:3001
      DATABASE_URL: postgres://${POSTGRES_USER:-kalla}:${POSTGRES_PASSWORD:-kalla_secret}@postgres:5432/${POSTGRES_DB:-kalla}
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
      ANTHROPIC_MODEL: ${ANTHROPIC_MODEL:-}
      SERVER_API_URL: http://server:3001
```

Note: `server` is the docker-compose service name — Docker's internal DNS resolves it to the correct container IP.

**Step 2: Commit**

```bash
git add docker-compose.yml
git commit -m "fix: set SERVER_API_URL for web container to reach backend via Docker network"
```

---

### Task 3: Verify the fix

**Step 1: Rebuild and restart the web container**

```bash
cd /Users/barock/code/kalla
docker compose up -d --build web
```

**Step 2: Test via the chat UI**

Open http://localhost:3000 and send "give me list of available data source". The agent should respond with the 4 registered sources (invoices, payments, invoices_csv, payments_csv) instead of "I'm having trouble connecting."

**Step 3: Run E2E tests**

```bash
cd /Users/barock/Library/Mobile Documents/com~apple~CloudDocs/Code/kalla/kalla-web
set -a && source .env && set +a && npx playwright test --reporter=list
```

Expected: All 6 tests pass. (E2E tests run the Next.js dev server locally where `localhost:3001` works, so they should be unaffected.)
