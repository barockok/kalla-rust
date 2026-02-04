# Reliable Dev Setup — Idempotent DB Init on `docker compose up`

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Guarantee that `docker compose up` always leaves the database fully initialized — schema, indexes, seed data, and chat_sessions — regardless of whether the postgres volume already exists.

**Architecture:** PostgreSQL's `/docker-entrypoint-initdb.d/` only runs on first database creation (empty data directory). We'll add a second volume mount that runs **every** container start by using PostgreSQL's built-in support for scripts in a custom entrypoint wrapper, or more simply: a dedicated "ensure schema" script that docker-compose runs via a healthcheck-gated `server` init. The simplest reliable approach: split init.sql into an idempotent `ensure-schema.sql` and keep seed data conditional, then run it on every postgres container start via a custom command that calls `psql` after the server is ready.

**Simplest approach:** Use docker-compose `postgres` service with a custom command that starts postgres normally, then use the existing `server` service's entrypoint or a new one-shot `db-init` service that runs `psql -f /scripts/init.sql` against the healthy postgres. Since `init.sql` already uses `CREATE TABLE IF NOT EXISTS` and bare `INSERT` (no `IF NOT EXISTS` on inserts), we need to make the seed inserts idempotent too (`ON CONFLICT DO NOTHING`).

**Tech Stack:** Docker Compose, PostgreSQL 16, bash

---

### Task 1: Make seed data inserts idempotent

The current `init.sql` uses bare `INSERT INTO` for seed data. If the script runs a second time (volume already has data), these inserts will fail with duplicate key violations. Add `ON CONFLICT DO NOTHING` to all INSERT statements.

**Files:**
- Modify: `/Users/barock/code/kalla/scripts/init.sql`

**Step 1: Add ON CONFLICT DO NOTHING to invoices insert**

In `scripts/init.sql`, change line 142 (the closing of the invoices INSERT):

```sql
-- Before:
('INV-2024-015', 'CUST-014', 'QuickPay Corp', '2024-01-05', '2024-02-05', 6000.00, 'USD', 'paid', 'Express service');

-- After:
('INV-2024-015', 'CUST-014', 'QuickPay Corp', '2024-01-05', '2024-02-05', 6000.00, 'USD', 'paid', 'Express service')
ON CONFLICT DO NOTHING;
```

**Step 2: Add ON CONFLICT DO NOTHING to payments insert**

In `scripts/init.sql`, change line 172 (the closing of the payments INSERT):

```sql
-- Before:
('PAY-2024-014', 'CUST-014', 'Quick Pay Corporation', '2024-02-01', 6000.00, 'USD', 'ach', 'INV-2024-015', 'ACH-99887766', 'Early payment');

-- After:
('PAY-2024-014', 'CUST-014', 'Quick Pay Corporation', '2024-02-01', 6000.00, 'USD', 'ach', 'INV-2024-015', 'ACH-99887766', 'Early payment')
ON CONFLICT DO NOTHING;
```

**Step 3: Add ON CONFLICT DO NOTHING to sources insert**

In `scripts/init.sql`, change line 182 (the closing of the sources INSERT):

```sql
-- Before:
('payments_csv', 'file:///app/testdata/payments.csv', 'csv', 'connected');

-- After:
('payments_csv', 'file:///app/testdata/payments.csv', 'csv', 'connected')
ON CONFLICT DO NOTHING;
```

**Step 4: Add ON CONFLICT DO NOTHING to recipes insert**

In `scripts/init.sql`, change line 237 (the closing of the recipes INSERT):

```sql
-- Before:
}'::jsonb);

-- After:
}'::jsonb)
ON CONFLICT DO NOTHING;
```

**Step 5: Remove bare COMMIT at end of file**

Line 331 has a bare `COMMIT;` but there's no explicit `BEGIN`. PostgreSQL in autocommit mode doesn't need this, and it will produce a warning. Remove it.

**Step 6: Verify init.sql is fully idempotent**

Run the script twice against the running database to confirm no errors:

```bash
docker exec kalla-postgres psql -U kalla -d kalla -f /docker-entrypoint-initdb.d/init.sql
docker exec kalla-postgres psql -U kalla -d kalla -f /docker-entrypoint-initdb.d/init.sql
```

Expected: Both runs complete with no errors (second run hits `IF NOT EXISTS` / `ON CONFLICT DO NOTHING` for everything).

**Step 7: Commit**

```bash
git add scripts/init.sql
git commit -m "fix: make init.sql fully idempotent for re-runs"
```

---

### Task 2: Add a `db-init` service to docker-compose

Add a one-shot service that runs after postgres is healthy and executes `init.sql`. This guarantees schema + seed data exist on every `docker compose up`, even when the postgres volume already has data from a previous run.

**Files:**
- Modify: `/Users/barock/code/kalla/docker-compose.yml`

**Step 1: Add db-init service**

Add the following service to `docker-compose.yml` after the `postgres` service:

```yaml
  db-init:
    image: postgres:16-alpine
    container_name: kalla-db-init
    depends_on:
      postgres:
        condition: service_healthy
    volumes:
      - ./scripts/init.sql:/scripts/init.sql:ro
    environment:
      PGPASSWORD: ${POSTGRES_PASSWORD:-kalla_secret}
    entrypoint: >
      sh -c "psql -h postgres -U $${POSTGRES_USER:-kalla} -d $${POSTGRES_DB:-kalla} -f /scripts/init.sql && echo 'DB init complete'"
    restart: "no"
```

**Step 2: Make server depend on db-init**

Change the `server` service's `depends_on` to also wait for `db-init`:

```yaml
  server:
    depends_on:
      postgres:
        condition: service_healthy
      db-init:
        condition: service_completed_successfully
```

This ensures the server only starts after the database is fully initialized.

**Step 3: Verify with a fresh start**

```bash
# Stop everything and remove the postgres volume to simulate fresh setup
docker compose down
docker volume rm kalla_postgres_data

# Start everything
docker compose up -d

# Verify all tables exist
docker exec kalla-postgres psql -U kalla -d kalla -c "\dt"
```

Expected: All 8 tables listed (sources, recipes, runs, matched_records, unmatched_records, invoices, payments, chat_sessions).

**Step 4: Verify with existing volume (the key scenario)**

```bash
# Stop and restart without removing volumes
docker compose down
docker compose up -d

# Verify db-init ran successfully
docker compose logs db-init

# Verify tables still exist and data is intact
docker exec kalla-postgres psql -U kalla -d kalla -c "SELECT count(*) FROM invoices; SELECT count(*) FROM chat_sessions;"
```

Expected: db-init logs show "DB init complete", invoices count = 15, chat_sessions accessible (count >= 0).

**Step 5: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add db-init service to guarantee schema on every startup"
```

---

### Task 3: Run E2E tests to confirm nothing broke

**Files:** None (verification only)

**Step 1: Ensure services are running**

```bash
docker compose up -d
docker compose ps
```

Expected: postgres healthy, db-init exited (0), server running, web running (if built).

**Step 2: Run E2E tests**

```bash
cd /Users/barock/Library/Mobile Documents/com~apple~CloudDocs/Code/kalla/kalla-web
set -a && source .env && set +a && npx playwright test --reporter=list
```

Expected: All 6 tests pass.

**Step 3: Check postgres logs are clean**

```bash
docker compose logs postgres 2>&1 | grep -i error | tail -20
```

Expected: No `relation "chat_sessions" does not exist` errors.
