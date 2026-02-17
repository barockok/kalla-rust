# Complex Match Pattern Benchmarks — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add 1:N (split payments), M:1 (batch payments), and M:N (cross-match) benchmark scenarios with data generators, run each locally at 30K rows, and verify matched/unmatched counts against Postgres ground truth.

**Architecture:** Extend existing `benchmarks/datagen.py` with three new generator functions. Add `--pattern` flag to `seed_postgres.py`. Add new scenario JSON files. Add a verification script that seeds data, runs the engine, then compares results to direct Postgres queries.

**Tech Stack:** Python (data gen + seeding), Rust/kallad (engine), PostgreSQL (source data + ground truth), bash (orchestration)

---

### Task 1: Add `batch_ref` column to invoice schema

The M:1 batch pattern needs a `batch_ref` column on invoices. Add it to both the data generator and the Postgres DDL. Existing 1:1 scenarios set `batch_ref` to NULL so they're unaffected.

**Files:**
- Modify: `benchmarks/datagen.py:35-56` (`generate_invoices` function)
- Modify: `benchmarks/seed_postgres.py:16-19` (`INVOICE_COLUMNS`)
- Modify: `benchmarks/seed_postgres.py:27-39` (`CREATE_INVOICES` DDL)

**Step 1: Add `batch_ref` to `generate_invoices`**

In `benchmarks/datagen.py`, add `"batch_ref": None` to every invoice dict in `generate_invoices()`:

```python
# In generate_invoices(), inside the loop, add to the dict:
"batch_ref": None,
```

The field defaults to `None` (NULL in Postgres). Pattern-specific generators will set it later.

**Step 2: Add `batch_ref` to INVOICE_COLUMNS and DDL**

In `benchmarks/seed_postgres.py`:

Add `"batch_ref"` to the `INVOICE_COLUMNS` list:

```python
INVOICE_COLUMNS = [
    "invoice_id", "customer_id", "customer_name", "invoice_date",
    "due_date", "amount", "currency", "status", "description",
    "batch_ref",
]
```

Add `batch_ref TEXT` to `CREATE_INVOICES`:

```sql
CREATE TABLE bench_invoices (
    invoice_id    TEXT PRIMARY KEY,
    customer_id   TEXT,
    customer_name TEXT,
    invoice_date  TEXT,
    due_date      TEXT,
    amount        DOUBLE PRECISION,
    currency      TEXT,
    status        TEXT,
    description   TEXT,
    batch_ref     TEXT
);
```

**Step 3: Verify existing 1:1 seeding still works**

Run:
```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla
python3 benchmarks/seed_postgres.py --rows 1000 --pg-url postgresql://kalla:kalla_secret@localhost:5432/kalla
```

Expected: `Seeded 1000 invoices` + `Seeded 900 payments` (75% match rate). No errors. The `batch_ref` column should be NULL for all rows.

Verify:
```sql
SELECT batch_ref, COUNT(*) FROM bench_invoices GROUP BY batch_ref;
-- Should show: NULL | 1000
```

**Step 4: Commit**

```bash
git add benchmarks/datagen.py benchmarks/seed_postgres.py
git commit -m "bench: add batch_ref column to invoice schema for M:1 pattern"
```

---

### Task 2: Implement `generate_split_payments` (1:N pattern)

Add a new function to `datagen.py` that generates 2–5 partial payments per invoice. The payments' amounts sum to the invoice amount. Each payment's `reference_number` = the invoice's `invoice_id`.

**Files:**
- Modify: `benchmarks/datagen.py` (add new function after `generate_payments`)

**Step 1: Write `generate_split_payments`**

Add this function to `benchmarks/datagen.py` after the existing `generate_payments` function:

```python
def generate_split_payments(n: int, invoices: list[dict],
                            pay_offset: int = 0) -> list[dict]:
    """Generate 1:N split payments — each invoice gets 2-5 partial payments.

    Every invoice is matched. Payment amounts sum to the invoice amount.
    10% of invoices also get orphan right-side payments.
    """
    payments: list[dict] = []
    pay_idx = pay_offset + 1

    for inv in invoices[:n]:
        num_parts = random.randint(2, 5)
        # Split amount into num_parts random portions that sum to inv["amount"]
        cuts = sorted(random.sample(range(1, 10000), num_parts - 1))
        cuts = [0] + cuts + [10000]
        fractions = [(cuts[j + 1] - cuts[j]) / 10000.0 for j in range(num_parts)]
        amounts = [round(inv["amount"] * f, 2) for f in fractions]
        # Fix rounding: adjust last part so sum is exact
        amounts[-1] = round(inv["amount"] - sum(amounts[:-1]), 2)

        for k, amt in enumerate(amounts):
            payments.append({
                "payment_id": _pad_id("PAY", pay_idx),
                "payer_id": inv["customer_id"],
                "payer_name": inv["customer_name"],
                "payment_date": _random_date(2024),
                "paid_amount": amt,
                "currency": inv["currency"],
                "payment_method": PAYMENT_METHODS[k % len(PAYMENT_METHODS)],
                "reference_number": inv["invoice_id"],
                "bank_reference": f"BR-PAY-{pay_idx:06d}",
                "notes": f"Split {k + 1}/{num_parts} for {inv['invoice_id']}",
            })
            pay_idx += 1

    # 10% right-side orphans
    orphan_count = int(n * 0.10)
    for i in range(orphan_count):
        payments.append({
            "payment_id": _pad_id("PAY", pay_idx),
            "payer_id": _pad_id("CUST", 300 + i),
            "payer_name": f"Orphan Payer {i + 1}",
            "payment_date": _random_date(2024),
            "paid_amount": _random_amount(),
            "currency": "USD",
            "payment_method": PAYMENT_METHODS[i % len(PAYMENT_METHODS)],
            "reference_number": f"ORPHAN-SPLIT-{i + 1}",
            "bank_reference": f"BR-PAY-{pay_idx:06d}",
            "notes": f"Orphan split payment {i + 1}",
        })
        pay_idx += 1

    return payments
```

**Step 2: Quick smoke test in Python REPL**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/benchmarks
python3 -c "
from datagen import generate_invoices, generate_split_payments
invs = generate_invoices(10)
pays = generate_split_payments(10, invs)
print(f'Invoices: {len(invs)}, Payments: {len(pays)}')
# Verify: each invoice_id appears 2-5 times in payments
from collections import Counter
refs = Counter(p['reference_number'] for p in pays if not p['reference_number'].startswith('ORPHAN'))
print(f'Ref counts: {dict(refs)}')
# Verify amounts sum correctly for first invoice
inv0 = invs[0]
parts = [p['paid_amount'] for p in pays if p['reference_number'] == inv0['invoice_id']]
print(f'Invoice amount: {inv0[\"amount\"]}, Parts sum: {sum(parts)}, Match: {abs(inv0[\"amount\"] - sum(parts)) < 0.01}')
"
```

Expected: 10 invoices, ~35 payments (avg 3.5 per invoice + 1 orphan). Each invoice_id appears 2-5 times. Amounts sum correctly.

**Step 3: Commit**

```bash
git add benchmarks/datagen.py
git commit -m "bench: add generate_split_payments for 1:N pattern"
```

---

### Task 3: Implement `generate_batch_payments` (M:1 pattern)

Add a function that groups invoices into batches of 10–50, assigns each group a `batch_ref`, and generates one bulk payment per batch whose amount = sum of the group's invoice amounts.

**Files:**
- Modify: `benchmarks/datagen.py` (add new function)

**Step 1: Write `generate_batch_payments`**

Add after `generate_split_payments`:

```python
def generate_batch_invoices(n: int, offset: int = 0,
                            batch_size_range: tuple[int, int] = (10, 50)) -> list[dict]:
    """Generate invoices with batch_ref assigned for M:1 pattern.

    Invoices are grouped into batches. Each batch shares a batch_ref.
    Returns invoices with batch_ref populated.
    """
    invoices = generate_invoices(n, offset=offset)
    batch_idx = offset // batch_size_range[1] + 1
    i = 0
    while i < len(invoices):
        batch_size = random.randint(*batch_size_range)
        batch_ref = f"BATCH-{batch_idx:06d}"
        for j in range(i, min(i + batch_size, len(invoices))):
            invoices[j]["batch_ref"] = batch_ref
        i += batch_size
        batch_idx += 1
    return invoices


def generate_batch_payments(n: int, invoices: list[dict],
                            pay_offset: int = 0) -> list[dict]:
    """Generate M:1 batch payments — one payment per batch of invoices.

    Each payment's reference_number = the batch_ref.
    Payment amount = sum of all invoice amounts in that batch.
    10% extra orphan payments added.
    """
    from itertools import groupby
    from operator import itemgetter

    payments: list[dict] = []
    pay_idx = pay_offset + 1

    # Group invoices by batch_ref
    sorted_invs = sorted(invoices, key=itemgetter("batch_ref"))
    for batch_ref, group in groupby(sorted_invs, key=itemgetter("batch_ref")):
        if batch_ref is None:
            continue
        group_list = list(group)
        total_amount = round(sum(inv["amount"] for inv in group_list), 2)
        first_inv = group_list[0]

        payments.append({
            "payment_id": _pad_id("PAY", pay_idx),
            "payer_id": first_inv["customer_id"],
            "payer_name": first_inv["customer_name"],
            "payment_date": _random_date(2024),
            "paid_amount": total_amount,
            "currency": first_inv["currency"],
            "payment_method": PAYMENT_METHODS[pay_idx % len(PAYMENT_METHODS)],
            "reference_number": batch_ref,
            "bank_reference": f"BR-PAY-{pay_idx:06d}",
            "notes": f"Batch payment for {len(group_list)} invoices",
        })
        pay_idx += 1

    # 10% right-side orphans
    orphan_count = max(1, int(len(payments) * 0.10))
    for i in range(orphan_count):
        payments.append({
            "payment_id": _pad_id("PAY", pay_idx),
            "payer_id": _pad_id("CUST", 400 + i),
            "payer_name": f"Orphan Batch Payer {i + 1}",
            "payment_date": _random_date(2024),
            "paid_amount": _random_amount(),
            "currency": "USD",
            "payment_method": PAYMENT_METHODS[i % len(PAYMENT_METHODS)],
            "reference_number": f"ORPHAN-BATCH-{i + 1}",
            "bank_reference": f"BR-PAY-{pay_idx:06d}",
            "notes": f"Orphan batch payment {i + 1}",
        })
        pay_idx += 1

    return payments
```

**Step 2: Smoke test**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/benchmarks
python3 -c "
from datagen import generate_batch_invoices, generate_batch_payments
invs = generate_batch_invoices(100)
pays = generate_batch_payments(100, invs)
print(f'Invoices: {len(invs)}, Payments: {len(pays)}')
# Count batches
batches = set(inv['batch_ref'] for inv in invs if inv['batch_ref'])
print(f'Unique batches: {len(batches)}')
# Non-orphan payments should equal number of batches
non_orphan = [p for p in pays if not p['reference_number'].startswith('ORPHAN')]
print(f'Non-orphan payments: {len(non_orphan)}, Matches batches: {len(non_orphan) == len(batches)}')
"
```

Expected: 100 invoices, ~5 payments (100/avg(10,50) ≈ 3-4 batches + orphans). Non-orphan payments count equals unique batch count.

**Step 3: Commit**

```bash
git add benchmarks/datagen.py
git commit -m "bench: add generate_batch_invoices/payments for M:1 pattern"
```

---

### Task 4: Implement `generate_cross_match_payments` (M:N pattern)

Add a function that generates payments independently (not derived from invoices), but sharing the same `customer_id` pool and date distribution so that joins on `customer_id + currency + month` produce M:N matches.

**Files:**
- Modify: `benchmarks/datagen.py` (add new function)

**Step 1: Write `generate_cross_match_payments`**

Add after `generate_batch_payments`:

```python
def generate_cross_match_payments(n: int, pay_offset: int = 0) -> list[dict]:
    """Generate M:N cross-match payments — independent of invoices.

    Payments share the same customer_id pool (CUST-000001..CUST-000020) and
    date range (2024) as invoices, so joining on customer_id + currency + month
    produces a cross-product within each (customer, currency, month) group.

    80% of payments use the shared customer pool (will match).
    20% use unique customer IDs (orphans, won't match).
    """
    payments: list[dict] = []
    pay_idx = pay_offset + 1
    match_count = int(n * 0.80)

    for i in range(match_count):
        # Use same customer_id distribution as generate_invoices
        cust_id = _pad_id("CUST", (i % 20) + 1)
        payments.append({
            "payment_id": _pad_id("PAY", pay_idx),
            "payer_id": cust_id,
            "payer_name": CUSTOMER_NAMES[i % len(CUSTOMER_NAMES)],
            "payment_date": _random_date(2024),
            "paid_amount": _random_amount(),
            "currency": CURRENCIES[i % len(CURRENCIES)],
            "payment_method": PAYMENT_METHODS[i % len(PAYMENT_METHODS)],
            "reference_number": f"XREF-{pay_idx:06d}",
            "bank_reference": f"BR-PAY-{pay_idx:06d}",
            "notes": f"Cross-match payment {i + 1}",
        })
        pay_idx += 1

    # 20% orphans — unique customer IDs that don't appear in invoices
    orphan_count = n - match_count
    for i in range(orphan_count):
        cust_id = _pad_id("CUST", 500 + i)
        payments.append({
            "payment_id": _pad_id("PAY", pay_idx),
            "payer_id": cust_id,
            "payer_name": f"Orphan Cross Payer {i + 1}",
            "payment_date": _random_date(2024),
            "paid_amount": _random_amount(),
            "currency": CURRENCIES[i % len(CURRENCIES)],
            "payment_method": PAYMENT_METHODS[i % len(PAYMENT_METHODS)],
            "reference_number": f"ORPHAN-CROSS-{i + 1}",
            "bank_reference": f"BR-PAY-{pay_idx:06d}",
            "notes": f"Orphan cross payment {i + 1}",
        })
        pay_idx += 1

    return payments
```

**Step 2: Smoke test**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla/benchmarks
python3 -c "
from datagen import generate_invoices, generate_cross_match_payments
invs = generate_invoices(100)
pays = generate_cross_match_payments(100)
print(f'Invoices: {len(invs)}, Payments: {len(pays)}')
# Check shared customer pool
inv_custs = set(inv['customer_id'] for inv in invs)
pay_custs = set(p['payer_id'] for p in pays if not p['reference_number'].startswith('ORPHAN'))
overlap = inv_custs & pay_custs
print(f'Shared customers: {len(overlap)} (should be 20)')
"
```

Expected: 100 invoices, 100 payments. 20 shared customer IDs.

**Step 3: Commit**

```bash
git add benchmarks/datagen.py
git commit -m "bench: add generate_cross_match_payments for M:N pattern"
```

---

### Task 5: Add `--pattern` flag to `seed_postgres.py`

Extend the seeder CLI to accept `--pattern {one_to_one,split,batch,cross}` and dispatch to the correct generator. Each pattern creates the same `bench_invoices` / `bench_payments` tables but with different data shapes.

**Files:**
- Modify: `benchmarks/seed_postgres.py`

**Step 1: Add pattern argument and dispatch logic**

Replace the `main()` and `seed_chunked()` functions in `seed_postgres.py`. The key changes:

1. Add `--pattern` CLI argument (default: `one_to_one`)
2. Import the new generator functions
3. In `seed_chunked`, dispatch to the correct generator based on pattern
4. For `batch` pattern, use `generate_batch_invoices` instead of `generate_invoices`

Update the import at the top:

```python
from datagen import (
    generate_invoices, generate_payments,
    generate_split_payments,
    generate_batch_invoices, generate_batch_payments,
    generate_cross_match_payments,
)
```

Replace `seed_chunked` with a version that accepts `pattern`:

```python
def seed_chunked(conn, total_rows: int, match_rate: float, pattern: str):
    """Seed bench data in chunks to limit memory usage."""
    with conn.cursor() as cur:
        cur.execute(CREATE_INVOICES)
        cur.execute(CREATE_PAYMENTS)
        conn.commit()

    total_invoices = 0
    total_payments = 0
    pay_offset = 0
    orphan_offset = 0

    for chunk_start in range(0, total_rows, CHUNK_SIZE):
        chunk_size = min(CHUNK_SIZE, total_rows - chunk_start)

        if pattern == "batch":
            invoices = generate_batch_invoices(chunk_size, offset=chunk_start)
        else:
            invoices = generate_invoices(chunk_size, offset=chunk_start)

        if pattern == "split":
            payments = generate_split_payments(
                chunk_size, invoices, pay_offset=pay_offset,
            )
        elif pattern == "batch":
            payments = generate_batch_payments(
                chunk_size, invoices, pay_offset=pay_offset,
            )
        elif pattern == "cross":
            payments = generate_cross_match_payments(
                chunk_size, pay_offset=pay_offset,
            )
        else:  # one_to_one (default)
            payments = generate_payments(
                chunk_size, invoices, match_rate=match_rate,
                pay_offset=pay_offset, orphan_offset=orphan_offset,
            )

        with conn.cursor() as cur:
            inv_buf = _rows_to_tsv(invoices, INVOICE_COLUMNS)
            cur.copy_from(inv_buf, "bench_invoices", columns=INVOICE_COLUMNS)

            pay_buf = _rows_to_tsv(payments, PAYMENT_COLUMNS)
            cur.copy_from(pay_buf, "bench_payments", columns=PAYMENT_COLUMNS)

            conn.commit()

        total_invoices += len(invoices)
        pay_offset += len(payments)
        orphan_offset += int(chunk_size * 0.10) if match_rate == 0.75 else 0
        total_payments += len(payments)
        del invoices, payments

    return total_invoices, total_payments
```

Update `main()`:

```python
def main():
    parser = argparse.ArgumentParser(description="Seed Postgres with benchmark data")
    parser.add_argument("--rows", type=int, required=True, help="Number of invoice rows")
    parser.add_argument("--pg-url", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--match-rate", type=float, default=0.75, help="Match rate 0.0-1.0 (default 0.75)")
    parser.add_argument("--pattern", choices=["one_to_one", "split", "batch", "cross"],
                        default="one_to_one", help="Match pattern (default: one_to_one)")
    args = parser.parse_args()

    conn = psycopg2.connect(args.pg_url)
    try:
        total_inv, total_pay = seed_chunked(conn, args.rows, args.match_rate, args.pattern)
        print(f"Seeded {total_inv} invoices into bench_invoices")
        print(f"Seeded {total_pay} payments into bench_payments")
    finally:
        conn.close()
```

**Step 2: Test each pattern seeds correctly**

```bash
PG_URL="postgresql://kalla:kalla_secret@localhost:5432/kalla"

# one_to_one (default — should work same as before)
python3 benchmarks/seed_postgres.py --rows 1000 --pg-url "$PG_URL" --pattern one_to_one

# split
python3 benchmarks/seed_postgres.py --rows 1000 --pg-url "$PG_URL" --pattern split

# batch
python3 benchmarks/seed_postgres.py --rows 1000 --pg-url "$PG_URL" --pattern batch

# cross
python3 benchmarks/seed_postgres.py --rows 1000 --pg-url "$PG_URL" --pattern cross
```

Each should succeed without errors. Verify row counts make sense:
- `one_to_one`: ~900 payments (75% match rate with orphans)
- `split`: ~3500 payments (avg 3.5 per invoice) + 100 orphans
- `batch`: ~40 payments (1000/avg(30) batches) + orphans
- `cross`: 1000 payments (80% matchable + 20% orphan)

**Step 3: Commit**

```bash
git add benchmarks/seed_postgres.py
git commit -m "bench: add --pattern flag to seed_postgres.py for 1:N/M:1/M:N"
```

---

### Task 6: Create scenario JSON files for each pattern

Add scenario files for local (single-node) and cluster benchmarks at 30K rows for verification, plus 1M for cluster-scale runs.

**Files:**
- Create: `benchmarks/scenarios/split_payments_30k.json`
- Create: `benchmarks/scenarios/batch_payments_30k.json`
- Create: `benchmarks/scenarios/cross_match_30k.json`
- Create: `benchmarks/scenarios/split_payments_1m.json`
- Create: `benchmarks/scenarios/batch_payments_1m.json`
- Create: `benchmarks/scenarios/cross_match_1m.json`

**Step 1: Create 30K verification scenarios**

`benchmarks/scenarios/split_payments_30k.json`:
```json
{
    "name": "split_payments_30k",
    "source_type": "postgres",
    "pattern": "split",
    "rows": 30000,
    "match_sql": "SELECT l.invoice_id, r.payment_id, l.amount AS invoice_amount, r.paid_amount AS payment_amount FROM left_src l JOIN right_src r ON l.invoice_id = r.reference_number"
}
```

`benchmarks/scenarios/batch_payments_30k.json`:
```json
{
    "name": "batch_payments_30k",
    "source_type": "postgres",
    "pattern": "batch",
    "rows": 30000,
    "match_sql": "SELECT l.invoice_id, r.payment_id, l.batch_ref, l.amount AS invoice_amount, r.paid_amount AS batch_total FROM left_src l JOIN right_src r ON l.batch_ref = r.reference_number"
}
```

`benchmarks/scenarios/cross_match_30k.json`:
```json
{
    "name": "cross_match_30k",
    "source_type": "postgres",
    "pattern": "cross",
    "rows": 30000,
    "match_sql": "SELECT l.invoice_id, r.payment_id, l.customer_id, l.amount AS invoice_amount, r.paid_amount AS payment_amount FROM left_src l JOIN right_src r ON l.customer_id = r.payer_id AND l.currency = r.currency AND SUBSTRING(l.invoice_date, 1, 7) = SUBSTRING(r.payment_date, 1, 7)"
}
```

**Step 2: Create 1M cluster scenarios**

Same SQL as above but with `"mode": "cluster"` and `"rows": 1000000`.

`benchmarks/scenarios/split_payments_1m.json`:
```json
{
    "name": "split_payments_1m",
    "mode": "cluster",
    "source_type": "postgres",
    "pattern": "split",
    "rows": 1000000,
    "match_sql": "SELECT l.invoice_id, r.payment_id, l.amount AS invoice_amount, r.paid_amount AS payment_amount FROM left_src l JOIN right_src r ON l.invoice_id = r.reference_number"
}
```

`benchmarks/scenarios/batch_payments_1m.json`:
```json
{
    "name": "batch_payments_1m",
    "mode": "cluster",
    "source_type": "postgres",
    "pattern": "batch",
    "rows": 1000000,
    "match_sql": "SELECT l.invoice_id, r.payment_id, l.batch_ref, l.amount AS invoice_amount, r.paid_amount AS batch_total FROM left_src l JOIN right_src r ON l.batch_ref = r.reference_number"
}
```

`benchmarks/scenarios/cross_match_1m.json`:
```json
{
    "name": "cross_match_1m",
    "mode": "cluster",
    "source_type": "postgres",
    "pattern": "cross",
    "rows": 1000000,
    "match_sql": "SELECT l.invoice_id, r.payment_id, l.customer_id, l.amount AS invoice_amount, r.paid_amount AS payment_amount FROM left_src l JOIN right_src r ON l.customer_id = r.payer_id AND l.currency = r.currency AND SUBSTRING(l.invoice_date, 1, 7) = SUBSTRING(r.payment_date, 1, 7)"
}
```

**Step 3: Commit**

```bash
git add benchmarks/scenarios/split_payments_30k.json benchmarks/scenarios/batch_payments_30k.json benchmarks/scenarios/cross_match_30k.json benchmarks/scenarios/split_payments_1m.json benchmarks/scenarios/batch_payments_1m.json benchmarks/scenarios/cross_match_1m.json
git commit -m "bench: add scenario files for 1:N, M:1, M:N patterns (30K + 1M)"
```

---

### Task 7: Update `inject_cluster_job.py` to support `--pattern`

The cluster job injector currently always seeds with the default 1:1 pattern. Add `--pattern` passthrough to `seed_data()`.

**Files:**
- Modify: `benchmarks/inject_cluster_job.py:131-141` (`seed_data` function)
- Modify: `benchmarks/inject_cluster_job.py:45-54` (`main` argument parsing)

**Step 1: Add `--pattern` argument**

In `main()`, add:
```python
parser.add_argument("--pattern", choices=["one_to_one", "split", "batch", "cross"],
                    default="one_to_one", help="Match pattern")
```

Pass it to `seed_data`:
```python
seed_data(args.pg_url, args.rows, args.pattern)
```

**Step 2: Update `seed_data` to pass pattern**

```python
def seed_data(pg_url, rows, pattern="one_to_one"):
    """Seed benchmark tables using the existing seed_postgres.py logic."""
    import subprocess
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    subprocess.run(
        ["python3", os.path.join(script_dir, "seed_postgres.py"),
         "--rows", str(rows), "--pg-url", pg_url, "--pattern", pattern],
        check=True,
    )
```

**Step 3: Update the `run_cluster_benchmark.sh` to pass pattern from scenario JSON**

In `run_cluster_benchmark.sh`, after extracting `MATCH_SQL`, add:
```bash
PATTERN=$(json_field "pattern" "$scenario_file")
PATTERN="${PATTERN:-one_to_one}"
```

And pass it to the injector:
```bash
INJECT_ARGS=(--rows "$ROWS" --pg-url "$PG_URL" --scheduler-url "http://localhost:8080" --match-sql "$MATCH_SQL" --timeout "$TIMEOUT_SECS" --json-output --pattern "$PATTERN")
```

**Step 4: Similarly update `run_benchmark.sh`**

After extracting `MATCH_SQL` and `MATCH_RATE`, add:
```bash
PATTERN=$(json_field "pattern" "$scenario_file")
PATTERN="${PATTERN:-one_to_one}"
```

Add `--pattern $PATTERN` to both the `seed_postgres.py` call (Postgres path) and `generate_data.py` call (CSV path, if applicable).

**Step 5: Commit**

```bash
git add benchmarks/inject_cluster_job.py benchmarks/run_cluster_benchmark.sh benchmarks/run_benchmark.sh
git commit -m "bench: pass --pattern through injector and benchmark scripts"
```

---

### Task 8: Create verification script `verify_ground_truth.py`

This is the most important task. Create a script that:
1. Seeds 30K rows for each pattern
2. Runs the engine (via HTTP job to local scheduler)
3. Runs the same match_sql directly against Postgres
4. Compares matched count, unmatched_left, unmatched_right
5. Reports PASS/FAIL for each and documents any bugs

**Files:**
- Create: `benchmarks/verify_ground_truth.py`

**Step 1: Write the verification script**

```python
#!/usr/bin/env python3
"""Verify engine results against Postgres ground truth for each match pattern.

Usage:
    python benchmarks/verify_ground_truth.py \
        --pg-url postgresql://kalla:kalla_secret@localhost:5432/kalla \
        --scheduler-url http://localhost:9090 \
        --rows 30000

Requires: kallad scheduler running locally (single mode).
"""

import argparse
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

import psycopg2

# --- Callback server to capture engine results ---

engine_result = {}
result_event = threading.Event()


class CallbackHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}

        if self.path.endswith("/complete"):
            engine_result.update(body)
            engine_result["status"] = "complete"
            result_event.set()
        elif self.path.endswith("/error"):
            engine_result.update(body)
            engine_result["status"] = "error"
            result_event.set()

        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


# --- Pattern definitions ---

PATTERNS = {
    "one_to_one": {
        "match_sql": (
            "SELECT i.invoice_id, p.payment_id, i.amount, p.paid_amount "
            "FROM left_src i JOIN right_src p "
            "ON i.invoice_id = p.reference_number "
            "AND tolerance_match(i.amount, p.paid_amount, 0.02)"
        ),
        "ground_truth_sql": (
            "SELECT COUNT(*) FROM bench_invoices i "
            "JOIN bench_payments p ON i.invoice_id = p.reference_number "
            "AND ABS(i.amount - p.paid_amount) <= 0.02 * GREATEST(ABS(i.amount), ABS(p.paid_amount))"
        ),
        "ground_truth_unmatched_left_sql": (
            "SELECT COUNT(*) FROM bench_invoices i "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM bench_payments p "
            "  WHERE i.invoice_id = p.reference_number "
            "  AND ABS(i.amount - p.paid_amount) <= 0.02 * GREATEST(ABS(i.amount), ABS(p.paid_amount)))"
        ),
        "ground_truth_unmatched_right_sql": (
            "SELECT COUNT(*) FROM bench_payments p "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM bench_invoices i "
            "  WHERE i.invoice_id = p.reference_number "
            "  AND ABS(i.amount - p.paid_amount) <= 0.02 * GREATEST(ABS(i.amount), ABS(p.paid_amount)))"
        ),
    },
    "split": {
        "match_sql": (
            "SELECT l.invoice_id, r.payment_id, l.amount AS invoice_amount, "
            "r.paid_amount AS payment_amount "
            "FROM left_src l JOIN right_src r ON l.invoice_id = r.reference_number"
        ),
        "ground_truth_sql": (
            "SELECT COUNT(*) FROM bench_invoices i "
            "JOIN bench_payments p ON i.invoice_id = p.reference_number"
        ),
        "ground_truth_unmatched_left_sql": (
            "SELECT COUNT(*) FROM bench_invoices i "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM bench_payments p "
            "  WHERE i.invoice_id = p.reference_number)"
        ),
        "ground_truth_unmatched_right_sql": (
            "SELECT COUNT(*) FROM bench_payments p "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM bench_invoices i "
            "  WHERE i.invoice_id = p.reference_number)"
        ),
    },
    "batch": {
        "match_sql": (
            "SELECT l.invoice_id, r.payment_id, l.batch_ref, "
            "l.amount AS invoice_amount, r.paid_amount AS batch_total "
            "FROM left_src l JOIN right_src r ON l.batch_ref = r.reference_number"
        ),
        "ground_truth_sql": (
            "SELECT COUNT(*) FROM bench_invoices i "
            "JOIN bench_payments p ON i.batch_ref = p.reference_number"
        ),
        "ground_truth_unmatched_left_sql": (
            "SELECT COUNT(*) FROM bench_invoices i "
            "WHERE i.batch_ref IS NULL OR NOT EXISTS ("
            "  SELECT 1 FROM bench_payments p "
            "  WHERE i.batch_ref = p.reference_number)"
        ),
        "ground_truth_unmatched_right_sql": (
            "SELECT COUNT(*) FROM bench_payments p "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM bench_invoices i "
            "  WHERE i.batch_ref = p.reference_number)"
        ),
    },
    "cross": {
        "match_sql": (
            "SELECT l.invoice_id, r.payment_id, l.customer_id, "
            "l.amount AS invoice_amount, r.paid_amount AS payment_amount "
            "FROM left_src l JOIN right_src r "
            "ON l.customer_id = r.payer_id AND l.currency = r.currency "
            "AND SUBSTRING(l.invoice_date, 1, 7) = SUBSTRING(r.payment_date, 1, 7)"
        ),
        "ground_truth_sql": (
            "SELECT COUNT(*) FROM bench_invoices i "
            "JOIN bench_payments p "
            "ON i.customer_id = p.payer_id AND i.currency = p.currency "
            "AND SUBSTRING(i.invoice_date, 1, 7) = SUBSTRING(p.payment_date, 1, 7)"
        ),
        "ground_truth_unmatched_left_sql": (
            "SELECT COUNT(*) FROM bench_invoices i "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM bench_payments p "
            "  WHERE i.customer_id = p.payer_id AND i.currency = p.currency "
            "  AND SUBSTRING(i.invoice_date, 1, 7) = SUBSTRING(p.payment_date, 1, 7))"
        ),
        "ground_truth_unmatched_right_sql": (
            "SELECT COUNT(*) FROM bench_payments p "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM bench_invoices i "
            "  WHERE i.customer_id = p.payer_id AND i.currency = p.currency "
            "  AND SUBSTRING(i.invoice_date, 1, 7) = SUBSTRING(p.payment_date, 1, 7))"
        ),
    },
}


def seed_data(pg_url, rows, pattern):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    subprocess.run(
        ["python3", os.path.join(script_dir, "seed_postgres.py"),
         "--rows", str(rows), "--pg-url", pg_url, "--pattern", pattern],
        check=True,
    )


def get_ground_truth(pg_url, pattern):
    """Run ground truth queries directly against Postgres."""
    p = PATTERNS[pattern]
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(p["ground_truth_sql"])
            matched = cur.fetchone()[0]

            cur.execute(p["ground_truth_unmatched_left_sql"])
            unmatched_left = cur.fetchone()[0]

            cur.execute(p["ground_truth_unmatched_right_sql"])
            unmatched_right = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM bench_invoices")
            total_left = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM bench_payments")
            total_right = cur.fetchone()[0]

        return {
            "matched": matched,
            "unmatched_left": unmatched_left,
            "unmatched_right": unmatched_right,
            "total_left": total_left,
            "total_right": total_right,
        }
    finally:
        conn.close()


def run_engine(scheduler_url, pg_url, match_sql, callback_port):
    """Submit job to the engine and wait for results."""
    global engine_result, result_event
    engine_result = {}
    result_event = threading.Event()

    server = HTTPServer(("0.0.0.0", callback_port), CallbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    import urllib.request
    callback_url = f"http://localhost:{callback_port}/api/worker"
    run_id = str(uuid.uuid4())
    pg = pg_url.replace("postgresql://", "postgres://", 1)

    job = {
        "run_id": run_id,
        "callback_url": callback_url,
        "match_sql": match_sql,
        "sources": [
            {"alias": "left_src", "uri": f"{pg}?table=bench_invoices"},
            {"alias": "right_src", "uri": f"{pg}?table=bench_payments"},
        ],
        "output_path": f"/tmp/verify-{run_id}",
        "primary_keys": {
            "left_src": ["invoice_id"],
            "right_src": ["payment_id"],
        },
    }

    req = urllib.request.Request(
        f"{scheduler_url}/api/jobs",
        data=json.dumps(job).encode(),
        headers={"Content-Type": "application/json"},
    )
    resp = urllib.request.urlopen(req)
    if resp.status not in (200, 202):
        return {"status": "error", "error": f"HTTP {resp.status}"}

    if result_event.wait(timeout=120):
        server.shutdown()
        return engine_result
    else:
        server.shutdown()
        return {"status": "timeout"}


def main():
    parser = argparse.ArgumentParser(description="Verify engine vs ground truth")
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--scheduler-url", default="http://localhost:9090")
    parser.add_argument("--rows", type=int, default=30000)
    parser.add_argument("--callback-port", type=int, default=9998)
    parser.add_argument("--patterns", nargs="*",
                        default=["one_to_one", "split", "batch", "cross"])
    args = parser.parse_args()

    results = []
    all_pass = True

    for pattern in args.patterns:
        print(f"\n{'='*60}")
        print(f"  Pattern: {pattern} ({args.rows} rows)")
        print(f"{'='*60}")

        # 1. Seed data
        print(f"  Seeding {pattern} data...")
        seed_data(args.pg_url, args.rows, pattern)

        # 2. Get ground truth from Postgres
        print(f"  Querying Postgres ground truth...")
        truth = get_ground_truth(args.pg_url, pattern)
        print(f"  Ground truth: matched={truth['matched']}, "
              f"unmatched_left={truth['unmatched_left']}, "
              f"unmatched_right={truth['unmatched_right']} "
              f"(total: {truth['total_left']}L / {truth['total_right']}R)")

        # 3. Run engine
        print(f"  Running engine...")
        engine = run_engine(
            args.scheduler_url, args.pg_url,
            PATTERNS[pattern]["match_sql"],
            args.callback_port,
        )

        if engine.get("status") != "complete":
            print(f"  ENGINE FAILED: {engine}")
            results.append({"pattern": pattern, "status": "ENGINE_ERROR", "detail": str(engine)})
            all_pass = False
            continue

        engine_matched = engine.get("matched_count", 0)
        engine_unmatched_left = engine.get("unmatched_left_count", 0)
        engine_unmatched_right = engine.get("unmatched_right_count", 0)

        print(f"  Engine:       matched={engine_matched}, "
              f"unmatched_left={engine_unmatched_left}, "
              f"unmatched_right={engine_unmatched_right}")

        # 4. Compare
        matched_ok = engine_matched == truth["matched"]
        left_ok = engine_unmatched_left == truth["unmatched_left"]
        right_ok = engine_unmatched_right == truth["unmatched_right"]

        status = "PASS" if (matched_ok and left_ok and right_ok) else "FAIL"
        detail = ""
        if not matched_ok:
            detail += f"matched: engine={engine_matched} vs truth={truth['matched']} (delta={engine_matched - truth['matched']}). "
        if not left_ok:
            detail += f"unmatched_left: engine={engine_unmatched_left} vs truth={truth['unmatched_left']} (delta={engine_unmatched_left - truth['unmatched_left']}). "
        if not right_ok:
            detail += f"unmatched_right: engine={engine_unmatched_right} vs truth={truth['unmatched_right']} (delta={engine_unmatched_right - truth['unmatched_right']}). "

        print(f"\n  >>> {status} {'— ' + detail if detail else ''}")
        results.append({
            "pattern": pattern,
            "status": status,
            "truth": truth,
            "engine": {
                "matched": engine_matched,
                "unmatched_left": engine_unmatched_left,
                "unmatched_right": engine_unmatched_right,
            },
            "detail": detail,
        })

        if status == "FAIL":
            all_pass = False

    # 5. Summary
    print(f"\n{'='*60}")
    print(f"  VERIFICATION SUMMARY")
    print(f"{'='*60}")
    for r in results:
        print(f"  {r['pattern']:15s}  {r['status']:6s}  {r.get('detail', '')}")

    print(f"\n  Overall: {'ALL PASS' if all_pass else 'FAILURES DETECTED'}")

    if not all_pass:
        # Write bug report
        report_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "results",
            f"ground-truth-bugs-{time.strftime('%Y%m%d-%H%M%S')}.md",
        )
        with open(report_path, "w") as f:
            f.write("# Ground Truth Verification — Bug Report\n\n")
            f.write(f"Date: {time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n")
            f.write(f"Rows: {args.rows}\n\n")
            for r in results:
                f.write(f"## {r['pattern']}: {r['status']}\n\n")
                if r.get("detail"):
                    f.write(f"**Issue:** {r['detail']}\n\n")
                if "truth" in r:
                    f.write(f"- Ground truth: matched={r['truth']['matched']}, "
                            f"unmatched_left={r['truth']['unmatched_left']}, "
                            f"unmatched_right={r['truth']['unmatched_right']}\n")
                    f.write(f"- Engine:       matched={r['engine']['matched']}, "
                            f"unmatched_left={r['engine']['unmatched_left']}, "
                            f"unmatched_right={r['engine']['unmatched_right']}\n\n")
        print(f"\n  Bug report saved to: {report_path}")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

**Step 2: Run the verification**

Prerequisites: Postgres running (`docker compose up -d`), scheduler running (`cargo run --bin kallad -- scheduler --http-port 9090`).

```bash
python3 benchmarks/verify_ground_truth.py \
    --pg-url postgresql://kalla:kalla_secret@localhost:5432/kalla \
    --scheduler-url http://localhost:9090 \
    --rows 30000
```

Expected output for each pattern: `PASS` or `FAIL` with delta details.

**Important note on `tolerance_match` ground truth:** The engine's `tolerance_match(a, b, threshold)` UDF computes `ABS(a - b) <= threshold`. But the ground truth SQL uses `ABS(i.amount - p.paid_amount) <= 0.02 * GREATEST(...)` (percentage-based). Verify the UDF implementation matches by checking `crates/kalla-core/src/lib.rs` — the ground truth SQL must use the same formula. If the UDF is `ABS(a-b) <= threshold` where threshold is a fixed number (0.02), the ground truth should be `ABS(i.amount - p.paid_amount) <= 0.02`. Adjust accordingly.

**Step 3: If any pattern FAILs, a bug report is auto-generated in `benchmarks/results/`.**

**Step 4: Commit**

```bash
git add benchmarks/verify_ground_truth.py
git commit -m "bench: add ground truth verification script for all match patterns"
```

---

### Task 9: Run verification and document findings

This is the hands-on verification task. Run the verification script, analyze results, and fix any discrepancies.

**Step 1: Ensure Docker services are running**

```bash
cd /Users/barock/Library/Mobile\ Documents/com~apple~CloudDocs/Code/kalla
docker compose up -d
```

**Step 2: Build and start the scheduler**

```bash
cargo build --release --bin kallad
./target/release/kallad scheduler --http-port 9090 &
```

Wait for health check:
```bash
curl -sf http://localhost:9090/health
```

**Step 3: Run verification at 30K rows**

```bash
python3 benchmarks/verify_ground_truth.py \
    --pg-url postgresql://kalla:kalla_secret@localhost:5432/kalla \
    --scheduler-url http://localhost:9090 \
    --rows 30000
```

**Step 4: Analyze results**

For each pattern, check:

1. **`one_to_one`**: Should match existing behavior. If FAIL, regression.
2. **`split` (1:N)**: `matched_count` should be the total number of matched (invoice, payment) pairs — since each invoice matches 2-5 payments, this will be ~3.5× the invoice count. `unmatched_left` = 0 (all invoices matched). `unmatched_right` = orphan count.
3. **`batch` (M:1)**: `matched_count` = total invoices with batch_ref (all of them). One payment per batch but each batch has 10-50 invoices, so matched = number of invoices. `unmatched_left` = 0. `unmatched_right` = orphan payments.
4. **`cross` (M:N)**: `matched_count` = cross-product size per (customer, currency, month) group. Will be larger than either source. `unmatched_left` = invoices with no matching payment in same (customer, currency, month). `unmatched_right` = payments with no matching invoice.

**Step 5: Known potential issues to watch for**

- **`tolerance_match` formula mismatch**: Verify that the ground truth SQL matches the UDF formula exactly. Read `crates/kalla-core/src/lib.rs` to confirm.
- **Unmatched counting for 1:N**: The engine uses `distinct_left_keys` from matched results. For split payments, each invoice appears 2-5 times in matched results. `distinct_left_keys` should still count it once. Verify.
- **Unmatched counting for M:1**: Each batch payment matches many invoices. `distinct_right_keys` should count each batch payment once. `unmatched_right` = total_payments - distinct_right_keys. Verify.
- **Cross-match explosion**: With 20 customers × 6 currencies × 12 months, the cross product could be large. Verify the engine handles it without OOM at 30K.

**Step 6: Document findings**

If all pass: commit the passing run report.
If any fail: the script auto-generates a bug report. Commit it with analysis.

```bash
git add benchmarks/results/
git commit -m "bench: verification results for complex match patterns at 30K rows"
```

---

## Summary

| Task | Description | Depends On |
|------|-------------|------------|
| 1 | Add `batch_ref` column to invoice schema | — |
| 2 | Implement `generate_split_payments` (1:N) | 1 |
| 3 | Implement `generate_batch_payments` (M:1) | 1 |
| 4 | Implement `generate_cross_match_payments` (M:N) | 1 |
| 5 | Add `--pattern` flag to `seed_postgres.py` | 2, 3, 4 |
| 6 | Create scenario JSON files | 5 |
| 7 | Update injector/runner scripts for `--pattern` | 5 |
| 8 | Create `verify_ground_truth.py` | 5 |
| 9 | Run verification, document findings | 6, 7, 8 |
