"""Shared data generation module for Kalla benchmarks.

Ported from scripts/generate-test-data.ts.
"""

import random
from itertools import groupby
from operator import itemgetter

CUSTOMER_NAMES = [
    "Acme Corp", "TechStart Inc", "Global Traders", "Innovate Solutions",
    "DataFlow Systems", "CloudNine Hosting", "Enterprise Holdings",
    "StartupXYZ", "MegaCorp Industries", "SmallBiz Co",
    "FutureTech Labs", "Euro Partners", "British Solutions",
    "QuickPay Corp", "Alpha Dynamics", "Beta Industries",
    "Gamma Solutions", "Delta Services", "Epsilon Tech", "Zeta Corp",
]

PAYMENT_METHODS = ["wire_transfer", "ach", "credit_card", "check"]
CURRENCIES = ["USD", "USD", "USD", "USD", "EUR", "GBP"]  # weighted toward USD


def _pad_id(prefix: str, n: int) -> str:
    return f"{prefix}-{n:06d}"


def _random_amount() -> float:
    return round(random.uniform(100, 50100), 2)


def _random_date(year: int) -> str:
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year}-{month:02d}-{day:02d}"


def generate_invoices(n: int, offset: int = 0) -> list[dict]:
    """Generate *n* invoice rows starting at *offset*.

    The returned list is later consumed by ``generate_payments`` which
    creates matching / non-matching payment rows according to the
    configured distribution.
    """
    invoices: list[dict] = []
    for i in range(offset + 1, offset + n + 1):
        cust_id = _pad_id("CUST", (i % 20) + 1)
        invoices.append({
            "invoice_id": _pad_id("INV", i),
            "customer_id": cust_id,
            "customer_name": CUSTOMER_NAMES[i % len(CUSTOMER_NAMES)],
            "invoice_date": _random_date(2024),
            "due_date": _random_date(2024),
            "amount": _random_amount(),
            "currency": CURRENCIES[i % len(CURRENCIES)],
            "status": "pending",
            "description": f"Service {i}",
            "batch_ref": None,
        })
    return invoices


def generate_payments(n: int, invoices: list[dict], match_rate: float = 0.75,
                      pay_offset: int = 0, orphan_offset: int = 0) -> list[dict]:
    """Generate payment rows for *invoices*.

    *match_rate* controls the fraction of invoices that get a matching payment
    (exact or tolerance).  The remainder are orphans.

    When match_rate == 1.0 every invoice gets a payment (no orphans).

    Default distribution (match_rate=0.75, relative to *n* = len(invoices)):
      - 60 % exact matches
      - 15 % tolerance matches (amount ±2 %)
      - 5 %  duplicates (two half-amount payments)
      - remainder left-side orphans (no payment generated)
      - 10 % extra right-side orphans (payment with no invoice)
    """
    if match_rate >= 1.0:
        exact_count = n
        tolerance_count = 0
        duplicate_count = 0
        orphan_right_count = 0
    elif match_rate != 0.75:
        # Custom match rate: all exact matches, no tolerance/duplicates
        exact_count = int(n * match_rate)
        tolerance_count = 0
        duplicate_count = 0
        orphan_right_count = 0
    else:
        exact_count = int(n * 0.60)
        tolerance_count = int(n * 0.15)
        duplicate_count = int(n * 0.05)
        orphan_right_count = int(n * 0.10)

    payments: list[dict] = []
    pay_idx = pay_offset + 1

    # --- Exact matches ---
    for i in range(exact_count):
        inv = invoices[i]
        payments.append({
            "payment_id": _pad_id("PAY", pay_idx),
            "payer_id": inv["customer_id"],
            "payer_name": inv["customer_name"],
            "payment_date": _random_date(2024),
            "paid_amount": inv["amount"],
            "currency": inv["currency"],
            "payment_method": PAYMENT_METHODS[i % len(PAYMENT_METHODS)],
            "reference_number": inv["invoice_id"],
            "bank_reference": f"BR-PAY-{pay_idx:06d}",
            "notes": f"Exact match for {inv['invoice_id']}",
        })
        pay_idx += 1

    # --- Tolerance matches (amount differs by up to 2 %) ---
    for i in range(tolerance_count):
        inv = invoices[exact_count + i]
        variance = 1 + random.uniform(-0.02, 0.02)
        paid_amount = round(inv["amount"] * variance, 2)
        payments.append({
            "payment_id": _pad_id("PAY", pay_idx),
            "payer_id": inv["customer_id"],
            "payer_name": inv["customer_name"],
            "payment_date": _random_date(2024),
            "paid_amount": paid_amount,
            "currency": inv["currency"],
            "payment_method": PAYMENT_METHODS[i % len(PAYMENT_METHODS)],
            "reference_number": inv["invoice_id"],
            "bank_reference": f"BR-PAY-{pay_idx:06d}",
            "notes": f"Tolerance match for {inv['invoice_id']}",
        })
        pay_idx += 1

    # --- Duplicate keys (two half-amount payments per invoice) ---
    for i in range(duplicate_count):
        inv = invoices[exact_count + tolerance_count + i]
        half = round(inv["amount"] / 2, 2)
        for j in range(2):
            payments.append({
                "payment_id": _pad_id("PAY", pay_idx),
                "payer_id": inv["customer_id"],
                "payer_name": inv["customer_name"],
                "payment_date": _random_date(2024),
                "paid_amount": half,
                "currency": "USD",
                "payment_method": "wire_transfer",
                "reference_number": f"{inv['invoice_id']}-PART{j + 1}",
                "bank_reference": f"BR-PAY-{pay_idx:06d}",
                "notes": f"Split payment {j + 1} for {inv['invoice_id']}",
            })
            pay_idx += 1

    # Left-side orphans: invoices beyond exact+tolerance+duplicate get no payment.

    # --- Right-side orphans (payments with no invoice) ---
    for i in range(orphan_right_count):
        cust_id = _pad_id("CUST", 200 + orphan_offset + i)
        payments.append({
            "payment_id": _pad_id("PAY", pay_idx),
            "payer_id": cust_id,
            "payer_name": f"Unknown Payer {orphan_offset + i + 1}",
            "payment_date": _random_date(2024),
            "paid_amount": _random_amount(),
            "currency": "USD",
            "payment_method": PAYMENT_METHODS[i % len(PAYMENT_METHODS)],
            "reference_number": f"UNKNOWN-{orphan_offset + i + 1}",
            "bank_reference": f"BR-PAY-{pay_idx:06d}",
            "notes": f"Orphan payment {orphan_offset + i + 1}",
        })
        pay_idx += 1

    return payments


def generate_batch_invoices(
    n: int,
    offset: int = 0,
    batch_size_range: tuple[int, int] = (10, 50),
) -> list[dict]:
    """Generate *n* invoices grouped into batches with a ``batch_ref``.

    Each batch contains a random number of invoices (drawn from
    *batch_size_range*) and every invoice in the batch shares the same
    ``batch_ref`` identifier (e.g. ``"BATCH-000001"``).
    """
    invoices = generate_invoices(n, offset)

    batch_num = 1
    idx = 0
    while idx < len(invoices):
        size = random.randint(*batch_size_range)
        ref = _pad_id("BATCH", batch_num)
        for inv in invoices[idx : idx + size]:
            inv["batch_ref"] = ref
        idx += size
        batch_num += 1

    return invoices


def generate_batch_payments(
    n: int,
    invoices: list[dict],
    pay_offset: int = 0,
) -> list[dict]:
    """Create one payment per batch plus ~10 % orphan payments.

    Invoices are grouped by ``batch_ref``.  Each batch produces a single
    payment whose ``paid_amount`` equals the sum of the invoice amounts in
    that batch and whose ``reference_number`` equals the ``batch_ref``.

    An additional ~10 % orphan payments (relative to *n*) are appended
    with ``reference_number`` starting with ``"ORPHAN-BATCH-"``.
    """
    payments: list[dict] = []
    pay_idx = pay_offset + 1

    # --- One payment per batch ---
    sorted_invs = sorted(invoices, key=itemgetter("batch_ref"))
    for batch_ref, group in groupby(sorted_invs, key=itemgetter("batch_ref")):
        if batch_ref is None:
            continue
        members = list(group)
        total = round(sum(inv["amount"] for inv in members), 2)
        representative = members[0]
        payments.append({
            "payment_id": _pad_id("PAY", pay_idx),
            "payer_id": representative["customer_id"],
            "payer_name": representative["customer_name"],
            "payment_date": _random_date(2024),
            "paid_amount": total,
            "currency": representative["currency"],
            "payment_method": PAYMENT_METHODS[pay_idx % len(PAYMENT_METHODS)],
            "reference_number": batch_ref,
            "bank_reference": f"BR-PAY-{pay_idx:06d}",
            "notes": f"Batch payment for {batch_ref} ({len(members)} invoices)",
        })
        pay_idx += 1

    # --- Orphan payments (~10 % of n) ---
    orphan_count = max(1, int(n * 0.10))
    for i in range(orphan_count):
        cust_id = _pad_id("CUST", 300 + i)
        payments.append({
            "payment_id": _pad_id("PAY", pay_idx),
            "payer_id": cust_id,
            "payer_name": f"Unknown Batch Payer {i + 1}",
            "payment_date": _random_date(2024),
            "paid_amount": _random_amount(),
            "currency": "USD",
            "payment_method": PAYMENT_METHODS[i % len(PAYMENT_METHODS)],
            "reference_number": f"ORPHAN-BATCH-{i + 1:06d}",
            "bank_reference": f"BR-PAY-{pay_idx:06d}",
            "notes": f"Orphan batch payment {i + 1}",
        })
        pay_idx += 1

    return payments
