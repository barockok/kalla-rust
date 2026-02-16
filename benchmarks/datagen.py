"""Shared data generation module for Kalla benchmarks.

Ported from scripts/generate-test-data.ts.
"""

import random

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


def generate_invoices(n: int) -> list[dict]:
    """Generate *n* invoice rows.

    The returned list is later consumed by ``generate_payments`` which
    creates matching / non-matching payment rows according to the
    configured distribution.
    """
    invoices: list[dict] = []
    for i in range(1, n + 1):
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
        })
    return invoices


def generate_payments(n: int, invoices: list[dict]) -> list[dict]:
    """Generate payment rows for *invoices*.

    Distribution (relative to *n* = len(invoices)):
      - 60 % exact matches
      - 15 % tolerance matches (amount ±2 %)
      - 5 %  duplicates (two half-amount payments)
      - remainder left-side orphans (no payment generated)
      - 10 % extra right-side orphans (payment with no invoice)
    """
    exact_count = int(n * 0.60)
    tolerance_count = int(n * 0.15)
    duplicate_count = int(n * 0.05)
    orphan_right_count = int(n * 0.10)

    payments: list[dict] = []
    pay_idx = 1

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
        cust_id = _pad_id("CUST", 200 + i)
        payments.append({
            "payment_id": _pad_id("PAY", pay_idx),
            "payer_id": cust_id,
            "payer_name": f"Unknown Payer {i + 1}",
            "payment_date": _random_date(2024),
            "paid_amount": _random_amount(),
            "currency": "USD",
            "payment_method": PAYMENT_METHODS[i % len(PAYMENT_METHODS)],
            "reference_number": f"UNKNOWN-{i + 1}",
            "bank_reference": f"BR-PAY-{pay_idx:06d}",
            "notes": f"Orphan payment {i + 1}",
        })
        pay_idx += 1

    return payments
