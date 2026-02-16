#!/usr/bin/env python3
"""CLI wrapper: seed Postgres with benchmark data.

Usage:
    python benchmarks/seed_postgres.py --rows 10000 --pg-url postgresql://user:pass@localhost/db
"""

import argparse
import io
import sys

import psycopg2

from datagen import generate_invoices, generate_payments

INVOICE_COLUMNS = [
    "invoice_id", "customer_id", "customer_name", "invoice_date",
    "due_date", "amount", "currency", "status", "description",
]

PAYMENT_COLUMNS = [
    "payment_id", "payer_id", "payer_name", "payment_date",
    "paid_amount", "currency", "payment_method", "reference_number",
    "bank_reference", "notes",
]

CREATE_INVOICES = """
DROP TABLE IF EXISTS bench_invoices;
CREATE TABLE bench_invoices (
    invoice_id    TEXT PRIMARY KEY,
    customer_id   TEXT,
    customer_name TEXT,
    invoice_date  TEXT,
    due_date      TEXT,
    amount        DOUBLE PRECISION,
    currency      TEXT,
    status        TEXT,
    description   TEXT
);
"""

CREATE_PAYMENTS = """
DROP TABLE IF EXISTS bench_payments;
CREATE TABLE bench_payments (
    payment_id       TEXT PRIMARY KEY,
    payer_id         TEXT,
    payer_name       TEXT,
    payment_date     TEXT,
    paid_amount      DOUBLE PRECISION,
    currency         TEXT,
    payment_method   TEXT,
    reference_number TEXT,
    bank_reference   TEXT,
    notes            TEXT
);
"""


def _rows_to_tsv(rows: list[dict], columns: list[str]) -> io.StringIO:
    buf = io.StringIO()
    for row in rows:
        line = "\t".join(str(row[c]) for c in columns)
        buf.write(line + "\n")
    buf.seek(0)
    return buf


def main():
    parser = argparse.ArgumentParser(description="Seed Postgres with benchmark data")
    parser.add_argument("--rows", type=int, required=True, help="Number of invoice rows")
    parser.add_argument("--pg-url", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--match-rate", type=float, default=0.75, help="Match rate 0.0-1.0 (default 0.75)")
    args = parser.parse_args()

    invoices = generate_invoices(args.rows)
    payments = generate_payments(args.rows, invoices, match_rate=args.match_rate)

    conn = psycopg2.connect(args.pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_INVOICES)
            cur.execute(CREATE_PAYMENTS)
            conn.commit()

            inv_buf = _rows_to_tsv(invoices, INVOICE_COLUMNS)
            cur.copy_from(inv_buf, "bench_invoices", columns=INVOICE_COLUMNS)

            pay_buf = _rows_to_tsv(payments, PAYMENT_COLUMNS)
            cur.copy_from(pay_buf, "bench_payments", columns=PAYMENT_COLUMNS)

            conn.commit()

        print(f"Seeded {len(invoices)} invoices into bench_invoices")
        print(f"Seeded {len(payments)} payments into bench_payments")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
