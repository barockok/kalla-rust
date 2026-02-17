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
    "batch_ref",
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
    description   TEXT,
    batch_ref     TEXT
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
        line = "\t".join("\\N" if row[c] is None else str(row[c]) for c in columns)
        buf.write(line + "\n")
    buf.seek(0)
    return buf


CHUNK_SIZE = 500_000


def seed_chunked(conn, total_rows: int, match_rate: float):
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
        invoices = generate_invoices(chunk_size, offset=chunk_start)
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


def main():
    parser = argparse.ArgumentParser(description="Seed Postgres with benchmark data")
    parser.add_argument("--rows", type=int, required=True, help="Number of invoice rows")
    parser.add_argument("--pg-url", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--match-rate", type=float, default=0.75, help="Match rate 0.0-1.0 (default 0.75)")
    args = parser.parse_args()

    conn = psycopg2.connect(args.pg_url)
    try:
        total_inv, total_pay = seed_chunked(conn, args.rows, args.match_rate)
        print(f"Seeded {total_inv} invoices into bench_invoices")
        print(f"Seeded {total_pay} payments into bench_payments")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
