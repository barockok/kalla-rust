#!/usr/bin/env python3
"""CLI wrapper: generate benchmark CSV files.

Usage:
    python benchmarks/generate_data.py --rows 10000 --output-dir /tmp/bench
"""

import argparse
import csv
import os
import sys

from datagen import generate_invoices, generate_payments


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark CSV data")
    parser.add_argument("--rows", type=int, required=True, help="Number of invoice rows")
    parser.add_argument("--output-dir", required=True, help="Directory to write CSVs into")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    invoices = generate_invoices(args.rows)
    payments = generate_payments(args.rows, invoices)

    inv_path = os.path.join(args.output_dir, "invoices.csv")
    pay_path = os.path.join(args.output_dir, "payments.csv")

    with open(inv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=invoices[0].keys())
        writer.writeheader()
        writer.writerows(invoices)

    with open(pay_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=payments[0].keys())
        writer.writeheader()
        writer.writerows(payments)

    print(f"Wrote {len(invoices)} invoices to {inv_path}")
    print(f"Wrote {len(payments)} payments to {pay_path}")


if __name__ == "__main__":
    main()
