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
# IMPORTANT: tolerance_match UDF uses ABS(a - b) <= threshold (FIXED, not percentage)

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
            "AND ABS(i.amount - p.paid_amount) <= 0.02"
        ),
        "ground_truth_unmatched_left_sql": (
            "SELECT COUNT(*) FROM bench_invoices i "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM bench_payments p "
            "  WHERE i.invoice_id = p.reference_number "
            "  AND ABS(i.amount - p.paid_amount) <= 0.02)"
        ),
        "ground_truth_unmatched_right_sql": (
            "SELECT COUNT(*) FROM bench_payments p "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM bench_invoices i "
            "  WHERE i.invoice_id = p.reference_number "
            "  AND ABS(i.amount - p.paid_amount) <= 0.02)"
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
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
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
