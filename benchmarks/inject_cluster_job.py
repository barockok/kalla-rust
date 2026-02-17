#!/usr/bin/env python3
"""Inject a benchmark job via HTTP POST to the scheduler."""

import argparse
import json
import sys
import time
import uuid
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# Callback state
result_data = {}
result_event = threading.Event()


class CallbackHandler(BaseHTTPRequestHandler):
    """Minimal HTTP server to receive worker callbacks."""

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}

        if self.path.endswith("/complete"):
            result_data["status"] = "complete"
            result_data.update(body)
            result_event.set()
        elif self.path.endswith("/error"):
            result_data["status"] = "error"
            result_data.update(body)
            result_event.set()
        elif self.path.endswith("/progress"):
            stage = body.get("stage", "")
            progress = body.get("progress", "")
            matched = body.get("matched_so_far", "")
            print(f"  Progress: stage={stage} progress={progress} matched={matched}", file=sys.stderr)

        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress default logging


def main():
    parser = argparse.ArgumentParser(description="Inject benchmark job via HTTP")
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--scheduler-url", default="http://localhost:8080")
    parser.add_argument("--match-sql", required=True)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--callback-port", type=int, default=9999)
    parser.add_argument("--json-output", action="store_true")
    parser.add_argument("--pattern", choices=["one_to_one", "split", "batch", "cross"],
                        default="one_to_one", help="Match pattern")
    args = parser.parse_args()

    # Seed benchmark data
    print(f"  Seeding {args.rows} rows to Postgres...", file=sys.stderr)
    seed_data(args.pg_url, args.rows, args.pattern)

    # Start callback server
    callback_server = HTTPServer(("0.0.0.0", args.callback_port), CallbackHandler)
    callback_thread = threading.Thread(target=callback_server.serve_forever, daemon=True)
    callback_thread.start()

    callback_url = f"http://localhost:{args.callback_port}/api/worker"
    run_id = str(uuid.uuid4())
    pg = args.pg_url.replace("postgresql://", "postgres://", 1)

    job = {
        "run_id": run_id,
        "callback_url": callback_url,
        "match_sql": args.match_sql,
        "sources": [
            {"alias": "left_src", "uri": f"{pg}?table=bench_invoices"},
            {"alias": "right_src", "uri": f"{pg}?table=bench_payments"},
        ],
        "output_path": f"/tmp/bench-output-{run_id}",
        "primary_keys": {
            "left_src": ["invoice_id"],
            "right_src": ["payment_id"],
        },
    }

    # POST job to scheduler
    import urllib.request
    req = urllib.request.Request(
        f"{args.scheduler_url}/api/jobs",
        data=json.dumps(job).encode(),
        headers={"Content-Type": "application/json"},
    )

    start = time.time()
    try:
        resp = urllib.request.urlopen(req)
        if resp.status not in (200, 202):
            print(json.dumps({"status": "error", "error": f"HTTP {resp.status}"}))
            sys.exit(1)
    except Exception as e:
        print(json.dumps({"status": "error", "error": str(e)}))
        sys.exit(1)

    print(f"  Job submitted (run_id={run_id}), waiting for callback...", file=sys.stderr)

    # Wait for callback
    if result_event.wait(timeout=args.timeout):
        elapsed = time.time() - start
        status = result_data.get("status", "unknown")
        matched = result_data.get("matched_count", 0)
        rows_per_sec = int(args.rows / elapsed) if elapsed > 0 else 0

        output = {
            "status": status,
            "elapsed_secs": f"{elapsed:.2f}",
            "rows_per_sec": rows_per_sec,
            "matched_count": matched,
            "unmatched_left_count": result_data.get("unmatched_left_count", 0),
            "unmatched_right_count": result_data.get("unmatched_right_count", 0),
        }
    else:
        elapsed = time.time() - start
        output = {"status": "timeout", "elapsed_secs": f"{elapsed:.2f}", "rows_per_sec": 0}

    callback_server.shutdown()

    if args.json_output:
        print(json.dumps(output))
    else:
        print(f"  Status: {output['status']} | Elapsed: {output['elapsed_secs']}s | Rows/sec: {output['rows_per_sec']}", file=sys.stderr)


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


if __name__ == "__main__":
    main()
