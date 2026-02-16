#!/usr/bin/env python3
"""Inject scaled-mode benchmark jobs into Postgres + NATS.

Seeds bench data, inserts job rows, publishes StagePlan messages to NATS
JetStream, then waits for the worker to POST completion via HTTP callback.

Usage:
    python benchmarks/inject_scaled_job.py \
        --rows 100000 \
        --pg-url postgresql://kalla:kalla_secret@localhost:5432/kalla \
        --nats-url nats://localhost:4222 \
        --staging-bucket kalla-staging \
        --match-sql "SELECT i.*, p.* FROM left_src i JOIN right_src p ..."

Dependencies: pip install nats-py psycopg2-binary
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid

import nats
import psycopg2

# Allow importing datagen from the benchmarks directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from datagen import generate_invoices, generate_payments
from seed_postgres import (
    CREATE_INVOICES, CREATE_PAYMENTS,
    INVOICE_COLUMNS, PAYMENT_COLUMNS,
    _rows_to_tsv,
)

DEFAULT_MATCH_SQL = (
    "SELECT i.*, p.* FROM left_src i JOIN right_src p "
    "ON i.invoice_id = p.reference_number "
    "AND tolerance_match(i.amount, p.paid_amount, 0.02)"
)


def seed_data(pg_url: str, rows: int, match_rate: float):
    """Seed bench_invoices and bench_payments into Postgres."""
    invoices = generate_invoices(rows)
    payments = generate_payments(rows, invoices, match_rate=match_rate)

    conn = psycopg2.connect(pg_url)
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
        print(f"Seeded {len(invoices)} invoices, {len(payments)} payments")
    finally:
        conn.close()


def start_callback_listener(result_file: str) -> tuple[subprocess.Popen, int]:
    """Start the callback listener and return (process, port)."""
    listener_script = os.path.join(SCRIPT_DIR, "callback_listener.py")
    proc = subprocess.Popen(
        [sys.executable, listener_script, "--port", "0", "--output", result_file],
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        text=True,
    )
    # Read the port from stdout
    line = proc.stdout.readline().strip()
    if not line.startswith("CALLBACK_PORT="):
        proc.kill()
        raise RuntimeError(f"Failed to start callback listener, got: {line!r}")
    port = int(line.split("=", 1)[1])
    return proc, port


def insert_jobs(pg_url: str, run_id: str, staging_bucket: str,
                match_sql: str, source_pg_url: str,
                callback_url: str | None = None,
                direct_exec: bool = False):
    """Insert stage_plan + exec jobs into the jobs table.

    When *direct_exec* is True, skip StagePlan jobs and include source_uris
    in the Exec payload so the worker reads directly from the source.
    """
    conn = psycopg2.connect(pg_url)

    # Build source URIs (worker expects postgres:// not postgresql://)
    pg_source = source_pg_url.replace("postgresql://", "postgres://", 1)
    left_uri = f"{pg_source}?table=bench_invoices"
    right_uri = f"{pg_source}?table=bench_payments"

    left_job_id = str(uuid.uuid4())
    right_job_id = str(uuid.uuid4())
    exec_job_id = str(uuid.uuid4())

    # StagePlan payloads (only used when not direct_exec)
    left_stage = {
        "type": "StagePlan",
        "job_id": left_job_id,
        "run_id": run_id,
        "source_uri": left_uri,
        "source_alias": "left_src",
        "partition_key": None,
    }
    right_stage = {
        "type": "StagePlan",
        "job_id": right_job_id,
        "run_id": run_id,
        "source_uri": right_uri,
        "source_alias": "right_src",
        "partition_key": None,
    }

    # Exec payload — will be picked up after staging completes (or directly)
    exec_payload = {
        "type": "Exec",
        "job_id": exec_job_id,
        "run_id": run_id,
        "recipe_json": json.dumps({
            "match_sql": match_sql,
            "sources": {
                "left": {
                    "alias": "left_src",
                    "primary_key": ["invoice_id"],
                },
                "right": {
                    "alias": "right_src",
                    "primary_key": ["payment_id"],
                },
            },
        }),
        "staged_sources": [
            {
                "alias": "left_src",
                "s3_path": f"s3://{staging_bucket}/staging/{run_id}/left_src/part-00.parquet",
                "is_native": False,
            },
            {
                "alias": "right_src",
                "s3_path": f"s3://{staging_bucket}/staging/{run_id}/right_src/part-00.parquet",
                "is_native": False,
            },
        ],
    }
    if callback_url:
        exec_payload["callback_url"] = callback_url
    if direct_exec:
        exec_payload["source_uris"] = [
            {"alias": "left_src", "uri": left_uri},
            {"alias": "right_src", "uri": right_uri},
        ]

    try:
        with conn.cursor() as cur:
            if direct_exec:
                # Direct exec: only insert the exec job
                cur.execute(
                    "INSERT INTO jobs (job_id, run_id, job_type, status, payload) "
                    "VALUES (%s, %s, %s, 'pending', %s)",
                    (exec_job_id, run_id, "exec", json.dumps(exec_payload)),
                )
                conn.commit()
                print(f"Inserted 1 direct-exec job for run {run_id}")
            else:
                for job_id, payload, jtype in [
                    (left_job_id, left_stage, "stage_plan"),
                    (right_job_id, right_stage, "stage_plan"),
                    (exec_job_id, exec_payload, "exec"),
                ]:
                    cur.execute(
                        "INSERT INTO jobs (job_id, run_id, job_type, status, payload) "
                        "VALUES (%s, %s, %s, 'pending', %s)",
                        (job_id, run_id, jtype, json.dumps(payload)),
                    )
                conn.commit()
                print(f"Inserted 3 jobs for run {run_id}")
    finally:
        conn.close()

    return left_stage, right_stage, exec_payload


async def publish_to_nats(nats_url: str, left_stage: dict, right_stage: dict,
                         direct_exec: bool = False,
                         exec_payload: dict | None = None):
    """Publish job messages to NATS JetStream.

    When *direct_exec* is True, publish the Exec payload directly to
    ``kalla.exec`` instead of the StagePlan messages.
    """
    nc = await nats.connect(nats_url)
    js = nc.jetstream()

    # Ensure streams exist
    try:
        await js.find_stream_name_by_subject("kalla.stage")
    except Exception:
        await js.add_stream(
            name="KALLA_STAGE",
            subjects=["kalla.stage"],
            retention="workqueue",
        )

    try:
        await js.find_stream_name_by_subject("kalla.exec")
    except Exception:
        await js.add_stream(
            name="KALLA_EXEC",
            subjects=["kalla.exec"],
            retention="workqueue",
        )

    if direct_exec and exec_payload is not None:
        await js.publish("kalla.exec", json.dumps(exec_payload).encode())
        print("Published 1 Exec message to NATS kalla.exec (direct exec)")
    else:
        await js.publish("kalla.stage", json.dumps(left_stage).encode())
        await js.publish("kalla.stage", json.dumps(right_stage).encode())
        print("Published 2 StagePlan messages to NATS kalla.stage")

    await nc.close()


def wait_for_callback(result_file: str, listener_proc: subprocess.Popen,
                      timeout_secs: int = 300) -> tuple[float, str, dict]:
    """Wait for the callback listener to receive a /complete or /error POST.

    Returns (elapsed_secs, status, result_dict).
    """
    start = time.time()
    while time.time() - start < timeout_secs:
        # Check if the listener wrote a result file
        if os.path.exists(result_file) and os.path.getsize(result_file) > 0:
            with open(result_file) as f:
                result = json.load(f)
            elapsed = time.time() - start
            cb_type = result.get("_callback_type", "unknown")
            status = "completed" if cb_type == "complete" else "error"
            return elapsed, status, result

        # Check if listener died unexpectedly
        if listener_proc.poll() is not None and not os.path.exists(result_file):
            return time.time() - start, "listener_died", {}

        time.sleep(0.5)

    return time.time() - start, "timeout", {}


def main():
    parser = argparse.ArgumentParser(description="Inject scaled benchmark jobs")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--pg-url", default="postgresql://kalla:kalla_secret@localhost:5432/kalla")
    parser.add_argument("--nats-url", default="nats://localhost:4222")
    parser.add_argument("--staging-bucket", default="kalla-staging")
    parser.add_argument("--match-sql", default=DEFAULT_MATCH_SQL)
    parser.add_argument("--match-rate", type=float, default=0.75)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--skip-seed", action="store_true",
                        help="Skip data seeding (assume already seeded)")
    parser.add_argument("--direct-exec", action="store_true",
                        help="Skip staging; send source_uris in Exec for direct reading")
    parser.add_argument("--json-output", action="store_true",
                        help="Output results as JSON")
    args = parser.parse_args()

    run_id = str(uuid.uuid4())
    print(f"Run ID: {run_id}")

    # Step 1: Start callback listener
    result_file = tempfile.mktemp(suffix=".json", prefix=f"kalla-bench-{run_id[:8]}-")
    print(f"Starting callback listener (result file: {result_file})...")
    listener_proc, listener_port = start_callback_listener(result_file)
    callback_url = f"http://127.0.0.1:{listener_port}"
    print(f"Callback listener on {callback_url}")

    try:
        # Step 2: Seed data
        if not args.skip_seed:
            print("Seeding benchmark data...")
            seed_data(args.pg_url, args.rows, args.match_rate)

        # Step 3: Insert jobs into Postgres (with callback_url in exec payload)
        print("Inserting jobs...")
        start_time = time.time()
        left_stage, right_stage, exec_payload = insert_jobs(
            args.pg_url, run_id, args.staging_bucket,
            args.match_sql, args.pg_url,
            callback_url=callback_url,
            direct_exec=args.direct_exec,
        )

        # Step 4: Publish to NATS
        print("Publishing to NATS...")
        asyncio.run(publish_to_nats(
            args.nats_url, left_stage, right_stage,
            direct_exec=args.direct_exec,
            exec_payload=exec_payload,
        ))

        # Step 5: Wait for callback
        print("Waiting for worker callback...")
        elapsed, status, result = wait_for_callback(
            result_file, listener_proc, args.timeout,
        )
        total_time = time.time() - start_time

        rows_per_sec = args.rows / total_time if total_time > 0 else 0
        matched = result.get("matched_count", 0)
        unmatched_left = result.get("unmatched_left_count", 0)
        unmatched_right = result.get("unmatched_right_count", 0)

        if args.json_output:
            output = {
                "run_id": run_id,
                "rows": args.rows,
                "status": status,
                "elapsed_secs": round(elapsed, 2),
                "total_time_secs": round(total_time, 2),
                "rows_per_sec": round(rows_per_sec, 0),
                "matched_count": matched,
                "unmatched_left_count": unmatched_left,
                "unmatched_right_count": unmatched_right,
            }
            print(json.dumps(output))
        else:
            print(f"\n=== Results ===")
            print(f"  Run ID:          {run_id}")
            print(f"  Rows:            {args.rows}")
            print(f"  Status:          {status}")
            print(f"  Elapsed:         {elapsed:.2f}s")
            print(f"  Total time:      {total_time:.2f}s")
            print(f"  Rows/sec:        {rows_per_sec:.0f}")
            print(f"  Matched:         {matched}")
            print(f"  Unmatched left:  {unmatched_left}")
            print(f"  Unmatched right: {unmatched_right}")

    finally:
        # Clean up listener
        if listener_proc.poll() is None:
            listener_proc.kill()
        try:
            os.unlink(result_file)
        except OSError:
            pass


if __name__ == "__main__":
    main()
