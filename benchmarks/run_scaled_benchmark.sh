#!/usr/bin/env bash
# run_scaled_benchmark.sh — Benchmark orchestrator for scaled (multi-worker) mode
#
# Usage:
#   bash benchmarks/run_scaled_benchmark.sh [scenario1.json scenario2.json ...]
#   (defaults to all *_scaled*.json or mode=scaled scenarios in benchmarks/scenarios/)
#
# Environment:
#   PG_URL          — Postgres conn URL    (default: postgresql://kalla:kalla_secret@localhost:5432/kalla)
#   NATS_URL        — NATS server URL      (default: nats://localhost:4222)
#   STAGING_BUCKET  — S3 bucket for staging (default: kalla-staging)
#   AWS_ENDPOINT_URL — S3/MinIO endpoint   (default: http://localhost:9000)
#   AWS_ACCESS_KEY_ID     — S3 access key  (default: minioadmin)
#   AWS_SECRET_ACCESS_KEY — S3 secret key  (default: minioadmin)
#   AWS_REGION      — S3 region            (default: us-east-1)
#   AWS_ALLOW_HTTP  — Allow HTTP for S3    (default: true)
#   WORKER_BINARY   — path to kalla-worker (default: ./target/release/kalla-worker)
#   NUM_WORKERS     — number of workers    (default: 2)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PG_URL="${PG_URL:-postgresql://kalla:kalla_secret@localhost:5432/kalla}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
STAGING_BUCKET="${STAGING_BUCKET:-kalla-staging}"
AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:9000}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_ALLOW_HTTP="${AWS_ALLOW_HTTP:-true}"
WORKER_BINARY="${WORKER_BINARY:-./target/release/kalla-worker}"
NUM_WORKERS="${NUM_WORKERS:-2}"
TIMEOUT_SECS=300

REPORT_DIR="${SCRIPT_DIR}/results"
mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/report-scaled-$(date +%Y%m%d-%H%M%S).md"

WORKER_PIDS=()

# ---- Collect scenario files ----

if [ $# -gt 0 ]; then
    SCENARIOS=("$@")
else
    shopt -s nullglob
    SCENARIOS=()
    for f in "${SCRIPT_DIR}"/scenarios/*.json; do
        # Include files with "scaled" in name or mode=scaled in JSON
        if [[ "$(basename "$f")" == *scaled* ]] || python3 -c "
import json, sys
d = json.load(open('$f'))
sys.exit(0 if d.get('mode') == 'scaled' else 1)
" 2>/dev/null; then
            SCENARIOS+=("$f")
        fi
    done
    shopt -u nullglob
fi

if [ ${#SCENARIOS[@]} -eq 0 ]; then
    echo "ERROR: No scaled scenario files found in ${SCRIPT_DIR}/scenarios/"
    exit 1
fi

# ---- Helpers ----

now_ns() {
    python3 -c "import time; print(int(time.time_ns()))"
}

json_field() {
    python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('$1',''))" < "$2"
}

cleanup_workers() {
    echo "Cleaning up worker processes..."
    for pid in ${WORKER_PIDS[@]+"${WORKER_PIDS[@]}"}; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
}

trap cleanup_workers EXIT

# ---- Start workers ----

start_workers() {
    local count="$1"
    echo "Starting ${count} workers..."

    for i in $(seq 1 "$count"); do
        local log_file="/tmp/kalla-scaled-worker-${i}.log"
        NATS_URL="${NATS_URL}" \
        DATABASE_URL="${PG_URL}" \
        AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL}" \
        AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
        AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
        AWS_REGION="${AWS_REGION}" \
        AWS_ALLOW_HTTP="${AWS_ALLOW_HTTP}" \
        STAGING_BUCKET="${STAGING_BUCKET}" \
        BALLISTA_ENABLED="true" \
        BALLISTA_PARTITIONS="4" \
        WORKER_ID="bench-worker-${i}" \
        METRICS_PORT=$((9090 + i)) \
        RUST_LOG=info \
        "${WORKER_BINARY}" > "${log_file}" 2>&1 &

        WORKER_PIDS+=($!)
        local last_pid=$!
        echo "  Worker ${i} started (PID ${last_pid}, log: ${log_file})"
    done

    # Wait for workers to connect to NATS
    echo "  Waiting for workers to initialize..."
    sleep 5

    # Verify workers are running
    local running=0
    for pid in ${WORKER_PIDS[@]+"${WORKER_PIDS[@]}"}; do
        if kill -0 "$pid" 2>/dev/null; then
            running=$((running + 1))
        fi
    done
    echo "  ${running}/${count} workers running"

    if [ "$running" -eq 0 ]; then
        echo "ERROR: No workers are running. Check logs in /tmp/kalla-scaled-worker-*.log"
        for i in $(seq 1 "$count"); do
            echo "--- Worker ${i} log ---"
            tail -20 "/tmp/kalla-scaled-worker-${i}.log" 2>/dev/null || true
        done
        exit 1
    fi
}

# ---- Report header ----

{
    echo "# Scaled Benchmark Report"
    echo ""
    echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "Workers: ${NUM_WORKERS}"
    echo ""
    echo "| Scenario | Rows | Workers | Elapsed (s) | Total (s) | Rows/sec | Status |"
    echo "|----------|------|---------|-------------|-----------|----------|--------|"
} > "${REPORT_FILE}"

SUMMARY_ROWS=""

# ---- Start workers once for all scenarios ----

start_workers "${NUM_WORKERS}"

# ---- Run each scenario ----

for scenario_file in "${SCENARIOS[@]}"; do
    SCENARIO_NAME=$(json_field "name" "$scenario_file")
    SOURCE_TYPE=$(json_field "source_type" "$scenario_file")
    ROWS=$(json_field "rows" "$scenario_file")
    MATCH_SQL=$(json_field "match_sql" "$scenario_file")
    SCENARIO_WORKERS=$(json_field "workers" "$scenario_file")
    SCENARIO_WORKERS="${SCENARIO_WORKERS:-${NUM_WORKERS}}"
    DIRECT_EXEC=$(json_field "direct_exec" "$scenario_file")

    echo ""
    echo "=== Scenario: ${SCENARIO_NAME} (${ROWS} rows, ${SOURCE_TYPE}, ${SCENARIO_WORKERS} workers) ==="

    START_TIME=$(now_ns)

    # Build injector arguments as array to preserve quoting
    INJECT_ARGS=(--rows "$ROWS" --pg-url "$PG_URL" --nats-url "$NATS_URL" --staging-bucket "$STAGING_BUCKET" --match-sql "$MATCH_SQL" --timeout "$TIMEOUT_SECS" --json-output)
    if [ "$DIRECT_EXEC" = "True" ] || [ "$DIRECT_EXEC" = "true" ]; then
        INJECT_ARGS+=(--direct-exec)
        echo "  (direct exec mode — skipping staging, Ballista enabled)"
    fi

    # Run the injector script
    RESULT=$(python3 "${SCRIPT_DIR}/inject_scaled_job.py" \
        "${INJECT_ARGS[@]}" \
    2>&1 | tee /dev/stderr | tail -1)

    END_TIME=$(now_ns)
    DURATION_NS=$((END_TIME - START_TIME))
    TOTAL_SECS=$(python3 -c "print(f'{${DURATION_NS} / 1e9:.2f}')")

    # Parse JSON output from injector
    STATUS=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(d.get('status','unknown'))" 2>/dev/null || echo "error")
    ELAPSED_SECS=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(d.get('elapsed_secs','N/A'))" 2>/dev/null || echo "N/A")
    ROWS_PER_SEC=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(int(d.get('rows_per_sec',0)))" 2>/dev/null || echo "N/A")
    MATCHED=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(d.get('matched_count',0))" 2>/dev/null || echo "0")

    echo "  Status: ${STATUS} | Elapsed: ${ELAPSED_SECS}s | Total: ${TOTAL_SECS}s | Rows/sec: ${ROWS_PER_SEC} | Matched: ${MATCHED}"

    SUMMARY_ROWS+="| ${SCENARIO_NAME} | ${ROWS} | ${SCENARIO_WORKERS} | ${ELAPSED_SECS} | ${TOTAL_SECS} | ${ROWS_PER_SEC} | ${STATUS} |\n"
done

# ---- Write report ----

printf '%b' "$SUMMARY_ROWS" >> "${REPORT_FILE}"

{
    echo ""
    echo "## Environment"
    echo ""
    echo "- NATS: ${NATS_URL}"
    echo "- Postgres: ${PG_URL}"
    echo "- S3 Endpoint: ${AWS_ENDPOINT_URL}"
    echo "- Staging Bucket: ${STAGING_BUCKET}"
    echo "- Workers: ${NUM_WORKERS}"
    echo "- Host: $(uname -n)"
    echo "- OS: $(uname -s) $(uname -r)"
} >> "${REPORT_FILE}"

echo ""
echo "=== Scaled Benchmark Report ==="
cat "${REPORT_FILE}"
echo ""
echo "Report saved to: ${REPORT_FILE}"

# Worker cleanup happens via EXIT trap
