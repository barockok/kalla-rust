#!/usr/bin/env bash
# run_benchmark.sh — Main benchmark orchestrator
#
# Usage:
#   bash benchmarks/run_benchmark.sh [scenario1.json scenario2.json ...]
#   (defaults to all scenarios in benchmarks/scenarios/)
#
# Environment:
#   WORKER_URL    — worker base URL    (default: http://localhost:9090)
#   CALLBACK_PORT — callback port      (default: 9099)
#   PG_URL        — Postgres conn URL  (default: postgresql://postgres:postgres@localhost:5432/postgres)
#   STAGING_PATH  — staging dir        (default: /tmp/kalla-staging)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKER_URL="${WORKER_URL:-http://localhost:9090}"
CALLBACK_PORT="${CALLBACK_PORT:-9099}"
CALLBACK_URL="http://localhost:${CALLBACK_PORT}"
PG_URL="${PG_URL:-postgresql://postgres:postgres@localhost:5432/postgres}"
STAGING_PATH="${STAGING_PATH:-/tmp/kalla-staging}"
TIMEOUT_SECS=300
POLL_INTERVAL=1
WORKER_PID=""
CB_PID=""

REPORT_DIR="${SCRIPT_DIR}/results"
mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/report-$(date +%Y%m%d-%H%M%S).md"

# ---- Cleanup trap ----

cleanup() {
    if [ -n "$CB_PID" ] && kill -0 "$CB_PID" 2>/dev/null; then
        kill "$CB_PID" 2>/dev/null || true
        wait "$CB_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ---- Collect scenario files ----

if [ $# -gt 0 ]; then
    SCENARIOS=("$@")
else
    shopt -s nullglob
    SCENARIOS=("${SCRIPT_DIR}"/scenarios/*.json)
    shopt -u nullglob
fi

if [ ${#SCENARIOS[@]} -eq 0 ]; then
    echo "ERROR: No scenario files found in ${SCRIPT_DIR}/scenarios/"
    exit 1
fi

# ---- Helpers ----

# Portable nanosecond timestamp (works on macOS + Linux)
now_ns() {
    python3 -c "import time; print(int(time.time_ns()))"
}

get_worker_pid() {
    # Find the kalla-worker process
    pgrep -f 'kalla-worker' | head -1 || true
}

get_rss_kb() {
    local pid="$1"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || echo "0"
    else
        echo "0"
    fi
}

json_field() {
    # Extract a simple string/number field from JSON using python
    python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('$1',''))" < "$2"
}

# ---- Report header ----

{
    echo "# Benchmark Report"
    echo ""
    echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo ""
    echo "| Scenario | Rows | Total (s) | Rows/sec | Peak RSS (MB) | Status |"
    echo "|----------|------|-----------|----------|---------------|--------|"
} > "${REPORT_FILE}"

SUMMARY_ROWS=""

# ---- Run each scenario ----

for scenario_file in "${SCENARIOS[@]}"; do
    SCENARIO_NAME=$(json_field "name" "$scenario_file")
    SOURCE_TYPE=$(json_field "source_type" "$scenario_file")
    ROWS=$(json_field "rows" "$scenario_file")
    MATCH_SQL=$(json_field "match_sql" "$scenario_file")

    echo "=== Scenario: ${SCENARIO_NAME} (${ROWS} rows, ${SOURCE_TYPE}) ==="

    DATA_DIR="/tmp/kalla-bench-${SCENARIO_NAME}"
    mkdir -p "${DATA_DIR}"

    # Step 1: Generate / seed data
    if [ "$SOURCE_TYPE" = "csv" ]; then
        echo "  Generating CSV data..."
        python3 "${SCRIPT_DIR}/generate_data.py" --rows "$ROWS" --output-dir "$DATA_DIR"
    elif [ "$SOURCE_TYPE" = "postgres" ]; then
        echo "  Seeding Postgres..."
        python3 "${SCRIPT_DIR}/seed_postgres.py" --rows "$ROWS" --pg-url "$PG_URL"
    fi

    # Step 2: Start callback server
    CALLBACK_PORT="${CALLBACK_PORT}" python3 "${SCRIPT_DIR}/callback_server.py" &
    CB_PID=$!
    sleep 0.5

    # Step 3: Record baseline
    WORKER_PID=$(get_worker_pid)
    BASELINE_RSS=$(get_rss_kb "$WORKER_PID")
    START_TIME=$(now_ns)

    # Step 4: Build job request via python3 (stdin, no shell interpolation)
    RUN_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
    OUTPUT_PATH="${STAGING_PATH}/bench-${SCENARIO_NAME}"
    mkdir -p "$OUTPUT_PATH"

    JOB_JSON=$(_BENCH_SOURCE_TYPE="$SOURCE_TYPE" \
        _BENCH_DATA_DIR="$DATA_DIR" \
        _BENCH_PG_URL="$PG_URL" \
        _BENCH_RUN_ID="$RUN_ID" \
        _BENCH_CALLBACK_URL="$CALLBACK_URL" \
        _BENCH_MATCH_SQL="$MATCH_SQL" \
        _BENCH_OUTPUT_PATH="$OUTPUT_PATH" \
        python3 - <<'PYEOF'
import json, os

source_type = os.environ["_BENCH_SOURCE_TYPE"]
data_dir = os.environ["_BENCH_DATA_DIR"]
pg_url = os.environ["_BENCH_PG_URL"]

if source_type == "csv":
    sources = [
        {"alias": "left_src", "uri": f"file://{data_dir}/invoices.csv"},
        {"alias": "right_src", "uri": f"file://{data_dir}/payments.csv"},
    ]
else:
    sources = [
        {"alias": "left_src", "uri": f"{pg_url}?table=bench_invoices"},
        {"alias": "right_src", "uri": f"{pg_url}?table=bench_payments"},
    ]

print(json.dumps({
    "run_id": os.environ["_BENCH_RUN_ID"],
    "callback_url": os.environ["_BENCH_CALLBACK_URL"],
    "match_sql": os.environ["_BENCH_MATCH_SQL"],
    "sources": sources,
    "output_path": os.environ["_BENCH_OUTPUT_PATH"],
    "primary_keys": {"left_src": ["invoice_id"], "right_src": ["payment_id"]},
}))
PYEOF
    )

    echo "  Posting job to ${WORKER_URL}/api/jobs ..."
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "${WORKER_URL}/api/jobs" \
        -H "Content-Type: application/json" \
        -d "$JOB_JSON")

    if [ "$HTTP_CODE" != "202" ] && [ "$HTTP_CODE" != "200" ]; then
        echo "  ERROR: Worker returned HTTP $HTTP_CODE"
        kill "$CB_PID" 2>/dev/null || true
        wait "$CB_PID" 2>/dev/null || true
        CB_PID=""
        SUMMARY_ROWS+="| ${SCENARIO_NAME} | ${ROWS} | - | - | - | FAILED (HTTP $HTTP_CODE) |\n"
        continue
    fi

    # Step 5: Poll callback server for completion
    echo "  Waiting for completion (timeout ${TIMEOUT_SECS}s)..."
    ELAPSED=0
    PEAK_RSS=$BASELINE_RSS
    STATUS="timeout"

    while [ "$ELAPSED" -lt "$TIMEOUT_SECS" ]; do
        sleep "$POLL_INTERVAL"
        ELAPSED=$((ELAPSED + POLL_INTERVAL))

        # Track peak RSS
        CURRENT_RSS=$(get_rss_kb "$WORKER_PID")
        if [ "$CURRENT_RSS" -gt "$PEAK_RSS" ] 2>/dev/null; then
            PEAK_RSS=$CURRENT_RSS
        fi

        # Check callback status
        CB_STATUS=$(curl -s "http://localhost:${CALLBACK_PORT}/status" 2>/dev/null || echo '{"status":"waiting"}')
        CURRENT=$(echo "$CB_STATUS" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status','waiting'))" 2>/dev/null || echo "waiting")

        if [ "$CURRENT" = "complete" ]; then
            STATUS="complete"
            break
        elif [ "$CURRENT" = "error" ]; then
            STATUS="error"
            break
        fi
    done

    END_TIME=$(now_ns)

    # Step 6: Compute metrics
    DURATION_NS=$((END_TIME - START_TIME))
    DURATION_S=$(python3 -c "print(f'{${DURATION_NS} / 1e9:.2f}')")
    ROWS_PER_SEC=$(python3 -c "
d = ${DURATION_NS} / 1e9
print(f'{${ROWS} / d:.0f}' if d > 0 else 'N/A')
")
    PEAK_RSS_MB=$(python3 -c "print(f'{${PEAK_RSS} / 1024:.1f}')")

    echo "  Status: ${STATUS} | Duration: ${DURATION_S}s | Rows/sec: ${ROWS_PER_SEC} | Peak RSS: ${PEAK_RSS_MB} MB"

    SUMMARY_ROWS+="| ${SCENARIO_NAME} | ${ROWS} | ${DURATION_S} | ${ROWS_PER_SEC} | ${PEAK_RSS_MB} | ${STATUS} |\n"

    # Step 7: Kill callback server
    kill "$CB_PID" 2>/dev/null || true
    wait "$CB_PID" 2>/dev/null || true
    CB_PID=""
done

# ---- Write report ----

printf '%b' "$SUMMARY_ROWS" >> "${REPORT_FILE}"

{
    echo ""
    echo "## Environment"
    echo ""
    echo "- Worker: ${WORKER_URL}"
    echo "- Staging: ${STAGING_PATH}"
    echo "- Host: $(uname -n)"
    echo "- OS: $(uname -s) $(uname -r)"
} >> "${REPORT_FILE}"

echo ""
echo "=== Benchmark Report ==="
cat "${REPORT_FILE}"
echo ""
echo "Report saved to: ${REPORT_FILE}"

# CI summary is appended by the workflow step (with if: always())
