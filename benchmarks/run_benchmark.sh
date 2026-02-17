#!/usr/bin/env bash
# run_benchmark.sh — Main benchmark orchestrator
#
# Usage:
#   bash benchmarks/run_benchmark.sh [scenario1.json scenario2.json ...]
#   (defaults to all scenarios in benchmarks/scenarios/)
#
# Environment:
#   WORKER_URL    — scheduler base URL  (default: http://localhost:9090)
#   PG_URL        — Postgres conn URL   (default: postgresql://postgres:postgres@localhost:5432/postgres)
#   WORKER_LOG    — scheduler log file  (default: /tmp/kalla-scheduler-bench.log)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKER_URL="${WORKER_URL:-http://localhost:9090}"
PG_URL="${PG_URL:-postgresql://postgres:postgres@localhost:5432/postgres}"
WORKER_LOG="${WORKER_LOG:-/tmp/kalla-scheduler-bench.log}"
TIMEOUT_SECS=300
POLL_INTERVAL=1
WORKER_PID=""

REPORT_DIR="${SCRIPT_DIR}/results"
mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/report-$(date +%Y%m%d-%H%M%S).md"

# ---- Collect scenario files ----

if [ $# -gt 0 ]; then
    SCENARIOS=("$@")
else
    shopt -s nullglob
    SCENARIOS=()
    for f in "${SCRIPT_DIR}"/scenarios/*.json; do
        # Skip files meant for scaled-mode or cluster-mode benchmarks
        if [[ "$(basename "$f")" == *scaled* ]] || [[ "$(basename "$f")" == *cluster* ]] || python3 -c "
import json, sys
d = json.load(open('$f'))
sys.exit(0 if d.get('mode') in ('scaled', 'cluster') else 1)
" 2>/dev/null; then
            continue
        fi
        SCENARIOS+=("$f")
    done
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
    # Find the kallad process
    pgrep -f 'kallad' | head -1 || true
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
    MATCH_RATE=$(json_field "match_rate" "$scenario_file")
    PATTERN=$(json_field "pattern" "$scenario_file")
    PATTERN="${PATTERN:-one_to_one}"

    echo "=== Scenario: ${SCENARIO_NAME} (${ROWS} rows, ${SOURCE_TYPE}) ==="

    DATA_DIR="/tmp/kalla-bench-${SCENARIO_NAME}"
    mkdir -p "${DATA_DIR}"

    # Step 1: Generate / seed data
    MATCH_RATE_ARG=""
    if [ -n "$MATCH_RATE" ] && [ "$MATCH_RATE" != "" ]; then
        MATCH_RATE_ARG="--match-rate $MATCH_RATE"
    fi

    if [ "$SOURCE_TYPE" = "csv" ]; then
        echo "  Generating CSV data..."
        python3 "${SCRIPT_DIR}/generate_data.py" --rows "$ROWS" --output-dir "$DATA_DIR" $MATCH_RATE_ARG
    elif [ "$SOURCE_TYPE" = "postgres" ]; then
        echo "  Seeding Postgres..."
        python3 "${SCRIPT_DIR}/seed_postgres.py" --rows "$ROWS" --pg-url "$PG_URL" $MATCH_RATE_ARG --pattern "$PATTERN"
    fi

    # Step 2: Record baseline
    WORKER_PID=$(get_worker_pid)
    BASELINE_RSS=$(get_rss_kb "$WORKER_PID")
    START_TIME=$(now_ns)

    # Step 3: Build job request via python3 (stdin, no shell interpolation)
    RUN_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
    OUTPUT_PATH="/tmp/kalla-bench-output-${SCENARIO_NAME}"
    mkdir -p "$OUTPUT_PATH"

    JOB_JSON=$(_BENCH_SOURCE_TYPE="$SOURCE_TYPE" \
        _BENCH_DATA_DIR="$DATA_DIR" \
        _BENCH_PG_URL="$PG_URL" \
        _BENCH_RUN_ID="$RUN_ID" \
        _BENCH_MATCH_SQL="$MATCH_SQL" \
        _BENCH_OUTPUT_PATH="$OUTPUT_PATH" \
        python3 - <<'PYEOF'
import json, os

source_type = os.environ["_BENCH_SOURCE_TYPE"]
data_dir = os.environ["_BENCH_DATA_DIR"]
pg_url = os.environ["_BENCH_PG_URL"]

if source_type == "csv":
    # Worker matches local CSV by .csv suffix on a plain path (no file:// prefix)
    sources = [
        {"alias": "left_src", "uri": f"{data_dir}/invoices.csv"},
        {"alias": "right_src", "uri": f"{data_dir}/payments.csv"},
    ]
else:
    # Worker checks uri.starts_with("postgres://"), not "postgresql://"
    pg = pg_url.replace("postgresql://", "postgres://", 1)
    sources = [
        {"alias": "left_src", "uri": f"{pg}?table=bench_invoices"},
        {"alias": "right_src", "uri": f"{pg}?table=bench_payments"},
    ]

job = {
    "run_id": os.environ["_BENCH_RUN_ID"],
    "callback_url": "http://127.0.0.1:0/noop",
    "match_sql": os.environ["_BENCH_MATCH_SQL"],
    "sources": sources,
    "output_path": os.environ["_BENCH_OUTPUT_PATH"],
    "primary_keys": {"left_src": ["invoice_id"], "right_src": ["payment_id"]},
}

print(json.dumps(job))
PYEOF
    )

    echo "  Posting job to ${WORKER_URL}/api/jobs ..."
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "${WORKER_URL}/api/jobs" \
        -H "Content-Type: application/json" \
        -d "$JOB_JSON")

    if [ "$HTTP_CODE" != "202" ] && [ "$HTTP_CODE" != "200" ]; then
        echo "  ERROR: Scheduler returned HTTP $HTTP_CODE"
        SUMMARY_ROWS+="| ${SCENARIO_NAME} | ${ROWS} | - | - | - | FAILED (HTTP $HTTP_CODE) |\n"
        continue
    fi

    # Step 4: Poll scheduler log for completion
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

        # Check scheduler log for run completion
        if grep -q "Run ${RUN_ID} completed" "$WORKER_LOG" 2>/dev/null; then
            STATUS="complete"
            break
        elif grep -q "Run ${RUN_ID} failed" "$WORKER_LOG" 2>/dev/null; then
            STATUS="error"
            break
        fi
    done

    END_TIME=$(now_ns)

    # Step 5: Compute metrics
    DURATION_NS=$((END_TIME - START_TIME))
    DURATION_S=$(python3 -c "print(f'{${DURATION_NS} / 1e9:.2f}')")
    ROWS_PER_SEC=$(python3 -c "
d = ${DURATION_NS} / 1e9
print(f'{${ROWS} / d:.0f}' if d > 0 else 'N/A')
")
    PEAK_RSS_MB=$(python3 -c "print(f'{${PEAK_RSS} / 1024:.1f}')")

    echo "  Status: ${STATUS} | Duration: ${DURATION_S}s | Rows/sec: ${ROWS_PER_SEC} | Peak RSS: ${PEAK_RSS_MB} MB"

    SUMMARY_ROWS+="| ${SCENARIO_NAME} | ${ROWS} | ${DURATION_S} | ${ROWS_PER_SEC} | ${PEAK_RSS_MB} | ${STATUS} |\n"
done

# ---- Write report ----

printf '%b' "$SUMMARY_ROWS" >> "${REPORT_FILE}"

{
    echo ""
    echo "## Environment"
    echo ""
    echo "- Scheduler: ${WORKER_URL}"
    echo "- Host: $(uname -n)"
    echo "- OS: $(uname -s) $(uname -r)"
} >> "${REPORT_FILE}"

echo ""
echo "=== Benchmark Report ==="
cat "${REPORT_FILE}"
echo ""
echo "Report saved to: ${REPORT_FILE}"

# CI summary is appended by the workflow step (with if: always())
