#!/usr/bin/env bash
# run_cluster_benchmark.sh — Benchmark orchestrator for Ballista cluster mode
#
# Usage:
#   bash benchmarks/run_cluster_benchmark.sh [scenario1.json scenario2.json ...]
#   (defaults to all mode=cluster scenarios in benchmarks/scenarios/)
#
# Environment:
#   PG_URL              — Postgres conn URL        (default: postgresql://kalla:kalla_secret@localhost:5432/kalla)
#   SCHEDULER_BINARY    — path to kallad binary    (default: ./target/release/kallad)
#   EXECUTOR_BINARY     — path to kallad binary    (default: ./target/release/kallad)
#   SCHEDULER_HOST      — scheduler hostname       (default: localhost)
#   SCHEDULER_PORT      — scheduler gRPC port      (default: 50050)
#   NUM_EXECUTORS       — number of executors      (default: 2)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PG_URL="${PG_URL:-postgresql://kalla:kalla_secret@localhost:5432/kalla}"
SCHEDULER_HOST="${SCHEDULER_HOST:-localhost}"
SCHEDULER_PORT="${SCHEDULER_PORT:-50050}"
NUM_EXECUTORS="${NUM_EXECUTORS:-2}"
TIMEOUT_SECS=300

# Binary paths
SCHEDULER_BINARY="${SCHEDULER_BINARY:-./target/release/kallad}"
EXECUTOR_BINARY="${EXECUTOR_BINARY:-./target/release/kallad}"

REPORT_DIR="${SCRIPT_DIR}/results"
mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/report-cluster-$(date +%Y%m%d-%H%M%S).md"

# PIDs to clean up on exit
SCHEDULER_PID=""
EXECUTOR_PIDS=()

# ---- Collect scenario files ----

if [ $# -gt 0 ]; then
    SCENARIOS=("$@")
else
    shopt -s nullglob
    SCENARIOS=()
    for f in "${SCRIPT_DIR}"/scenarios/*.json; do
        # Include files with "cluster" in name or mode=cluster in JSON
        if [[ "$(basename "$f")" == *cluster* ]] || python3 -c "
import json, sys
d = json.load(open('$f'))
sys.exit(0 if d.get('mode') == 'cluster' else 1)
" 2>/dev/null; then
            SCENARIOS+=("$f")
        fi
    done
    shopt -u nullglob
fi

if [ ${#SCENARIOS[@]} -eq 0 ]; then
    echo "ERROR: No cluster scenario files found in ${SCRIPT_DIR}/scenarios/"
    exit 1
fi

echo "Found ${#SCENARIOS[@]} cluster scenario(s)"

# ---- Helpers ----

now_ns() {
    python3 -c "import time; print(int(time.time_ns()))"
}

json_field() {
    python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('$1',''))" < "$2"
}

cleanup() {
    echo ""
    echo "Cleaning up processes..."

    # Stop executors
    for pid in ${EXECUTOR_PIDS[@]+"${EXECUTOR_PIDS[@]}"}; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "  Stopping executor (PID ${pid})"
            kill "$pid" 2>/dev/null || true
        fi
    done

    # Stop scheduler
    if [ -n "$SCHEDULER_PID" ] && kill -0 "$SCHEDULER_PID" 2>/dev/null; then
        echo "  Stopping scheduler (PID ${SCHEDULER_PID})"
        kill "$SCHEDULER_PID" 2>/dev/null || true
    fi

    wait 2>/dev/null || true
    echo "Cleanup complete."
}

trap cleanup EXIT

# ---- Build binary if needed ----

build_if_needed() {
    local need_build=false
    if [ ! -f "$SCHEDULER_BINARY" ]; then
        echo "kallad binary not found at ${SCHEDULER_BINARY}"
        need_build=true
    fi

    if [ "$need_build" = true ]; then
        echo "Building release binary..."
        cargo build --release --bin kallad --manifest-path "${PROJECT_ROOT}/Cargo.toml"
        echo "Build complete."
    fi
}

build_if_needed

# ---- Start Ballista scheduler ----

start_scheduler() {
    local log_file="/tmp/kalla-cluster-scheduler.log"
    echo "Starting kallad scheduler on port 8080 (gRPC ${SCHEDULER_PORT})..."

    RUST_LOG=info \
    "${SCHEDULER_BINARY}" scheduler --http-port 8080 --grpc-port "${SCHEDULER_PORT}" > "${log_file}" 2>&1 &

    SCHEDULER_PID=$!
    echo "  Scheduler started (PID ${SCHEDULER_PID}, log: ${log_file})"

    # Wait for scheduler to be ready
    echo "  Waiting for scheduler health check..."
    for i in $(seq 1 30); do
        if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
            echo "  Scheduler is healthy."
            return
        fi
        sleep 1
    done

    if ! kill -0 "$SCHEDULER_PID" 2>/dev/null; then
        echo "ERROR: Scheduler failed to start. Log:"
        tail -20 "${log_file}" 2>/dev/null || true
        exit 1
    fi
    echo "  WARNING: Health check did not respond, but process is running."
}

# ---- Start Ballista executors ----

start_executors() {
    local count="$1"
    echo "Starting ${count} kallad executor(s)..."

    for i in $(seq 1 "$count"); do
        local log_file="/tmp/kalla-cluster-executor-${i}.log"
        local flight_port=$((50050 + i))
        local grpc_port=$((50052 + i))

        RUST_LOG=info \
        "${EXECUTOR_BINARY}" executor \
            --scheduler-host "${SCHEDULER_HOST}" \
            --scheduler-port "${SCHEDULER_PORT}" \
            --flight-port "${flight_port}" \
            --grpc-port "${grpc_port}" \
            --external-host localhost > "${log_file}" 2>&1 &

        EXECUTOR_PIDS+=($!)
        local last_pid=$!
        echo "  Executor ${i} started (PID ${last_pid}, flight: ${flight_port}, grpc: ${grpc_port}, log: ${log_file})"
    done

    # Wait for executors to register with scheduler
    echo "  Waiting for executors to register..."
    sleep 5

    # Verify executors are running
    local running=0
    for pid in ${EXECUTOR_PIDS[@]+"${EXECUTOR_PIDS[@]}"}; do
        if kill -0 "$pid" 2>/dev/null; then
            running=$((running + 1))
        fi
    done
    echo "  ${running}/${count} executors running"

    if [ "$running" -eq 0 ]; then
        echo "ERROR: No executors are running. Check logs in /tmp/kalla-cluster-executor-*.log"
        for i in $(seq 1 "$count"); do
            echo "--- Executor ${i} log ---"
            tail -20 "/tmp/kalla-cluster-executor-${i}.log" 2>/dev/null || true
        done
        exit 1
    fi
}

# ---- Report header ----

{
    echo "# Cluster Benchmark Report"
    echo ""
    echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "Executors: ${NUM_EXECUTORS}"
    echo ""
    echo "| Scenario | Rows | Executors | Elapsed (s) | Total (s) | Rows/sec | Status |"
    echo "|----------|------|-----------|-------------|-----------|----------|--------|"
} > "${REPORT_FILE}"

SUMMARY_ROWS=""

# ---- Start cluster components ----

start_scheduler
start_executors "${NUM_EXECUTORS}"

# ---- Run each scenario ----

for scenario_file in "${SCENARIOS[@]}"; do
    SCENARIO_NAME=$(json_field "name" "$scenario_file")
    SOURCE_TYPE=$(json_field "source_type" "$scenario_file")
    ROWS=$(json_field "rows" "$scenario_file")
    MATCH_SQL=$(json_field "match_sql" "$scenario_file")
    PATTERN=$(json_field "pattern" "$scenario_file")
    PATTERN="${PATTERN:-one_to_one}"
    # Extract filters JSON (compact, empty string if absent)
    FILTERS_JSON=$(python3 -c "
import json, sys
d = json.load(open('$scenario_file'))
f = d.get('filters')
print(json.dumps(f) if f else '')
" 2>/dev/null || echo "")

    echo ""
    if [ -n "$FILTERS_JSON" ]; then
        echo "=== Scenario: ${SCENARIO_NAME} (${ROWS} rows, ${SOURCE_TYPE}, ${NUM_EXECUTORS} executors, filtered) ==="
    else
        echo "=== Scenario: ${SCENARIO_NAME} (${ROWS} rows, ${SOURCE_TYPE}, ${NUM_EXECUTORS} executors) ==="
    fi

    START_TIME=$(now_ns)

    # Build injector arguments as array to preserve quoting
    INJECT_ARGS=(--rows "$ROWS" --pg-url "$PG_URL" --scheduler-url "http://localhost:8080" --match-sql "$MATCH_SQL" --timeout "$TIMEOUT_SECS" --json-output --pattern "$PATTERN")

    # Append filters if present
    if [ -n "$FILTERS_JSON" ]; then
        INJECT_ARGS+=(--filters-json "$FILTERS_JSON")
    fi

    # Run the injector script (HTTP-based)
    RESULT=$(python3 "${SCRIPT_DIR}/inject_cluster_job.py" \
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

    SUMMARY_ROWS+="| ${SCENARIO_NAME} | ${ROWS} | ${NUM_EXECUTORS} | ${ELAPSED_SECS} | ${TOTAL_SECS} | ${ROWS_PER_SEC} | ${STATUS} |\n"
done

# ---- Write report ----

printf '%b' "$SUMMARY_ROWS" >> "${REPORT_FILE}"

{
    echo ""
    echo "## Environment"
    echo ""
    echo "- Scheduler: ${SCHEDULER_HOST}:${SCHEDULER_PORT}"
    echo "- Executors: ${NUM_EXECUTORS}"
    echo "- Postgres: ${PG_URL}"
    echo "- Host: $(uname -n)"
    echo "- OS: $(uname -s) $(uname -r)"
} >> "${REPORT_FILE}"

echo ""
echo "=== Cluster Benchmark Report ==="
cat "${REPORT_FILE}"
echo ""
echo "Report saved to: ${REPORT_FILE}"

# Cleanup happens via EXIT trap
