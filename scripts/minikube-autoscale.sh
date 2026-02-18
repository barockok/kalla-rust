#!/usr/bin/env bash
# minikube-autoscale.sh — Local Kubernetes autoscale demo for Kalla
#
# Demonstrates Ballista executor autoscaling using minikube + HPA.
# Deploys scheduler, executors (with HPA), and postgres, then
# fires concurrent benchmark jobs to trigger scale-up.
#
# Usage:
#   ./scripts/minikube-autoscale.sh start              # Start minikube + deploy
#   ./scripts/minikube-autoscale.sh build              # Build kalla:latest in minikube
#   ./scripts/minikube-autoscale.sh seed [ROWS]        # Seed postgres (default: 10000)
#   ./scripts/minikube-autoscale.sh load [JOBS] [ROWS] # Fire N concurrent jobs
#   ./scripts/minikube-autoscale.sh watch              # Watch autoscaling live
#   ./scripts/minikube-autoscale.sh status             # Show cluster status
#   ./scripts/minikube-autoscale.sh logs [component]   # Show logs (scheduler|executor)
#   ./scripts/minikube-autoscale.sh cleanup            # Destroy minikube cluster

set -euo pipefail

# ---- Configuration ----

PROFILE="kalla"
CPUS="${MINIKUBE_CPUS:-4}"
MEMORY="${MINIKUBE_MEMORY:-5g}"
PG_FWD_PORT=15432
SCHED_FWD_PORT=18080
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Default to GHCR image; set KALLA_IMAGE=kalla:latest for local builds
KALLA_IMAGE="${KALLA_IMAGE:-ghcr.io/barockok/kalla-rust/kallad:latest}"

# imagePullPolicy: Never for local images, IfNotPresent for registry images
# Always use Never — images are pre-loaded into minikube via `minikube image load`
IMAGE_PULL_POLICY="Never"

# kubectl context for minikube profile
CTX="kalla"

# ---- Colors ----

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERR]${NC}   $*" >&2; }
header(){ echo -e "\n${CYAN}=== $* ===${NC}"; }

# ---- Dependency checks ----

check_deps() {
    local missing=()
    for cmd in minikube kubectl docker python3; do
        if ! command -v "$cmd" &>/dev/null; then
            missing+=("$cmd")
        fi
    done
    if [ ${#missing[@]} -gt 0 ]; then
        err "Missing dependencies: ${missing[*]}"
        err "Install with: brew install ${missing[*]}"
        exit 1
    fi
}

# ---- K8s manifest generation ----

apply_manifests() {
    header "Applying Kubernetes manifests"

    # Substitute image reference into manifests
    kubectl --context "$CTX" apply -f - <<YAML
---
# Postgres
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  labels:
    app: kalla
    component: postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kalla
      component: postgres
  template:
    metadata:
      labels:
        app: kalla
        component: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:16-alpine
        ports:
        - containerPort: 5432
        env:
        - name: POSTGRES_USER
          value: kalla
        - name: POSTGRES_PASSWORD
          value: kalla_secret
        - name: POSTGRES_DB
          value: kalla
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            cpu: "1"
            memory: 512Mi
        readinessProbe:
          exec:
            command: ["pg_isready", "-U", "kalla"]
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  labels:
    app: kalla
    component: postgres
spec:
  selector:
    app: kalla
    component: postgres
  ports:
  - port: 5432
    targetPort: 5432
  type: ClusterIP
---
# Ballista Scheduler
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kalla-scheduler
  labels:
    app: kalla
    component: scheduler
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kalla
      component: scheduler
  template:
    metadata:
      labels:
        app: kalla
        component: scheduler
    spec:
      containers:
      - name: scheduler
        image: ${KALLA_IMAGE}
        imagePullPolicy: ${IMAGE_PULL_POLICY}
        command: ["kallad", "scheduler", "--http-port", "8080", "--grpc-port", "50050", "--partitions", "4"]
        ports:
        - containerPort: 8080
          name: http
        - containerPort: 50050
          name: grpc
        env:
        - name: RUST_LOG
          value: info
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            cpu: 500m
            memory: 1Gi
        readinessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 5
          periodSeconds: 5
        livenessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 10
          periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: kalla-scheduler
  labels:
    app: kalla
    component: scheduler
spec:
  selector:
    app: kalla
    component: scheduler
  ports:
  - port: 8080
    targetPort: http
    name: http
  - port: 50050
    targetPort: grpc
    name: grpc
  type: ClusterIP
---
# Ballista Executor — headless service for StatefulSet DNS
apiVersion: v1
kind: Service
metadata:
  name: kalla-executor
  labels:
    app: kalla
    component: executor
spec:
  clusterIP: None
  selector:
    app: kalla
    component: executor
  ports:
  - port: 50051
    targetPort: flight
    name: flight
  - port: 50052
    targetPort: grpc
    name: grpc
---
# Ballista Executor StatefulSet
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kalla-executor
  labels:
    app: kalla
    component: executor
spec:
  serviceName: kalla-executor
  replicas: 1
  selector:
    matchLabels:
      app: kalla
      component: executor
  template:
    metadata:
      labels:
        app: kalla
        component: executor
    spec:
      volumes:
      - name: work-dir
        emptyDir:
          sizeLimit: 2Gi
      containers:
      - name: executor
        image: ${KALLA_IMAGE}
        imagePullPolicy: ${IMAGE_PULL_POLICY}
        command: ["sh", "-c"]
        args:
        - |
          kallad executor \
            --scheduler-host kalla-scheduler \
            --scheduler-port 50050 \
            --flight-port 50051 \
            --grpc-port 50052 \
            --external-host "\$(hostname).kalla-executor.default.svc.cluster.local"
        ports:
        - containerPort: 50051
          name: flight
        - containerPort: 50052
          name: grpc
        env:
        - name: RUST_LOG
          value: info
        - name: TMPDIR
          value: /work
        volumeMounts:
        - name: work-dir
          mountPath: /work
        resources:
          requests:
            cpu: 100m
            memory: 512Mi
          limits:
            cpu: "2"
            memory: 2Gi
        readinessProbe:
          tcpSocket:
            port: flight
          initialDelaySeconds: 5
          periodSeconds: 5
        livenessProbe:
          tcpSocket:
            port: flight
          initialDelaySeconds: 10
          periodSeconds: 10
---
# HPA for Executors — aggressive thresholds for demo
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: kalla-executor
  labels:
    app: kalla
    component: executor
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: kalla-executor
  minReplicas: 2
  maxReplicas: 4
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 10
      policies:
      - type: Pods
        value: 2
        periodSeconds: 15
    scaleDown:
      stabilizationWindowSeconds: 120
      policies:
      - type: Pods
        value: 1
        periodSeconds: 60
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 25
YAML

    ok "All manifests applied"
}

# ---- Commands ----

cmd_start() {
    check_deps
    header "Starting minikube cluster (profile: ${PROFILE})"

    if minikube status -p "$PROFILE" &>/dev/null; then
        info "Minikube profile '${PROFILE}' already running"
    else
        minikube start \
            --profile "$PROFILE" \
            --cpus "$CPUS" \
            --memory "$MEMORY" \
            --addons metrics-server \
            --driver docker
        ok "Minikube started"
    fi

    # Verify metrics-server is enabled
    if ! minikube addons list -p "$PROFILE" 2>/dev/null | grep -q "metrics-server.*enabled"; then
        info "Enabling metrics-server addon..."
        minikube addons enable metrics-server -p "$PROFILE"
    fi

    cmd_build

    apply_manifests

    header "Waiting for pods to be ready"
    info "Waiting for postgres..."
    kubectl --context "$CTX" wait --for=condition=ready pod -l component=postgres --timeout=120s

    info "Waiting for scheduler..."
    kubectl --context "$CTX" wait --for=condition=ready pod -l component=scheduler --timeout=120s

    info "Waiting for executor(s)..."
    kubectl --context "$CTX" wait --for=condition=ready pod -l component=executor --timeout=120s

    ok "All pods ready"
    echo ""
    cmd_status
    echo ""
    info "Next steps:"
    info "  1. Open watch:    ./scripts/minikube-autoscale.sh watch"
    info "  2. Fire load:     ./scripts/minikube-autoscale.sh load       # seeds + fires 3 jobs"
}

cmd_build() {
    header "Building/pulling kallad image"

    if [[ "$KALLA_IMAGE" == *"/"* ]]; then
        # Registry image — pull on host, then load into minikube
        info "Image: ${KALLA_IMAGE}"
        if ! minikube ssh -p "$PROFILE" -- "docker image inspect ${KALLA_IMAGE}" &>/dev/null; then
            info "Pulling on host (--platform linux/amd64)..."
            docker pull --platform linux/amd64 "${KALLA_IMAGE}"
            info "Loading into minikube..."
            minikube image load "${KALLA_IMAGE}" -p "$PROFILE"
        fi
        ok "Image ready: ${KALLA_IMAGE}"
    else
        # Local image — check if already exists, otherwise build
        if eval $(minikube docker-env -p "$PROFILE" 2>/dev/null) && docker image inspect "${KALLA_IMAGE}" &>/dev/null; then
            ok "Local ${KALLA_IMAGE} image found in minikube"
            return
        fi

        info "Building ${KALLA_IMAGE} inside minikube (this takes ~60 min on first run)..."
        cd "$PROJECT_ROOT"
        DOCKER_BUILDKIT=1 minikube image build -t "${KALLA_IMAGE}" -p "$PROFILE" .
        ok "Image built: ${KALLA_IMAGE}"

        info "Pruning build cache to free disk..."
        minikube ssh -p "$PROFILE" -- "docker builder prune -af" 2>/dev/null || true
        ok "Build cache pruned"
    fi
}

cmd_seed() {
    local rows="${1:-10000}"
    local pattern="${2:-one_to_one}"

    header "Seeding postgres with ${rows} rows (pattern: ${pattern})"

    # Check psycopg2
    if ! python3 -c "import psycopg2" 2>/dev/null; then
        err "psycopg2 not installed. Run: pip install psycopg2-binary"
        exit 1
    fi

    # Port-forward postgres
    info "Port-forwarding postgres to localhost:${PG_FWD_PORT}..."
    kubectl --context "$CTX" port-forward svc/postgres "${PG_FWD_PORT}":5432 &
    local pf_pid=$!
    sleep 2

    # Verify port-forward is working
    if ! kill -0 "$pf_pid" 2>/dev/null; then
        err "Port-forward failed. Is postgres pod running?"
        exit 1
    fi

    local pg_url="postgresql://kalla:kalla_secret@localhost:${PG_FWD_PORT}/kalla"

    info "Running seed_postgres.py..."
    python3 "${SCRIPT_DIR}/../benchmarks/seed_postgres.py" \
        --rows "$rows" \
        --pg-url "$pg_url" \
        --pattern "$pattern"

    kill "$pf_pid" 2>/dev/null || true
    wait "$pf_pid" 2>/dev/null || true

    ok "Seeded ${rows} rows (pattern: ${pattern})"
}

cmd_load() {
    local num_jobs="${1:-3}"
    local rows="${2:-10000}"
    local pattern="${3:-one_to_one}"

    # Match SQL for the given pattern
    local match_sql
    case "$pattern" in
        one_to_one)
            match_sql="SELECT l.invoice_id, r.payment_id, l.customer_id, l.amount AS invoice_amount, r.paid_amount AS payment_amount FROM left_src l JOIN right_src r ON l.customer_id = r.payer_id AND l.currency = r.currency"
            ;;
        split)
            match_sql="SELECT l.invoice_id, r.payment_id, l.customer_id, l.amount AS invoice_amount, r.paid_amount AS payment_amount FROM left_src l JOIN right_src r ON l.invoice_id = r.reference_number AND l.currency = r.currency"
            ;;
        batch)
            match_sql="SELECT l.invoice_id, r.payment_id, l.batch_ref, l.amount AS invoice_amount, r.paid_amount AS payment_amount FROM left_src l JOIN right_src r ON l.batch_ref = r.reference_number AND l.currency = r.currency"
            ;;
        cross)
            match_sql="SELECT l.invoice_id, r.payment_id, l.customer_id, l.amount AS invoice_amount, r.paid_amount AS payment_amount FROM left_src l JOIN right_src r ON l.customer_id = r.payer_id AND l.currency = r.currency AND SUBSTRING(l.invoice_date, 1, 7) = SUBSTRING(r.payment_date, 1, 7)"
            ;;
    esac

    # Internal postgres URL (cluster-internal DNS)
    local pg_internal="postgres://kalla:kalla_secret@postgres:5432/kalla"

    header "Autoscale load test"
    info "Jobs: ${num_jobs} | Rows: ${rows} | Pattern: ${pattern}"
    echo ""

    # Seed data first
    cmd_seed "$rows" "$pattern"
    echo ""

    # Port-forward scheduler
    info "Port-forwarding scheduler to localhost:${SCHED_FWD_PORT}..."
    kubectl --context "$CTX" port-forward svc/kalla-scheduler "${SCHED_FWD_PORT}":8080 &
    local pf_pid=$!
    sleep 2

    if ! kill -0 "$pf_pid" 2>/dev/null; then
        err "Port-forward failed. Is scheduler pod running?"
        exit 1
    fi

    # Verify scheduler is healthy
    if ! curl -sf "http://localhost:${SCHED_FWD_PORT}/health" >/dev/null; then
        err "Scheduler health check failed"
        kill "$pf_pid" 2>/dev/null || true
        exit 1
    fi
    ok "Scheduler is healthy"

    # Show initial state
    echo ""
    info "Initial executor state:"
    kubectl --context "$CTX" get pods -l component=executor -o wide 2>/dev/null || true
    kubectl --context "$CTX" get hpa kalla-executor 2>/dev/null || true
    echo ""

    # Submit jobs in waves for a better autoscale demo
    local wave_size=$(( (num_jobs + 1) / 2 ))
    local wave1=$wave_size
    local wave2=$(( num_jobs - wave1 ))

    info "Wave 1: Submitting ${wave1} jobs..."
    local curl_pids=()
    for i in $(seq 1 "$wave1"); do
        local run_id
        run_id=$(python3 -c "import uuid; print(uuid.uuid4())")
        curl -sf -X POST "http://localhost:${SCHED_FWD_PORT}/api/jobs" \
            -H 'Content-Type: application/json' \
            -d "{
                \"run_id\": \"${run_id}\",
                \"callback_url\": \"http://localhost:9999/api/worker\",
                \"match_sql\": \"${match_sql}\",
                \"sources\": [
                    {\"alias\": \"left_src\", \"uri\": \"${pg_internal}?table=bench_invoices\"},
                    {\"alias\": \"right_src\", \"uri\": \"${pg_internal}?table=bench_payments\"}
                ],
                \"output_path\": \"/tmp/bench-${run_id}\",
                \"primary_keys\": {
                    \"left_src\": [\"invoice_id\"],
                    \"right_src\": [\"payment_id\"]
                }
            }" >/dev/null &
        curl_pids+=($!)
        echo "  Job ${i}/${num_jobs} submitted (run_id: ${run_id})"
    done
    for pid in "${curl_pids[@]}"; do wait "$pid" 2>/dev/null || true; done

    ok "Wave 1 complete (${wave1} jobs)"

    if [ "$wave2" -gt 0 ]; then
        echo ""
        info "Waiting 30s for HPA to detect load and scale up..."
        sleep 30

        info "Current state:"
        kubectl --context "$CTX" get pods -l component=executor -o wide 2>/dev/null || true
        kubectl --context "$CTX" get hpa kalla-executor 2>/dev/null || true
        echo ""

        info "Wave 2: Submitting ${wave2} more jobs..."
        curl_pids=()
        for i in $(seq 1 "$wave2"); do
            local run_id
            run_id=$(python3 -c "import uuid; print(uuid.uuid4())")
            curl -sf -X POST "http://localhost:${SCHED_FWD_PORT}/api/jobs" \
                -H 'Content-Type: application/json' \
                -d "{
                    \"run_id\": \"${run_id}\",
                    \"callback_url\": \"http://localhost:9999/api/worker\",
                    \"match_sql\": \"${match_sql}\",
                    \"sources\": [
                        {\"alias\": \"left_src\", \"uri\": \"${pg_internal}?table=bench_invoices\"},
                        {\"alias\": \"right_src\", \"uri\": \"${pg_internal}?table=bench_payments\"}
                    ],
                    \"output_path\": \"/tmp/bench-${run_id}\",
                    \"primary_keys\": {
                        \"left_src\": [\"invoice_id\"],
                        \"right_src\": [\"payment_id\"]
                    }
                }" >/dev/null &
            curl_pids+=($!)
            echo "  Job $((wave1 + i))/${num_jobs} submitted (run_id: ${run_id})"
        done
        for pid in "${curl_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
        ok "Wave 2 complete (${wave2} jobs)"
    fi

    kill "$pf_pid" 2>/dev/null || true
    wait "$pf_pid" 2>/dev/null || true

    echo ""
    ok "All ${num_jobs} jobs submitted"
    info "Run './scripts/minikube-autoscale.sh watch' in another terminal to observe scaling"
    info "Run './scripts/minikube-autoscale.sh logs scheduler' to see job processing"
}

cmd_watch() {
    header "Watching autoscaling (Ctrl+C to stop)"
    info "Refreshing every 3 seconds..."
    echo ""

    watch -n 3 "
echo '=== HPA Status ==='
kubectl --context ${CTX} get hpa kalla-executor 2>/dev/null
echo ''
echo '=== Executor Pods ==='
kubectl --context ${CTX} get pods -l component=executor -o wide 2>/dev/null
echo ''
echo '=== All Pods ==='
kubectl --context ${CTX} get pods -l app=kalla -o wide 2>/dev/null
echo ''
echo '=== Recent Events ==='
kubectl --context ${CTX} get events --sort-by=.lastTimestamp --field-selector involvedObject.kind=Pod 2>/dev/null | tail -10
"
}

cmd_status() {
    header "Cluster Status"

    echo ""
    info "Pods:"
    kubectl --context "$CTX" get pods -l app=kalla -o wide 2>/dev/null || true

    echo ""
    info "Services:"
    kubectl --context "$CTX" get svc -l app=kalla 2>/dev/null || true

    echo ""
    info "HPA:"
    kubectl --context "$CTX" get hpa kalla-executor 2>/dev/null || true

    echo ""
    info "Executor pod resource usage:"
    kubectl --context "$CTX" top pods -l component=executor 2>/dev/null || warn "metrics-server not ready yet"
}

cmd_logs() {
    local component="${1:-scheduler}"

    case "$component" in
        scheduler)
            kubectl --context "$CTX" logs -l component=scheduler -f --tail=100
            ;;
        executor|executors)
            kubectl --context "$CTX" logs -l component=executor -f --tail=100 --max-log-requests=10
            ;;
        postgres)
            kubectl --context "$CTX" logs -l component=postgres -f --tail=50
            ;;
        *)
            err "Unknown component: ${component}. Use: scheduler, executor, postgres"
            exit 1
            ;;
    esac
}

cmd_scale() {
    local replicas="${1:-}"
    if [ -z "$replicas" ]; then
        err "Usage: $0 scale <replicas>"
        exit 1
    fi

    header "Manually scaling executors to ${replicas}"
    kubectl --context "$CTX" scale statefulset kalla-executor --replicas="$replicas"
    ok "Scaled to ${replicas} executor(s)"
}

cmd_cleanup() {
    header "Cleaning up minikube cluster"

    if minikube status -p "$PROFILE" &>/dev/null; then
        info "Deleting minikube profile '${PROFILE}'..."
        minikube delete -p "$PROFILE"
        ok "Minikube cluster deleted"
    else
        warn "Minikube profile '${PROFILE}' not found or already stopped"
    fi
}

cmd_dashboard() {
    header "Opening minikube dashboard"
    minikube dashboard -p "$PROFILE"
}

# ---- Usage ----

usage() {
    cat <<EOF
Kalla Autoscale Demo (minikube)

Usage: $0 <command> [args]

Commands:
  start              Start minikube, build image, deploy all services
  build              Build kalla:latest in minikube (or pull from GHCR)
  seed [ROWS] [PAT]  Seed postgres (default: 10000 rows, one_to_one)
  load [N] [ROWS]    Fire N concurrent jobs (default: 3 jobs, 10000 rows)
  watch              Watch HPA and pod scaling in real-time
  status             Show current cluster status
  logs [COMPONENT]   Tail logs (scheduler|executor|postgres)
  scale <N>          Manually set executor replica count
  dashboard          Open minikube K8s dashboard
  cleanup            Destroy minikube cluster

Examples:
  # Full demo flow (first run takes ~60 min for Rust build)
  $0 start                          # build image + deploy (~60 min first, ~2 min after)
  $0 watch                          # in another terminal
  $0 load 3 10000 one_to_one         # fire 3 concurrent 10K row jobs
  $0 cleanup                        # tear down when done

  # Heavier load test (needs more disk — use MINIKUBE_DISK=40g)
  $0 load 5 50000                   # 5 jobs x 50K rows

Environment:
  MINIKUBE_CPUS     CPU count (default: 4)
  MINIKUBE_MEMORY   Memory (default: 4g)
  KALLA_IMAGE       Docker image (default: kalla:latest, set to GHCR URL to pull)
EOF
}

# ---- Main dispatch ----

case "${1:-}" in
    start)     cmd_start ;;
    build)     cmd_build ;;
    seed)      shift; cmd_seed "$@" ;;
    load)      shift; cmd_load "$@" ;;
    watch)     cmd_watch ;;
    status)    cmd_status ;;
    logs)      shift; cmd_logs "$@" ;;
    scale)     shift; cmd_scale "$@" ;;
    dashboard) cmd_dashboard ;;
    cleanup)   cmd_cleanup ;;
    help|-h|--help) usage ;;
    *)
        if [ -n "${1:-}" ]; then
            err "Unknown command: $1"
        fi
        usage
        exit 1
        ;;
esac
