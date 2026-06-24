#!/usr/bin/env bash
# stress-test.sh — Apache Bench load test against the job-submitter service.
#
# Sends 5000 POST /submit requests at 200 concurrency to flood the Redis queue
# and trigger the HPA to scale worker pods from 2 → up to 10.
#
# Usage:
#   ./scripts/stress-test.sh              # default: 5000 requests, 200 concurrency
#   ./scripts/stress-test.sh -n 2000 -c 50  # custom: 2000 requests, 50 concurrency
#   ./scripts/stress-test.sh --watch-only   # just watch HPA without sending load
#
# What to watch:
#   Terminal 1: this script (ab output + HPA poll)
#   Browser:    kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring
#               → http://localhost:3000  (admin/admin123)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── defaults ──────────────────────────────────────────────────────────────────
REQUESTS=5000
CONCURRENCY=200
WATCH_ONLY=false
JOB_TYPE="bcrypt"   # bcrypt is the most CPU-intensive — best for triggering HPA

for arg in "$@"; do
  case $arg in
    -n) shift; REQUESTS=$1 ;;
    -c) shift; CONCURRENCY=$1 ;;
    --watch-only) WATCH_ONLY=true ;;
    --prime)  JOB_TYPE="prime"  ;;
    --sort)   JOB_TYPE="sort"   ;;
    --bcrypt) JOB_TYPE="bcrypt" ;;
  esac
done

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
ok()      { echo -e "${GREEN}✓${NC} $*"; }
info()    { echo -e "${YELLOW}→${NC} $*"; }
section() { echo ""; echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"; echo -e "${BLUE}  $*${NC}"; echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"; }

# ── preflight ─────────────────────────────────────────────────────────────────
command -v ab >/dev/null 2>&1 || {
  echo "Apache Bench (ab) not found. Install with:"
  echo "  brew install httpd      # macOS"
  echo "  apt-get install apache2-utils  # Ubuntu"
  exit 1
}

# ── port-forward setup ────────────────────────────────────────────────────────
# Minikube with Docker/Colima driver on macOS does not route the VM subnet
# to the host — we forward port 8097 locally and point ab at localhost.
PF_PID=""
cleanup() {
  [ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null || true
  rm -f /tmp/job-stress.json
}
trap cleanup EXIT

section "Setup"

info "Port-forwarding job-submitter :8097 → :8080..."
kubectl port-forward svc/job-submitter 8097:8080 -n jobs >/dev/null 2>&1 &
PF_PID=$!
sleep 2

TARGET="http://localhost:8097/submit"

# Apache Bench reads the POST body from a file
cat > /tmp/job-stress.json <<EOF
{"type":"${JOB_TYPE}"}
EOF

ok "Target: ${TARGET}"
ok "Job type: ${JOB_TYPE}"
echo ""

# ── baseline HPA snapshot ─────────────────────────────────────────────────────
section "Baseline (before load)"
kubectl get hpa worker -n jobs
kubectl get pods -n jobs -l app=worker --no-headers | awk '{print $1, $2, $3}'
echo ""

if [ "$WATCH_ONLY" = true ]; then
  info "Watching HPA (Ctrl+C to stop)..."
  kubectl get hpa worker -n jobs --watch
  exit 0
fi

# ── start background HPA watcher ──────────────────────────────────────────────
section "Stress Test — ${REQUESTS} requests at concurrency ${CONCURRENCY}"

info "Starting background HPA watcher (prints every 10s)..."
(
  while true; do
    HPA=$(kubectl get hpa worker -n jobs --no-headers 2>/dev/null || echo "unavailable")
    PODS=$(kubectl get pods -n jobs -l app=worker --no-headers 2>/dev/null | wc -l | tr -d ' ')
    echo -e "  [HPA] ${HPA}   [worker pods: ${PODS}]"
    sleep 10
  done
) &
WATCHER_PID=$!

echo ""
info "Running: ab -n ${REQUESTS} -c ${CONCURRENCY} -p /tmp/job-stress.json -T application/json ${TARGET}"
echo ""

# ab flags:
#   -n  total number of requests
#   -c  number of concurrent requests
#   -p  POST body file
#   -T  Content-Type header
#   -k  HTTP keep-alive (faster for high concurrency)
#   -r  don't exit on socket receive errors (graceful for high concurrency)
ab \
  -n "${REQUESTS}" \
  -c "${CONCURRENCY}" \
  -p /tmp/job-stress.json \
  -T "application/json" \
  -k \
  -r \
  "${TARGET}"

kill "$WATCHER_PID" 2>/dev/null || true

# ── post-load HPA snapshot ────────────────────────────────────────────────────
section "Post-load HPA snapshot"
echo ""
info "Immediately after load (queue still processing):"
kubectl get hpa worker -n jobs
kubectl get pods -n jobs -l app=worker --no-headers

echo ""
info "Watching HPA scale-down over next 3 minutes (Ctrl+C to stop early)..."
echo "  (scale-down stabilizationWindowSeconds: 120 — expect ~2 min)"
echo ""

# Poll every 15s for 3 minutes
for i in $(seq 1 12); do
  sleep 15
  HPA=$(kubectl get hpa worker -n jobs --no-headers 2>/dev/null || echo "unavailable")
  PODS=$(kubectl get pods -n jobs -l app=worker --no-headers 2>/dev/null | wc -l | tr -d ' ')
  echo -e "  +$((i*15))s  [HPA] ${HPA}   [worker pods: ${PODS}]"
  REPLICAS=$(echo "$HPA" | awk '{print $7}')
  if [ "${REPLICAS:-0}" -le "2" ] 2>/dev/null; then
    echo ""
    ok "Workers scaled back down to ${REPLICAS} replicas"
    break
  fi
done

section "Done"
echo ""
ok "Stress test complete."
echo ""
echo "  To view Grafana dashboard:"
echo "    kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring"
echo "    http://localhost:3000  (admin / admin123)"
echo ""
echo "  To check worker logs:"
echo "    kubectl logs -l app=worker -n jobs --tail=20"
echo ""
echo "  To check queue drain:"
echo "    kubectl port-forward svc/stats-aggregator 8082:8081 -n jobs &"
echo "    curl -s http://localhost:8082/stats | python3 -m json.tool"
