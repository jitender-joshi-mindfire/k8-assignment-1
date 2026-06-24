#!/usr/bin/env bash
# verify.sh — Post-deploy smoke tests.
# Run after deploy.sh to confirm the cluster is healthy end-to-end.
#
# Usage:
#   ./scripts/verify.sh
#   ./scripts/verify.sh --skip-monitoring

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SKIP_MONITORING=false
for arg in "$@"; do
  case $arg in --skip-monitoring) SKIP_MONITORING=true ;; esac
done

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; BLUE='\033[0;34m'; NC='\033[0m'
ok()      { echo -e "${GREEN}✓${NC} $*"; }
fail()    { echo -e "${RED}✗${NC} $*"; FAILURES=$((FAILURES+1)); }
info()    { echo -e "${YELLOW}→${NC} $*"; }
section() { echo ""; echo -e "${BLUE}── $* ──${NC}"; }

FAILURES=0

# Track all background port-forward PIDs for cleanup
PF_PIDS=()
cleanup() {
  for pid in "${PF_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

# ── 1: pod health ─────────────────────────────────────────────────────────────
section "Pod health (namespace: jobs)"

kubectl get pods -n jobs --no-headers 2>/dev/null
echo ""

for svc in redis job-submitter worker stats-aggregator; do
  READY=$(kubectl get pods -n jobs -l app=${svc} --no-headers 2>/dev/null \
    | awk '{print $2}' | head -1)
  if echo "$READY" | grep -qE '^[1-9][0-9]*/[1-9]'; then
    ok "${svc} pods Ready (${READY})"
  else
    fail "${svc} pods not ready (${READY:-missing})"
  fi
done

# ── 2: HPA status ─────────────────────────────────────────────────────────────
section "HPA status"

HPA=$(kubectl get hpa worker -n jobs --no-headers 2>/dev/null || echo "")
if [ -z "$HPA" ]; then
  fail "HPA 'worker' not found in namespace jobs"
else
  echo "$HPA"
  TARGETS=$(echo "$HPA" | awk '{print $4}')
  if echo "$TARGETS" | grep -q "<unknown>"; then
    fail "HPA TARGETS is <unknown> — metrics-server may not be ready yet (wait 60s and retry)"
  else
    ok "HPA TARGETS: ${TARGETS}"
  fi
fi

# ── 3: ingress ────────────────────────────────────────────────────────────────
section "Ingress"

INGRESS=$(kubectl get ingress -n jobs --no-headers 2>/dev/null || echo "")
echo "$INGRESS"
if echo "$INGRESS" | grep -q "job-submitter"; then
  ok "Ingress resource exists"
else
  fail "Ingress not found"
fi

# ── 4: end-to-end job flow ────────────────────────────────────────────────────
section "End-to-end job flow (via port-forward)"

# Use port-forward — Minikube with Docker/Colima driver on macOS does not
# route the VM subnet (192.168.49.x) to the host. port-forward is the
# correct approach for local testing on all driver types.
info "Port-forwarding job-submitter :8098 → :8080..."
kubectl port-forward svc/job-submitter 8098:8080 -n jobs >/dev/null 2>&1 &
PF_PIDS+=($!)
sleep 2

BASE_URL="http://localhost:8098"

info "Submitting prime job to ${BASE_URL}/submit ..."
SUBMIT_RESP=$(curl -s -X POST "${BASE_URL}/submit" \
  -H "Content-Type: application/json" \
  -d '{"type":"prime"}' \
  --connect-timeout 5 --max-time 10 2>/dev/null || echo "")

if echo "$SUBMIT_RESP" | grep -q "job_id"; then
  JOB_ID=$(echo "$SUBMIT_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null || echo "")
  ok "Job submitted — ID: ${JOB_ID}"
else
  fail "Submit failed. Response: ${SUBMIT_RESP}"
  JOB_ID=""
fi

if [ -n "$JOB_ID" ]; then
  info "Polling GET /status/${JOB_ID} (up to 60s)..."
  FINAL_STATUS=""
  STATUS_RESP=""
  for i in $(seq 1 12); do
    STATUS_RESP=$(curl -s "${BASE_URL}/status/${JOB_ID}" \
      --connect-timeout 5 --max-time 10 2>/dev/null || echo "")
    JOB_STATUS=$(echo "$STATUS_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
    echo "  attempt ${i}: status=${JOB_STATUS}"
    if [ "$JOB_STATUS" = "done" ]; then
      FINAL_STATUS="done"
      break
    fi
    sleep 5
  done

  if [ "$FINAL_STATUS" = "done" ]; then
    ok "Job completed successfully"
    RESULT=$(echo "$STATUS_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('result',''))" 2>/dev/null || echo "")
    ok "Result: ${RESULT}"
  else
    fail "Job did not reach 'done' status within 60s (last status: ${JOB_STATUS:-unknown})"
  fi
fi

# ── 5: worker metrics ─────────────────────────────────────────────────────────
section "Worker metrics (port-forward :9191)"

kubectl port-forward svc/worker 9191:9090 -n jobs >/dev/null 2>&1 &
PF_PIDS+=($!)
sleep 3

METRICS=$(curl -s http://localhost:9191/metrics --connect-timeout 5 --max-time 10 2>/dev/null || echo "")
if echo "$METRICS" | grep -q "worker_jobs_processed_total"; then
  ok "worker_jobs_processed_total metric present"
  echo "$METRICS" | grep "^worker_jobs_processed_total" | head -5 | sed 's/^/  /'
else
  fail "worker metrics endpoint did not return expected metrics"
fi

# ── 6: stats-aggregator ───────────────────────────────────────────────────────
section "Stats aggregator (port-forward :8082)"

kubectl port-forward svc/stats-aggregator 8082:8081 -n jobs >/dev/null 2>&1 &
PF_PIDS+=($!)
sleep 2

STATS=$(curl -s http://localhost:8082/stats --connect-timeout 5 --max-time 10 2>/dev/null || echo "")
if echo "$STATS" | grep -q "total_submitted"; then
  ok "Stats endpoint responding"
  echo "$STATS" | python3 -m json.tool 2>/dev/null | sed 's/^/  /' || echo "  $STATS"
else
  fail "Stats endpoint did not return expected JSON. Got: ${STATS}"
fi

# ── 7: monitoring ─────────────────────────────────────────────────────────────
if [ "$SKIP_MONITORING" = false ]; then
  section "Monitoring stack"

  PROM_PODS=$(kubectl get pods -n monitoring --no-headers 2>/dev/null | grep -c "Running" || echo "0")
  if [ "$PROM_PODS" -gt 0 ]; then
    ok "${PROM_PODS} pods running in namespace: monitoring"
  else
    fail "No pods running in namespace: monitoring"
  fi

  if kubectl get crd servicemonitors.monitoring.coreos.com >/dev/null 2>&1; then
    ok "ServiceMonitor CRD installed"
  else
    fail "ServiceMonitor CRD not found"
  fi

  for sm in worker-monitor stats-monitor; do
    if kubectl get servicemonitor "$sm" -n jobs >/dev/null 2>&1; then
      ok "ServiceMonitor '$sm' exists"
    else
      fail "ServiceMonitor '$sm' not found in namespace jobs"
    fi
  done

  info "To open Grafana:"
  info "  kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring"
  info "  http://localhost:3000  →  admin / admin123"
fi

# ── summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"
if [ "$FAILURES" -eq 0 ]; then
  echo -e "${GREEN}  All checks passed ✓${NC}"
else
  echo -e "${RED}  ${FAILURES} check(s) failed ✗${NC}"
fi
echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"
echo ""

exit "$FAILURES"
