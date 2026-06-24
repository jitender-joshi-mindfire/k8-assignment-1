#!/usr/bin/env bash
# deploy.sh — Full cluster deployment from scratch.
#
# Usage:
#   ./scripts/deploy.sh              # full deploy (build + k8s + monitoring)
#   ./scripts/deploy.sh --skip-build # skip image builds (images already in Minikube)
#   ./scripts/deploy.sh --skip-monitoring  # skip Helm/Prometheus install
#
# Prerequisites:
#   minikube start --cpus=4 --memory=8192
#   minikube addons enable ingress
#   minikube addons enable metrics-server
#   helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
#   helm repo update

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SKIP_BUILD=false
SKIP_MONITORING=false

for arg in "$@"; do
  case $arg in
    --skip-build)      SKIP_BUILD=true ;;
    --skip-monitoring) SKIP_MONITORING=true ;;
  esac
done

# ── colour helpers ─────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; BLUE='\033[0;34m'; NC='\033[0m'
ok()      { echo -e "${GREEN}✓${NC} $*"; }
info()    { echo -e "${YELLOW}→${NC} $*"; }
err()     { echo -e "${RED}✗${NC} $*" >&2; exit 1; }
section() { echo ""; echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"; echo -e "${BLUE}  $*${NC}"; echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"; }

# ── preflight checks ──────────────────────────────────────────────────────────
section "Preflight checks"

command -v minikube >/dev/null 2>&1 || err "minikube not found. Install: https://minikube.sigs.k8s.io/docs/start/"
command -v kubectl  >/dev/null 2>&1 || err "kubectl not found."
command -v docker   >/dev/null 2>&1 || err "docker not found."
if [ "$SKIP_MONITORING" = false ]; then
  command -v helm >/dev/null 2>&1 || err "helm not found. Install: https://helm.sh/docs/intro/install/"
fi

minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running" \
  || err "Minikube is not running. Start it with:\n  minikube start --cpus=4 --memory=8192"

INGRESS_ENABLED=$(minikube addons list --output=json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ingress',{}).get('Status','disabled'))" 2>/dev/null || echo "unknown")
METRICS_ENABLED=$(minikube addons list --output=json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('metrics-server',{}).get('Status','disabled'))" 2>/dev/null || echo "unknown")

if [ "$INGRESS_ENABLED" != "enabled" ]; then
  info "Enabling ingress addon..."
  minikube addons enable ingress
fi
if [ "$METRICS_ENABLED" != "enabled" ]; then
  info "Enabling metrics-server addon (required for HPA)..."
  minikube addons enable metrics-server
fi

ok "Minikube running, addons ready"

# ── step 1: build images inside Minikube daemon ───────────────────────────────
if [ "$SKIP_BUILD" = false ]; then
  section "Step 1 — Build images inside Minikube"

  info "Pointing Docker CLI to Minikube's internal daemon..."
  eval "$(minikube docker-env)"
  ok "Docker env → Minikube"

  info "Building job-submitter:latest..."
  docker build -t job-submitter:latest "${REPO_ROOT}/services/job-submitter" --quiet
  ok "job-submitter:latest built"

  info "Building worker:latest..."
  docker build -t worker:latest "${REPO_ROOT}/services/worker" --quiet
  ok "worker:latest built"

  info "Building stats-aggregator:latest..."
  docker build -t stats-aggregator:latest "${REPO_ROOT}/services/stats-aggregator" --quiet
  ok "stats-aggregator:latest built"
else
  info "Skipping image build (--skip-build)"
fi

# ── step 2: namespace ─────────────────────────────────────────────────────────
section "Step 2 — Namespace"
kubectl apply -f "${REPO_ROOT}/k8s/namespace.yaml"
ok "Namespace 'jobs' ready"

# ── step 3: redis ─────────────────────────────────────────────────────────────
section "Step 3 — Redis"
kubectl apply -f "${REPO_ROOT}/k8s/redis/"
info "Waiting for Redis rollout..."
kubectl rollout status deployment/redis -n jobs --timeout=120s
ok "Redis running"

# ── step 4: core services ─────────────────────────────────────────────────────
section "Step 4 — Core Services"

kubectl apply -f "${REPO_ROOT}/k8s/service-a/"
kubectl apply -f "${REPO_ROOT}/k8s/service-b/"
kubectl apply -f "${REPO_ROOT}/k8s/service-c/"

info "Waiting for job-submitter rollout..."
kubectl rollout status deployment/job-submitter -n jobs --timeout=120s
ok "job-submitter running"

info "Waiting for worker rollout..."
kubectl rollout status deployment/worker -n jobs --timeout=120s
ok "worker running"

info "Waiting for stats-aggregator rollout..."
kubectl rollout status deployment/stats-aggregator -n jobs --timeout=120s
ok "stats-aggregator running"

# ── step 5: monitoring ────────────────────────────────────────────────────────
if [ "$SKIP_MONITORING" = false ]; then
  section "Step 5 — Monitoring (Prometheus + Grafana)"

  # Add Helm repo if not present
  if ! helm repo list 2>/dev/null | grep -q prometheus-community; then
    info "Adding prometheus-community Helm repo..."
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
  fi
  helm repo update --fail-on-repo-update-fail 2>/dev/null || helm repo update

  # Install or upgrade — idempotent
  if helm status prometheus -n monitoring >/dev/null 2>&1; then
    info "Upgrading kube-prometheus-stack (already installed)..."
    helm upgrade prometheus prometheus-community/kube-prometheus-stack \
      --namespace monitoring \
      --values "${REPO_ROOT}/monitoring/prometheus-values.yaml" \
      --wait --timeout=300s
    ok "kube-prometheus-stack upgraded"
  else
    info "Installing kube-prometheus-stack..."
    helm install prometheus prometheus-community/kube-prometheus-stack \
      --namespace monitoring \
      --create-namespace \
      --values "${REPO_ROOT}/monitoring/prometheus-values.yaml" \
      --wait --timeout=300s
    ok "kube-prometheus-stack installed"
  fi

  # ServiceMonitors — CRD now exists after Helm install
  info "Applying ServiceMonitors..."
  kubectl apply -f "${REPO_ROOT}/monitoring/servicemonitor-worker.yaml"
  kubectl apply -f "${REPO_ROOT}/monitoring/servicemonitor-stats.yaml"
  ok "ServiceMonitors applied"

  # Grafana dashboard ConfigMap
  info "Loading Grafana dashboard..."
  kubectl apply -f "${REPO_ROOT}/monitoring/grafana-dashboard-configmap.yaml"
  ok "Dashboard ConfigMap applied"
else
  info "Skipping monitoring (--skip-monitoring)"
fi

# ── summary ───────────────────────────────────────────────────────────────────
section "Deployment Complete"

MINIKUBE_IP=$(minikube ip)

echo ""
ok "All services deployed to namespace: jobs"
echo ""
echo "  Minikube IP : ${MINIKUBE_IP}"
echo ""
echo "  Add to /etc/hosts (run once):"
echo -e "  ${YELLOW}echo '${MINIKUBE_IP} jobs.local' | sudo tee -a /etc/hosts${NC}"
echo ""
echo "  Quick smoke test:"
echo "    curl -X POST http://jobs.local/submit -H 'Content-Type: application/json' -d '{\"type\":\"prime\"}'"
echo "    kubectl get hpa -n jobs"
echo "    kubectl get pods -n jobs"
echo ""
if [ "$SKIP_MONITORING" = false ]; then
  echo "  Grafana:"
  echo "    kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring"
  echo "    open http://localhost:3000   (admin / admin123)"
  echo ""
fi
echo "  Full verification:"
echo "    ./scripts/verify.sh"
