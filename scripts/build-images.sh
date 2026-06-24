#!/usr/bin/env bash
# build-images.sh — Build all three service images and load them into Minikube.
#
# Usage:
#   ./scripts/build-images.sh              # build all, load into Minikube
#   ./scripts/build-images.sh --local      # build all, keep in local Docker only
#   ./scripts/build-images.sh --no-cache   # force full rebuild (no layer cache)
#
# Prerequisites:
#   • Docker running
#   • Minikube running (unless --local flag used)
#   • Called from the repo root: kubernetes-project/

set -euo pipefail

# ── config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVICES_DIR="${REPO_ROOT}/services"

LOCAL_ONLY=false
NO_CACHE=""

for arg in "$@"; do
  case $arg in
    --local)    LOCAL_ONLY=true ;;
    --no-cache) NO_CACHE="--no-cache" ;;
  esac
done

# ── colour helpers ─────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

ok()   { echo -e "${GREEN}✓${NC} $*"; }
info() { echo -e "${YELLOW}→${NC} $*"; }
err()  { echo -e "${RED}✗${NC} $*" >&2; }

# ── Minikube Docker environment ───────────────────────────────────────────────
# `eval $(minikube docker-env)` re-points the local docker CLI to the Docker
# daemon running INSIDE Minikube. Images built this way are immediately
# available to Kubernetes pods without pushing to a registry — just set
# imagePullPolicy: Never in the Deployment.
#
# Why not push to a registry?
#   For local development and assignment purposes this is simpler and faster.
#   In production you'd push to ECR/GCR/Docker Hub and use imagePullSecrets.
if [ "$LOCAL_ONLY" = false ]; then
  if ! minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
    err "Minikube is not running. Start it with: minikube start --cpus=4 --memory=8192"
    err "Or build for local Docker only with: $0 --local"
    exit 1
  fi
  info "Pointing Docker CLI to Minikube's internal daemon..."
  eval "$(minikube docker-env)"
  ok "Docker env set to Minikube"
else
  info "Building for local Docker daemon (--local mode, Minikube not required)"
fi

# ── build helper ──────────────────────────────────────────────────────────────
build_image() {
  local NAME=$1
  local DIR=$2
  local PORT=$3

  info "Building ${NAME}:latest (context: ${DIR})"
  docker build \
    ${NO_CACHE} \
    --tag "${NAME}:latest" \
    --label "build-date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --label "service=${NAME}" \
    "${DIR}"

  # Show the final image size — useful to confirm the multi-stage build is
  # working (distroless final image should be < 20MB).
  local SIZE
  SIZE=$(docker image inspect "${NAME}:latest" \
    --format='{{.Size}}' | awk '{printf "%.1f MB", $1/1024/1024}')
  ok "${NAME}:latest — ${SIZE} (port ${PORT})"
}

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  Phase 4: Building Docker images"
echo "══════════════════════════════════════════════════════════════"
echo ""

START=$(date +%s)

build_image "job-submitter"    "${SERVICES_DIR}/job-submitter"    "8080"
echo ""
build_image "worker"           "${SERVICES_DIR}/worker"           "9090"
echo ""
build_image "stats-aggregator" "${SERVICES_DIR}/stats-aggregator" "8081"

END=$(date +%s)
ELAPSED=$((END - START))

echo ""
echo "══════════════════════════════════════════════════════════════"
ok "All images built in ${ELAPSED}s"
echo ""

# ── summary ───────────────────────────────────────────────────────────────────
echo "Images available:"
docker images --filter "reference=job-submitter" \
              --filter "reference=worker" \
              --filter "reference=stats-aggregator" \
              --format "  {{.Repository}}:{{.Tag}}  {{.Size}}  ({{.CreatedSince}})"

if [ "$LOCAL_ONLY" = false ]; then
  echo ""
  info "Images are loaded into Minikube's Docker daemon."
  info "Use imagePullPolicy: Never in Kubernetes Deployments."
  info "Next: kubectl apply -f k8s/"
fi
