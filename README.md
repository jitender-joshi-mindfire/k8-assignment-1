# Kubernetes Microservices Monitoring — Job Queue System

A production-grade, observable job queue system built with **Go**, **Redis**, **Kubernetes**, **Prometheus**, and **Grafana**. Demonstrates horizontal pod autoscaling (HPA), multi-service observability, and queue-based worker patterns on a local Minikube cluster.

---

## Table of Contents

1. [Assignment Overview](#assignment-overview)
2. [Architecture](#architecture)
3. [Services Overview](#services-overview)
4. [Technology Stack](#technology-stack)
5. [Project Structure](#project-structure)
6. [Prerequisites & Setup](#prerequisites--setup)
7. [Quick Start](#quick-start)
8. [Local Deployment — Step by Step](#local-deployment--step-by-step)
9. [Accessing Services](#accessing-services)
10. [How Monitoring Works](#how-monitoring-works)
11. [Stress Testing & HPA Observation](#stress-testing--hpa-observation)
12. [Observed Scaling Behaviour](#observed-scaling-behaviour)
13. [Cleanup](#cleanup)

---

## Assignment Overview

This project implements the **Kubernetes Microservices Monitoring Assignment** with the following requirements:

| Requirement.                | Implementation                                                              |
|-----------------------------|-----------------------------------------------------------------------------|
| 3 microservices             | Service A (job-submitter), Service B (worker), Service C (stats-aggregator) |
| Queue-based worker model    | Redis `LPUSH` / `BRPOP` pattern                                             |
| Horizontal Pod Autoscaler   | HPA on Service B, min 2 → max 10 pods at CPU > 70%                          |
| Prometheus metrics scraping | ServiceMonitor CRDs for B and C                                             |
| Grafana dashboards          | 11-panel dashboard auto-loaded via ConfigMap sidecar                        |
| Stress testing              | Apache Bench, 5000 requests at 200 concurrency                              |
| Language                    | Go                                                                          |

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────────────-┐
│  Kubernetes Cluster (Minikube)  —  Namespace: jobs                          │
│                                                                             │
│  ┌──────────┐   HTTP    ┌──────────────────┐   LPUSH    ┌───────────────┐   │
│  │  Client  │ ────────► │  Ingress (nginx) │ ─────────► │  Service A    │   │
│  │  /ab     │           │  jobs.local      │            │  job-submitter│   │
│  └──────────┘           └──────────────────┘            │  port 8080    │   │
│                                                         │  1 replica    │   │
│                                  ┌──────────────────────┤               │   │
│                                  │  HSET (metadata)     └───────┬───────┘   │
│                                  │                              │           │
│                                  ▼                              │ LPUSH     │
│                         ┌─────────────────┐                     │           │
│                         │     Redis       │ ◄───────────────────┘           │
│                         │  redis:7-alpine │                                 │
│                         │  port 6379      │                                 │ 
│                         │  job_queue list │                                 │
│                         │  job:* hashes   │                                 │
│                         └────────┬────────┘                                 │
│                                  │  BRPOP                                   │
│                                  ▼                                          │
│                    ┌─────────────────────────────┐                          │
│                    │  Service B  —  worker       │                          │
│                    │  2–10 replicas (HPA)        │                          │
│                    │  port 9090 (metrics only)   │                          │
│                    │                             │                          │
│                    │  prime sieve (10M)          │  HSET result             │
│                    │  bcrypt (cost=14)    ───────┼─────────────► Redis      │
│                    │  sort (500k ints)           │                          │
│                    └─────────────────────────────┘                          │
│                          ▲   CPU > 70%?                                     │
│                          │                                                  │
│                    ┌─────┴──────┐         ┌───────────────────────────┐     │
│                    │    HPA     │         │  Service C                │     │
│                    │  min:2     │         │  stats-aggregator         │     │
│                    │  max:10    │         │  port 8081                │     │
│                    │  cpu: 70%  │         │  polls Redis every 5s     │     │
│                    └────────────┘         │  LLEN + SCAN + pipeline   │     │
│                                           └───────────────────────────┘     │
│                                                        │                    │
│  ┌─────────────────────────────────────────────────────┼──────────────────┐ │
│  │  Namespace: monitoring                              │ scrape /metrics  │ │
│  │                                                     ▼                  │ │
│  │  Prometheus  ◄──── ServiceMonitor (worker:9090, stats-aggregator:8081) │ │
│  │      │                                                                 │ │
│  │      ▼                                                                 │ │
│  │  Grafana  (11-panel dashboard — auto-loaded via ConfigMap sidecar)     │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow — One Job Lifecycle

```
1.  Client          POST /submit {"type":"bcrypt"}
2.  Service A       Validate → UUID → HSET job:<id> status:pending → LPUSH job_queue
3.  Service A       Return 202 {"job_id":"<uuid>","status":"queued"}
4.  Service B       BRPOP job_queue (blocks up to 5s) → picks up job atomically
5.  Service B       Execute CPU-bound work (bcrypt cost=14 ≈ 2–4s)
6.  Service B       HSET job:<id> status:done result:"..." duration_ms:2340
7.  Client          GET /status/<id> → Service A → HGETALL job:<id> → 200 + result
8.  Service C       LLEN job_queue + SCAN job:* pipeline → update in-memory Snapshot
9.  Prometheus      Scrapes :9090/metrics and :8081/metrics every 15s
10. Grafana         Renders dashboard: queue depth, CPU, replicas, throughput, latency
```

---

## Services Overview

### Service A — `job-submitter` (HTTP API Gateway)

| Property | Value                                                             |
|----------|-------------------------------------------------------------------|
| Port     | 8080                                                              |
| Replicas | 1 (not scaled)                                                    |
| Image    | `job-submitter:latest` (~6 MB)                                    |
| Role     | Accepts job submissions, enqueues to Redis, serves status queries |

**Endpoints:**

| Method |     Path      | Description                                                                      |
|--------|---------------|----------------------------------------------------------------------------------|
| `POST` | `/submit`     | Submit a job. Body: `{"type":"prime"\|"bcrypt"\|"sort"}`. Returns 202 + `job_id` |
| `GET`  | `/status/:id` | Poll job status. Returns `pending` / `processing` / `done` / `error`             |
| `GET`  | `/metrics`    | Prometheus metrics                                                               | 
| `GET`  | `/healthz`    | Liveness probe (always 200 if process alive)                                     |
| `GET`  | `/readyz`     | Readiness probe (503 if Redis unreachable)                                       |

**Key design decisions:**
- Returns `202 Accepted` immediately — does not wait for the job to execute
- `HSET` metadata **before** `LPUSH` so `/status` never returns 404 on a valid job
- Uses `go-chi/chi/v5` router with 5 middleware layers (RequestID, RealIP, Logger, Recoverer, Timeout)

**Prometheus metrics:**
```
http_requests_total{method, route, status_class}   Counter
jobs_submitted_total{type}                          Counter
```

---

### Service B — `worker` (CPU-Bound Job Processor)

| Property | Value                                                                    |
|----------|--------------------------------------------------------------------------|
| Port     | 9090 (metrics only — no job HTTP endpoints)                              |
| Replicas | 2–10 (HPA controlled)                                                    |
| Image    | `worker:latest` (~5.8 MB)                                                |
| Role     | Pulls jobs from Redis queue, executes CPU-intensive work, writes results |

**Job types:**

| Type     | Implementation                       | Duration (in K8s) | CPU impact |
|----------|--------------------------------------|-------------------|------------|
| `prime`  | Sieve of Eratosthenes to 10,000,000  | ~150ms            | Medium     |
| `bcrypt` | `golang.org/x/crypto/bcrypt` cost=14 | ~2–4s             |  **High**  |
| `sort`   | Sort 500,000 int64s                  | ~50ms             | Low-Medium |

> **Why bcrypt cost=14?** The assignment uses bcrypt specifically because it is intentionally slow and CPU-bound. Cost=14 means 2^14 = 16,384 rounds. A single bcrypt hash takes ~2–4s — enough to keep worker CPUs pegged above 70% during load, triggering the HPA.

> **Why prime to 10M and sort to 500k?** Go processes the original assignment values (prime 100k, sort 100k) in under 1ms — too fast to trigger HPA. Sizes were tuned so each job takes long enough to saturate CPU when many jobs run concurrently.

**Queue mechanics:**
- `BRPOP job_queue 5` — blocks up to 5 seconds waiting for work, then loops
- Atomic pop: Redis guarantees exactly one worker receives each job
- `terminationGracePeriodSeconds: 60` — gives in-flight jobs time to complete before pod is killed

**Prometheus metrics:**
```
worker_jobs_processed_total{type, status}              Counter
worker_job_processing_time_seconds{type}               Histogram  [0.1,0.5,1,2,5,10,30]
worker_job_errors_total{type}                          Counter
worker_active_jobs                                     Gauge
```

---

### Service C — `stats-aggregator` (Read Model / Stats API)

| Property | Value |
|---|---|
| Port | 8081 |
| Replicas | 1 |
| Image | `stats-aggregator:latest` (~6 MB) |
| Role | Polls Redis for aggregate stats, exposes JSON + Prometheus metrics |

**Endpoints:**

| Method | Path | Description |
|---|---|---|
| `GET` | `/stats` | Current system stats as JSON |
| `GET` | `/metrics` | Prometheus gauges |
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe (503 if Redis unavailable or snapshot stale) |

**`/stats` response:**
```json
{
  "queue_length": 0,
  "total_submitted": 5002,
  "total_completed": 5002,
  "total_errors": 0,
  "in_flight": 0,
  "avg_processing_time_seconds": 4.03,
  "data_age_seconds": 2.1
}
```

**Key design decisions:**
- HTTP handlers **never touch Redis** — they read only from an in-memory `Snapshot` (RWMutex protected)
- Background goroutine polls Redis every 5s (configurable via `COLLECT_INTERVAL_SECS`)
- Uses **`SCAN` not `KEYS`** — KEYS blocks Redis during iteration; SCAN is cursor-based and non-blocking
- Uses **Redis pipeline** for `HMGET` — batches all field reads in one round-trip instead of N individual calls (critical for 5000+ job keys under load)

**Prometheus metrics:**
```
statsaggregator_total_jobs_submitted      Gauge
statsaggregator_total_jobs_completed      Gauge
statsaggregator_queue_length              Gauge
statsaggregator_total_job_errors          Gauge
statsaggregator_avg_processing_time_seconds  Gauge
statsaggregator_collector_duration_seconds   Gauge
```

---

### Redis

| Property | Value |
|---|---|
| Image | `redis:7-alpine` |
| Port | 6379 (ClusterIP — internal only) |
| Storage | `emptyDir` (ephemeral, acceptable for assignment) |
| Memory policy | `allkeys-lru` at 200MB limit |

**Data structures used:**

| Key pattern | Type | Written by | Read by |
|---|---|---|---|
| `job_queue` | List | Service A (`LPUSH`) | Service B (`BRPOP`) |
| `job:<uuid>` | Hash | Service A + B (`HSET`) | Service A (`HGETALL`), Service C (`HMGET` pipeline) |

**Redis hash fields per job:**
```
status       pending → processing → done | error
type         prime | bcrypt | sort
submitted_at unix timestamp (ms)
result       output string (set by worker on completion)
duration_ms  processing time in milliseconds (set by worker)
```

---

## Technology Stack

### Core Services

| Technology | Version | Purpose |
|---|---|---|
| **Go** | 1.26 | Service language (client override from Node.js) |
| **go-chi/chi** | v5.3.0 | HTTP router for Services A and C |
| **redis/go-redis** | v9.21.0 | Redis client for all three services |
| **prometheus/client_golang** | v1.23.2 | Prometheus metrics instrumentation |
| **golang.org/x/crypto** | v0.53.0 | bcrypt for CPU-bound job simulation |
| **google/uuid** | v1.6.0 | Job ID generation |

### Infrastructure

| Technology | Version | Purpose |
|---|---|---|
| **Docker** | 27+ | Container runtime and image builds |
| **Minikube** | 1.35+ | Local Kubernetes cluster |
| **Kubernetes** | 1.32+ | Container orchestration |
| **Redis** | 7-alpine | Job queue and state store |
| **Helm** | 3.x | Deploy kube-prometheus-stack |

### Observability

| Technology | Version | Purpose |
|---|---|---|
| **Prometheus** | (via kube-prometheus-stack) | Metrics scraping and storage |
| **Grafana** | (via kube-prometheus-stack) | Dashboards and visualisation |
| **kube-prometheus-stack** | Helm chart | Prometheus Operator, Grafana, kube-state-metrics, node-exporter |
| **ServiceMonitor CRD** | monitoring.coreos.com/v1 | Declarative scrape target configuration |
| **kube-state-metrics** | (included) | K8s resource metrics (HPA replica counts, pod states) |

### Build & Deployment

| Technology | Purpose |
|---|---|
| **Docker multi-stage builds** | `golang:alpine` builder → `distroless/static-debian12` runtime (~6MB images) |
| **Apache Bench (ab)** | HTTP load generation for stress testing |
| `scripts/build-images.sh` | Build all 3 images into Minikube's Docker daemon |
| `scripts/deploy.sh` | Full cluster deployment (idempotent) |
| `scripts/verify.sh` | Post-deploy smoke test suite |
| `scripts/stress-test.sh` | Stress test with HPA observation |

---

## Project Structure

```
kubernetes-project/
├── services/
│   ├── job-submitter/              # Service A
│   │   ├── main.go                 # HTTP server, chi router, graceful shutdown
│   │   ├── handler/
│   │   │   ├── submit.go           # POST /submit — validates, enqueues, returns 202
│   │   │   └── status.go           # GET /status/:id — HGETALL from Redis
│   │   ├── queue/
│   │   │   └── redis.go            # PushJob (HSET+LPUSH), GetJobStatus (HGETALL)
│   │   ├── metrics/
│   │   │   └── metrics.go          # Private Prometheus registry, counters
│   │   ├── Dockerfile              # Multi-stage build → distroless
│   │   ├── .dockerignore
│   │   ├── go.mod
│   │   └── go.sum
│   │
│   ├── worker/                     # Service B
│   │   ├── main.go                 # Consumer loop + metrics HTTP server goroutines
│   │   ├── consumer/
│   │   │   └── redis.go            # BRPOP loop, dispatches jobs, HSET results
│   │   ├── jobs/
│   │   │   ├── prime.go            # Sieve of Eratosthenes (limit: 10,000,000)
│   │   │   ├── bcrypt.go           # bcrypt hash (cost: 14)
│   │   │   ├── sort.go             # Sort 500,000 int64s
│   │   │   └── dispatcher.go       # Switch dispatch by job type
│   │   ├── metrics/
│   │   │   └── metrics.go          # Counter, Histogram, Gauge — private registry
│   │   ├── Dockerfile
│   │   ├── .dockerignore
│   │   ├── go.mod
│   │   └── go.sum
│   │
│   └── stats-aggregator/           # Service C
│       ├── main.go                 # HTTP server, background collector, graceful shutdown
│       ├── collector/
│       │   └── redis.go            # LLEN + SCAN + pipeline HMGET, Snapshot, RWMutex
│       ├── handler/
│       │   └── stats.go            # GET /stats — reads from in-memory Store only
│       ├── metrics/
│       │   └── metrics.go          # 6 Prometheus Gauges — private registry
│       ├── Dockerfile
│       ├── .dockerignore
│       ├── go.mod
│       └── go.sum
│
├── k8s/
│   ├── namespace.yaml              # Namespace: jobs
│   ├── redis/
│   │   ├── deployment.yaml         # redis:7-alpine, emptyDir, PING probes
│   │   └── service.yaml            # ClusterIP :6379
│   ├── service-a/
│   │   ├── deployment.yaml         # 1 replica, initContainer: wait-for-redis
│   │   ├── service.yaml            # ClusterIP :8080
│   │   └── ingress.yaml            # nginx, jobs.local → /submit /status /healthz
│   ├── service-b/
│   │   ├── deployment.yaml         # 2 replicas, cpu requests:200m, terminationGrace:60s
│   │   ├── service.yaml            # ClusterIP :9090, portName: metrics
│   │   └── hpa.yaml                # autoscaling/v2, min:2 max:10 cpu:70%
│   └── service-c/
│       ├── deployment.yaml         # 1 replica, COLLECT_INTERVAL_SECS: 5
│       └── service.yaml            # ClusterIP :8081
│
├── monitoring/
│   ├── prometheus-values.yaml          # Helm values: sidecar, namespace selector
│   ├── servicemonitor-worker.yaml      # Scrape Service B :9090/metrics
│   ├── servicemonitor-stats.yaml       # Scrape Service C :8081/metrics
│   ├── grafana-dashboard.json          # Standalone dashboard JSON (11 panels)
│   └── grafana-dashboard-configmap.yaml # Same JSON as ConfigMap (auto-loaded)
│
├── scripts/
│   ├── build-images.sh             # Build all images → Minikube daemon
│   ├── deploy.sh                   # Full cluster deploy (idempotent)
│   ├── verify.sh                   # Smoke test suite (7 checks)
│   └── stress-test.sh              # ab load test + HPA watcher
│
├── IMPLEMENTATION_PLAN.md          # Full 9-phase implementation plan
└── README.md                       # This file
```

---

## Prerequisites & Setup

### Required Tools

```bash
# 1. Go (1.21+)
brew install go
go version                          # go version go1.21+

# 2. Docker (with Colima or Docker Desktop)
brew install colima docker
colima start --cpu 4 --memory 8     # or open Docker Desktop
docker info                         # verify daemon is running

# 3. Minikube
brew install minikube
minikube version                    # minikube v1.35+

# 4. kubectl
brew install kubectl
kubectl version --client

# 5. Helm
brew install helm
helm version                        # v3.x

# 6. Apache Bench (for stress testing)
brew install httpd                  # installs ab
ab -V
```

### Start Minikube

```bash
# Start with sufficient resources for all services + monitoring stack
minikube start --cpus=4 --memory=8192

# Enable required addons
minikube addons enable ingress          # nginx ingress controller
minikube addons enable metrics-server   # required for HPA CPU metrics

# Verify addons
minikube addons list | grep -E "ingress|metrics-server"
```

### Add Helm Repository

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

---

## Quick Start

Five commands to go from zero to a running cluster:

```bash
git clone <repo-url>
cd kubernetes-project

# 1. Build + deploy everything
./scripts/deploy.sh

# 2. Verify the cluster
./scripts/verify.sh

# 3. Open Grafana
kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring &
open http://localhost:3000   # admin / admin123

# 4. Submit a test job
kubectl port-forward svc/job-submitter 8080:8080 -n jobs &
curl -X POST http://localhost:8080/submit \
  -H "Content-Type: application/json" \
  -d '{"type":"prime"}'

# 5. Run the stress test
./scripts/stress-test.sh
```

---

## Local Deployment — Step by Step

### Step 1 — Build Docker Images

```bash
# Build all 3 images and load them into Minikube's internal Docker daemon
./scripts/build-images.sh

# Verify images are available inside Minikube
eval "$(minikube docker-env)"
docker images | grep -E "job-submitter|worker|stats-aggregator"

# Expected output:
# job-submitter      latest   ...   6MB
# worker             latest   ...   5.8MB
# stats-aggregator   latest   ...   6MB
```

> **Why `eval $(minikube docker-env)`?**
> Minikube runs its own Docker daemon inside the VM. This command re-points your local `docker` CLI at that daemon so images you build are immediately available to Kubernetes pods — no registry push needed. `imagePullPolicy: Never` in all Deployments tells Kubernetes to use the locally-present image.

### Step 2 — Deploy to Kubernetes

```bash
# Full deploy (builds images, applies all YAMLs, installs Helm chart)
./scripts/deploy.sh

# Or step by step:
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/redis/
kubectl rollout status deployment/redis -n jobs

kubectl apply -f k8s/service-a/
kubectl apply -f k8s/service-b/
kubectl apply -f k8s/service-c/
kubectl rollout status deployment/job-submitter -n jobs
kubectl rollout status deployment/worker -n jobs
kubectl rollout status deployment/stats-aggregator -n jobs

# Install Prometheus + Grafana
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --values monitoring/prometheus-values.yaml \
  --wait --timeout=300s

# Apply ServiceMonitors (after CRDs exist)
kubectl apply -f monitoring/servicemonitor-worker.yaml
kubectl apply -f monitoring/servicemonitor-stats.yaml
kubectl apply -f monitoring/grafana-dashboard-configmap.yaml
```

### Step 3 — Verify Deployment

```bash
./scripts/verify.sh

# Expected: All checks passed ✓
#
# ── Pod health (namespace: jobs) ──
# ✓ redis pods Ready (1/1)
# ✓ job-submitter pods Ready (1/1)
# ✓ worker pods Ready (1/1)
# ✓ stats-aggregator pods Ready (1/1)
#
# ── HPA status ──
# ✓ HPA TARGETS: 1%/70%   ← not <unknown> = metrics-server working
#
# ── End-to-end job flow ──
# ✓ Job submitted — ID: acf1854f-...
# ✓ Job completed successfully
# ✓ Result: found 664579 primes up to 10000000
```

---

## Accessing Services

All services are `ClusterIP` (internal only). Access them from your host machine via `kubectl port-forward`:

```bash
# Service A — job submission API
kubectl port-forward svc/job-submitter 8080:8080 -n jobs
# → http://localhost:8080/submit
# → http://localhost:8080/status/<id>

# Service B — worker Prometheus metrics
kubectl port-forward svc/worker 9090:9090 -n jobs
# → http://localhost:9090/metrics

# Service C — aggregate stats
kubectl port-forward svc/stats-aggregator 8081:8081 -n jobs
# → http://localhost:8081/stats
# → http://localhost:8081/metrics

# Prometheus UI
kubectl port-forward svc/prometheus-kube-prometheus-prometheus 9091:9090 -n monitoring
# → http://localhost:9091

# Grafana
kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring
# → http://localhost:3000  (admin / admin123)
```

### Submit and track a job

```bash
# Submit a prime job
curl -X POST http://localhost:8080/submit \
  -H "Content-Type: application/json" \
  -d '{"type":"prime"}'
# → {"job_id":"acf1854f-...","status":"queued"}

# Poll status
curl http://localhost:8080/status/acf1854f-...
# → {"job_id":"...","status":"processing"}
# → {"job_id":"...","status":"done","result":"found 664579 primes up to 10000000","duration_ms":148}

# Check aggregate stats
curl http://localhost:8081/stats | python3 -m json.tool
```

---

## How Monitoring Works

### The Full Monitoring Pipeline

```
Go Service (worker / stats-aggregator)
  │
  │  prometheus/client_golang registers metrics in a private Registry
  │  (not the default global registry — avoids contamination from
  │   Go runtime metrics in shared binaries)
  │
  ▼
GET /metrics  →  promhttp.HandlerFor(registry, ...)
  │
  │  Prometheus text format:
  │  worker_jobs_processed_total{status="success",type="prime"} 42
  │  worker_job_processing_time_seconds_bucket{type="bcrypt",le="5"} 38
  │
  ▼
ServiceMonitor (monitoring.coreos.com/v1)
  │  Deployed in namespace: jobs
  │  Labels: release: prometheus  ← must match Helm release name
  │  spec.selector.matchLabels: app: worker
  │  spec.endpoints[0].port: metrics  ← matches Service portName
  │
  ▼
Prometheus Operator
  │  Watches for ServiceMonitor CRDs across all namespaces
  │  (enabled by: serviceMonitorSelectorNilUsesHelmValues: false)
  │  Generates scrape configs and injects them into Prometheus
  │
  ▼
Prometheus
  │  Scrapes :9090/metrics and :8081/metrics every 15s
  │  Stores time-series in TSDB (15-day retention)
  │
  ▼
Grafana
   Sidecar container watches for ConfigMaps labelled grafana_dashboard:"1"
   Auto-loads grafana-dashboard-configmap.yaml  →  11-panel dashboard
   Queries Prometheus via PromQL
```

### Prometheus Metrics — Complete Reference

#### Service B (worker) — scraped at `:9090/metrics`

| Metric | Type | Labels | What it measures |
|---|---|---|---|
| `worker_jobs_processed_total` | Counter | `type`, `status` | Jobs completed (success or error) |
| `worker_job_processing_time_seconds` | Histogram | `type` | Duration per job — enables p50/p99 |
| `worker_job_errors_total` | Counter | `type` | Failed jobs by type |
| `worker_active_jobs` | Gauge | — | Currently executing jobs |

Histogram buckets: `[0.1, 0.5, 1, 2, 5, 10, 30]` seconds — designed around bcrypt (2–4s) and prime (~150ms).

#### Service C (stats-aggregator) — scraped at `:8081/metrics`

| Metric | Type | What it measures |
|---|---|---|
| `statsaggregator_total_jobs_submitted` | Gauge | Total job hashes found in Redis |
| `statsaggregator_total_jobs_completed` | Gauge | Jobs with status=done |
| `statsaggregator_queue_length` | Gauge | Current `LLEN job_queue` |
| `statsaggregator_total_job_errors` | Gauge | Jobs with status=error |
| `statsaggregator_avg_processing_time_seconds` | Gauge | Rolling average of duration_ms |
| `statsaggregator_collector_duration_seconds` | Gauge | How long the last Redis poll took |

#### kube-state-metrics (auto-collected by Prometheus)

| Metric | What it measures |
|---|---|
| `kube_deployment_status_replicas_ready` | Ready pods per Deployment |
| `kube_horizontalpodautoscaler_status_current_replicas` | Current HPA replica count |
| `kube_horizontalpodautoscaler_status_desired_replicas` | Desired HPA replica count |
| `kube_horizontalpodautoscaler_spec_max_replicas` | HPA max replicas |

### Grafana Dashboard — 11 Panels

Open at `http://localhost:3000` → Dashboards → "Job Queue — Worker Autoscaling"

| Panel | Type | PromQL | What to watch during stress test |
|---|---|---|---|
| Worker Pod Count | Stat | `kube_deployment_status_replicas_ready{deployment="worker"}` | Climbs from 2 to 10 |
| HPA Replica Target vs Current | Time series | `kube_horizontalpodautoscaler_status_*` | Desired > current during scale-up lag |
| Queue Length | Stat | `statsaggregator_queue_length` | Spikes to 5000, then drains |
| In-Flight Jobs | Stat | `worker_active_jobs` | Active across all pods |
| CPU per Worker Pod | Time series | `rate(container_cpu_usage_seconds_total[1m])` | Each pod line spikes |
| Memory per Worker Pod | Time series | `container_memory_working_set_bytes` | Flat (memory is not the trigger) |
| Jobs Processed Rate | Time series | `rate(worker_jobs_processed_total[1m])` | Increases as more workers come online |
| P99 / P50 Processing Time | Time series | `histogram_quantile(0.99/0.50, ...)` | bcrypt p99 ≈ 4s, prime p50 ≈ 150ms |
| Total Submitted vs Completed | Time series | `statsaggregator_total_jobs_*` | Gap = backlog in queue |
| Error Rate | Time series | `rate(worker_job_errors_total[1m])` | Should be 0 |
| Avg Processing Time | Gauge | `statsaggregator_avg_processing_time_seconds` | ~4s during bcrypt load |

### Docker — How Images Are Built

Each service uses a **two-stage Docker build**:

```dockerfile
# Stage 1 — Builder (~400MB, discarded after build)
FROM golang:alpine AS builder
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download                        # cached layer
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -trimpath -o service .
#            └── strip debug symbols       └── reproducible builds

# Stage 2 — Runtime (~6MB, what actually ships)
FROM gcr.io/distroless/static-debian12     # no shell, no libc, no package manager
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/service /service
USER nonroot:nonroot                        # uid 65532 — never runs as root
ENTRYPOINT ["/service"]
```

| Build flag | Effect |
|---|---|
| `CGO_ENABLED=0` | Pure Go binary — no C dependencies — runs in distroless (no libc) |
| `GOOS=linux GOARCH=amd64` | Cross-compile from macOS to Linux x86-64 |
| `-ldflags="-s -w"` | Strip debug symbols and DWARF — ~30% size reduction |
| `-trimpath` | Remove local filesystem paths from the binary |
| `distroless/static` | No shell, no package manager — minimal attack surface |

---

## Stress Testing & HPA Observation

### Run the Stress Test

```bash
./scripts/stress-test.sh
# Sends 5000 POST /submit (bcrypt) at 200 concurrency
# Watches HPA every 10s
# Polls queue drain for 3 minutes after ab completes

# Custom parameters:
./scripts/stress-test.sh -n 2000 -c 50     # lighter load
./scripts/stress-test.sh --prime            # use prime jobs instead
./scripts/stress-test.sh --watch-only       # just watch HPA (no load)
```

### Why bcrypt triggers HPA

The stress test uses **bcrypt** (default) because:
1. Each bcrypt job takes 2–4 seconds of pure CPU time
2. 5000 bcrypt jobs queued → 2 workers each running bcrypt → CPU > 200% of their 200m request
3. HPA observes `averageUtilization > 70%` → triggers scale-up
4. More workers come online → drain the queue faster → CPU drops → scale-down after 120s

### What to Watch

Open these in parallel during the stress test:

```bash
# Terminal 1: run the test
./scripts/stress-test.sh

# Terminal 2: watch HPA in real time
kubectl get hpa worker -n jobs --watch

# Terminal 3: watch pods scaling
kubectl get pods -n jobs -l app=worker --watch

# Terminal 4: open Grafana
kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring
open http://localhost:3000
```

### Expected HPA Progression

```
TIME      CPU        REPLICAS   EVENT
baseline  4%/70%     2          idle
+15s      147%/70%   5          first scale-up (15s stabilization window)
+60s      390%/70%   9          second scale-up
+75s      390%/70%   10         at maximum
peak      390%/70%   10         all 10 pods draining bcrypt queue
drain     210%/70%   10         queue shrinking, CPU dropping
+120s     50%/70%    10         (scale-down window starts)
+240s     10%/70%    2          scaled back down
```

---

## Observed Scaling Behaviour

Results from the actual stress test run (5000 bcrypt jobs, 200 concurrency):

| Metric | Observed Value |
|---|---|
| ab throughput | 770 req/s (submit endpoint) |
| ab completion time | 6.5 seconds (submit phase) |
| ab failed requests | **0** |
| Peak CPU utilisation | **390%** of 200m request (= 780m per pod) |
| Max worker replicas reached | **10 / 10** |
| Time to reach max replicas | ~75 seconds after queue fill |
| Scale-up stabilisation window | 15s (configured in HPA) |
| Scale-down stabilisation window | 120s (configured in HPA) |
| Total jobs processed without error | **5002** |
| Avg bcrypt job processing time | ~4.0 seconds |
| stats-aggregator cycle time (idle) | ~320ms (pipelined HMGET) |

**Key observation:** Submit throughput (770 req/s) far exceeds worker throughput (~0.25 bcrypt/s per pod). This is by design — the system demonstrates backpressure: the queue fills fast, workers drain slowly, HPA responds to the CPU signal, and 10 workers together drain the queue ~5× faster than 2.

---

## Cleanup

```bash
# Delete all application resources
kubectl delete namespace jobs

# Uninstall Prometheus + Grafana
helm uninstall prometheus -n monitoring
kubectl delete namespace monitoring

# Stop Minikube (keeps cluster state)
minikube stop

# Delete Minikube cluster entirely (full reset)
minikube delete

# Remove /etc/hosts entry (if added)
sudo sed -i '' '/jobs.local/d' /etc/hosts
```

---

## Environment Variables Reference

| Service | Variable | Default | Description |
|---|---|---|---|
| A, B, C | `REDIS_ADDR` | `redis:6379` | Redis server address |
| A | `PORT` | `8080` | HTTP listen port |
| B | `METRICS_PORT` | `9090` | Metrics HTTP listen port |
| C | `PORT` | `8081` | HTTP listen port |
| C | `COLLECT_INTERVAL_SECS` | `15` | Redis poll interval (set to 5 in K8s deployment) |

---

## Kubernetes Resources Reference

```bash
# All resources in the jobs namespace
kubectl get all -n jobs

# HPA details
kubectl describe hpa worker -n jobs

# Service logs
kubectl logs deployment/job-submitter -n jobs --tail=50
kubectl logs deployment/worker -n jobs --tail=50
kubectl logs deployment/stats-aggregator -n jobs --tail=50

# Prometheus targets (verify scraping is UP)
kubectl port-forward svc/prometheus-kube-prometheus-prometheus 9091:9090 -n monitoring
open http://localhost:9091/targets

# Grafana dashboard
kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring
open http://localhost:3000   # admin / admin123
```
