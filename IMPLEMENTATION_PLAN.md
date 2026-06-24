# Kubernetes Microservices Monitoring — Implementation Plan
**Stack:** Go · Redis · Kubernetes · Prometheus · Grafana  
**Target environment:** Minikube (local) / Kind  
**Assignment source:** Kubernetes Microservices Monitoring Assignment (June 2026)

---

## Table of Contents

1. [Project Structure](#1-project-structure)
2. [Phase 0 — Prerequisites & Local Tooling](#2-phase-0--prerequisites--local-tooling)
3. [Phase 1 — Go Service: job-submitter (Service A)](#3-phase-1--go-service-job-submitter-service-a)
4. [Phase 2 — Go Service: worker (Service B)](#4-phase-2--go-service-worker-service-b)
5. [Phase 3 — Go Service: stats-aggregator (Service C)](#5-phase-3--go-service-stats-aggregator-service-c)
6. [Phase 4 — Docker Images](#6-phase-4--docker-images)
7. [Phase 5 — Kubernetes Infrastructure YAMLs](#7-phase-5--kubernetes-infrastructure-yamls)
8. [Phase 6 — Prometheus & Grafana Setup](#8-phase-6--prometheus--grafana-setup)
9. [Phase 7 — Local Cluster Deployment (Minikube)](#9-phase-7--local-cluster-deployment-minikube)
10. [Phase 8 — Stress Testing](#10-phase-8--stress-testing)
11. [Phase 9 — README & Documentation](#11-phase-9--readme--documentation)
12. [Dependency Map](#12-dependency-map)
13. [Key Design Decisions](#13-key-design-decisions)

---

## 1. Project Structure

```
kubernetes-project/
│
├── services/
│   ├── job-submitter/              # Service A — API Gateway
│   │   ├── main.go
│   │   ├── handler/
│   │   │   ├── submit.go           # POST /submit
│   │   │   └── status.go           # GET /status/:id
│   │   ├── queue/
│   │   │   └── redis.go            # Redis LPUSH producer
│   │   ├── metrics/
│   │   │   └── metrics.go          # Prometheus counters for HTTP
│   │   ├── Dockerfile
│   │   ├── go.mod
│   │   └── go.sum
│   │
│   ├── worker/                     # Service B — Scalable Worker
│   │   ├── main.go
│   │   ├── consumer/
│   │   │   └── redis.go            # BRPOP loop
│   │   ├── jobs/
│   │   │   ├── dispatcher.go       # routes job type to processor
│   │   │   ├── prime.go            # Sieve of Eratosthenes
│   │   │   ├── bcrypt.go           # bcrypt hashing
│   │   │   └── sort.go             # generate + sort large int slice
│   │   ├── metrics/
│   │   │   └── metrics.go          # jobs_processed_total, histogram, errors
│   │   ├── Dockerfile
│   │   ├── go.mod
│   │   └── go.sum
│   │
│   └── stats-aggregator/           # Service C — Stats & Aggregator
│       ├── main.go
│       ├── handler/
│       │   └── stats.go            # GET /stats
│       ├── collector/
│       │   └── redis.go            # background Redis polling goroutine
│       ├── metrics/
│       │   └── metrics.go          # total_jobs_submitted/completed, queue_length
│       ├── Dockerfile
│       ├── go.mod
│       └── go.sum
│
├── k8s/
│   ├── namespace.yaml
│   ├── redis/
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   └── configmap.yaml
│   ├── service-a/
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   └── ingress.yaml
│   ├── service-b/
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   └── hpa.yaml
│   └── service-c/
│       ├── deployment.yaml
│       └── service.yaml
│
├── monitoring/
│   ├── servicemonitor-b.yaml
│   ├── servicemonitor-c.yaml
│   └── grafana-dashboard.json
│
├── scripts/
│   ├── build-images.sh             # build all 3 Docker images
│   ├── deploy.sh                   # kubectl apply in correct order
│   └── stress-test.sh              # ab load test wrapper
│
├── IMPLEMENTATION_PLAN.md          # this file
└── README.md
```

---

## 2. Phase 0 — Prerequisites & Local Tooling

### Goal
Ensure all local tools are installed and configured before writing a single line of code.

### Checklist

| Tool | Version | Purpose |
|------|---------|---------|
| Go | 1.22+ | Build all 3 services |
| Docker Desktop | Latest | Build & load images into Minikube |
| Minikube | 1.32+ | Local K8s cluster |
| kubectl | 1.29+ | Apply YAMLs, inspect pods |
| Helm | 3.14+ | Install Prometheus stack |
| Apache Bench (`ab`) | any | Stress testing |

### Minikube Addons Required
```
minikube addons enable ingress          # nginx ingress controller
minikube addons enable metrics-server   # required for HPA CPU metrics
```

### Helm Repos Required
```
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```

### Deliverable
- All tools verified: `go version`, `docker info`, `minikube version`, `helm version`
- Minikube running with addons enabled

---

## 3. Phase 1 — Go Service: job-submitter (Service A)

### Goal
Build a lightweight HTTP API that accepts job submissions, pushes them to Redis, and allows status polling.

### Dependencies
```
go get github.com/go-chi/chi/v5          # HTTP router
go get github.com/redis/go-redis/v9      # Redis client
go get github.com/prometheus/client_golang/prometheus
go get github.com/prometheus/client_golang/prometheus/promhttp
go get github.com/google/uuid
```

### Files & Responsibilities

#### `main.go`
- Initialize Redis client (address from `REDIS_ADDR` env var, default `redis:6379`)
- Register routes on chi router
- Start HTTP server on port `8080`
- Register Prometheus metrics registry
- Expose `GET /metrics` via `promhttp.Handler()`

#### `queue/redis.go`
```
Function: PushJob(ctx, redisClient, job Job) (string, error)
- Generate UUID as job ID
- Marshal job to JSON
- LPUSH job_queue <json>
- HSET job:<id> status "pending" submitted_at <unix_ts>
- Return job ID
```

#### `handler/submit.go`
```
POST /submit
Body: { "type": "prime" | "bcrypt" | "sort" }
Response: { "job_id": "<uuid>", "status": "queued" }

- Validate body (400 if type unknown)
- Call queue.PushJob()
- Increment http_requests_total counter
- Return 202 Accepted
```

#### `handler/status.go`
```
GET /status/:id
Response: { "job_id": "...", "status": "pending|processing|done|error", "result": "..." }

- HGETALL job:<id>
- 404 if not found
```

#### `metrics/metrics.go`
```
Counters:
- http_requests_total{method, path, status}
- jobs_submitted_total{type}
```

### Port Layout
| Port | Purpose |
|------|---------|
| 8080 | HTTP API (`/submit`, `/status/:id`, `/metrics`) |

### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_ADDR` | `redis:6379` | Redis address |
| `PORT` | `8080` | HTTP listen port |

### Deliverable
- Service builds: `go build ./...`
- Manual test: `curl -X POST localhost:8080/submit -d '{"type":"prime"}'`
- Returns job ID, job visible in Redis via `redis-cli LLEN job_queue`

---

## 4. Phase 2 — Go Service: worker (Service B)

### Goal
Build the horizontally-scalable worker that pulls jobs from Redis and performs CPU-intensive operations while exposing Prometheus metrics.

> This is the critical service — HPA watches its CPU. The work must be genuinely CPU-heavy to trigger autoscaling.

### Dependencies
```
go get github.com/redis/go-redis/v9
go get github.com/prometheus/client_golang/prometheus
go get github.com/prometheus/client_golang/prometheus/promhttp
go get golang.org/x/crypto/bcrypt
```

### Files & Responsibilities

#### `main.go`
- Initialize Redis client
- Start metrics HTTP server on port `9090` (separate from job-processing goroutine)
- Launch `consumer.Start()` in the main goroutine
- Handle OS signals for graceful shutdown

#### `consumer/redis.go`
```
Function: Start(ctx, redisClient, dispatcher)
- Infinite loop:
  - BRPOP job_queue 0   (blocks until job available)
  - HSET job:<id> status "processing"
  - Call dispatcher.Dispatch(job)
  - On success: HSET job:<id> status "done" result <val>
  - On error:   HSET job:<id> status "error"  error  <msg>
  - Record metrics
```

#### `jobs/dispatcher.go`
```
Function: Dispatch(job Job) (result string, err error)
- Switch job.Type:
  - "prime"  → prime.Run()
  - "bcrypt" → bcrypt.Run()
  - "sort"   → sort.Run()
  - default  → return error "unknown job type"
```

#### `jobs/prime.go`
```
Function: Run() string
- Sieve of Eratosthenes up to 1,000,000
- Return count of primes found
- NOTE: limit bumped from 100k to 1M because Go is faster than Node.js
  and we need to generate enough CPU pressure to trigger HPA at 70%
```

#### `jobs/bcrypt.go`
```
Function: Run() string
- Generate bcrypt hash of fixed string with cost=14
- Return hash string
- NOTE: cost bumped from 10 to 14 for same CPU pressure reason
```

#### `jobs/sort.go`
```
Function: Run() string
- Allocate []int64 of size 500,000
- Fill with random int64s (seeded)
- sort.Slice()
- Return sorted[0] as string
- NOTE: size bumped from 100k to 500k
```

#### `metrics/metrics.go`
```
Prometheus metrics (as required by assignment):

jobs_processed_total        Counter     {type, status}
  - incremented after each job completes (status=success|error)

job_processing_time_seconds Histogram   {type}
  - observed with time.Since(start) in seconds
  - buckets: .1, .5, 1, 2, 5, 10, 30

job_errors_total            Counter     {type}
  - incremented on any job error
```

### Port Layout
| Port | Purpose |
|------|---------|
| 9090 | Prometheus `/metrics` endpoint only |

### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_ADDR` | `redis:6379` | Redis address |
| `METRICS_PORT` | `9090` | Prometheus metrics port |

### Deliverable
- Worker starts, connects to Redis, blocks on BRPOP
- Submit a job via Service A, worker picks it up within 1 second
- `/metrics` returns valid Prometheus text format
- `jobs_processed_total` increments correctly

---

## 5. Phase 3 — Go Service: stats-aggregator (Service C)

### Goal
Expose aggregated system stats as both a JSON REST endpoint and Prometheus metrics.

### Dependencies
```
go get github.com/redis/go-redis/v9
go get github.com/prometheus/client_golang/prometheus
go get github.com/prometheus/client_golang/prometheus/promhttp
```

### Files & Responsibilities

#### `main.go`
- Initialize Redis client
- Start background collector goroutine (every 15s)
- Register routes
- Start HTTP server on port `8081`

#### `collector/redis.go`
```
Function: CollectLoop(ctx, redisClient, gauges)
- Every 15 seconds:
  - LLEN job_queue                    → queue_length gauge
  - HSCAN job:* status "pending"      → jobs_submitted count
  - HSCAN job:* status "done"         → jobs_completed count
  - Calculate avg processing time from histogram data
  - Update all Prometheus gauges
```

#### `handler/stats.go`
```
GET /stats
Response:
{
  "queue_length": 42,
  "total_submitted": 1500,
  "total_completed": 1458,
  "total_errors": 3,
  "avg_processing_time_seconds": 2.3
}
```

#### `metrics/metrics.go`
```
Prometheus metrics (as required by assignment):

total_jobs_submitted    Gauge
total_jobs_completed    Gauge
queue_length            Gauge
```

### Port Layout
| Port | Purpose |
|------|---------|
| 8081 | HTTP (`/stats`, `/metrics`) |

### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_ADDR` | `redis:6379` | Redis address |
| `PORT` | `8081` | HTTP listen port |
| `COLLECT_INTERVAL` | `15` | Seconds between Redis polls |

### Deliverable
- `GET /stats` returns valid JSON
- `GET /metrics` returns Prometheus format with all 3 gauges
- Gauges update every 15 seconds

---

## 6. Phase 4 — Docker Images

### Goal
Build minimal, production-grade Docker images for all 3 services using multi-stage builds.

### Pattern (same for all 3 services)

```dockerfile
# Stage 1: Build
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o service .

# Stage 2: Runtime
FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/service /service
EXPOSE <port>
ENTRYPOINT ["/service"]
```

### Image Names & Ports

| Service | Image Name | Exposed Port |
|---------|-----------|-------------|
| job-submitter | `job-submitter:latest` | 8080 |
| worker | `worker:latest` | 9090 |
| stats-aggregator | `stats-aggregator:latest` | 8081 |

### `scripts/build-images.sh`
```bash
#!/usr/bin/env bash
# Build all images and load into Minikube's Docker daemon
eval $(minikube docker-env)

docker build -t job-submitter:latest ./services/job-submitter
docker build -t worker:latest         ./services/worker
docker build -t stats-aggregator:latest ./services/stats-aggregator
```

> Loading into Minikube's daemon (`eval $(minikube docker-env)`) avoids the need to push to a registry. Set `imagePullPolicy: Never` in all Deployments.

### Deliverable
- All 3 images build successfully
- Images visible inside Minikube: `minikube ssh -- docker images`
- Target image size: < 20MB each

---

## 7. Phase 5 — Kubernetes Infrastructure YAMLs

### Goal
Define all K8s resources: Namespace, Redis, 3 Service Deployments, Services, Ingress, HPA, ConfigMaps.

### 7.1 Namespace

```yaml
# k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: jobs
```
All resources live in the `jobs` namespace.

---

### 7.2 Redis

**`k8s/redis/deployment.yaml`**
```
- image: redis:7-alpine
- single replica (not HA — sufficient for assignment)
- resource requests: 100m CPU, 128Mi memory
- volume mount for /data (emptyDir sufficient for assignment)
```

**`k8s/redis/service.yaml`**
```
- ClusterIP
- port 6379
- name: redis
```

---

### 7.3 Service A — job-submitter

**`k8s/service-a/deployment.yaml`**
```
replicas: 1
image: job-submitter:latest
imagePullPolicy: Never
env:
  - REDIS_ADDR: redis:6379
  - PORT: "8080"
resources:
  requests: { cpu: "100m", memory: "128Mi" }
  limits:   { cpu: "300m", memory: "256Mi" }
readinessProbe: GET /metrics :8080
livenessProbe:  GET /metrics :8080
```

**`k8s/service-a/service.yaml`**
```
type: ClusterIP
port 8080 → targetPort 8080
```

**`k8s/service-a/ingress.yaml`**
```
apiVersion: networking.k8s.io/v1
class: nginx
rules:
  - host: jobs.local
    paths:
      - /submit   → service-a:8080
      - /status   → service-a:8080
```

> Add `jobs.local` to `/etc/hosts` pointing to `minikube ip`.

---

### 7.4 Service B — worker

**`k8s/service-b/deployment.yaml`**
```
replicas: 2           # HPA minimum
image: worker:latest
imagePullPolicy: Never
env:
  - REDIS_ADDR: redis:6379
  - METRICS_PORT: "9090"
resources:
  requests: { cpu: "200m", memory: "128Mi" }   ← CRITICAL: HPA uses this baseline
  limits:   { cpu: "1000m", memory: "512Mi" }
ports:
  - name: metrics
    containerPort: 9090
readinessProbe: GET /metrics :9090
```

**`k8s/service-b/service.yaml`**
```
type: ClusterIP
port 9090 → targetPort 9090
name: worker
portName: metrics       ← required for ServiceMonitor
```

**`k8s/service-b/hpa.yaml`**
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
spec:
  scaleTargetRef:
    kind: Deployment
    name: worker
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 15    # aggressive scale-up for demo
    scaleDown:
      stabilizationWindowSeconds: 120   # slower scale-down to observe
```

---

### 7.5 Service C — stats-aggregator

**`k8s/service-c/deployment.yaml`**
```
replicas: 1
image: stats-aggregator:latest
imagePullPolicy: Never
env:
  - REDIS_ADDR: redis:6379
  - PORT: "8081"
resources:
  requests: { cpu: "50m", memory: "64Mi" }
  limits:   { cpu: "200m", memory: "128Mi" }
ports:
  - containerPort: 8081
    name: http
```

**`k8s/service-c/service.yaml`**
```
type: ClusterIP
port 8081 → targetPort 8081
```

---

### Deliverable
- `kubectl get all -n jobs` shows all pods Running
- `kubectl get hpa -n jobs` shows worker HPA with TARGETS visible (not `<unknown>`)
- Ingress accessible: `curl http://jobs.local/submit`

---

## 8. Phase 6 — Prometheus & Grafana Setup

### Goal
Deploy monitoring stack, configure metric scraping from Service B & C, and create a Grafana dashboard showing job metrics and scaling behavior.

### 8.1 Deploy Prometheus Stack

```bash
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --set grafana.adminPassword=admin123 \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false
```

The last flag is critical — it tells Prometheus to pick up ServiceMonitors from ALL namespaces, not just the Helm release namespace.

---

### 8.2 ServiceMonitor for Service B

```yaml
# monitoring/servicemonitor-b.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: worker-monitor
  namespace: jobs
  labels:
    release: prometheus        # must match kube-prometheus-stack release label
spec:
  selector:
    matchLabels:
      app: worker
  endpoints:
  - port: metrics              # matches portName in service-b/service.yaml
    path: /metrics
    interval: 15s
  namespaceSelector:
    matchNames:
    - jobs
```

---

### 8.3 ServiceMonitor for Service C

```yaml
# monitoring/servicemonitor-c.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: stats-monitor
  namespace: jobs
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      app: stats-aggregator
  endpoints:
  - port: http
    path: /metrics
    interval: 15s
  namespaceSelector:
    matchNames:
    - jobs
```

---

### 8.4 Grafana Dashboard

**`monitoring/grafana-dashboard.json`** — a ConfigMap-backed dashboard with the following panels:

| Panel | PromQL | Visualization |
|-------|--------|--------------|
| Worker Pod Count | `kube_deployment_status_replicas{deployment="worker"}` | Stat |
| CPU Usage per Worker Pod | `rate(container_cpu_usage_seconds_total{pod=~"worker.*"}[1m])` | Time series |
| Memory per Worker Pod | `container_memory_working_set_bytes{pod=~"worker.*"}` | Time series |
| Jobs Processed Rate | `rate(jobs_processed_total[1m])` | Time series |
| Job Processing Time P99 | `histogram_quantile(0.99, rate(job_processing_time_seconds_bucket[5m]))` | Time series |
| Job Processing Time P50 | `histogram_quantile(0.50, rate(job_processing_time_seconds_bucket[5m]))` | Time series |
| Queue Length | `queue_length` | Gauge |
| Total Jobs Submitted | `total_jobs_submitted` | Stat |
| Total Jobs Completed | `total_jobs_completed` | Stat |
| Job Error Rate | `rate(job_errors_total[1m])` | Time series |
| HPA Scale Events | `kube_horizontalpodautoscaler_status_current_replicas{horizontalpodautoscaler="worker"}` | Time series |

Deploy dashboard as a ConfigMap with label `grafana_dashboard: "1"` so Grafana sidecar auto-loads it.

---

### Access
```bash
kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring
# open http://localhost:3000  admin/admin123
```

### Deliverable
- Prometheus Targets page shows service-b and service-c as `UP`
- All 3 custom metrics appear in `http://localhost:9090/graph`
- Grafana dashboard loads with all panels showing data

---

## 9. Phase 7 — Local Cluster Deployment (Minikube)

### Goal
Full end-to-end deployment from scratch on a clean Minikube cluster.

### Ordered Deploy Script — `scripts/deploy.sh`

```bash
#!/usr/bin/env bash
set -e

# 1. Build images inside Minikube daemon
eval $(minikube docker-env)
docker build -t job-submitter:latest    ./services/job-submitter
docker build -t worker:latest           ./services/worker
docker build -t stats-aggregator:latest ./services/stats-aggregator

# 2. Apply namespace first
kubectl apply -f k8s/namespace.yaml

# 3. Redis (must be Running before services start)
kubectl apply -f k8s/redis/
kubectl rollout status deployment/redis -n jobs

# 4. Core services
kubectl apply -f k8s/service-a/
kubectl apply -f k8s/service-b/
kubectl apply -f k8s/service-c/
kubectl rollout status deployment/job-submitter   -n jobs
kubectl rollout status deployment/worker          -n jobs
kubectl rollout status deployment/stats-aggregator -n jobs

# 5. Monitoring
kubectl apply -f monitoring/

echo "Deployment complete."
echo "Ingress IP: $(minikube ip)"
echo "Add to /etc/hosts: $(minikube ip) jobs.local"
```

### Verification Steps

```bash
# All pods running
kubectl get pods -n jobs

# HPA is active (not <unknown>)
kubectl get hpa -n jobs

# Test submit endpoint
curl -X POST http://jobs.local/submit \
  -H "Content-Type: application/json" \
  -d '{"type":"prime"}'

# Check job status
curl http://jobs.local/status/<job_id>

# Check stats
kubectl port-forward svc/stats-aggregator 8081:8081 -n jobs
curl http://localhost:8081/stats

# Check worker metrics
kubectl port-forward svc/worker 9090:9090 -n jobs
curl http://localhost:9090/metrics | grep jobs_
```

### Deliverable
- All pods `Running` or `Ready`
- Job submitted → status transitions: `pending` → `processing` → `done`
- HPA shows `TARGETS: X%/70%`

---

## 10. Phase 8 — Stress Testing

### Goal
Generate enough load to trigger HPA scale-up and observe it in Grafana.

### Preparation

```bash
# job.json — POST body for ab
echo '{"type":"bcrypt"}' > /tmp/job.json

# Confirm ingress IP
INGRESS_IP=$(minikube ip)
echo $INGRESS_IP
```

### `scripts/stress-test.sh`

```bash
#!/usr/bin/env bash
INGRESS_IP=$(minikube ip)

echo "=== Starting stress test ==="
echo "Target: http://${INGRESS_IP}/submit"
echo "Watching HPA in background..."

# Watch HPA in a separate terminal (copy this command)
# kubectl get hpa worker -n jobs --watch

# Run load test: 5000 requests, 200 concurrent
ab -n 5000 -c 200 \
   -p /tmp/job.json \
   -T "application/json" \
   "http://${INGRESS_IP}/submit"

echo "=== Load test complete ==="
echo "Watch Grafana for queue drain as workers scale up"
```

### What to Observe

| Metric | Expected Behavior |
|--------|------------------|
| Redis queue length | Spikes rapidly during load burst |
| Worker CPU % | Crosses 70% threshold within ~30s |
| Worker replica count | Scales from 2 → up to 10 |
| `jobs_processed_total` rate | Increases as more workers come online |
| Queue length (post-load) | Drains as autoscaled workers consume backlog |
| Worker replicas (post-load) | Scales back to 2 after ~2 min cooldown |

### HPA Watch Command
```bash
kubectl get hpa worker -n jobs --watch
# NAME     REFERENCE           TARGETS    MINPODS   MAXPODS   REPLICAS
# worker   Deployment/worker   12%/70%    2         10        2
# worker   Deployment/worker   89%/70%    2         10        4    ← scaling!
# worker   Deployment/worker   95%/70%    2         10        8
```

### Deliverable
- Screenshot: HPA scaling event in terminal
- Screenshot: Grafana dashboard during peak load (CPU spike, queue spike)
- Screenshot: Grafana dashboard after cooldown (queue drained, replicas reduced)

---

## 11. Phase 9 — README & Documentation

### Goal
Complete README.md that allows someone to reproduce everything from scratch.

### README Sections

```markdown
## Prerequisites
## Quick Start (5 commands)
## Architecture Diagram
## Services Overview
## Local Deployment
  ### Minikube Setup
  ### Build Images
  ### Deploy Everything
  ### Verify Deployment
## Accessing Services
## Monitoring (Prometheus + Grafana)
  ### Dashboard Guide
## Stress Testing
  ### Running the Test
  ### What to Watch
## Observations on Scaling
## Cleanup
```

### Deliverable
- README is self-contained — a new engineer can follow it without prior context
- Includes architecture ASCII diagram
- Includes annotated Grafana screenshots
- Includes scaling observations section

---

## 12. Dependency Map

```
Phase 0 (Tools)
    │
    ├── Phase 1 (Service A code)  ──┐
    ├── Phase 2 (Service B code)  ──┤── Phase 4 (Docker images)
    └── Phase 3 (Service C code)  ──┘         │
                                              │
                                    Phase 5 (K8s YAMLs)
                                              │
                                    Phase 6 (Prometheus/Grafana)
                                              │
                                    Phase 7 (Deploy & Verify)
                                              │
                                    Phase 8 (Stress Test)
                                              │
                                    Phase 9 (README)
```

**Phases 1, 2, 3 are independent — they can be built in parallel.**  
**Phase 4 depends on all three completing.**  
**Phase 5 can be written in parallel with Phases 1-4** (it's just YAML authoring).  
**Phase 6 can be written in parallel with Phase 5.**  
**Phases 7, 8, 9 must be sequential.**

---

## 13. Key Design Decisions

### Why Go over Node.js

| Factor | Node.js (Original) | Go (Our impl) |
|--------|-------------------|---------------|
| CPU-intensive work | Single-threaded event loop; blocks other requests | True parallelism via goroutines |
| Docker image size | ~150MB (node:alpine) | ~10MB (distroless/static) |
| Prometheus client | `prom-client` npm package | `client_golang` — reference implementation |
| K8s startup time | ~2s | ~100ms |
| Adaptation needed | None | Workload sizes bumped (see below) |

### CPU Workload Size Adaptation
Go processes the original workload sizes (prime up to 100k, bcrypt 10 rounds, sort 100k) too fast to sustain 70%+ CPU. Adjusted sizes:

| Job | Assignment | Our impl | Reason |
|-----|-----------|----------|--------|
| prime | up to 100,000 | up to 1,000,000 | 10× to sustain CPU pressure |
| bcrypt | 10 rounds | 14 rounds | Each +1 round doubles work |
| sort | 100,000 ints | 500,000 int64s | 5× for measurable CPU time |

### HPA `resources.requests` Baseline
The HPA calculates CPU utilization as `actual_cpu / requests.cpu`. Worker is set to `requests: cpu: 200m`. At 70% threshold, scaling triggers when actual usage exceeds ~140m. With our workload sizes, a single bcrypt job at cost=14 uses ~400-600m for ~1-2 seconds, reliably crossing the threshold under concurrent load.

### Redis as Job Queue
Using `LPUSH`/`BRPOP` on a single Redis list as a simple, reliable queue. No message broker (Kafka, RabbitMQ) needed for this assignment scope. Trade-off: no job persistence across Redis restarts. Acceptable for a demo/assignment context.

### Separate Metrics Port for Service B
Service B doesn't expose HTTP for job submissions — it only pulls from Redis. Running a dedicated HTTP server on `:9090` solely for `/metrics` keeps the worker's blocking BRPOP loop clean and allows Kubernetes readiness probes to check the metrics endpoint without interfering with job processing.

### Prometheus ServiceMonitor vs. Annotation-Based Scraping
Using `ServiceMonitor` CRDs (kube-prometheus-stack pattern) instead of pod annotations (`prometheus.io/scrape: "true"`) because:
1. It's the current standard with the Prometheus Operator
2. Finer-grained control over scrape interval and path per service
3. Works with the Helm-installed stack without extra configuration

---

*Plan version: 1.0 | Date: 2026-06-22 | Stack: Go 1.22 · Redis 7 · K8s 1.29 · Prometheus Operator*
