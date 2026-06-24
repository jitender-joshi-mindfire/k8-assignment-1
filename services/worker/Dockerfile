# ─── Stage 1: Builder ────────────────────────────────────────────────────────
FROM golang:alpine AS builder

RUN apk add --no-cache ca-certificates git

WORKDIR /build

# Dependency layer cached separately from source layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# The worker performs bcrypt hashing which links against golang.org/x/crypto.
# That package is pure Go so CGO_ENABLED=0 is safe here too.
#
# GOMAXPROCS note: the worker is intentionally single-threaded at the job level
# (one BRPOP loop) but Go's scheduler still uses GOMAXPROCS goroutines for the
# runtime, GC, and the metrics HTTP server. Leaving GOMAXPROCS at default (==
# container CPU limit) is correct — the HPA measures CPU usage over time, not
# peak goroutine count.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -ldflags="-s -w" \
      -trimpath \
      -o worker \
      .

# ─── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/worker /worker

# Port 9090 is the Prometheus metrics endpoint.
# The consumer loop has no HTTP port — it pulls from Redis directly.
# The Kubernetes Service and ServiceMonitor target this port.
EXPOSE 9090

USER nonroot:nonroot

ENTRYPOINT ["/worker"]
