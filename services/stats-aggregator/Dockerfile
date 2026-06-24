# ─── Stage 1: Builder ────────────────────────────────────────────────────────
FROM golang:alpine AS builder

RUN apk add --no-cache ca-certificates git

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -ldflags="-s -w" \
      -trimpath \
      -o stats-aggregator \
      .

# ─── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/stats-aggregator /stats-aggregator

# Port 8081: serves /stats and /metrics HTTP endpoints.
EXPOSE 8081

USER nonroot:nonroot

ENTRYPOINT ["/stats-aggregator"]
