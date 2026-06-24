# ─── Stage 1: Builder ────────────────────────────────────────────────────────
# golang:alpine always pulls the latest stable Go release, ensuring the builder
# can satisfy the go directive in go.mod regardless of which minor version is
# declared. Pinning to a specific tag (e.g. golang:1.24-alpine) would be better
# for reproducible CI builds — swap in the exact tag once the go.mod version
# stabilises.
FROM golang:alpine AS builder

# Install ca-certificates and git.
#   ca-certificates: needed so the binary can make TLS connections at runtime
#                    even inside the scratch/distroless image (no OS cert store).
#   git: needed by `go mod download` when a dependency is fetched from a VCS.
RUN apk add --no-cache ca-certificates git

WORKDIR /build

# Copy dependency manifests first so Docker can cache the download layer.
# If only source files change (not go.mod/go.sum), this layer is reused and
# `go mod download` is skipped — significantly faster incremental builds.
COPY go.mod go.sum ./
RUN go mod download

# Copy the full source tree and build.
COPY . .

# Build flags explained:
#   CGO_ENABLED=0  — pure Go binary, no C runtime dependency. Required for
#                    distroless/scratch base images which have no libc.
#   GOOS=linux     — cross-compile to Linux even when building on macOS/Windows.
#   GOARCH=amd64   — target x86-64. Change to arm64 for ARM nodes (AWS Graviton,
#                    Apple Silicon native). For multi-arch builds use BuildKit.
#   -ldflags "-s -w" — strip debug symbols (-s) and DWARF tables (-w).
#                      Reduces binary size by ~30% with no runtime impact.
#   -trimpath      — remove local filesystem paths from the binary. Makes builds
#                    reproducible and avoids leaking developer machine paths in
#                    stack traces.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -ldflags="-s -w" \
      -trimpath \
      -o job-submitter \
      .

# ─── Stage 2: Runtime ────────────────────────────────────────────────────────
# distroless/static contains:
#   • CA certificates (for TLS — the redis client uses TLS in prod)
#   • /etc/passwd (so non-root USER works)
#   • timezone data
#   • Nothing else — no shell, no package manager, no libc
#
# Why distroless over scratch?
#   • scratch has no CA certs and no /etc/passwd — we'd need to COPY them in.
#   • distroless gives us those for free while still being ~3MB.
#   • Container scanners (Trivy, Snyk) report far fewer CVEs vs alpine/ubuntu.
FROM gcr.io/distroless/static-debian12

# Copy the CA bundle from the builder so Redis TLS connections work at runtime.
# distroless already includes this, but explicitly copying ensures the version
# matches whatever was current at build time.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the compiled binary from the builder stage.
COPY --from=builder /build/job-submitter /job-submitter

# Document the port this service listens on. Does not actually publish it —
# that happens in docker run -p or the Kubernetes Service definition.
EXPOSE 8080

# Run as a non-root user (distroless provides uid 65532 "nonroot").
# Required for Kubernetes PodSecurityPolicy / SecurityContext runAsNonRoot.
USER nonroot:nonroot

ENTRYPOINT ["/job-submitter"]
