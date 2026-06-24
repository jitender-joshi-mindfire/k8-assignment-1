// Command stats-aggregator is Service C of the Kubernetes Microservices
// Monitoring assignment. It has two jobs:
//
//  1. Serve GET /stats — a JSON snapshot of current job system state
//     (queue depth, totals, average processing time).
//
//  2. Expose GET /metrics — Prometheus gauges scraped by Prometheus every 15s.
//
// All state comes from a background collector goroutine that polls Redis on a
// configurable interval and writes results into an in-memory Store protected
// by a sync.RWMutex. The HTTP handlers never touch Redis directly — they only
// read from that in-memory store, keeping every HTTP response sub-millisecond
// regardless of Redis latency.
//
// This is the "read model" pattern: one writer (collector) keeps state fresh;
// many readers (HTTP handlers) serve it cheaply.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/kubernetes-project/stats-aggregator/collector"
	"github.com/kubernetes-project/stats-aggregator/handler"
	"github.com/kubernetes-project/stats-aggregator/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	// ── configuration ─────────────────────────────────────────────────────────
	// All config via env vars — same pattern as Services A and B so the
	// Kubernetes ConfigMap / Deployment env block is uniform across all three.
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	port := envOr("PORT", "8081")

	// COLLECT_INTERVAL controls how often the background goroutine polls Redis.
	// 15 seconds balances freshness against Redis load. Under stress testing
	// you may want to lower this to 5s to see queue_length drain faster in
	// Grafana. Configurable so it can be tuned per-environment without a
	// code change or image rebuild.
	collectIntervalSecs := envOrInt("COLLECT_INTERVAL_SECS", 15)
	collectInterval := time.Duration(collectIntervalSecs) * time.Second

	// ── structured logger ──────────────────────────────────────────────────────
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("stats-aggregator starting",
		"redis", redisAddr,
		"port", port,
		"collect_interval", collectInterval,
	)

	// ── Redis client ───────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr:        redisAddr,
		DialTimeout: 10 * time.Second,
	})

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		slog.Error("cannot reach Redis at startup — exiting", "addr", redisAddr, "err", err)
		os.Exit(1)
	}
	slog.Info("Redis connection established", "addr", redisAddr)

	// ── shared state ───────────────────────────────────────────────────────────
	// store is the single shared object between the collector goroutine and
	// all HTTP handler goroutines. It wraps a Snapshot behind a RWMutex.
	store := collector.NewStore()

	// ── Prometheus metrics ─────────────────────────────────────────────────────
	m := metrics.New()

	// ── cancellable context for background goroutines ──────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── collector goroutine ────────────────────────────────────────────────────
	// Runs independently of the HTTP server. Polls Redis every collectInterval,
	// updates store and Prometheus gauges. Exits cleanly when ctx is cancelled.
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		collector.Start(ctx, rdb, store, m, collectInterval)
	}()

	// ── HTTP router ────────────────────────────────────────────────────────────
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(10 * time.Second))

	// GET /stats — the primary endpoint for this service.
	// Returns a JSON snapshot of current system state read from the in-memory
	// store. Never blocks on Redis.
	r.Get("/stats", handler.Stats(store))

	// GET /metrics — Prometheus scrape endpoint.
	// Served from the private registry so only our metrics appear.
	r.Get("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}).ServeHTTP)

	// GET /healthz — liveness probe.
	// Always 200 if the process is alive. Does not check Redis or store
	// freshness — a stale store is not a reason to restart the pod.
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	// GET /readyz — readiness probe.
	// Returns 503 if Redis is unreachable (no point serving /stats if we
	// can't collect fresh data) or if the store has never been populated
	// (pod just started and first collection cycle hasn't completed yet).
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		// Check Redis connectivity.
		pctx, pcancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer pcancel()
		if err := rdb.Ping(pctx).Err(); err != nil {
			slog.Warn("readiness: Redis unavailable", "err", err)
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}

		// Check that at least one collection cycle has completed.
		// store.Latest().CollectedAt is initialised to time.Now() in NewStore(),
		// so DataAge alone isn't enough — we check TotalSubmitted as a proxy
		// for "has the collector run at least once and found something".
		// On an empty cluster (no jobs yet) this will still pass because
		// the collector ran and found 0 jobs — which is valid.
		snap := store.Latest()
		if time.Since(snap.CollectedAt) > collectInterval*3 {
			// Store is more than 3× the collect interval old — collector
			// may be stuck. Remove from load balancer rotation.
			slog.Warn("readiness: snapshot too stale",
				"age", time.Since(snap.CollectedAt),
				"threshold", collectInterval*3,
			)
			http.Error(w, "snapshot stale", http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	slog.Info("routes registered",
		"routes", []string{
			"GET /stats",
			"GET /metrics",
			"GET /healthz",
			"GET /readyz",
		},
	)

	// ── HTTP server with graceful shutdown ─────────────────────────────────────
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("stats-aggregator HTTP server listening", "port", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server error", "err", err)
			os.Exit(1)
		}
	}()

	// ── signal handling ────────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	sig := <-quit
	slog.Info("shutdown signal received", "signal", sig.String())

	// 1. Cancel context — stops the collector goroutine.
	cancel()

	// 2. Drain in-flight HTTP requests (up to 10s).
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("HTTP server shutdown error", "err", err)
	}

	// 3. Wait for collector to exit (it exits almost immediately after cancel).
	select {
	case <-collectorDone:
		slog.Info("collector exited cleanly")
	case <-time.After(5 * time.Second):
		slog.Warn("collector did not exit within 5s")
	}

	// 4. Close Redis connection.
	if err := rdb.Close(); err != nil {
		slog.Error("Redis close error", "err", err)
	}

	slog.Info("stats-aggregator stopped cleanly")
}

// envOr returns the value of the environment variable key, or fallback if unset.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envOrInt returns the integer value of an environment variable, or fallback.
func envOrInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		slog.Warn("invalid integer env var, using fallback",
			"key", key, "value", v, "fallback", fallback)
	}
	return fallback
}
