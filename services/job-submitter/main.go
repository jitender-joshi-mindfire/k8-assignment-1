// Command job-submitter is Service A of the Kubernetes Microservices Monitoring
// assignment. It exposes three HTTP endpoints:
//
//   POST /submit          — accepts a job type, enqueues it in Redis, returns job ID
//   GET  /status/{id}     — returns the current status of a job
//   GET  /metrics         — Prometheus metrics endpoint (scraped by Prometheus)
//
// Configuration is entirely through environment variables so the binary runs
// identically in Docker, Kubernetes, and locally with `go run .`.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/kubernetes-project/job-submitter/handler"
	apimw "github.com/kubernetes-project/job-submitter/middleware"
	"github.com/kubernetes-project/job-submitter/metrics"
	"github.com/kubernetes-project/job-submitter/queue"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	// ── configuration ────────────────────────────────────────────────────────
	// All config is read from environment variables with sensible defaults so
	// the service works locally (go run .), in Docker, and in Kubernetes without
	// any code changes — only the env vars differ.
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	port := envOr("PORT", "8080")

	// ── logger ───────────────────────────────────────────────────────────────
	// slog is the standard structured logger from Go 1.21+. JSON output makes
	// log lines parseable by Grafana Loki, Datadog, or any log aggregator
	// without a custom parser.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ── Redis client ─────────────────────────────────────────────────────────
	// go-redis/v9 uses context-aware methods throughout. We validate the
	// connection at startup so the pod fails fast (and Kubernetes restarts it)
	// rather than silently accepting requests that will all fail.
	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
		MinIdleConns: 2,
	})

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		slog.Error("cannot reach Redis at startup", "addr", redisAddr, "err", err)
		os.Exit(1)
	}
	slog.Info("Redis connection established", "addr", redisAddr)

	// ── Prometheus metrics ───────────────────────────────────────────────────
	m := metrics.New()

	// ── HTTP router ──────────────────────────────────────────────────────────
	// chi is used because:
	//   • It is stdlib-compatible (http.Handler everywhere).
	//   • URL parameters (chi.URLParam) are stored in context — no global state.
	//   • The middleware stack is explicit and composable.
	r := chi.NewRouter()

	// RequestID: adds a unique X-Request-ID to every request. Useful when
	//   correlating logs across Service A → B → C in a distributed trace.
	r.Use(chimw.RequestID)

	// RealIP: reads X-Forwarded-For / X-Real-IP so logs show the client IP,
	//   not the ingress controller's IP.
	r.Use(chimw.RealIP)

	// Logger: emits one structured log line per request with method, path,
	//   status, latency. This is the first line of defence for debugging.
	r.Use(chimw.Logger)

	// Recoverer: catches any panic in a handler and returns 500 instead of
	//   crashing the whole process. Essential in production.
	r.Use(chimw.Recoverer)

	// Timeout: enforces a 30-second request deadline. Without this, a slow
	//   Redis call could hold a goroutine open indefinitely under load.
	r.Use(chimw.Timeout(30 * time.Second))
	r.Use(apimw.CORS)
	r.Use(apimw.RateLimit(200, time.Minute))

	// ── routes ───────────────────────────────────────────────────────────────
	r.Post("/submit", handler.Submit(rdb, m))
	r.Get("/status/{id}", handler.Status(rdb, m))

	// /metrics is served from the custom registry, not the default global one.
	// This means only metrics explicitly registered in metrics.New() appear,
	// preventing accidental leakage of metrics from third-party libraries.
	r.Get("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true, // enables OpenMetrics text format (Content-Type negotiation)
	}).ServeHTTP)

	// /healthz is a simple liveness probe that Kubernetes calls to decide
	// whether to restart the pod. It does not check Redis — if Redis is down
	// the pod should stay up and return errors on /submit rather than
	// restarting in a loop (which would not fix Redis anyway).
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	// /readyz is the readiness probe. Unlike liveness it does check Redis so
	// Kubernetes removes the pod from the load balancer rotation if Redis
	// becomes unreachable, preventing the ingress from routing new requests to
	// a pod that cannot serve them.
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			slog.Warn("readiness check failed", "err", err)
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	// ── print registered routes (development convenience) ────────────────────
	slog.Info("registered routes",
		"routes", []string{
			"POST /submit",
			"GET  /status/{id}",
			"GET  /metrics",
			"GET  /healthz",
			"GET  /readyz",
		},
	)

	// ── print queue key so it is visible in logs ──────────────────────────────
	slog.Info("queue config", "redis_list", queue.JobQueueKey)

	// ── HTTP server with graceful shutdown ────────────────────────────────────
	// Graceful shutdown is not optional in Kubernetes: when a pod is terminated
	// Kubernetes sends SIGTERM and waits for the process to exit cleanly.
	// If the process ignores SIGTERM and keeps accepting requests, Kubernetes
	// force-kills it after the terminationGracePeriodSeconds (default 30s),
	// which drops in-flight requests. The pattern below:
	//   1. Catches SIGTERM / SIGINT.
	//   2. Calls srv.Shutdown(ctx) which stops accepting new connections and
	//      waits for active requests to finish (up to 15s).
	//   3. Closes the Redis client cleanly.
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 35 * time.Second, // must be > chimw.Timeout (30s)
		IdleTimeout:  60 * time.Second,
	}

	// Run the server in a goroutine so main can block on the signal channel.
	go func() {
		slog.Info("job-submitter starting", "port", port, "redis", redisAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	// Block until we receive SIGTERM (Kubernetes) or SIGINT (Ctrl-C locally).
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	sig := <-quit
	slog.Info("shutdown signal received", "signal", sig.String())

	// Give in-flight requests up to 15 seconds to complete.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()

	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("graceful shutdown failed", "err", err)
	}

	if err := rdb.Close(); err != nil {
		slog.Error("Redis close error", "err", err)
	}

	slog.Info("job-submitter stopped cleanly")
}

// envOr returns the value of the environment variable named key, or fallback
// if the variable is not set or is empty.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
