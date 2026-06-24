// Command worker is Service B of the Kubernetes Microservices Monitoring
// assignment. It is the horizontally-scalable component: Kubernetes HPA
// scales it from 2 → 10 pods when CPU utilisation exceeds 70%.
//
// The service has two concurrent responsibilities:
//
//  1. Consumer loop  — a single goroutine that runs a blocking BRPOP loop,
//     popping jobs from Redis and processing them one at a time.
//
//  2. Metrics server — a separate HTTP server on METRICS_PORT that serves
//     GET /metrics for Prometheus scraping and GET /healthz for Kubernetes
//     liveness/readiness probes.
//
// Separating these onto different goroutines (and different ports) means:
//   - Prometheus can always scrape /metrics even when a long-running job
//     (e.g. bcrypt at cost-14) is blocking the consumer goroutine.
//   - Kubernetes probe failures are isolated from job-processing failures.
//   - The consumer goroutine never holds an HTTP connection open.
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

	"github.com/kubernetes-project/worker/consumer"
	"github.com/kubernetes-project/worker/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	// ── configuration ─────────────────────────────────────────────────────────
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	metricsPort := envOr("METRICS_PORT", "9090")

	// ── structured logger ──────────────────────────────────────────────────────
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("worker starting",
		"redis", redisAddr,
		"metrics_port", metricsPort,
	)

	// ── Redis client ───────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		// DialTimeout is set explicitly: if Redis is unreachable at startup
		// we want a fast fail rather than hanging for 30+ seconds.
		DialTimeout: 10 * time.Second,
	})

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		slog.Error("cannot reach Redis at startup — exiting", "addr", redisAddr, "err", err)
		os.Exit(1)
	}
	slog.Info("Redis connection established", "addr", redisAddr)

	// ── Prometheus metrics ─────────────────────────────────────────────────────
	m := metrics.New()

	// ── context for graceful shutdown ──────────────────────────────────────────
	// ctx is passed to consumer.Start(). When we cancel it on SIGTERM, the
	// consumer loop exits cleanly after finishing its current job (or after the
	// BRPOP 5-second timeout if it is currently blocked waiting for a job).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── metrics HTTP server ────────────────────────────────────────────────────
	// Run the HTTP server in a separate goroutine so it never contends with
	// the consumer loop. The consumer is deliberately blocking (BRPOP) so it
	// must own its goroutine exclusively.
	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	// /healthz — liveness probe.
	// Always returns 200 if the process is alive. Does NOT check Redis: if
	// Redis is down we want the pod to stay up and retry, not enter a restart
	// loop. The consumer's back-off logic handles transient Redis failures.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	// /readyz — readiness probe.
	// Returns 503 when Redis is unreachable so Kubernetes temporarily removes
	// this pod from service endpoints. Once Redis recovers, the pod becomes
	// ready again without a restart.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pctx, pcancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer pcancel()
		if err := rdb.Ping(pctx).Err(); err != nil {
			slog.Warn("readiness check failed", "err", err)
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	metricsSrv := &http.Server{
		Addr:         ":" + metricsPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	go func() {
		slog.Info("metrics server listening", "port", metricsPort,
			"endpoints", []string{"/metrics", "/healthz", "/readyz"})
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server error", "err", err)
			// A metrics server failure is not a reason to kill the worker.
			// The consumer can continue processing jobs; Prometheus just won't
			// be able to scrape until the server recovers.
		}
	}()

	// ── consumer loop ──────────────────────────────────────────────────────────
	// consumer.Start blocks until ctx is cancelled. Run it in a goroutine so
	// main() can also block on the OS signal channel below.
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		consumer.Start(ctx, rdb, m)
	}()

	// ── signal handling & graceful shutdown ────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	sig := <-quit
	slog.Info("shutdown signal received", "signal", sig.String())

	// 1. Cancel the consumer context — it will finish its current job (or
	//    exit the BRPOP timeout loop) and then return from consumer.Start.
	cancel()

	// 2. Wait for the consumer to finish, but give it at most 35 seconds
	//    (bcrypt at cost-14 takes ~1.6s; 35s is generous and well under the
	//    default terminationGracePeriodSeconds of 30s — we'll update that in
	//    the Kubernetes Deployment YAML to 60s to be safe).
	shutTimer := time.NewTimer(35 * time.Second)
	select {
	case <-consumerDone:
		slog.Info("consumer exited cleanly")
	case <-shutTimer.C:
		slog.Warn("consumer did not exit within 35s — proceeding with shutdown")
	}
	shutTimer.Stop()

	// 3. Shut down the metrics HTTP server.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	if err := metricsSrv.Shutdown(shutCtx); err != nil {
		slog.Error("metrics server shutdown error", "err", err)
	}

	// 4. Close the Redis connection.
	if err := rdb.Close(); err != nil {
		slog.Error("Redis close error", "err", err)
	}

	slog.Info("worker stopped cleanly")
}

// envOr returns the value of the environment variable key, or fallback if unset.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
