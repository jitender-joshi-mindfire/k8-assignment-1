// Package consumer owns the Redis interaction for the worker service.
// It runs a blocking BRPOP loop that pops one job at a time from the queue,
// dispatches it to the jobs package for processing, and writes the result
// back to the job's Redis Hash so Service A's /status/:id can return it.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/kubernetes-project/worker/jobs"
	"github.com/kubernetes-project/worker/metrics"
	"github.com/redis/go-redis/v9"
)

const (
	// jobQueueKey must match the constant in job-submitter/queue/redis.go.
	// Both services share this string as their only coupling point — changing
	// it in one place requires changing it in the other.
	jobQueueKey = "job_queue"

	// jobHashPrefix must also match job-submitter/queue/redis.go.
	jobHashPrefix = "job:"

	// brpopTimeout is how long BRPOP blocks waiting for a new item before
	// returning redis.Nil. After the timeout the loop checks whether the
	// context has been cancelled (SIGTERM received) and either re-blocks or
	// exits cleanly.
	//
	// Why 5s and not 0 (block forever)?
	// BRPOP with timeout=0 blocks indefinitely — the only way to unblock it
	// on shutdown is to push a sentinel value or forcefully close the
	// connection. A 5s timeout lets the graceful-shutdown path exit within
	// 5 seconds of SIGTERM, well within Kubernetes' 30s grace period.
	brpopTimeout = 5 * time.Second
)

// jobHeader is a minimal struct used only to extract the ID and Type from a
// raw payload before calling the full dispatcher. Keeping it separate from
// jobs.Job avoids a circular import and makes the intent clear.
type jobHeader struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// Start runs the main consumer loop. It blocks until ctx is cancelled.
//
// Loop body (one iteration = one job):
//  1. BRPOP job_queue <timeout>   — block until a job is available
//  2. HSET job:<id> status "processing"
//  3. jobs.Dispatch(payload)      — CPU-intensive work happens here
//  4. Record Prometheus metrics (duration histogram, processed/error counters)
//  5. HSET job:<id> status "done"|"error"
//
// The loop is deliberately single-threaded (one goroutine, one job at a time).
// CPU-intensive jobs saturate a single core; running two concurrently on one
// pod would halve throughput while doubling latency. Kubernetes HPA handles
// scale-out by adding more pods — each pod processes one job at a time.
// This also makes the active_jobs gauge cleanly binary: 0 (idle) or 1 (busy).
func Start(ctx context.Context, rdb *redis.Client, m *metrics.Metrics) {
	slog.Info("consumer started", "queue", jobQueueKey, "brpop_timeout", brpopTimeout)

	for {
		// Check for shutdown before blocking — handles the case where ctx was
		// cancelled while we were in the processing path, not in BRPOP.
		select {
		case <-ctx.Done():
			slog.Info("consumer stopping: context cancelled before BRPOP")
			return
		default:
		}

		// BRPOP blocks for up to brpopTimeout.
		// On success: result[0]=key, result[1]=JSON payload.
		// On timeout: returns ("", redis.Nil).
		result, err := rdb.BRPop(ctx, brpopTimeout, jobQueueKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				// Normal timeout — no job available. Loop back to check ctx.
				continue
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				slog.Info("consumer stopping: context cancelled during BRPOP")
				return
			}
			// Unexpected Redis error (network blip, failover).
			// Log and back-off — do NOT exit; a transient error should not
			// kill the worker pod and trigger a Kubernetes restart loop.
			slog.Error("BRPOP unexpected error", "err", err)
			time.Sleep(2 * time.Second)
			continue
		}

		payload := []byte(result[1])

		// ── extract header ───────────────────────────────────────────────────
		// Parse only ID and Type before the full dispatch so we can update the
		// Redis Hash to "processing" status immediately.
		var hdr jobHeader
		if err := json.Unmarshal(payload, &hdr); err != nil {
			slog.Error("failed to parse job header, skipping", "payload", string(payload), "err", err)
			continue
		}
		if hdr.ID == "" {
			slog.Error("job has empty ID, skipping", "payload", string(payload))
			continue
		}

		slog.Info("job received", "id", hdr.ID, "type", hdr.Type)

		hashKey := jobHashPrefix + hdr.ID

		// ── mark processing ──────────────────────────────────────────────────
		// Write "processing" before starting work so a concurrent /status/:id
		// poll returns the right state. If this write fails, carry on — the
		// status will just stay "pending" until the final write.
		if err := rdb.HSet(ctx, hashKey, "status", "processing").Err(); err != nil {
			slog.Warn("could not set status=processing", "id", hdr.ID, "err", err)
		}

		m.ActiveJobs.Inc()

		// ── dispatch ─────────────────────────────────────────────────────────
		// This is the CPU-intensive call. It blocks this goroutine for anywhere
		// from ~60ms (sort) to ~1600ms (bcrypt) depending on job type.
		res := jobs.Dispatch(payload)

		m.ActiveJobs.Dec()

		// ── record metrics ───────────────────────────────────────────────────
		// Always observe duration, even on error, so the histogram captures the
		// full distribution including failed (and therefore short-circuited) jobs.
		m.JobProcessingTimeSeconds.WithLabelValues(hdr.Type).Observe(res.Duration.Seconds())

		if res.Err != nil {
			m.JobErrorsTotal.WithLabelValues(hdr.Type).Inc()
			m.JobsProcessedTotal.WithLabelValues(hdr.Type, "error").Inc()

			slog.Error("job failed",
				"id", hdr.ID,
				"type", hdr.Type,
				"duration_ms", res.Duration.Milliseconds(),
				"err", res.Err,
			)

			if err := rdb.HSet(ctx, hashKey,
				"status", "error",
				"error", res.Err.Error(),
			).Err(); err != nil {
				slog.Error("failed to write error status", "id", hdr.ID, "err", err)
			}
			continue
		}

		// ── success ──────────────────────────────────────────────────────────
		m.JobsProcessedTotal.WithLabelValues(hdr.Type, "success").Inc()

		slog.Info("job done",
			"id", hdr.ID,
			"type", hdr.Type,
			"duration_ms", res.Duration.Milliseconds(),
			"result", res.Output,
		)

		if err := rdb.HSet(ctx, hashKey,
			"status", "done",
			"result", res.Output,
			"duration_ms", fmt.Sprintf("%d", res.Duration.Milliseconds()),
		).Err(); err != nil {
			slog.Error("failed to write done status", "id", hdr.ID, "err", err)
		}
	}
}
