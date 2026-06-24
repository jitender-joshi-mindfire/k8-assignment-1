// Package collector owns the background Redis polling loop for the
// stats-aggregator service.
//
// Design: polling vs event-driven
// ─────────────────────────────────────────────────────────────────────────────
// The "ideal" design would be event-driven: Service B publishes completion
// events to a Redis Pub/Sub channel and Service C subscribes. This gives
// real-time updates with zero polling overhead.
//
// We deliberately use polling instead because:
//  1. It is simpler — no Pub/Sub setup, no subscriber reconnection logic.
//  2. The assignment asks for aggregate stats on a scrape interval, not a
//     real-time stream. 15-second staleness is acceptable.
//  3. Polling is self-healing: if Service C restarts it re-reads all state
//     from Redis without needing a replay mechanism.
//  4. It matches how Prometheus itself works — pull-based scraping.
//
// Concurrency model
// ─────────────────────────────────────────────────────────────────────────────
// The collector runs in a single goroutine (launched from main.go).
// The HTTP handler (GET /stats) runs in the HTTP server's goroutine pool.
// They share a *Snapshot value protected by a sync.RWMutex:
//
//   collector goroutine  →  mu.Lock()   → writes Snapshot → mu.Unlock()
//   HTTP handler         →  mu.RLock()  → reads  Snapshot → mu.RUnlock()
//
// Multiple readers (concurrent /stats requests) can hold RLock simultaneously.
// The writer (collector) blocks all readers while it updates — but only for
// the nanoseconds it takes to copy a small struct, not for the entire
// Redis poll duration.
package collector

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/kubernetes-project/stats-aggregator/metrics"
	"github.com/redis/go-redis/v9"
)

const (
	// jobQueueKey is the Redis list that job-submitter pushes to and worker pops from.
	// Must match the constants in job-submitter/queue/redis.go and worker/consumer/redis.go.
	jobQueueKey = "job_queue"

	// jobHashPattern is the glob pattern used with SCAN to find all job hashes.
	// Redis SCAN is preferred over KEYS because:
	//   - KEYS blocks Redis while it runs (O(N) on all keys).
	//   - SCAN iterates in small batches, interleaved with other commands.
	//   For this assignment the number of job keys is small, but it's good
	//   practice to never use KEYS in production code.
	jobHashPattern = "job:*"

	// scanBatchSize is the COUNT hint for SCAN. Redis may return more or fewer
	// than this per batch — it is only a hint, not a guarantee.
	scanBatchSize = 100
)

// Snapshot holds a consistent point-in-time view of job system state.
// It is written by the collector goroutine and read by the HTTP handler.
// All fields are safe to read without a lock once you hold the RLock —
// the entire struct is replaced atomically (pointer swap under Lock),
// never partially updated.
type Snapshot struct {
	QueueLength              int64
	TotalSubmitted           int64
	TotalCompleted           int64
	TotalErrors              int64
	AvgProcessingTimeSeconds float64
	CollectedAt              time.Time
}

// Store is the shared state between the collector goroutine and HTTP handlers.
// Embedding sync.RWMutex into a struct (rather than using a global mutex) keeps
// the lock co-located with the data it protects — the data and its lock travel
// together as a single dependency injection.
type Store struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

// NewStore creates and returns an empty Store.
func NewStore() *Store {
	return &Store{
		snapshot: Snapshot{CollectedAt: time.Now()},
	}
}

// Latest returns a copy of the most recent Snapshot.
// Returning a copy (not a pointer) means the caller gets a stable value
// that cannot be overwritten by a concurrent collector update mid-read.
func (s *Store) Latest() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot // copy by value — safe because Snapshot has no pointer fields
}

// update atomically replaces the stored snapshot.
// Called only by the collector goroutine — no contention on the write path.
func (s *Store) update(snap Snapshot) {
	s.mu.Lock()
	s.snapshot = snap
	s.mu.Unlock()
	// No defer here — the critical section is a single assignment,
	// so the lock is held for nanoseconds. Defer adds a small overhead
	// that is worth avoiding on a very hot path. For less critical paths,
	// always use defer for correctness.
}

// Start runs the background collection loop. It blocks until ctx is cancelled.
// Call it in a goroutine from main.go:
//
//	go collector.Start(ctx, rdb, store, m, collectInterval)
func Start(
	ctx context.Context,
	rdb *redis.Client,
	store *Store,
	m *metrics.Metrics,
	collectInterval time.Duration,
) {
	slog.Info("collector started", "interval", collectInterval)

	// Run once immediately on startup so /stats returns real data right away
	// rather than zeros until the first tick fires.
	collect(ctx, rdb, store, m)

	ticker := time.NewTicker(collectInterval)
	defer ticker.Stop() // always stop the ticker to free its goroutine

	for {
		select {
		case <-ctx.Done():
			slog.Info("collector stopping: context cancelled")
			return

		case <-ticker.C:
			collect(ctx, rdb, store, m)
		}
	}
}

// collect performs one full Redis poll cycle:
//  1. LLEN job_queue                 → queue length
//  2. SCAN job:* + HGETALL each key  → count statuses, sum durations
//  3. Update Store and Prometheus gauges
//
// It is a regular function (not a method) so it can be called from Start
// and also directly in tests without needing to run the ticker loop.
func collect(ctx context.Context, rdb *redis.Client, store *Store, m *metrics.Metrics) {
	cycleStart := time.Now()

	// ── 1. Queue length ────────────────────────────────────────────────────
	// LLEN is O(1) — Redis stores the list length in the list header.
	queueLen, err := rdb.LLen(ctx, jobQueueKey).Result()
	if err != nil {
		slog.Error("collector: LLEN failed", "err", err)
		// Non-fatal: use zero for this cycle. The next cycle will retry.
		queueLen = 0
	}

	// ── 2. Scan all job hashes ─────────────────────────────────────────────
	// SCAN iterates over keyspace in cursor-based batches. Each call returns:
	//   - a new cursor (0 means the full iteration is complete)
	//   - a batch of matching keys
	//
	// We accumulate counts across all batches by looping until cursor == 0.
	var (
		totalSubmitted int64
		totalCompleted int64
		totalErrors    int64
		durationSum    float64
		durationCount  int64
		cursor         uint64
	)

	for {
		// SCAN with MATCH and COUNT — non-blocking, interleaved with other cmds.
		keys, nextCursor, err := rdb.Scan(ctx, cursor, jobHashPattern, scanBatchSize).Result()
		if err != nil {
			slog.Error("collector: SCAN failed", "cursor", cursor, "err", err)
			break
		}

		// Process each key in this batch.
		for _, key := range keys {
			totalSubmitted++

			// HGETALL returns all field-value pairs for the hash.
			fields, err := rdb.HGetAll(ctx, key).Result()
			if err != nil {
				slog.Warn("collector: HGETALL failed", "key", key, "err", err)
				continue
			}

			switch fields["status"] {
			case "done":
				totalCompleted++
				// Parse duration_ms stored by the worker.
				// Not all done jobs have duration_ms (e.g. jobs processed before
				// we added that field), so we use the ok pattern.
				if dms, ok := fields["duration_ms"]; ok {
					if ms, err := strconv.ParseInt(dms, 10, 64); err == nil {
						durationSum += float64(ms) / 1000.0 // convert ms → seconds
						durationCount++
					}
				}
			case "error":
				totalErrors++
			}
			// "pending" and "processing" are implicitly counted in totalSubmitted
			// but not in completed/errors. The in-flight count is:
			//   totalSubmitted - totalCompleted - totalErrors
		}

		// cursor == 0 means SCAN has returned to the beginning — full iteration done.
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	// ── 3. Compute average processing time ────────────────────────────────
	var avgSecs float64
	if durationCount > 0 {
		avgSecs = durationSum / float64(durationCount)
	}

	cycleDuration := time.Since(cycleStart)

	// ── 4. Build snapshot ─────────────────────────────────────────────────
	snap := Snapshot{
		QueueLength:              queueLen,
		TotalSubmitted:           totalSubmitted,
		TotalCompleted:           totalCompleted,
		TotalErrors:              totalErrors,
		AvgProcessingTimeSeconds: avgSecs,
		CollectedAt:              time.Now(),
	}

	// ── 5. Write snapshot (under lock) ────────────────────────────────────
	store.update(snap)

	// ── 6. Update Prometheus gauges ────────────────────────────────────────
	// Gauge.Set() replaces the current value — this is why we use Gauges
	// and not Counters here. We're setting "current state", not "increment".
	m.QueueLength.Set(float64(queueLen))
	m.TotalJobsSubmitted.Set(float64(totalSubmitted))
	m.TotalJobsCompleted.Set(float64(totalCompleted))
	m.TotalJobErrors.Set(float64(totalErrors))
	m.AvgProcessingTimeSeconds.Set(avgSecs)
	m.CollectorDurationSeconds.Set(cycleDuration.Seconds())

	slog.Info("collector cycle complete",
		"queue_length", queueLen,
		"submitted", totalSubmitted,
		"completed", totalCompleted,
		"errors", totalErrors,
		"avg_secs", avgSecs,
		"cycle_ms", cycleDuration.Milliseconds(),
	)
}
