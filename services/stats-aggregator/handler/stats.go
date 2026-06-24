// Package handler contains the HTTP handlers for the stats-aggregator service.
package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/kubernetes-project/stats-aggregator/collector"
)

// statsResponse is the JSON shape returned by GET /stats.
// Every field is exported (capital letter) so encoding/json serialises it.
// Snake_case JSON keys match the Prometheus metric names, making it easy for
// a reader to correlate what they see in /stats with what they see in Grafana.
type statsResponse struct {
	// QueueLength is the number of jobs waiting to be picked up by a worker.
	// This is the key operational signal: a growing queue means workers can't
	// keep up and HPA should be scaling out.
	QueueLength int64 `json:"queue_length"`

	// TotalSubmitted is every job ever enqueued regardless of outcome.
	TotalSubmitted int64 `json:"total_submitted"`

	// TotalCompleted is every job that reached status="done".
	TotalCompleted int64 `json:"total_completed"`

	// TotalErrors is every job that reached status="error".
	TotalErrors int64 `json:"total_errors"`

	// InFlight is the derived count of jobs currently being processed:
	//   submitted - completed - errors - queue_length
	// It should equal the number of active worker pods (since each pod
	// processes exactly one job at a time).
	InFlight int64 `json:"in_flight"`

	// AvgProcessingTimeSecs is the mean duration of all completed jobs
	// computed from the duration_ms fields stored in Redis.
	AvgProcessingTimeSecs float64 `json:"avg_processing_time_seconds"`

	// DataAge is how many seconds old the underlying snapshot is.
	// Helps callers understand the freshness of the data — it will be at
	// most COLLECT_INTERVAL seconds old under normal operation.
	DataAgeSecs float64 `json:"data_age_seconds"`
}

// errorResponse is the uniform error envelope for non-2xx replies.
type errorResponse struct {
	Error string `json:"error"`
}

// Stats returns an http.HandlerFunc that reads the latest Snapshot from the
// Store and renders it as JSON.
//
// The handler never touches Redis directly — it only reads the in-memory
// Snapshot that the background collector keeps fresh. This keeps /stats fast
// (microseconds, not milliseconds) and means a Redis outage does not make
// /stats return errors — it returns stale data with a visible DataAgeSecs.
func Stats(store *collector.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snap := store.Latest() // copy under RLock — goroutine-safe, fast

		// Derive in-flight: jobs that are neither waiting in queue, done, nor errored.
		// In normal operation this equals the number of worker pods actively processing.
		inFlight := snap.TotalSubmitted - snap.TotalCompleted - snap.TotalErrors - snap.QueueLength
		if inFlight < 0 {
			// Guard against momentary inconsistency between the queue length
			// (LLEN, real-time) and the job hash counts (SCAN, slightly lagged).
			inFlight = 0
		}

		resp := statsResponse{
			QueueLength:           snap.QueueLength,
			TotalSubmitted:        snap.TotalSubmitted,
			TotalCompleted:        snap.TotalCompleted,
			TotalErrors:           snap.TotalErrors,
			InFlight:              inFlight,
			AvgProcessingTimeSecs: snap.AvgProcessingTimeSeconds,
			DataAgeSecs:           time.Since(snap.CollectedAt).Seconds(),
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// writeError writes a uniform JSON error response.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: msg})
}
