// Package handler contains the HTTP handlers for the job-submitter service.
// Each handler is a plain function that accepts the shared dependencies it
// needs rather than relying on package-level globals — this makes testing
// straightforward and keeps the dependency graph explicit.
package handler

import (
	"encoding/json"
	"net/http"

	"github.com/kubernetes-project/job-submitter/metrics"
	"github.com/kubernetes-project/job-submitter/queue"
	"github.com/redis/go-redis/v9"
)

// submitRequest is the JSON body expected on POST /submit.
type submitRequest struct {
	// Type must be one of the keys in queue.ValidJobTypes.
	Type string `json:"type"`
}

// submitResponse is the JSON body returned on a successful submission.
type submitResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

// errorResponse is the uniform error envelope for all 4xx/5xx replies.
type errorResponse struct {
	Error string `json:"error"`
}

// Submit returns an http.HandlerFunc that:
//  1. Decodes and validates the incoming JSON body.
//  2. Pushes a new job onto the Redis queue via queue.PushJob.
//  3. Increments the relevant Prometheus counters.
//  4. Returns 202 Accepted with the new job ID.
//
// It is constructed once at startup with closed-over dependencies, so the
// handler itself is a plain function value with no receiver — easy to test.
func Submit(rdb *redis.Client, m *metrics.Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// --- decode ---
		var req submitRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
			m.HTTPRequestsTotal.WithLabelValues(r.Method, "/submit", "4xx").Inc()
			return
		}

		// --- validate ---
		// Only the job types listed in queue.ValidJobTypes are accepted.
		// Rejecting unknown types here keeps the worker simple: it can trust
		// that every job on the queue has a known type.
		if !queue.ValidJobTypes[req.Type] {
			writeJSON(w, http.StatusBadRequest, errorResponse{
				Error: "unknown job type; valid types: prime, bcrypt, sort",
			})
			m.HTTPRequestsTotal.WithLabelValues(r.Method, "/submit", "4xx").Inc()
			return
		}

		// --- enqueue ---
		jobID, err := queue.PushJob(r.Context(), rdb, req.Type)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{
				Error: "failed to enqueue job: " + err.Error(),
			})
			m.HTTPRequestsTotal.WithLabelValues(r.Method, "/submit", "5xx").Inc()
			return
		}

		// --- metrics ---
		// Count successful submissions by type so the Grafana dashboard can
		// show a "submissions per second by type" breakdown.
		m.JobsSubmittedTotal.WithLabelValues(req.Type).Inc()
		m.HTTPRequestsTotal.WithLabelValues(r.Method, "/submit", "2xx").Inc()

		// --- respond ---
		// 202 Accepted (not 200 OK) because the job has been queued, not yet
		// processed. The caller must poll /status/:id to learn the outcome.
		writeJSON(w, http.StatusAccepted, submitResponse{
			JobID:  jobID,
			Status: "queued",
		})
	}
}

// writeJSON serialises v as JSON and writes it to w with the given status code.
// It sets Content-Type before WriteHeader so the header is included correctly.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
