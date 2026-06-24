package handler

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/kubernetes-project/job-submitter/metrics"
	"github.com/kubernetes-project/job-submitter/queue"
	"github.com/redis/go-redis/v9"
)

// Status returns an http.HandlerFunc that reads the current state of a job
// from Redis and returns it as JSON.
//
// Three outcomes are possible:
//   - 200 OK        — job found; body contains full JobStatus (including result
//                     or error once the worker has finished).
//   - 404 Not Found — no Redis Hash exists for this ID; either the ID is wrong
//                     or the job was never successfully enqueued.
//   - 502 Bad Gateway — Redis returned an unexpected error; surfaced as 502
//                       because from the client's perspective this is an upstream
//                       failure, not a client mistake.
func Status(rdb *redis.Client, m *metrics.Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// chi stores URL path parameters in the request context.
		// chi.URLParam returns "" for unknown parameters, so we validate below.
		id := chi.URLParam(r, "id")
		if id == "" {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "missing job id"})
			m.HTTPRequestsTotal.WithLabelValues(r.Method, "/status/{id}", "4xx").Inc()
			return
		}

		jobStatus, err := queue.GetJobStatus(r.Context(), rdb, id)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, errorResponse{
				Error: "failed to query job status: " + err.Error(),
			})
			m.HTTPRequestsTotal.WithLabelValues(r.Method, "/status/{id}", "5xx").Inc()
			return
		}

		// queue.GetJobStatus returns (nil, nil) when the key does not exist.
		if jobStatus == nil {
			writeJSON(w, http.StatusNotFound, errorResponse{
				Error: "job not found: " + id,
			})
			m.HTTPRequestsTotal.WithLabelValues(r.Method, "/status/{id}", "4xx").Inc()
			return
		}

		m.HTTPRequestsTotal.WithLabelValues(r.Method, "/status/{id}", "2xx").Inc()
		writeJSON(w, http.StatusOK, jobStatus)
	}
}
