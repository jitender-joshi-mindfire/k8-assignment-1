// Package metrics registers all Prometheus metrics for the job-submitter service.
// It exposes a single registry so main.go can hand it to promhttp.HandlerFor,
// keeping the default process/Go collector metrics out of the response when not needed,
// and making unit testing trivial (no global state).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds every counter and histogram this service exposes.
type Metrics struct {
	// HTTPRequestsTotal counts every inbound HTTP request, labelled by
	// HTTP method, route pattern, and response status code class (2xx, 4xx …).
	HTTPRequestsTotal *prometheus.CounterVec

	// JobsSubmittedTotal counts every job successfully enqueued, labelled by
	// job type (prime | bcrypt | sort). Incrementing here — not in the worker —
	// gives us a submission rate independent of processing rate, which is the
	// first signal that the queue is growing.
	JobsSubmittedTotal *prometheus.CounterVec

	// Registry is the non-default registry all metrics above are registered on.
	// Exposing it lets main.go pass it straight to promhttp.HandlerFor so we
	// don't accidentally include metrics from other packages.
	Registry *prometheus.Registry
}

// New creates, registers, and returns a fully initialised Metrics value.
// Panics on duplicate registration (programming error, not a runtime condition).
func New() *Metrics {
	reg := prometheus.NewRegistry()

	// Include the standard Go runtime and process collectors so Grafana can
	// show goroutine counts, GC pause times, and memory allocations without
	// any extra work.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	httpReqs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobsubmitter",
			Name:      "http_requests_total",
			Help:      "Total number of HTTP requests handled, by method, route and status class.",
		},
		[]string{"method", "route", "status_class"},
	)

	jobsSubmitted := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobsubmitter",
			Name:      "jobs_submitted_total",
			Help:      "Total number of jobs successfully pushed onto the Redis queue, by job type.",
		},
		[]string{"type"},
	)

	reg.MustRegister(httpReqs, jobsSubmitted)

	return &Metrics{
		HTTPRequestsTotal:  httpReqs,
		JobsSubmittedTotal: jobsSubmitted,
		Registry:           reg,
	}
}
