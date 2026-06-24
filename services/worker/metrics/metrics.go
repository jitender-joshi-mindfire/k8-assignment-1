// Package metrics registers all Prometheus metrics for the worker service.
//
// The three metrics here are exactly what the assignment specifies:
//
//   jobs_processed_total          — Counter
//   job_processing_time_seconds   — Histogram
//   job_errors_total              — Counter
//
// They are namespaced under "worker" so they are unambiguous in Grafana when
// both service-b and service-c metrics land in the same Prometheus instance.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds every counter, histogram, and gauge this service exposes.
// All fields are exported so the consumer and dispatcher packages can increment
// them directly without any getter indirection.
type Metrics struct {
	// JobsProcessedTotal counts every job that exits the processing path,
	// regardless of success or failure. The "status" label ("success"|"error")
	// lets Grafana draw success rate = success / (success + error).
	// The "type" label ("prime"|"bcrypt"|"sort") lets you see per-job-type
	// throughput, which is useful for capacity planning.
	JobsProcessedTotal *prometheus.CounterVec

	// JobProcessingTimeSeconds records the wall-clock duration of each job
	// from the moment the worker pops it off the queue to the moment it writes
	// the result back to Redis.
	//
	// Histogram (not Summary) because:
	//   • Histograms are aggregatable across pods — you can add bucket counts
	//     from 8 worker pods and get the fleet-wide p99 in a single PromQL query.
	//   • Summaries compute quantiles per-process and cannot be aggregated.
	//
	// Bucket boundaries are chosen to match the expected job durations on
	// the tuned workload sizes (prime 1M, bcrypt cost-14, sort 500k):
	//   - 0.1s  — fast sort jobs
	//   - 0.5s  — typical sort, fast prime
	//   - 1s    — typical prime
	//   - 2s    — slow prime, fast bcrypt
	//   - 5s    — typical bcrypt
	//   - 10s   — slow bcrypt
	//   - 30s   — absolute ceiling (anything longer is a bug)
	JobProcessingTimeSeconds *prometheus.HistogramVec

	// JobErrorsTotal counts jobs that terminated with an error.
	// Keeping this separate from JobsProcessedTotal (which also records errors
	// via the "error" label) makes alerting simpler: a single threshold rule on
	// rate(job_errors_total[1m]) > 0 is enough for a paging alert without
	// complex label matching.
	JobErrorsTotal *prometheus.CounterVec

	// ActiveJobs tracks how many jobs are currently being processed by this
	// pod. It starts at 0, increments when a job is popped from the queue,
	// and decrements when the job finishes (success or error).
	// This gauge is particularly useful during stress testing: if it reaches 1
	// and stays there while the queue is growing, it confirms that each pod
	// is single-threaded (by design — BRPOP is synchronous).
	ActiveJobs prometheus.Gauge

	// Registry is the private registry all metrics above belong to.
	// main.go passes it to promhttp.HandlerFor so only these metrics are served
	// on /metrics — no leakage from third-party packages.
	Registry *prometheus.Registry
}

// New creates, registers, and returns a fully initialised Metrics value.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	jobsProcessed := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Name:      "jobs_processed_total",
			Help:      "Total number of jobs processed (both success and error), labelled by type and status.",
		},
		[]string{"type", "status"},
	)

	jobDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Name:      "job_processing_time_seconds",
			Help:      "Wall-clock duration of each job from queue-pop to result-write, in seconds.",
			Buckets:   []float64{0.1, 0.5, 1, 2, 5, 10, 30},
		},
		[]string{"type"},
	)

	jobErrors := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "worker",
			Name:      "job_errors_total",
			Help:      "Total number of jobs that terminated with an error, labelled by type.",
		},
		[]string{"type"},
	)

	activeJobs := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "worker",
		Name:      "active_jobs",
		Help:      "Number of jobs currently being processed by this pod.",
	})

	reg.MustRegister(jobsProcessed, jobDuration, jobErrors, activeJobs)

	return &Metrics{
		JobsProcessedTotal:       jobsProcessed,
		JobProcessingTimeSeconds: jobDuration,
		JobErrorsTotal:           jobErrors,
		ActiveJobs:               activeJobs,
		Registry:                 reg,
	}
}
