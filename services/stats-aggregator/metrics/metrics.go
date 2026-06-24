// Package metrics registers all Prometheus metrics for the stats-aggregator service.
//
// Service C (stats-aggregator) exposes system-wide aggregate metrics that
// Prometheus scrapes every 15 seconds. Unlike Service B's counters and
// histograms, every metric here is a Gauge — because each represents a
// current absolute value that can go up AND down:
//
//   total_jobs_submitted  — increases as new jobs are queued
//   total_jobs_completed  — increases as workers finish jobs
//   queue_length          — increases under load, decreases as workers drain it
//
// Gauge vs Counter vs Histogram:
//
//   Counter   — monotonically increasing (never goes down): use for totals over time
//   Gauge     — can go up or down: use for current state snapshots
//   Histogram — samples observations into configurable buckets: use for latency/size
//
// total_jobs_submitted and total_jobs_completed could technically be Counters
// (they never decrease), but we collect them by scanning Redis state on a
// polling interval rather than by incrementing on each event. That means we
// SET the value to whatever Redis currently says — which requires a Gauge's
// Set() method. A Counter only supports Inc() and Add(), not Set().
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds every Prometheus metric exposed by this service.
type Metrics struct {
	// TotalJobsSubmitted is the total number of jobs ever enqueued,
	// measured by counting all job hashes in Redis (status=any).
	// Prometheus query: total_jobs_submitted
	TotalJobsSubmitted prometheus.Gauge

	// TotalJobsCompleted is the number of jobs that reached status="done".
	// The difference (submitted - completed) is the backlog.
	// Prometheus query: total_jobs_submitted - total_jobs_completed
	TotalJobsCompleted prometheus.Gauge

	// QueueLength is the current number of items waiting in the Redis
	// job_queue list — jobs that have been submitted but not yet picked
	// up by any worker pod.
	//
	// This is the most operationally important metric: a growing queue_length
	// under steady load means workers are not keeping up and HPA should
	// scale out. A draining queue_length after load ends confirms that the
	// autoscaled pods are doing their job.
	// Prometheus query: queue_length
	QueueLength prometheus.Gauge

	// TotalJobErrors is the number of jobs that reached status="error".
	// Kept separate from completed so dashboards can show an error rate
	// without requiring label filtering.
	TotalJobErrors prometheus.Gauge

	// AvgProcessingTimeSeconds is the rolling average job processing time
	// computed from the duration_ms fields stored in Redis job hashes.
	// This is an approximation — a proper p99 comes from Service B's
	// histogram — but it gives a quick "how are jobs doing" number
	// visible on the /stats endpoint.
	AvgProcessingTimeSeconds prometheus.Gauge

	// CollectorDurationSeconds records how long each Redis poll cycle takes.
	// If the collector itself starts taking > collect_interval, it means
	// Redis is under pressure or there are too many job hashes to scan.
	CollectorDurationSeconds prometheus.Gauge

	// Registry is the private Prometheus registry all metrics belong to.
	Registry *prometheus.Registry
}

// New creates, registers, and returns a fully initialised Metrics value.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	submitted := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "statsaggregator",
		Name:      "total_jobs_submitted",
		Help:      "Total number of jobs ever submitted (all statuses), measured by Redis scan.",
	})

	completed := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "statsaggregator",
		Name:      "total_jobs_completed",
		Help:      "Total number of jobs that reached status=done.",
	})

	queueLen := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "statsaggregator",
		Name:      "queue_length",
		Help:      "Current number of jobs waiting in the Redis job_queue list (submitted but not yet picked up by a worker).",
	})

	errors := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "statsaggregator",
		Name:      "total_job_errors",
		Help:      "Total number of jobs that reached status=error.",
	})

	avgTime := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "statsaggregator",
		Name:      "avg_processing_time_seconds",
		Help:      "Rolling average job processing time in seconds, computed from Redis job hash duration_ms fields.",
	})

	collectorDuration := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "statsaggregator",
		Name:      "collector_duration_seconds",
		Help:      "Time taken by the last Redis poll cycle in seconds. High values indicate Redis pressure.",
	})

	reg.MustRegister(submitted, completed, queueLen, errors, avgTime, collectorDuration)

	return &Metrics{
		TotalJobsSubmitted:       submitted,
		TotalJobsCompleted:       completed,
		QueueLength:              queueLen,
		TotalJobErrors:           errors,
		AvgProcessingTimeSeconds: avgTime,
		CollectorDurationSeconds: collectorDuration,
		Registry:                 reg,
	}
}
