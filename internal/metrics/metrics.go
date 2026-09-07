// Package metrics defines the Prometheus collectors for the queue and folds job
// events into them. The dashboard already sees every transition on the event bus,
// so it's the aggregation point: counters/histograms come from events, gauges from
// polling Redis. (A large fleet would instrument each worker and scrape all; the
// event bus gives us one clean seam here.)
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/aryan-bhokare/distributed-job-queue/internal/events"
)

var (
	Enqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_enqueued_total",
		Help: "Total jobs enqueued, by type.",
	}, []string{"type"})

	Processed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_processed_total",
		Help: "Total jobs that reached a terminal outcome, by type and status.",
	}, []string{"type", "status"}) // status: succeeded | dead | skipped

	Retried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_retried_total",
		Help: "Total retries scheduled.",
	})

	Reclaimed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_reclaimed_total",
		Help: "Total jobs reclaimed from crashed workers by the reaper.",
	})

	Duration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "job_duration_seconds",
		Help:    "Handler execution time, by type.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2, 5, 10},
	}, []string{"type"})

	QueueDepth     = promauto.NewGauge(prometheus.GaugeOpts{Name: "queue_depth", Help: "Entries retained in the default stream."})
	ScheduledDepth = promauto.NewGauge(prometheus.GaugeOpts{Name: "scheduled_depth", Help: "Delayed/retry jobs waiting to become due."})
	InFlight       = promauto.NewGauge(prometheus.GaugeOpts{Name: "jobs_in_flight", Help: "Jobs delivered but not yet acked (PEL size)."})
	DLQSize        = promauto.NewGauge(prometheus.GaugeOpts{Name: "dlq_size", Help: "Jobs in the dead-letter queue."})
)

// Record folds one job event into the counters/histograms.
func Record(e events.Event) {
	switch e.Kind {
	case events.Enqueued:
		Enqueued.WithLabelValues(e.JobType).Inc()
	case events.Succeeded:
		Processed.WithLabelValues(e.JobType, "succeeded").Inc()
		if e.DurationMs > 0 {
			Duration.WithLabelValues(e.JobType).Observe(float64(e.DurationMs) / 1000)
		}
	case events.Retrying:
		Retried.Inc()
	case events.Dead:
		Processed.WithLabelValues(e.JobType, "dead").Inc()
	case events.Reclaimed:
		Reclaimed.Inc()
	case events.NoHandler:
		Processed.WithLabelValues(e.JobType, "skipped").Inc()
	}
}
