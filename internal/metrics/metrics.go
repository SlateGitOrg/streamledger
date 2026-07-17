// Package metrics defines StreamLedger's Prometheus instrumentation: a
// throughput/latency tap on the command API and a lag/dedupe tap on the
// CQRS projections, exactly the two things you need on a dashboard to tell
// "healthy under load" apart from "silently falling behind".
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics bundles every collector StreamLedger registers. Construct one
// with New and register it on a *prometheus.Registry (or the default one)
// with Register.
type Metrics struct {
	CommandsTotal          *prometheus.CounterVec
	CommandDuration        *prometheus.HistogramVec
	DuplicateCommandsTotal *prometheus.CounterVec

	EventsAppendedTotal *prometheus.CounterVec
	VersionConflicts    *prometheus.CounterVec

	ProjectionEventsAppliedTotal     *prometheus.CounterVec
	ProjectionDuplicatesSkippedTotal *prometheus.CounterVec
	ProjectionLag                    *prometheus.GaugeVec
}

// New constructs all collectors, unregistered.
func New() *Metrics {
	return &Metrics{
		CommandsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "streamledger",
			Name:      "commands_total",
			Help:      "Total commands handled by the gRPC command API, by command name and result.",
		}, []string{"command", "result"}),

		CommandDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "streamledger",
			Name:      "command_duration_seconds",
			Help:      "Command handling latency in seconds, by command name.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"command"}),

		DuplicateCommandsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "streamledger",
			Name:      "duplicate_commands_total",
			Help:      "Commands recognized as duplicates of an in-flight or completed dedupe_key.",
		}, []string{"command"}),

		EventsAppendedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "streamledger",
			Name:      "events_appended_total",
			Help:      "Events successfully appended to the event log, by aggregate type.",
		}, []string{"aggregate_type"}),

		VersionConflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "streamledger",
			Name:      "version_conflicts_total",
			Help:      "Optimistic-concurrency conflicts encountered while appending, by aggregate type.",
		}, []string{"aggregate_type"}),

		ProjectionEventsAppliedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "streamledger",
			Name:      "projection_events_applied_total",
			Help:      "Events applied by a projection, by projection name and event type.",
		}, []string{"projection", "event_type"}),

		ProjectionDuplicatesSkippedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "streamledger",
			Name:      "projection_duplicates_skipped_total",
			Help:      "Events skipped by a projection because they were at or below its checkpoint (proof of no double-apply).",
		}, []string{"projection"}),

		ProjectionLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "streamledger",
			Name:      "projection_lag_events",
			Help:      "Number of events a projection's checkpoint is behind the event log's latest global sequence.",
		}, []string{"projection"}),
	}
}

// Register adds every collector to reg.
func (m *Metrics) Register(reg *prometheus.Registry) error {
	collectors := []prometheus.Collector{
		m.CommandsTotal,
		m.CommandDuration,
		m.DuplicateCommandsTotal,
		m.EventsAppendedTotal,
		m.VersionConflicts,
		m.ProjectionEventsAppliedTotal,
		m.ProjectionDuplicatesSkippedTotal,
		m.ProjectionLag,
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return err
		}
	}
	return nil
}

// ProjectionMetrics adapts Metrics to projections.Metrics.
type ProjectionMetrics struct {
	M *Metrics
}

func (p ProjectionMetrics) EventApplied(projectionName, eventType string) {
	p.M.ProjectionEventsAppliedTotal.WithLabelValues(projectionName, eventType).Inc()
}

func (p ProjectionMetrics) DuplicateSkipped(projectionName string) {
	p.M.ProjectionDuplicatesSkippedTotal.WithLabelValues(projectionName).Inc()
}

func (p ProjectionMetrics) Lag(projectionName string, lag int64) {
	p.M.ProjectionLag.WithLabelValues(projectionName).Set(float64(lag))
}
