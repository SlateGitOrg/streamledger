package projections

import (
	"context"

	"streamledger/internal/eventstore"
)

// Metrics is the observability hook a Projector reports through. It is an
// interface (rather than a direct Prometheus dependency) so this package
// stays unit-testable without pulling in the metrics registry; see
// internal/metrics for the real Prometheus-backed implementation and
// NoopMetrics below for tests.
type Metrics interface {
	EventApplied(projectionName, eventType string)
	DuplicateSkipped(projectionName string)
	Lag(projectionName string, lag int64)
}

// NoopMetrics discards everything; useful in tests that don't assert on
// metrics.
type NoopMetrics struct{}

func (NoopMetrics) EventApplied(string, string) {}
func (NoopMetrics) DuplicateSkipped(string)     {}
func (NoopMetrics) Lag(string, int64)           {}

// EventApplier contains the projection-specific logic: which events it
// cares about and how to fold one into the read model.
type EventApplier interface {
	// Relevant reports whether Apply should be called for this event.
	// Events that are not relevant still advance the projection's
	// checkpoint (see Projector.ApplyEvent) so that lag is measured
	// against the whole log, not just the events a given projection cares
	// about.
	Relevant(event eventstore.Event) bool
	// Apply folds event into the read model. It is only ever called with
	// events for which Relevant returned true, and only once per event
	// (Projector guarantees the checkpoint dedupe).
	Apply(ctx context.Context, event eventstore.Event) error
}

// Projector is a named, idempotent event consumer that folds events into a
// read model behind a monotonically increasing checkpoint. The checkpoint
// is what actually provides the "effectively-once" guarantee: no matter how
// many times an event is delivered (bus redelivery, overlapping catch-up
// polls, at-least-once retries), an event whose GlobalSeq is at or below
// the checkpoint is a no-op.
type Projector struct {
	Name    string
	Store   ReadModelStore
	Applier EventApplier
	Metrics Metrics
}

// NewProjector constructs a Projector. metrics may be nil (NoopMetrics is
// used in that case).
func NewProjector(name string, store ReadModelStore, applier EventApplier, metrics Metrics) *Projector {
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	return &Projector{Name: name, Store: store, Applier: applier, Metrics: metrics}
}

// ApplyEvent applies a single event if (and only if) it has not already
// been applied. It returns applied=true if the event caused a state
// mutation (as opposed to being skipped as a duplicate or irrelevant).
//
// The whole check-checkpoint / mutate-view / advance-checkpoint sequence
// runs inside one Store.WithTx call, which is what makes this safe against
// the same event being handed to ApplyEvent twice concurrently (bus
// redelivery racing a CatchUp poll, two projector replicas, etc): without
// it, two callers could both read the pre-advance checkpoint before either
// had written the advanced one, and both would apply — a double count.
func (p *Projector) ApplyEvent(ctx context.Context, event eventstore.Event) (applied bool, err error) {
	err = p.Store.WithTx(ctx, func(ctx context.Context) error {
		checkpoint, err := p.Store.GetCheckpoint(ctx, p.Name)
		if err != nil {
			return err
		}
		if event.GlobalSeq <= checkpoint {
			p.Metrics.DuplicateSkipped(p.Name)
			return nil
		}

		if p.Applier.Relevant(event) {
			if err := p.Applier.Apply(ctx, event); err != nil {
				return err
			}
			applied = true
			p.Metrics.EventApplied(p.Name, event.EventType)
		}

		return p.Store.SetCheckpoint(ctx, p.Name, event.GlobalSeq)
	})
	if err != nil {
		return false, err
	}
	return applied, nil
}

// CatchUp pulls every event since this projection's checkpoint from store
// and applies it. It is safe to call concurrently with live event delivery
// (e.g. from a bus subscription) and safe to call repeatedly: already
// applied events are skipped by ApplyEvent's checkpoint check. This is the
// durability backstop for the pub/sub fast path — even if every bus message
// were lost, periodic CatchUp calls make the read model eventually
// consistent with the log.
func (p *Projector) CatchUp(ctx context.Context, store eventstore.Store, batchSize int) (applied int, err error) {
	for {
		checkpoint, err := p.Store.GetCheckpoint(ctx, p.Name)
		if err != nil {
			return applied, err
		}
		events, err := store.LoadSince(ctx, checkpoint, batchSize)
		if err != nil {
			return applied, err
		}
		if len(events) == 0 {
			return applied, nil
		}
		for _, e := range events {
			ok, err := p.ApplyEvent(ctx, e)
			if err != nil {
				return applied, err
			}
			if ok {
				applied++
			}
		}
		if len(events) < batchSize {
			return applied, nil
		}
	}
}

// Lag reports how many events (by GlobalSeq) this projection is behind the
// given latestGlobalSeq, and records it via Metrics.
func (p *Projector) Lag(ctx context.Context, latestGlobalSeq int64) (int64, error) {
	checkpoint, err := p.Store.GetCheckpoint(ctx, p.Name)
	if err != nil {
		return 0, err
	}
	lag := latestGlobalSeq - checkpoint
	if lag < 0 {
		lag = 0
	}
	p.Metrics.Lag(p.Name, lag)
	return lag, nil
}

// Checkpoint exposes the current checkpoint (used by query handlers that
// report per-projection lag).
func (p *Projector) Checkpoint(ctx context.Context) (int64, error) {
	return p.Store.GetCheckpoint(ctx, p.Name)
}
