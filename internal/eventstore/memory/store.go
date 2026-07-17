// Package memory provides an in-process implementation of eventstore.Store.
// It is used by unit tests and can back a single-process demo deployment; it
// is not durable across process restarts.
package memory

import (
	"context"
	"sync"
	"time"

	"streamledger/internal/eventstore"
)

type streamKey struct {
	aggregateType string
	aggregateID   string
}

// Store is a goroutine-safe in-memory eventstore.Store.
type Store struct {
	mu sync.Mutex

	globalSeq int64
	events    []eventstore.Event            // all events, in append order (== global order)
	versions  map[streamKey]int64           // current version per stream
	dedupe    map[string][]eventstore.Event // dedupeKey -> events produced the first time
	now       func() time.Time
}

// New constructs an empty Store.
func New() *Store {
	return &Store{
		versions: make(map[streamKey]int64),
		dedupe:   make(map[string][]eventstore.Event),
		now:      time.Now,
	}
}

// WithClock overrides the time source (used in tests for deterministic
// timestamps). Returns the store for chaining.
func (s *Store) WithClock(now func() time.Time) *Store {
	s.now = now
	return s
}

func (s *Store) Append(_ context.Context, dedupeKey string, toAppend []eventstore.AppendEvent) ([]eventstore.Event, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if dedupeKey != "" {
		if prior, ok := s.dedupe[dedupeKey]; ok {
			out := make([]eventstore.Event, len(prior))
			copy(out, prior)
			return out, true, nil
		}
	}

	// Validate optimistic concurrency for every stream touched *before*
	// mutating any state, so the batch is all-or-nothing.
	pending := make(map[streamKey]int64, len(toAppend))
	for _, e := range toAppend {
		key := streamKey{e.AggregateType, e.AggregateID}
		cur, seen := pending[key]
		if !seen {
			cur = s.versions[key]
		}
		if e.ExpectedVersion != eventstore.NoVersionCheck && e.ExpectedVersion != cur {
			return nil, false, eventstore.ErrVersionConflict
		}
		pending[key] = cur + 1
	}

	produced := make([]eventstore.Event, 0, len(toAppend))
	now := s.now()
	for _, e := range toAppend {
		key := streamKey{e.AggregateType, e.AggregateID}
		s.versions[key]++
		s.globalSeq++

		payload := make([]byte, len(e.Payload))
		copy(payload, e.Payload)

		ev := eventstore.Event{
			GlobalSeq:     s.globalSeq,
			AggregateType: e.AggregateType,
			AggregateID:   e.AggregateID,
			Version:       s.versions[key],
			EventType:     e.EventType,
			Payload:       payload,
			Metadata: eventstore.Metadata{
				DedupeKey: dedupeKey,
			},
			CreatedAt: now,
		}
		s.events = append(s.events, ev)
		produced = append(produced, ev)
	}

	if dedupeKey != "" {
		cached := make([]eventstore.Event, len(produced))
		copy(cached, produced)
		s.dedupe[dedupeKey] = cached
	}

	return produced, false, nil
}

func (s *Store) LookupDedupe(_ context.Context, dedupeKey string) ([]eventstore.Event, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if dedupeKey == "" {
		return nil, false, nil
	}
	prior, ok := s.dedupe[dedupeKey]
	if !ok {
		return nil, false, nil
	}
	out := make([]eventstore.Event, len(prior))
	copy(out, prior)
	return out, true, nil
}

func (s *Store) LoadStream(_ context.Context, aggregateType, aggregateID string) ([]eventstore.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []eventstore.Event
	for _, e := range s.events {
		if e.AggregateType == aggregateType && e.AggregateID == aggregateID {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *Store) LoadSince(_ context.Context, afterGlobalSeq int64, limit int) ([]eventstore.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []eventstore.Event
	for _, e := range s.events {
		if e.GlobalSeq > afterGlobalSeq {
			out = append(out, e)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) LatestGlobalSeq(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.globalSeq, nil
}

var _ eventstore.Store = (*Store)(nil)
