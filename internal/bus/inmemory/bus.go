// Package inmemory is a goroutine-safe, in-process bus.Bus implementation.
// It backs unit tests and a single-process demo deployment. Unlike the NATS
// JetStream implementation, it never redelivers on its own — which is
// exactly why bus.DuplicatingPublisher exists: to simulate the redelivery a
// real broker performs, so idempotency can be tested without one.
package inmemory

import (
	"context"
	"sync"

	"streamledger/internal/bus"
	"streamledger/internal/eventstore"
)

// Bus is a simple synchronous fan-out: Publish calls every subscribed
// handler in-line, in subscription order, on the caller's goroutine.
type Bus struct {
	mu       sync.Mutex
	handlers map[int]bus.Handler
	nextID   int
	closed   bool
}

// New constructs an empty Bus.
func New() *Bus {
	return &Bus{handlers: make(map[int]bus.Handler)}
}

func (b *Bus) Publish(ctx context.Context, events []eventstore.Event) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	handlers := make([]bus.Handler, 0, len(b.handlers))
	for _, h := range b.handlers {
		handlers = append(handlers, h)
	}
	b.mu.Unlock()

	for _, e := range events {
		msg := bus.Message{Event: e}
		for _, h := range handlers {
			if err := h(ctx, msg); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Bus) Subscribe(_ context.Context, handler bus.Handler) (func() error, error) {
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	b.handlers[id] = handler
	b.mu.Unlock()

	unsubscribe := func() error {
		b.mu.Lock()
		delete(b.handlers, id)
		b.mu.Unlock()
		return nil
	}
	return unsubscribe, nil
}

func (b *Bus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.handlers = map[int]bus.Handler{}
	return nil
}

var _ bus.Bus = (*Bus)(nil)
