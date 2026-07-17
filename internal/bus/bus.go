// Package bus defines the low-latency event-notification abstraction that
// sits between the append-only log and the CQRS projections. It is
// deliberately a thin, best-effort fast path: the durable source of truth
// is always the event log, and every subscriber must be safe to catch up
// from eventstore.Store directly, because at-least-once (never
// exactly-once) is the only honest delivery guarantee a message broker can
// make. See README.md's "Why effectively-once, not exactly-once" section.
package bus

import (
	"context"

	"streamledger/internal/eventstore"
)

// Message is what gets published for one appended event.
type Message struct {
	Event eventstore.Event
}

// Handler processes one delivered message. Returning an error means the
// message was not successfully processed; implementations may retry or
// redeliver it, which is exactly why downstream consumers must be
// idempotent.
type Handler func(ctx context.Context, msg Message) error

// Publisher publishes events, best-effort, after they have already been
// durably appended to the event log.
type Publisher interface {
	Publish(ctx context.Context, events []eventstore.Event) error
}

// Subscriber delivers published messages to handler until the returned
// unsubscribe func is called or ctx is cancelled. Delivery is at-least-once:
// a message may be delivered more than once (redelivery after a slow ack,
// consumer restart, network partition, ...).
type Subscriber interface {
	Subscribe(ctx context.Context, handler Handler) (unsubscribe func() error, err error)
}

// Bus is the full pub/sub contract used by StreamLedger.
type Bus interface {
	Publisher
	Subscriber
	Close() error
}
