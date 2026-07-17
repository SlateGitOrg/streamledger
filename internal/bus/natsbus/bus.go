// Package natsbus is the production bus.Bus implementation, backed by NATS
// JetStream for durable, replayable pub/sub.
//
// It is intentionally not exercised by the default test suite: doing so
// honestly would require either a running NATS server or an embedded one,
// and this project's offline-verifiable contract is "no Docker daemon, no
// network at test time". The code is real and complete; internal/bus/inmemory
// plus internal/bus.DuplicatingPublisher stand in for it in tests, since the
// property under test — idempotent projections under duplicate delivery —
// is a property of the *consumer*, not of which broker redelivered the
// message. See the optional Testcontainers integration test for the
// Postgres side of the same argument.
package natsbus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"streamledger/internal/bus"
	"streamledger/internal/eventstore"
)

// wireEvent is the JSON-on-the-wire representation of eventstore.Event. It
// is kept separate from eventstore.Event so the wire format can evolve
// independently of the internal struct's field names/types.
type wireEvent struct {
	GlobalSeq     int64     `json:"global_seq"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id"`
	Version       int64     `json:"version"`
	EventType     string    `json:"event_type"`
	Payload       []byte    `json:"payload"`
	DedupeKey     string    `json:"dedupe_key,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

func toWire(e eventstore.Event) wireEvent {
	return wireEvent{
		GlobalSeq:     e.GlobalSeq,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID,
		Version:       e.Version,
		EventType:     e.EventType,
		Payload:       e.Payload,
		DedupeKey:     e.Metadata.DedupeKey,
		CorrelationID: e.Metadata.CorrelationID,
		CreatedAt:     e.CreatedAt,
	}
}

func (w wireEvent) toEvent() eventstore.Event {
	return eventstore.Event{
		GlobalSeq:     w.GlobalSeq,
		AggregateType: w.AggregateType,
		AggregateID:   w.AggregateID,
		Version:       w.Version,
		EventType:     w.EventType,
		Payload:       w.Payload,
		Metadata: eventstore.Metadata{
			DedupeKey:     w.DedupeKey,
			CorrelationID: w.CorrelationID,
		},
		CreatedAt: w.CreatedAt,
	}
}

// Options configures a Bus.
type Options struct {
	// StreamName is both the JetStream stream name and the subject
	// prefix; events publish to "<StreamName>.<aggregate_type>".
	StreamName string
	// DurableConsumerName identifies the durable pull consumer used by
	// Subscribe, so that a restarted process resumes from where it left
	// off rather than replaying (or skipping) the whole stream.
	DurableConsumerName string
}

func (o Options) withDefaults() Options {
	if o.StreamName == "" {
		o.StreamName = "streamledger_events"
	}
	if o.DurableConsumerName == "" {
		o.DurableConsumerName = "streamledger_projections"
	}
	return o
}

// Bus is a NATS JetStream-backed bus.Bus.
type Bus struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	stream jetstream.Stream
	opts   Options
}

// Connect dials natsURL, ensures the configured JetStream stream exists,
// and returns a ready-to-use Bus.
func Connect(ctx context.Context, natsURL string, opts Options) (*Bus, error) {
	opts = opts.withDefaults()

	nc, err := nats.Connect(natsURL, nats.Name("streamledger"))
	if err != nil {
		return nil, fmt.Errorf("natsbus: connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("natsbus: jetstream: %w", err)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     opts.StreamName,
		Subjects: []string{opts.StreamName + ".>"},
		// Idempotency window: NATS itself will drop exact duplicate
		// Nats-Msg-Id publishes within this window. This is a
		// performance optimization, NOT the correctness mechanism —
		// the window is finite, so the real guarantee comes from the
		// projection checkpoint (see internal/projections.Projector).
		Duplicates: 2 * time.Minute,
		Storage:    jetstream.FileStorage,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("natsbus: create stream: %w", err)
	}

	return &Bus{nc: nc, js: js, stream: stream, opts: opts}, nil
}

func (b *Bus) Publish(ctx context.Context, events []eventstore.Event) error {
	for _, e := range events {
		data, err := json.Marshal(toWire(e))
		if err != nil {
			return fmt.Errorf("natsbus: marshal event: %w", err)
		}
		subject := fmt.Sprintf("%s.%s", b.opts.StreamName, e.AggregateType)
		msg := nats.NewMsg(subject)
		msg.Data = data
		// De-duplication hint for NATS' bounded dedupe window (see
		// Duplicates above). GlobalSeq is already globally unique and
		// monotonic, making it a natural message ID.
		msg.Header.Set(nats.MsgIdHdr, fmt.Sprintf("%d", e.GlobalSeq))

		if _, err := b.js.PublishMsg(ctx, msg); err != nil {
			return fmt.Errorf("natsbus: publish: %w", err)
		}
	}
	return nil
}

func (b *Bus) Subscribe(ctx context.Context, handler bus.Handler) (func() error, error) {
	consumer, err := b.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       b.opts.DurableConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    -1, // redeliver indefinitely on Nak/ack-wait timeout; consumers MUST be idempotent
	})
	if err != nil {
		return nil, fmt.Errorf("natsbus: create consumer: %w", err)
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		var w wireEvent
		if err := json.Unmarshal(msg.Data(), &w); err != nil {
			// Malformed message: ack it away rather than poison-pilling
			// the consumer forever. In production this would also emit
			// a metric/log.
			_ = msg.Ack()
			return
		}
		if err := handler(ctx, bus.Message{Event: w.toEvent()}); err != nil {
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		return nil, fmt.Errorf("natsbus: consume: %w", err)
	}

	unsubscribe := func() error {
		consumeCtx.Stop()
		return nil
	}
	return unsubscribe, nil
}

func (b *Bus) Close() error {
	return b.nc.Drain()
}

var _ bus.Bus = (*Bus)(nil)
