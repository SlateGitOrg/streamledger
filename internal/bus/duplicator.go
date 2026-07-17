package bus

import (
	"context"

	"streamledger/internal/eventstore"
)

// DuplicatingPublisher wraps another Publisher and republishes every event
// ExtraDeliveries additional times. It exists to simulate, deterministically
// and without a real broker, the at-least-once redelivery that Kafka/NATS
// perform in production (consumer restarts mid-batch, slow acks past a
// redelivery timeout, rebalances, etc). Tests use it to prove the
// projections layer is genuinely idempotent under duplicate delivery,
// rather than merely happening not to receive duplicates in-process.
type DuplicatingPublisher struct {
	Inner           Publisher
	ExtraDeliveries int
}

func (d *DuplicatingPublisher) Publish(ctx context.Context, events []eventstore.Event) error {
	if err := d.Inner.Publish(ctx, events); err != nil {
		return err
	}
	for i := 0; i < d.ExtraDeliveries; i++ {
		if err := d.Inner.Publish(ctx, events); err != nil {
			return err
		}
	}
	return nil
}

var _ Publisher = (*DuplicatingPublisher)(nil)
