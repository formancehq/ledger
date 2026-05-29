package events

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

// Sink publishes events to an external system.
// Implementations must be safe for concurrent use.
type Sink interface {
	// Publish sends events to the external system.
	// Returns an error if any event could not be delivered.
	// Events are provided in sequence order.
	Publish(ctx context.Context, events []*eventspb.Event) error

	// Close releases resources held by the sink.
	Close() error
}
