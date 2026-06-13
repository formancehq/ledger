package events

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source sink.go -destination sink_generated_test.go -typed -package events . Sink

// Sink publishes events to an external system.
// Implementations must be safe for concurrent use.
type Sink interface {
	// Publish sends events to the external system.
	// It must return promptly when ctx is canceled, including while I/O is in flight.
	// Returns an error if any event could not be delivered.
	// Events are provided in sequence order.
	Publish(ctx context.Context, events []*eventspb.Event) error

	// Close releases resources held by the sink.
	Close() error
}
