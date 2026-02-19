package events

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
)

// NoopSink discards all events. Used when no sink is configured.
type NoopSink struct{}

func (n *NoopSink) Publish(_ context.Context, _ []*eventspb.Event) error {
	return nil
}

func (n *NoopSink) Close() error {
	return nil
}
