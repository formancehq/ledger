//go:build nats

package events

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

// Only Publish is used. This external JetStream interface is not mockgen-managed.
type natsPublishFailure struct {
	jetstream.JetStream

	cause error
}

func (s natsPublishFailure) Publish(context.Context, string, []byte, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, s.cause
}

func TestNATSSinkPublishSanitizesDriverError(t *testing.T) {
	t.Parallel()
	cause := errors.New("nats.example rejected password nats-secret")
	sink := &NATSSink{
		js:     natsPublishFailure{cause: cause},
		topic:  "ledger-events",
		format: FormatProto,
		errors: newSinkErrorSanitizer(nil, "nats-secret"),
	}
	err := sink.Publish(t.Context(), []*eventspb.Event{{LogSequence: 42}})
	require.ErrorIs(t, err, cause)
	require.Contains(t, err.Error(), "publishing event seq=42 to ledger-events.")
	require.Contains(t, err.Error(), "nats.example rejected password")
	require.NotContains(t, err.Error(), "nats-secret")
}
