package events

import (
	"context"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
	"github.com/stretchr/testify/require"
)

func TestNoopSink_Publish(t *testing.T) {
	t.Parallel()

	sink := &NoopSink{}

	events := []*eventspb.Event{
		{Type: commonpb.EventType_COMMITTED_TRANSACTION, Ledger: "test", LogSequence: 1},
		{Type: commonpb.EventType_SAVED_METADATA, Ledger: "test", LogSequence: 2},
	}

	err := sink.Publish(context.Background(), events)
	require.NoError(t, err)
}

func TestNoopSink_Close(t *testing.T) {
	t.Parallel()

	sink := &NoopSink{}
	err := sink.Close()
	require.NoError(t, err)
}
