//go:build nats

package events

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
)

func TestCreateSink_NATS_FailsWithoutServer(t *testing.T) {
	t.Parallel()

	m := &Manager{}

	cfg := &ledgerpb.SinkConfig{
		Name: "nats-sink",
		Type: &ledgerpb.SinkConfig_Nats{
			Nats: &ledgerpb.NatsSinkConfig{
				Url:   "nats://localhost:99999",
				Topic: "test-events",
			},
		},
		Format: "json",
	}

	sink, err := m.createSink(t.Context(), cfg)
	// NATS connection will fail since there is no server
	require.Error(t, err)
	require.Nil(t, sink)
}
