//go:build nats

package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNATSSinkConstructorSanitizesMalformedConnectionError(t *testing.T) {
	t.Parallel()
	sink, err := NewNATSSink(NATSSinkConfig{
		URL:    "nats://alice:nats-password@localhost:invalid-port",
		Topic:  "ledger-events",
		Format: FormatProto,
	})
	require.Nil(t, sink)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connecting to NATS")
	require.Contains(t, err.Error(), "invalid port")
	require.NotContains(t, err.Error(), "nats-password")
}
