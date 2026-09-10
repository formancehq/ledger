//go:build nats

package events

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestCreateSink_NATS_FailsWithoutServer(t *testing.T) {
	t.Parallel()

	m := &Manager{}

	cfg := &commonpb.SinkConfig{
		Name: "nats-sink",
		Type: &commonpb.SinkConfig_Nats{
			Nats: &commonpb.NatsSinkConfig{
				Servers: []*commonpb.ConnectionURL{{Scheme: "nats", Address: &commonpb.ConnectionAddress{Host: "localhost", Port: proto.Uint32(99999)}}},
				Topic:   "test-events",
			},
		},
		Format: "json",
	}

	sink, err := m.createSink(cfg)
	// NATS connection will fail since there is no server
	require.Error(t, err)
	require.Nil(t, sink)
}
