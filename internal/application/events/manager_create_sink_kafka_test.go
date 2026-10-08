//go:build kafka

package events

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestCreateSink_Kafka_FailsWithoutBroker(t *testing.T) {
	t.Parallel()

	m := &Manager{}

	cfg := &ledgerpb.SinkConfig{
		Name: "kafka-sink",
		Type: &ledgerpb.SinkConfig_Kafka{
			Kafka: &ledgerpb.KafkaSinkConfig{
				Brokers: []string{"localhost:99999"},
				Topic:   "test-events",
			},
		},
		Format: "json",
	}

	sink, err := m.createSink(cfg)
	// Kafka connection will fail since there is no broker
	require.Error(t, err)
	require.Nil(t, sink)
}
