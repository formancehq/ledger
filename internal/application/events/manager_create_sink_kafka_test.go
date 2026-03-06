//go:build kafka

package events

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func TestCreateSink_Kafka_FailsWithoutBroker(t *testing.T) {
	t.Parallel()

	m := &Manager{}

	cfg := &commonpb.SinkConfig{
		Name: "kafka-sink",
		Type: &commonpb.SinkConfig_Kafka{
			Kafka: &commonpb.KafkaSinkConfig{
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
