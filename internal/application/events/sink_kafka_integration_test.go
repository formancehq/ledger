//go:build kafka

package events_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/go-libs/v4/logging"
	libtime "github.com/formancehq/go-libs/v4/time"

	"github.com/formancehq/ledger-v3-poc/internal/application/events"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
)

// consumeKafkaMessages reads up to expectedCount messages from a Kafka topic within a timeout.
func consumeKafkaMessages(t *testing.T, brokers []string, topic string, expectedCount int, timeout time.Duration) []*sarama.ConsumerMessage {
	t.Helper()

	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer(brokers, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close() })

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pc.Close() })

	var msgs []*sarama.ConsumerMessage

	deadline := time.After(timeout)

	for len(msgs) < expectedCount {
		select {
		case msg := <-pc.Messages():
			msgs = append(msgs, msg)
		case <-deadline:
			require.Len(t, msgs, expectedCount, "expected %d Kafka messages within %v", expectedCount, timeout)

			return msgs
		}
	}

	return msgs
}

func TestKafkaSinkIntegration_PublishAndConsume(t *testing.T) {
	t.Parallel()

	brokers := sharedKafkaBrokers
	topic := uniqueTopic("ledger-events")

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "orders")

	now := libtime.Now()

	appendTestLogs(t, store,
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreateLedgerLog{
						Info: &commonpb.LedgerInfo{Name: "orders", CreatedAt: commonpb.NewTimestamp(now)},
					},
				},
			},
		},
		&commonpb.Log{
			Sequence: 2,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "bank", "USD", big.NewInt(1000))).
										WithID(1).WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
	)

	sink, err := events.NewKafkaSink(events.KafkaSinkConfig{
		Brokers: brokers,
		Topic:   topic,
		Format:  events.FormatJSON,
	})
	require.NoError(t, err)

	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "kafka-sink", proposer, logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store, "kafka-sink")

		return err == nil && cursor >= 2
	}, 10*time.Second, 10*time.Millisecond, "emitter should process all logs")

	emitter.Stop()

	// Consume and verify events from Kafka
	msgs := consumeKafkaMessages(t, brokers, topic, 2, 10*time.Second)

	// Verify CREATED_LEDGER event (JSON)
	var evt1 eventspb.Event
	require.NoError(t, protojson.Unmarshal(msgs[0].Value, &evt1))
	require.Equal(t, commonpb.EventType_CREATED_LEDGER, evt1.GetType())
	require.Equal(t, "orders", evt1.GetLedger())
	require.Equal(t, uint64(1), evt1.GetLogSequence())

	// Verify COMMITTED_TRANSACTION event (JSON)
	var evt2 eventspb.Event
	require.NoError(t, protojson.Unmarshal(msgs[1].Value, &evt2))
	require.Equal(t, commonpb.EventType_COMMITTED_TRANSACTION, evt2.GetType())
	require.Equal(t, "orders", evt2.GetLedger())
	require.Equal(t, uint64(2), evt2.GetLogSequence())

	// Verify event_type header
	require.Equal(t, "created_ledger", headerValue(msgs[0].Headers, "event_type"))
	require.Equal(t, "committed_transaction", headerValue(msgs[1].Headers, "event_type"))
}

func TestKafkaSinkIntegration_MessageKeyIsLedger(t *testing.T) {
	t.Parallel()

	brokers := sharedKafkaBrokers
	topic := uniqueTopic("ledger-keys")

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "payments")

	now := libtime.Now()

	appendTestLogs(t, store,
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "payments",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "merchant", "EUR", big.NewInt(500))).
										WithID(1).WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
	)

	sink, err := events.NewKafkaSink(events.KafkaSinkConfig{
		Brokers: brokers,
		Topic:   topic,
		Format:  events.FormatJSON,
	})
	require.NoError(t, err)

	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "kafka-key-sink", proposer, logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store, "kafka-key-sink")

		return err == nil && cursor >= 1
	}, 10*time.Second, 10*time.Millisecond, "emitter should process log")

	emitter.Stop()

	msgs := consumeKafkaMessages(t, brokers, topic, 1, 10*time.Second)

	// Verify message key is the ledger name
	require.Equal(t, "payments", string(msgs[0].Key))
}

func TestKafkaSinkIntegration_ProtobufFormat(t *testing.T) {
	t.Parallel()

	brokers := sharedKafkaBrokers
	topic := uniqueTopic("ledger-proto")

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "payments")

	now := libtime.Now()

	appendTestLogs(t, store,
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "payments",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "merchant", "EUR", big.NewInt(500))).
										WithID(1).WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
	)

	sink, err := events.NewKafkaSink(events.KafkaSinkConfig{
		Brokers: brokers,
		Topic:   topic,
		Format:  events.FormatProto,
	})
	require.NoError(t, err)

	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "kafka-proto-sink", proposer, logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store, "kafka-proto-sink")

		return err == nil && cursor >= 1
	}, 10*time.Second, 10*time.Millisecond, "emitter should process log")

	emitter.Stop()

	msgs := consumeKafkaMessages(t, brokers, topic, 1, 10*time.Second)

	// Deserialize protobuf and verify
	var evt eventspb.Event
	require.NoError(t, evt.UnmarshalVT(msgs[0].Value))
	require.Equal(t, commonpb.EventType_COMMITTED_TRANSACTION, evt.GetType())
	require.Equal(t, "payments", evt.GetLedger())
	require.Equal(t, uint64(1), evt.GetLogSequence())
	require.NotNil(t, evt.GetLog(), "event should carry the full Log")
}

// headerValue extracts a header value from Kafka record headers by key.
func headerValue(headers []*sarama.RecordHeader, key string) string {
	for _, h := range headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}

	return ""
}
