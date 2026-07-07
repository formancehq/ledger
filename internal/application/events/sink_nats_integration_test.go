//go:build nats

package events_test

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/application/events"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// startTestNATSServer starts an embedded NATS server with JetStream enabled.
// It returns the server instance; cleanup is handled via t.Cleanup.
func startTestNATSServer(t *testing.T) *server.Server {
	t.Helper()

	// Use os.MkdirTemp instead of t.TempDir() to control cleanup ordering.
	// t.TempDir() registers its own cleanup that can race with JetStream
	// file handles not yet released after WaitForShutdown() on macOS.
	storeDir, err := os.MkdirTemp("", t.Name()) //nolint:usetesting // intentional: t.TempDir() cleanup races with JetStream file handles
	require.NoError(t, err)

	opts := &server.Options{
		Host:               "127.0.0.1",
		Port:               -1, // random port
		JetStream:          true,
		StoreDir:           storeDir,
		JetStreamMaxMemory: 64 * 1024 * 1024,  // 64 MB
		JetStreamMaxStore:  128 * 1024 * 1024, // 128 MB
	}

	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "NATS server failed to become ready")

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
		_ = os.RemoveAll(storeDir)
	})

	return ns
}

// createTestStream creates a JetStream stream that captures all subjects under the given topic prefix.
func createTestStream(t *testing.T, js jetstream.JetStream, streamName, topicPrefix string) {
	t.Helper()

	ctx := context.Background()
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{topicPrefix + ".>"},
	})
	require.NoError(t, err)
}

// consumeEvents reads up to expectedCount events from a JetStream consumer within a timeout.
// It uses a push-based Messages() iterator instead of polling Fetch() to avoid missed messages
// under system load.
func consumeEvents(t *testing.T, cons jetstream.Consumer, expectedCount int, timeout time.Duration) []jetstream.Msg {
	t.Helper()

	iter, err := cons.Messages()
	require.NoError(t, err)

	// Stop the iterator after timeout to unblock Next().
	timer := time.AfterFunc(timeout, func() { iter.Stop() })
	defer timer.Stop()
	defer iter.Stop()

	var msgs []jetstream.Msg
	for len(msgs) < expectedCount {
		msg, err := iter.Next()
		if err != nil {
			break
		}

		msgs = append(msgs, msg)
	}

	require.Len(t, msgs, expectedCount, "expected %d events from NATS", expectedCount)

	return msgs
}

func TestNATSSinkIntegration_PublishAndConsume(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)

	// Connect a consumer to verify events arrive
	conn, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)

	defer conn.Close()

	js, err := jetstream.New(conn)
	require.NoError(t, err)

	const topic = "ledger-events"
	createTestStream(t, js, "EVENTS", topic)

	cons, err := js.CreateConsumer(context.Background(), "EVENTS", jetstream.ConsumerConfig{
		Name:          "test-consumer",
		FilterSubject: topic + ".>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	// Set up emitter with real NATSSink
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
					CreateLedger: &commonpb.CreatedLedgerLog{
						Name: "orders", CreatedAt: uint64(commonpb.NewTimestamp(now)),
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

	sink, err := events.NewNATSSink(events.NATSSinkConfig{
		URL:    ns.ClientURL(),
		Topic:  topic,
		Format: events.FormatJSON,
	})
	require.NoError(t, err)

	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "nats-sink", proposer, newPlanBuilder(t, store), logger, cfg)
	emitter.Start()

	// Wait for cursor to advance
	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store, "nats-sink")

		return err == nil && cursor >= 2
	}, 5*time.Second, 10*time.Millisecond, "emitter should process all logs")

	emitter.Stop()

	// Consume and verify events from NATS
	msgs := consumeEvents(t, cons, 2, 5*time.Second)

	// Verify CREATED_LEDGER event (JSON)
	var evt1 map[string]any
	require.NoError(t, json.Unmarshal(msgs[0].Data(), &evt1))
	require.Equal(t, "CREATED_LEDGER", evt1["type"])
	require.Equal(t, "orders", evt1["ledger"])
	require.Equal(t, float64(1), evt1["logSequence"])

	// Verify COMMITTED_TRANSACTION event (JSON)
	var evt2 map[string]any
	require.NoError(t, json.Unmarshal(msgs[1].Data(), &evt2))
	require.Equal(t, "COMMITTED_TRANSACTION", evt2["type"])
	require.Equal(t, "orders", evt2["ledger"])
	require.Equal(t, float64(2), evt2["logSequence"])

	// Verify NATS subject routing
	require.Equal(t, topic+".orders.created_ledger", msgs[0].Subject())
	require.Equal(t, topic+".orders.committed_transaction", msgs[1].Subject())
}

func TestNATSSinkIntegration_ProtobufFormat(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)

	conn, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)

	defer conn.Close()

	js, err := jetstream.New(conn)
	require.NoError(t, err)

	const topic = "ledger-proto"
	createTestStream(t, js, "PROTO", topic)

	cons, err := js.CreateConsumer(context.Background(), "PROTO", jetstream.ConsumerConfig{
		Name:          "proto-consumer",
		FilterSubject: topic + ".>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

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

	sink, err := events.NewNATSSink(events.NATSSinkConfig{
		URL:    ns.ClientURL(),
		Topic:  topic,
		Format: events.FormatProto,
	})
	require.NoError(t, err)

	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "proto-sink", proposer, newPlanBuilder(t, store), logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store, "proto-sink")

		return err == nil && cursor >= 1
	}, 5*time.Second, 10*time.Millisecond, "emitter should process log")

	emitter.Stop()

	msgs := consumeEvents(t, cons, 1, 5*time.Second)

	// Deserialize protobuf and verify
	var evt eventspb.Event
	require.NoError(t, evt.UnmarshalVT(msgs[0].Data()))
	require.Equal(t, commonpb.EventType_COMMITTED_TRANSACTION, evt.GetType())
	require.Equal(t, "payments", evt.GetLedger())
	require.Equal(t, uint64(1), evt.GetLogSequence())
	require.NotNil(t, evt.GetLog(), "event should carry the full Log")
}

func TestNATSSinkIntegration_SubjectRouting(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)

	conn, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)

	defer conn.Close()

	js, err := jetstream.New(conn)
	require.NoError(t, err)

	const topic = "ledger-routing"
	createTestStream(t, js, "ROUTING", topic)

	// Create two filtered consumers: one for "orders", one for "payments"
	ordersConsumer, err := js.CreateConsumer(context.Background(), "ROUTING", jetstream.ConsumerConfig{
		Name:          "orders-consumer",
		FilterSubject: topic + ".orders.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	paymentsConsumer, err := js.CreateConsumer(context.Background(), "ROUTING", jetstream.ConsumerConfig{
		Name:          "payments-consumer",
		FilterSubject: topic + ".payments.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "orders")
	registerLedger(t, store, "payments")

	now := libtime.Now()

	appendTestLogs(t, store,
		// Log 1: Create "orders" ledger
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreatedLedgerLog{
						Name: "orders", CreatedAt: uint64(commonpb.NewTimestamp(now)),
					},
				},
			},
		},
		// Log 2: Create "payments" ledger
		&commonpb.Log{
			Sequence: 2,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreatedLedgerLog{
						Name: "payments", CreatedAt: uint64(commonpb.NewTimestamp(now)),
					},
				},
			},
		},
		// Log 3: Transaction on "orders"
		&commonpb.Log{
			Sequence: 3,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "shop", "USD", big.NewInt(100))).
										WithID(1).WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
		// Log 4: Transaction on "payments"
		&commonpb.Log{
			Sequence: 4,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "payments",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "merchant", "EUR", big.NewInt(200))).
										WithID(1).WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
	)

	sink, err := events.NewNATSSink(events.NATSSinkConfig{
		URL:    ns.ClientURL(),
		Topic:  topic,
		Format: events.FormatJSON,
	})
	require.NoError(t, err)

	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "routing-sink", proposer, newPlanBuilder(t, store), logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store, "routing-sink")

		return err == nil && cursor >= 4
	}, 5*time.Second, 10*time.Millisecond, "emitter should process all 4 logs")

	emitter.Stop()

	// Verify "orders" consumer gets exactly 2 events (CREATED_LEDGER + COMMITTED_TRANSACTION)
	ordersMsgs := consumeEvents(t, ordersConsumer, 2, 5*time.Second)

	var ordEvt1, ordEvt2 map[string]any
	require.NoError(t, json.Unmarshal(ordersMsgs[0].Data(), &ordEvt1))
	require.NoError(t, json.Unmarshal(ordersMsgs[1].Data(), &ordEvt2))

	require.Equal(t, "CREATED_LEDGER", ordEvt1["type"])
	require.Equal(t, "orders", ordEvt1["ledger"])
	require.Equal(t, "COMMITTED_TRANSACTION", ordEvt2["type"])
	require.Equal(t, "orders", ordEvt2["ledger"])

	// Verify "payments" consumer gets exactly 2 events
	paymentsMsgs := consumeEvents(t, paymentsConsumer, 2, 5*time.Second)

	var payEvt1, payEvt2 map[string]any
	require.NoError(t, json.Unmarshal(paymentsMsgs[0].Data(), &payEvt1))
	require.NoError(t, json.Unmarshal(paymentsMsgs[1].Data(), &payEvt2))

	require.Equal(t, "CREATED_LEDGER", payEvt1["type"])
	require.Equal(t, "payments", payEvt1["ledger"])
	require.Equal(t, "COMMITTED_TRANSACTION", payEvt2["type"])
	require.Equal(t, "payments", payEvt2["ledger"])
}
