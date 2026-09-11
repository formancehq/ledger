//go:build nats

package events_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/events"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// observedNATSSink instruments the real broker publication, keeping each result
// at a barrier until the test has inspected the durable cursor and stream.
type observedNATSSink struct {
	*events.NATSSink

	results chan error
	resume  chan struct{}
}

func (s *observedNATSSink) Publish(ctx context.Context, batch []*eventspb.Event) error {
	err := s.NATSSink.Publish(ctx, batch)
	select {
	case s.results <- err:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.resume:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *observedNATSSink) awaitResult(t *testing.T) error {
	t.Helper()
	select {
	case err := <-s.results:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("no observed NATS publication")

		return nil
	}
}

func TestNATSSinkIntegration_AdmittedLedgerNames(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, token string }{
		{"a..b", "a%2E%2Eb"}, {".orders", "%2Eorders"}, {"orders.", "orders%2E"},
		{"orders", "orders"}, {"a.b", "a%2Eb"}, {"_systemx", "_systemx"}, {"A_z:0-9", "A_z:0-9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Admission.Admit coverage is in TestAdmit_CreateLedgerNamesForEventRouting; this
			// fixture independently pins the shared name contract before log conversion.
			require.Nil(t, domain.ValidateLedgerName(tc.name))
			ns := startTestNATSServer(t)
			conn, err := nats.Connect(ns.ClientURL())
			require.NoError(t, err)
			defer conn.Close()
			js, err := jetstream.New(conn)
			require.NoError(t, err)
			createTestStream(t, js, "EVENTS", "events")
			stream, err := js.Stream(t.Context(), "EVENTS")
			require.NoError(t, err)
			store := newTestStore(t)
			appendTestLogs(t, store,
				&commonpb.Log{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: tc.name}}}},
				&commonpb.Log{Sequence: 2, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "normal"}}}},
			)
			realSink, err := events.NewNATSSink(events.NATSSinkConfig{URL: ns.ClientURL(), Topic: "events", Format: events.FormatJSON})
			require.NoError(t, err)
			defer func() { require.NoError(t, realSink.Close()) }()
			sink := &observedNATSSink{NATSSink: realSink, results: make(chan error), resume: make(chan struct{})}
			cfg := events.DefaultEmitterConfig()
			cfg.BatchSize = 2 // nil selection includes CREATED_LEDGER and all later events.
			emitter := events.NewEmitter(store, sink, "nats", &directProposer{store: store}, newPlanBuilder(t, store), logging.Testing(), cfg)
			healthy := &recordingSink{}
			second := events.NewEmitter(store, healthy, "second", &directProposer{store: store}, newPlanBuilder(t, store), logging.Testing(), cfg)
			emitter.Start()
			defer emitter.Stop()
			second.Start()
			defer second.Stop()
			firstErr := sink.awaitResult(t)
			cursor, err := query.ReadSinkCursor(store, "nats")
			require.NoError(t, err)
			require.Zero(t, cursor, "publication must not be acknowledged before Publish returns")
			require.Eventually(t, func() bool {
				c, e := query.ReadSinkCursor(store, "second")

				return e == nil && c == 2
			}, 5*time.Second, 10*time.Millisecond)
			require.Len(t, healthy.getEvents(), 2, "second sink progresses independently")
			if firstErr != nil {
				// Diagnostic assertions preserve the fail-before evidence: a second real
				// retry cannot bypass the invalid subject or acknowledge either event.
				require.Contains(t, firstErr.Error(), "publishing event seq=1")
				require.ErrorIs(t, firstErr, jetstream.ErrNoStreamResponse)
				sink.resume <- struct{}{}
				retryErr := sink.awaitResult(t)
				require.Error(t, retryErr)
				info, err := stream.Info(t.Context())
				require.NoError(t, err)
				require.Zero(t, info.State.Msgs)
				cursor, err = query.ReadSinkCursor(store, "nats")
				require.NoError(t, err)
				require.Zero(t, cursor)
				t.Logf("two failed real publications; cursor=%d; messages=%d; second sink cursor=2: %v", cursor, info.State.Msgs, firstErr)
			}
			require.NoError(t, firstErr, "every admitted ledger name must be routable")
			sink.resume <- struct{}{}
			require.Eventually(t, func() bool {
				c, e := query.ReadSinkCursor(store, "nats")

				return e == nil && c == 2
			}, 5*time.Second, 10*time.Millisecond)
			info, err := stream.Info(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 2, info.State.Msgs)
			for filter, count := range map[string]uint64{
				"events.*.created_ledger":                2,
				"events." + tc.token + ".created_ledger": 1,
			} {
				consumer, err := js.CreateConsumer(t.Context(), "EVENTS", jetstream.ConsumerConfig{FilterSubject: filter, AckPolicy: jetstream.AckExplicitPolicy})
				require.NoError(t, err)
				consumerInfo, err := consumer.Info(t.Context())
				require.NoError(t, err)
				require.Equal(t, count, consumerInfo.NumPending, filter)
			}
			for i, want := range []struct{ name, subject string }{{tc.name, "events." + tc.token + ".created_ledger"}, {"normal", "events.normal.created_ledger"}} {
				msg, err := stream.GetMsg(t.Context(), uint64(i+1))
				require.NoError(t, err)
				require.Equal(t, want.subject, msg.Subject)
				var payload map[string]any
				require.NoError(t, json.Unmarshal(msg.Data, &payload))
				require.Equal(t, want.name, payload["ledger"])
				require.Equal(t, "CREATED_LEDGER", payload["type"])
				require.Equal(t, float64(i+1), payload["logSequence"])
			}
		})
	}
}

func TestNATSSinkIntegration_NameRetryAfterStreamRecovery(t *testing.T) {
	t.Parallel()
	ns := startTestNATSServer(t)
	conn, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer conn.Close()
	js, err := jetstream.New(conn)
	require.NoError(t, err)
	store := newTestStore(t)
	logs := []*commonpb.Log{
		{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "a..b"}}}},
		{Sequence: 2, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "normal"}}}},
	}
	appendTestLogs(t, store, logs...)
	realSink, err := events.NewNATSSink(events.NATSSinkConfig{URL: ns.ClientURL(), Topic: "events", Format: events.FormatProto})
	require.NoError(t, err)
	defer func() { require.NoError(t, realSink.Close()) }()
	sink := &observedNATSSink{NATSSink: realSink, results: make(chan error), resume: make(chan struct{})}
	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 2
	cfg.EventTypes = map[commonpb.EventType]struct{}{} // Empty also selects all events.
	emitter := events.NewEmitter(store, sink, "retry", &directProposer{store: store}, newPlanBuilder(t, store), logging.Testing(), cfg)
	emitter.Start()
	defer emitter.Stop()
	// A real missing stream produces one failed attempt. Provisioning it is
	// synchronized before allowing the emitter to retry the same pending batch.
	err = sink.awaitResult(t)
	require.ErrorIs(t, err, jetstream.ErrNoStreamResponse)
	require.Contains(t, err.Error(), "events.a%2E%2Eb.created_ledger")
	cursor, err := query.ReadSinkCursor(store, "retry")
	require.NoError(t, err)
	require.Zero(t, cursor)
	createTestStream(t, js, "EVENTS", "events")
	stream, err := js.Stream(t.Context(), "EVENTS")
	require.NoError(t, err)
	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	require.Zero(t, info.State.Msgs)
	sink.resume <- struct{}{}
	require.NoError(t, sink.awaitResult(t)) // Exactly the second observed attempt succeeds.
	sink.resume <- struct{}{}
	require.Eventually(t, func() bool {
		c, e := query.ReadSinkCursor(store, "retry")

		return e == nil && c == 2
	}, 5*time.Second, 10*time.Millisecond)
	emitter.Stop()
	info, err = stream.Info(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, info.State.Msgs)
	for i, name := range []string{"a..b", "normal"} {
		msg, err := stream.GetMsg(t.Context(), uint64(i+1))
		require.NoError(t, err)
		var event eventspb.Event
		require.NoError(t, event.UnmarshalVT(msg.Data))
		require.Equal(t, name, event.GetLedger())
		require.Equal(t, uint64(i+1), event.GetLogSequence())
		expected, err := events.SerializeEvent(events.LogToEvent(logs[i]), events.FormatProto)
		require.NoError(t, err)
		require.Equal(t, expected, msg.Data, "retry must preserve final bytes")
	}
}
