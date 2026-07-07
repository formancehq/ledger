package events_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/application/events"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// webhookReceiver collects HTTP requests for test assertions.
type webhookReceiver struct {
	mu       sync.Mutex
	requests []receivedWebhook
}

type receivedWebhook struct {
	Body        []byte
	ContentType string
	EventType   string
	Ledger      string
	LogSequence string
	Signature   string
}

func (r *webhookReceiver) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)

		r.mu.Lock()
		r.requests = append(r.requests, receivedWebhook{
			Body:        body,
			ContentType: req.Header.Get("Content-Type"),
			EventType:   req.Header.Get("X-Event-Type"),
			Ledger:      req.Header.Get("X-Ledger"),
			LogSequence: req.Header.Get("X-Log-Sequence"),
			Signature:   req.Header.Get("X-Webhook-Signature"),
		})
		r.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}
}

func (r *webhookReceiver) getRequests() []receivedWebhook {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]receivedWebhook, len(r.requests))
	copy(out, r.requests)

	return out
}

func TestHTTPSinkIntegration_PublishAndReceive(t *testing.T) {
	t.Parallel()

	receiver := &webhookReceiver{}

	server := httptest.NewServer(receiver.handler())
	defer server.Close()

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

	sink, err := events.NewHTTPSink(events.HTTPSinkConfig{
		Endpoint: server.URL,
		Format:   events.FormatJSON,
	})
	require.NoError(t, err)

	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "http-sink", proposer, newPlanBuilder(t, store), logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store, "http-sink")

		return err == nil && cursor >= 2
	}, 5*time.Second, 10*time.Millisecond, "emitter should process all logs")

	emitter.Stop()

	reqs := receiver.getRequests()
	require.Len(t, reqs, 2)

	// Verify CREATED_LEDGER event
	var evt1 map[string]any
	require.NoError(t, json.Unmarshal(reqs[0].Body, &evt1))
	require.Equal(t, "CREATED_LEDGER", evt1["type"])
	require.Equal(t, "orders", evt1["ledger"])
	require.Equal(t, float64(1), evt1["logSequence"])
	require.Equal(t, "application/json", reqs[0].ContentType)
	require.Equal(t, "created_ledger", reqs[0].EventType)
	require.Equal(t, "orders", reqs[0].Ledger)
	require.Equal(t, "1", reqs[0].LogSequence)

	// Verify COMMITTED_TRANSACTION event
	var evt2 map[string]any
	require.NoError(t, json.Unmarshal(reqs[1].Body, &evt2))
	require.Equal(t, "COMMITTED_TRANSACTION", evt2["type"])
	require.Equal(t, "orders", evt2["ledger"])
	require.Equal(t, float64(2), evt2["logSequence"])
	require.Equal(t, "committed_transaction", reqs[1].EventType)
	require.Equal(t, "2", reqs[1].LogSequence)
}

func TestHTTPSinkIntegration_HMACSignature(t *testing.T) {
	t.Parallel()

	const secret = "test-webhook-secret"

	receiver := &webhookReceiver{}

	server := httptest.NewServer(receiver.handler())
	defer server.Close()

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

	sink, err := events.NewHTTPSink(events.HTTPSinkConfig{
		Endpoint: server.URL,
		Secret:   secret,
		Format:   events.FormatJSON,
	})
	require.NoError(t, err)

	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "http-hmac-sink", proposer, newPlanBuilder(t, store), logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store, "http-hmac-sink")

		return err == nil && cursor >= 1
	}, 5*time.Second, 10*time.Millisecond, "emitter should process log")

	emitter.Stop()

	reqs := receiver.getRequests()
	require.Len(t, reqs, 1)

	// Verify HMAC signature
	require.NotEmpty(t, reqs[0].Signature)
	require.True(t, verifyHMAC(reqs[0].Body, secret, reqs[0].Signature),
		"HMAC signature should be valid")
}

func TestHTTPSinkIntegration_ProtobufFormat(t *testing.T) {
	t.Parallel()

	receiver := &webhookReceiver{}

	server := httptest.NewServer(receiver.handler())
	defer server.Close()

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

	sink, err := events.NewHTTPSink(events.HTTPSinkConfig{
		Endpoint: server.URL,
		Format:   events.FormatProto,
	})
	require.NoError(t, err)

	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "http-proto-sink", proposer, newPlanBuilder(t, store), logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store, "http-proto-sink")

		return err == nil && cursor >= 1
	}, 5*time.Second, 10*time.Millisecond, "emitter should process log")

	emitter.Stop()

	reqs := receiver.getRequests()
	require.Len(t, reqs, 1)
	require.Equal(t, "application/protobuf", reqs[0].ContentType)

	// Deserialize protobuf
	var evt eventspb.Event
	require.NoError(t, evt.UnmarshalVT(reqs[0].Body))
	require.Equal(t, commonpb.EventType_COMMITTED_TRANSACTION, evt.GetType())
	require.Equal(t, "payments", evt.GetLedger())
	require.Equal(t, uint64(1), evt.GetLogSequence())
	require.NotNil(t, evt.GetLog(), "event should carry the full Log")
}

func TestHTTPSinkIntegration_ServerError(t *testing.T) {
	t.Parallel()

	// Server returns 500 for all requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	sink, err := events.NewHTTPSink(events.HTTPSinkConfig{
		Endpoint: server.URL,
		Format:   events.FormatJSON,
	})
	require.NoError(t, err)

	defer func() { _ = sink.Close() }()

	// Publish directly (not through emitter) to verify error propagation
	evt := &eventspb.Event{
		Type:        commonpb.EventType_COMMITTED_TRANSACTION,
		Ledger:      "test",
		LogSequence: 1,
	}
	err = sink.Publish(t.Context(), []*eventspb.Event{evt})
	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
}

// verifyHMAC verifies a "sha256=<hex>" signature against a body and secret.
func verifyHMAC(body []byte, secret, signature string) bool {
	if len(signature) < 8 || signature[:7] != "sha256=" {
		return false
	}

	expectedSig, err := hex.DecodeString(signature[7:])
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)

	return hmac.Equal(mac.Sum(nil), expectedSig)
}
