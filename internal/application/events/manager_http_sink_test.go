package events_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/application/events"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func saveHTTPSinkConfig(t *testing.T, s *dal.Store, name, endpoint string) {
	t.Helper()

	cfg := &commonpb.SinkConfig{
		Name: name,
		Type: &commonpb.SinkConfig_Http{
			Http: &commonpb.HttpSinkConfig{
				Endpoint: endpoint,
			},
		},
		Format: "json",
	}
	attr := attributes.NewAttribute[*commonpb.SinkConfig](dal.SubAttrSinkConfig)
	batch := s.NewBatch()
	_, err := attr.Set(batch, domain.SinkConfigKey{Name: name}.Bytes(), cfg)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

// TestManager_HTTPSink_StartStop tests that the Manager can create, start, and
// stop an HTTP sink emitter through the full reconcile lifecycle. This covers
// createSink (HTTP path), startSink, and stopSink.
func TestManager_HTTPSink_StartStop(t *testing.T) {
	t.Parallel()

	var requestCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()
	notifications := signal.NewNotifications()

	// Pre-save an HTTP sink config
	saveHTTPSinkConfig(t, store, "http-sink", server.URL)

	manager := events.NewManager(store, attributes.New(), proposer, logger, notifications)
	manager.Start()

	// Become leader -- triggers reconcile which creates and starts the HTTP sink
	manager.OnLeadershipChange(true)

	// Lose leadership -- triggers teardown which stops and closes the sink
	manager.OnLeadershipChange(false)

	// Regain leadership -- should restart cleanly
	manager.OnLeadershipChange(true)

	// Stop the manager
	manager.Stop()
}

// TestManager_HTTPSink_ConfigChangeRemovesSink tests that removing a sink config
// causes the Manager to stop the emitter and close the sink.
func TestManager_HTTPSink_ConfigChangeRemovesSink(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()
	notifications := signal.NewNotifications()

	// Pre-save an HTTP sink config
	saveHTTPSinkConfig(t, store, "http-sink", server.URL)

	manager := events.NewManager(store, attributes.New(), proposer, logger, notifications)

	manager.Start()
	defer manager.Stop()

	// Become leader
	manager.OnLeadershipChange(true)

	// Remove the sink config
	batch := store.NewBatch()
	require.NoError(t, attributes.NewAttribute[*commonpb.SinkConfig](dal.SubAttrSinkConfig).Delete(batch, domain.SinkConfigKey{Name: "http-sink"}.Bytes()))
	require.NoError(t, batch.Commit())

	// Notify config change
	notifications.NotifyConfigChanged()

	// Give the run() goroutine time to process the config change
	require.Eventually(t, func() bool {
		// Signal again to confirm the config change was processed
		notifications.NotifyConfigChanged()

		return true
	}, 2*time.Second, 50*time.Millisecond)
}

func TestManager_HTTPSink_LeadershipLossDuringInitialCatchup(t *testing.T) {
	t.Parallel()

	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	releaseRequest := make(chan struct{})
	var started atomic.Bool
	var canceled atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if started.CompareAndSwap(false, true) {
			close(requestStarted)
		}

		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()

		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("response writer does not support hijacking")

			return
		}

		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijacking response connection: %v", err)

			return
		}

		connClosed := make(chan struct{})
		go func() {
			var buf [1]byte
			_, _ = conn.Read(buf[:])
			close(connClosed)
		}()

		select {
		case <-releaseRequest:
			_ = conn.Close()
		case <-connClosed:
			if canceled.CompareAndSwap(false, true) {
				close(requestCanceled)
			}
		}
	}))
	defer server.Close()
	defer close(releaseRequest)

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()
	notifications := signal.NewNotifications()

	appendTestLogs(t, store, &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{
					Name:      "orders",
					CreatedAt: commonpb.NewTimestamp(libtime.Now()),
				},
			},
		},
	})
	saveHTTPSinkConfig(t, store, "http-sink", server.URL)

	manager := events.NewManager(store, attributes.New(), proposer, logger, notifications)
	manager.Start()
	defer manager.Stop()

	leadershipStarted := make(chan struct{})
	go func() {
		manager.OnLeadershipChange(true)
		close(leadershipStarted)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-leadershipStarted:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "initial catch-up must not hold the manager mutex")

	require.Eventually(t, func() bool {
		select {
		case <-requestStarted:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "initial catch-up should start publishing asynchronously")

	leadershipLost := make(chan struct{})
	go func() {
		manager.OnLeadershipChange(false)
		close(leadershipLost)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-leadershipLost:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "leadership loss must tear down a catch-up emitter")

	require.Eventually(t, func() bool {
		select {
		case <-requestCanceled:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "leadership loss must cancel the in-flight publish request")
}
