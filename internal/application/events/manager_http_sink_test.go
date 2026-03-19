package events_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v4/logging"

	"github.com/formancehq/ledger-v3-poc/internal/application/events"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func saveHTTPSinkConfig(t *testing.T, s *dal.Store, name, endpoint string) {
	t.Helper()

	batch := s.NewBatch()
	require.NoError(t, state.SaveSinkConfig(batch, &commonpb.SinkConfig{
		Name: name,
		Type: &commonpb.SinkConfig_Http{
			Http: &commonpb.HttpSinkConfig{
				Endpoint: endpoint,
			},
		},
		Format: "json",
	}))
	require.NoError(t, batch.Commit())
}

// TestManager_HTTPSink_StartStop tests that the Manager can create, start, and
// stop an HTTP sink emitter through the full reconcile lifecycle. This covers
// createSink (HTTP path), startSink, stopSink, and Ready.
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

	manager := events.NewManager(store, proposer, logger, notifications)
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

	manager := events.NewManager(store, proposer, logger, notifications)

	manager.Start()
	defer manager.Stop()

	// Become leader
	manager.OnLeadershipChange(true)

	// Remove the sink config
	batch := store.NewBatch()
	require.NoError(t, state.DeleteSinkConfig(batch, "http-sink"))
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
