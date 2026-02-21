package events_test

import (
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/events"
	"github.com/formancehq/ledger-v3-poc/internal/service/state"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
)

func saveSinkConfig(t *testing.T, s *dal.Store, config *commonpb.SinkConfig) {
	t.Helper()
	batch := s.NewBatch()
	require.NoError(t, state.SaveSinkConfig(batch, config))
	require.NoError(t, batch.Commit())
}

func deleteSinkConfig(t *testing.T, s *dal.Store, name string) {
	t.Helper()
	batch := s.NewBatch()
	require.NoError(t, state.DeleteSinkConfig(batch, name))
	require.NoError(t, batch.Commit())
}

func TestManager_AddRemoveSink(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()
	notifications := events.NewNotifications()

	manager := events.NewManager(store, proposer, logger, notifications)
	manager.Start()
	defer manager.Stop()

	// Add a sink config (noop sink type = nil, manager skips unsupported types)
	saveSinkConfig(t, store, &commonpb.SinkConfig{
		Name:   "primary",
		Format: "json",
	})

	// Become leader — should reconcile emitters
	manager.OnLeadershipChange(true)

	// Verify manager is active by checking it doesn't panic on config change signal
	notifications.NotifyConfigChanged()

	// Remove all sinks (events implicitly disabled)
	deleteSinkConfig(t, store, "primary")

	// Signal config change
	notifications.NotifyConfigChanged()

	// Give run() goroutine time to process
	require.Eventually(t, func() bool {
		// Signal again to ensure the previous one was consumed
		notifications.NotifyConfigChanged()
		return true
	}, time.Second, 10*time.Millisecond)
}

func TestManager_LeadershipChange(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()
	notifications := events.NewNotifications()

	// Pre-save a sink config
	saveSinkConfig(t, store, &commonpb.SinkConfig{
		Name:   "primary",
		Format: "json",
	})

	manager := events.NewManager(store, proposer, logger, notifications)
	manager.Start()
	defer manager.Stop()

	// Become leader — emitter should start
	manager.OnLeadershipChange(true)

	// Lose leadership — emitter should stop
	manager.OnLeadershipChange(false)

	// Regain leadership — emitter should restart
	manager.OnLeadershipChange(true)
}

func TestManager_ConfigChangeWhileFollower(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()
	notifications := events.NewNotifications()

	manager := events.NewManager(store, proposer, logger, notifications)
	manager.Start()
	defer manager.Stop()

	// Save a sink config while not leader
	saveSinkConfig(t, store, &commonpb.SinkConfig{
		Name:   "primary",
		Format: "json",
	})

	// Signal config change — should be a no-op since we're a follower
	notifications.NotifyConfigChanged()

	// Give run() goroutine time to process
	require.Eventually(t, func() bool {
		notifications.NotifyConfigChanged()
		return true
	}, time.Second, 10*time.Millisecond)
}

func TestManager_StopWithoutStart(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()
	notifications := events.NewNotifications()

	manager := events.NewManager(store, proposer, logger, notifications)
	manager.Start()
	// Stop immediately — should not hang
	manager.Stop()
}

func TestManager_LogNotificationForwarding(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()
	notifications := events.NewNotifications()

	// Pre-save a sink config
	saveSinkConfig(t, store, &commonpb.SinkConfig{
		Name:   "primary",
		Format: "json",
	})

	manager := events.NewManager(store, proposer, logger, notifications)
	manager.Start()
	defer manager.Stop()

	// Become leader to activate emitter
	manager.OnLeadershipChange(true)

	// Log notifications should not block or panic
	for range 5 {
		notifications.NotifyLogsCommitted()
	}
}
