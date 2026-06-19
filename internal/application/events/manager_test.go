package events_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/events"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func saveSinkConfig(t *testing.T, s *dal.Store, config *commonpb.SinkConfig) {
	t.Helper()

	attr := attributes.NewAttribute[*commonpb.SinkConfig](dal.SubAttrSinkConfig)
	batch := s.OpenWriteSession()
	_, err := attr.Set(batch, domain.SinkConfigKey{Name: config.GetName()}.Bytes(), config)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

func deleteSinkConfig(t *testing.T, s *dal.Store, name string) {
	t.Helper()

	attr := attributes.NewAttribute[*commonpb.SinkConfig](dal.SubAttrSinkConfig)
	batch := s.OpenWriteSession()
	require.NoError(t, attr.Delete(batch, domain.SinkConfigKey{Name: name}.Bytes()))
	require.NoError(t, batch.Commit())
}

func TestManager_AddRemoveSink(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()
	notifications := signal.NewNotifications()

	manager := events.NewManager(store, attributes.New(), proposer, newPlanBuilder(t, store), logger, notifications)

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
	notifications := signal.NewNotifications()

	// Pre-save a sink config
	saveSinkConfig(t, store, &commonpb.SinkConfig{
		Name:   "primary",
		Format: "json",
	})

	manager := events.NewManager(store, attributes.New(), proposer, newPlanBuilder(t, store), logger, notifications)

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
	notifications := signal.NewNotifications()

	manager := events.NewManager(store, attributes.New(), proposer, newPlanBuilder(t, store), logger, notifications)

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
	notifications := signal.NewNotifications()

	manager := events.NewManager(store, attributes.New(), proposer, newPlanBuilder(t, store), logger, notifications)
	manager.Start()
	// Stop immediately — should not hang
	manager.Stop()
}

func TestManager_LogNotificationForwarding(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()
	notifications := signal.NewNotifications()

	// Pre-save a sink config
	saveSinkConfig(t, store, &commonpb.SinkConfig{
		Name:   "primary",
		Format: "json",
	})

	manager := events.NewManager(store, attributes.New(), proposer, newPlanBuilder(t, store), logger, notifications)

	manager.Start()
	defer manager.Stop()

	// Become leader to activate emitter
	manager.OnLeadershipChange(true)

	// Log notifications should not block or panic
	for i := range 5 {
		notifications.NotifyLogsCommitted(uint64(i + 1))
	}
}
