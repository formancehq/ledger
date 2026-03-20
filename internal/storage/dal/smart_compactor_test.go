package dal_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func newTestStoreForCompactor(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func TestSmartCompactorStartStop(t *testing.T) {
	t.Parallel()

	store := newTestStoreForCompactor(t)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	coldCh := make(chan struct{}, 1)
	notifications := signal.NewNotifications()

	compactor := dal.NewSmartCompactor(store, logger, notifications, coldCh, 10_000)
	compactor.Start()
	compactor.Stop()
}

func TestSmartCompactorColdRequest(t *testing.T) {
	t.Parallel()

	store := newTestStoreForCompactor(t)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	coldCh := make(chan struct{}, 1)
	notifications := signal.NewNotifications()

	compactor := dal.NewSmartCompactor(store, logger, notifications, coldCh, 10_000)
	compactor.Start()

	// Send a cold compaction signal.
	coldCh <- struct{}{}

	// Stop waits for both the main loop and any in-flight compaction goroutine,
	// so if we reach here without panic or deadlock the signal was processed.
	compactor.Stop()
}

func TestSmartCompactorColdRequestMultiple(t *testing.T) {
	t.Parallel()

	store := newTestStoreForCompactor(t)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	coldCh := make(chan struct{}, 2)
	notifications := signal.NewNotifications()

	compactor := dal.NewSmartCompactor(store, logger, notifications, coldCh, 10_000)
	compactor.Start()

	// Send two signals. The concurrency guard may skip the second one, but
	// neither should cause a deadlock or panic.
	coldCh <- struct{}{}

	coldCh <- struct{}{}

	// Wait for the channel to be drained by the compactor.
	require.Eventually(t, func() bool {
		return len(coldCh) == 0
	}, 5*time.Second, 10*time.Millisecond)

	compactor.Stop()
}

func TestSmartCompactorIncrementalCompact(t *testing.T) {
	t.Parallel()

	store := newTestStoreForCompactor(t)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	coldCh := make(chan struct{}, 1)
	notifications := signal.NewNotifications()

	compactor := dal.NewSmartCompactor(store, logger, notifications, coldCh, 10_000)
	compactor.Start()

	// Simulate writing 15,000 logs (above the 10,000 threshold).
	notifications.NotifyLogsCommitted(15_000)

	// Give the compactor time to process the signal and run the compaction.
	time.Sleep(100 * time.Millisecond)

	compactor.Stop()
}
