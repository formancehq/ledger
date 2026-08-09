package bootstrap

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	historydomain "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

func TestBalanceHistoryMaintenanceBoundsCompactionsBeforeColdPass(t *testing.T) {
	t.Parallel()

	config := DefaultBalanceHistoryConfig()
	config.RunCompactionThreshold = 7
	config.MaxCompactionsPerPass = 4
	config.MaintenanceInterval = time.Hour
	config.TierInterval = time.Hour
	config.RemoteGCInterval = time.Hour
	config.RemoteGCScanLimit = 7
	config.RemoteGCDeleteLimit = 3

	var (
		compactionCalls atomic.Int64
		thresholdSeen   atomic.Int64
		eventsMu        sync.Mutex
		events          []string
	)
	type observation struct {
		budget balancehistorystore.RemoteGCBudget
		events []string
	}
	observationCh := make(chan observation, 1)
	worker := newBalanceHistoryMaintenanceWorker(
		logging.Testing(),
		config,
		func(_ context.Context, threshold int) (bool, error) {
			thresholdSeen.Store(int64(threshold))
			compactionCalls.Add(1)
			eventsMu.Lock()
			events = append(events, "compact")
			eventsMu.Unlock()

			return true, nil
		},
		func(context.Context) (int, error) {
			eventsMu.Lock()
			events = append(events, "tier")
			eventsMu.Unlock()

			return 1, nil
		},
		func(_ context.Context, budget balancehistorystore.RemoteGCBudget) (balancehistorystore.RemoteGCResult, error) {
			eventsMu.Lock()
			events = append(events, "collect")
			observedEvents := append([]string(nil), events...)
			eventsMu.Unlock()
			observationCh <- observation{budget: budget, events: observedEvents}

			return balancehistorystore.RemoteGCResult{}, nil
		},
	)
	worker.Start()
	t.Cleanup(worker.Stop)

	var observed observation
	select {
	case observed = <-observationCh:
	case <-time.After(time.Second):
		t.Fatal("initial maintenance pass did not reach remote collection")
	}
	require.Equal(t, int64(4), compactionCalls.Load())
	require.Equal(t, int64(7), thresholdSeen.Load())
	require.Equal(t, balancehistorystore.RemoteGCBudget{ScanLimit: 7, DeleteLimit: 3}, observed.budget)
	require.Equal(t, []string{"compact", "compact", "compact", "compact", "tier", "collect"}, observed.events)
}

func TestBalanceHistoryMaintenanceOptionalPathsAreNoOps(t *testing.T) {
	t.Parallel()

	var nilWorker *balanceHistoryMaintenanceWorker
	require.NotPanics(t, func() {
		nilWorker.Start()
		nilWorker.Stop()
	})

	worker := &balanceHistoryMaintenanceWorker{}
	require.True(t, worker.runTier(context.Background()))
	require.NotPanics(t, func() {
		worker.runRemoteGC(context.Background())
	})
}

func TestBalanceHistoryMaintenanceCompactsWithoutColdTier(t *testing.T) {
	t.Parallel()

	store := newBalanceHistoryProviderTestStore(t)
	for sequence := uint64(1); sequence <= 4; sequence++ {
		_, err := store.Publish(balancehistorystore.Publication{
			Effects: []historydomain.Effect{{
				LedgerID:      7,
				AuditSequence: sequence,
				LogSequence:   sequence,
				EffectiveAt:   sequence,
				InsertedAt:    sequence,
				Account:       "cash",
				AssetBase:     "USD",
				Input:         historydomain.AmountFromUint64(sequence),
			}},
			Coverage: balancehistorystore.Coverage{
				AuditSequence:  sequence,
				LogSequence:    sequence,
				SourceComplete: true,
			},
		})
		require.NoError(t, err)
	}

	config := DefaultBalanceHistoryConfig()
	config.RunCompactionThreshold = 2
	config.MaxCompactionsPerPass = 4
	config.MaintenanceInterval = time.Hour
	worker := newBalanceHistoryMaintenanceWorker(
		logging.Testing(),
		config,
		store.CompactContext,
		nil,
		nil,
	)
	worker.Start()
	t.Cleanup(worker.Stop)

	require.Eventually(t, func() bool {
		manifest, err := store.Manifest()
		if err != nil || len(manifest.Runs) != 1 {
			return false
		}

		return manifest.Runs[0].Level == 2
	}, time.Second, time.Millisecond)
}

func TestBalanceHistoryMaintenanceRecoversAfterCompactionError(t *testing.T) {
	t.Parallel()

	config := DefaultBalanceHistoryConfig()
	config.MaintenanceInterval = 5 * time.Millisecond
	config.MaxCompactionsPerPass = 1
	config.TierInterval = time.Hour
	config.RemoteGCInterval = time.Hour

	var calls atomic.Int64
	recovered := make(chan struct{})
	worker := newBalanceHistoryMaintenanceWorker(
		logging.Testing(),
		config,
		func(context.Context, int) (bool, error) {
			if calls.Add(1) == 1 {
				return false, errors.New("transient compaction failure")
			}
			select {
			case <-recovered:
			default:
				close(recovered)
			}

			return false, nil
		},
		nil,
		nil,
	)
	worker.Start()
	t.Cleanup(worker.Stop)

	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("maintenance did not retry after a transient compaction error")
	}
	require.GreaterOrEqual(t, calls.Load(), int64(2))
}

func TestBalanceHistoryMaintenanceStopCancelsBlockedCompaction(t *testing.T) {
	t.Parallel()

	config := DefaultBalanceHistoryConfig()
	config.MaintenanceInterval = time.Hour
	config.TierInterval = time.Hour
	config.RemoteGCInterval = time.Hour

	entered := make(chan struct{})
	cancelled := make(chan struct{})
	var tierCalled atomic.Bool
	worker := newBalanceHistoryMaintenanceWorker(
		logging.Testing(),
		config,
		func(ctx context.Context, _ int) (bool, error) {
			close(entered)
			<-ctx.Done()
			close(cancelled)

			return false, ctx.Err()
		},
		func(context.Context) (int, error) {
			tierCalled.Store(true)

			return 0, nil
		},
		nil,
	)
	worker.Start()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("initial compaction did not start")
	}

	stopped := make(chan struct{})
	go func() {
		worker.Stop()
		close(stopped)
	}()

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("worker stop did not cancel the blocked compaction")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("worker stop did not wait for cancellation to complete")
	}

	worker.Stop()
	require.False(t, tierCalled.Load(), "cold maintenance must not start after a cancelled compaction")
}

func TestBalanceHistoryTickerCadenceStaysWithinMaintenanceCapacityAndReconverges(t *testing.T) {
	t.Parallel()

	store := newBalanceHistoryProviderTestStore(t)
	config := DefaultBalanceHistoryConfig()
	config.RunCompactionThreshold = 4
	config.MaxCompactionsPerPass = 4
	worker := newBalanceHistoryMaintenanceWorker(
		logging.Testing(),
		config,
		store.CompactContext,
		nil,
		nil,
	)

	// The builder's production 200 ms ticker permits at most five publications
	// per second. Two threshold-four merges every second remove up to six runs,
	// so this deterministic schedule models the release planning
	// rate without wall-clock sleeps.
	const simulatedSeconds = 30
	maxRuns := 0
	sequence := uint64(0)
	for range simulatedSeconds {
		for range 5 {
			sequence++
			_, err := store.Publish(balancehistorystore.Publication{
				Effects: []historydomain.Effect{{
					LedgerID:      7,
					AuditSequence: sequence,
					LogSequence:   sequence,
					EffectiveAt:   sequence,
					InsertedAt:    sequence,
					Account:       "assets:cash",
					AssetBase:     "USD",
					Input:         historydomain.AmountFromUint64(sequence),
				}},
				Coverage: balancehistorystore.Coverage{
					AuditSequence:  sequence,
					LogSequence:    sequence,
					SourceComplete: true,
				},
			})
			require.NoError(t, err)
		}
		require.True(t, worker.runCompactions(context.Background()))
		manifest, err := store.Manifest()
		require.NoError(t, err)
		maxRuns = max(maxRuns, len(manifest.Runs))
	}

	// A logarithmic threshold-four LSM has at most three runs per occupied
	// level. The observed working set must remain bounded while the producer is
	// active, then a bounded number of ordinary passes must fully drain debt.
	require.LessOrEqual(t, maxRuns, 15)
	for range 10 {
		require.True(t, worker.runCompactions(context.Background()))
	}
	manifest, err := store.Manifest()
	require.NoError(t, err)
	runsByLevel := map[uint32]int{}
	for _, run := range manifest.Runs {
		runsByLevel[run.Level]++
	}
	for level, runs := range runsByLevel {
		require.Less(t, runs, config.RunCompactionThreshold, "level %d did not reconverge", level)
	}
}
