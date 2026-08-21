package bootstrap

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	historydomain "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

func TestBalanceHistoryMaintenanceBoundsCompactions(t *testing.T) {
	t.Parallel()

	config := DefaultBalanceHistoryConfig()
	config.SegmentCompactionThreshold = 7
	config.MaxCompactionsPerPass = 4
	var calls atomic.Int64
	worker := newBalanceHistoryMaintenanceWorker(logging.Testing(), config, func(_ context.Context, threshold int) (bool, error) {
		require.Equal(t, 7, threshold)
		calls.Add(1)

		return true, nil
	}, nil)
	worker.runCompactions(context.Background())
	require.Equal(t, int64(4), calls.Load())
}

func TestBalanceHistoryMaintenanceCompactsLocalSegments(t *testing.T) {
	t.Parallel()

	store := newBalanceHistoryProviderTestStore(t)
	for sequence := uint64(1); sequence <= 4; sequence++ {
		_, err := store.Publish(balancehistorystore.Publication{
			Effects: []historydomain.Effect{{
				LedgerName: "ledger", AuditSequence: sequence, LogSequence: sequence,
				EffectiveAt: sequence, InsertedAt: sequence, Account: "cash", AssetBase: "USD",
				Input: historydomain.AmountFromUint64(sequence),
			}},
			Coverage: balancehistorystore.Coverage{AuditSequence: sequence, LogSequence: sequence, SourceComplete: true},
		})
		require.NoError(t, err)
	}

	config := DefaultBalanceHistoryConfig()
	config.SegmentCompactionThreshold = 2
	config.MaxCompactionsPerPass = 4
	worker := newBalanceHistoryMaintenanceWorker(logging.Testing(), config, store.CompactContext, store.Changes)
	worker.runCompactions(context.Background())
	manifest, err := store.Manifest()
	require.NoError(t, err)
	require.Len(t, manifest.Segments, 1)
	require.Equal(t, uint32(2), manifest.Segments[0].Level)
}

func TestBalanceHistoryMaintenanceRecoversAfterCompactionError(t *testing.T) {
	t.Parallel()

	config := DefaultBalanceHistoryConfig()
	config.MaintenanceInterval = time.Millisecond
	config.MaxCompactionsPerPass = 1
	var calls atomic.Int64
	recovered := make(chan struct{})
	worker := newBalanceHistoryMaintenanceWorker(logging.Testing(), config, func(context.Context, int) (bool, error) {
		if calls.Add(1) == 1 {
			return false, errors.New("transient compaction failure")
		}
		select {
		case <-recovered:
		default:
			close(recovered)
		}

		return false, nil
	}, nil)
	worker.Start()
	t.Cleanup(worker.Stop)
	require.Eventually(t, func() bool {
		select {
		case <-recovered:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestBalanceHistoryMaintenanceContinuesAfterExhaustedPass(t *testing.T) {
	t.Parallel()

	config := DefaultBalanceHistoryConfig()
	config.MaintenanceInterval = time.Hour
	config.MaxCompactionsPerPass = 1
	var calls atomic.Int64
	worker := newBalanceHistoryMaintenanceWorker(
		logging.Testing(),
		config,
		func(context.Context, int) (bool, error) {
			return calls.Add(1) <= 3, nil
		},
		nil,
	)
	worker.Start()
	t.Cleanup(worker.Stop)
	require.Eventually(t, func() bool { return calls.Load() == 4 }, time.Second, time.Millisecond)
}

func TestBalanceHistoryMaintenanceStopCancelsBlockedCompaction(t *testing.T) {
	t.Parallel()

	config := DefaultBalanceHistoryConfig()
	config.MaintenanceInterval = time.Hour
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	worker := newBalanceHistoryMaintenanceWorker(logging.Testing(), config, func(ctx context.Context, _ int) (bool, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)

		return false, ctx.Err()
	}, nil)
	worker.Start()
	require.Eventually(t, func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	worker.Stop()
	select {
	case <-cancelled:
	default:
		t.Fatal("stop did not cancel the compaction")
	}
}
