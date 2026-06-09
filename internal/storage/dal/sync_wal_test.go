package dal_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestStore_SyncWAL_PersistsBatchAcrossReopen verifies that data committed
// with NoSync survives a Close+Reopen cycle once SyncWAL has been called.
// This exercises the SyncWAL → durable-WAL → recover-on-Open path.
func TestStore_SyncWAL_PersistsBatchAcrossReopen(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()

	s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
	require.NoError(t, err)

	const wantIndex uint64 = 42

	batch := s.NewBatch()
	require.NoError(t, state.SetAppliedIndex(batch, wantIndex))
	require.NoError(t, batch.Commit())

	require.NoError(t, s.SyncWAL())
	require.NoError(t, s.Close())

	reopened, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	gotIndex, err := query.ReadLastAppliedIndex(reopened)
	require.NoError(t, err)
	require.Equal(t, wantIndex, gotIndex)
}

// TestStore_SyncWAL_OnClosedStore checks that SyncWAL surfaces ErrStoreClosed
// once the store has been closed, rather than panicking on a nil DB.
func TestStore_SyncWAL_OnClosedStore(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	require.NoError(t, s.Close())

	err = s.SyncWAL()
	require.ErrorIs(t, err, dal.ErrStoreClosed)
}

// TestStore_SyncWAL_ConcurrentWithWrites exercises SyncWAL while other
// goroutines commit batches. Run with -race to catch any concurrency bug
// in the dbMu/getDB pattern.
func TestStore_SyncWAL_ConcurrentWithWrites(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	const (
		writers    = 4
		syncers    = 2
		iterations = 50
	)

	var nextIndex atomic.Uint64

	var wg sync.WaitGroup

	wg.Add(writers + syncers)

	for range writers {
		go func() {
			defer wg.Done()

			for range iterations {
				idx := nextIndex.Add(1)
				batch := s.NewBatch()
				require.NoError(t, state.SetAppliedIndex(batch, idx))
				require.NoError(t, batch.Commit())
			}
		}()
	}

	for range syncers {
		go func() {
			defer wg.Done()

			for range iterations {
				require.NoError(t, s.SyncWAL())
			}
		}()
	}

	wg.Wait()
}
