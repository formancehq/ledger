package command

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestState(t *testing.T) {
	state := Load(AlwaysEmptyStore, false)
	reserve, _, err := state.Reserve(context.Background(), ReserveRequest{
		Timestamp: core.Now(),
	})
	require.NoError(t, err)
	reserve.Clear(nil)
}

func TestStateInsertInPastWithNotAllowedPastTimestamp(t *testing.T) {
	state := Load(AlwaysEmptyStore, false)
	now := core.Now()
	reserve1, _, err := state.Reserve(context.Background(), ReserveRequest{
		Timestamp: now,
	})
	require.NoError(t, err)
	defer reserve1.Clear(nil)

	_, _, err = state.Reserve(context.Background(), ReserveRequest{
		Timestamp: now.Add(-time.Second),
	})
	require.Error(t, err)
	require.True(t, IsPastTransactionError(err))
}

func TestStateInsertInPastWithAllowPastTimestamps(t *testing.T) {
	state := Load(AlwaysEmptyStore, true)
	now := core.Now()
	reserve1, _, err := state.Reserve(context.Background(), ReserveRequest{
		Timestamp: now,
	})
	require.NoError(t, err)
	defer reserve1.Clear(nil)

	reserve2, _, err := state.Reserve(context.Background(), ReserveRequest{
		Timestamp: now.Add(-time.Second),
	})
	require.NoError(t, err)
	defer reserve2.Clear(nil)
}

func TestStateWithError(t *testing.T) {
	state := Load(AlwaysEmptyStore, false)
	now := core.Now()

	_, _, err := state.Reserve(context.Background(), ReserveRequest{
		Timestamp: now,
	})
	require.NoError(t, err)

	_, _, err = state.Reserve(context.Background(), ReserveRequest{
		Timestamp: now.Add(-10 * time.Millisecond),
	})
	require.Error(t, err)
	require.True(t, IsPastTransactionError(err))
}

func BenchmarkState(b *testing.B) {
	state := Load(AlwaysEmptyStore, false)
	b.ResetTimer()
	now := core.Now()
	eg := errgroup.Group{}
	for i := 0; i < b.N; i++ {
		eg.Go(func() error {
			reserve, _, err := state.Reserve(context.Background(), ReserveRequest{
				Timestamp: now,
			})
			require.NoError(b, err)
			<-time.After(10 * time.Millisecond)
			reserve.Clear(nil)
			return nil
		})
	}
	require.NoError(b, eg.Wait())
}
