package state

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestQueryCheckpointSchedulerRecoveredNeverFiringSchedule(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		machine, store, _ := newTestMachine(t)
		order := &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_SetQueryCheckpointSchedule{SetQueryCheckpointSchedule: &raftcmdpb.SetQueryCheckpointScheduleOrder{Cron: "0 0 31 2 *"}},
		}}}
		result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, order)))
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)

		recovered := recoverMachineOnStore(t, store)
		installTestClusterPolicy(recovered)
		require.Equal(t, "0 0 31 2 *", recovered.QueryCheckpointSchedule())
		create := &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{}},
		}}}
		var attempts atomic.Int32
		changed := signal.New()
		scheduler := NewQueryCheckpointScheduler(logging.Testing(), func() bool { return true }, recovered.QueryCheckpointSchedule, func() error {
			index := uint64(attempts.Add(1)) + 1
			result, err := recovered.ApplyEntries(t.Context(), store, makeEntry(t, index, makeProposal(index, create)))
			if err != nil {
				return err
			}

			return result.Results[0].Error
		}, changed)
		scheduler.Start()
		synctest.Wait()
		time.Sleep(24 * time.Hour)
		synctest.Wait()
		require.Zero(t, attempts.Load())
		scheduler.Stop()
		for index := uint64(2); index <= 11; index++ {
			result, err := recovered.ApplyEntries(t.Context(), store, makeEntry(t, index, makeProposal(index, create)))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error, "all ten manual checkpoint slots must remain available")
		}
		require.Len(t, recovered.State.LiveQueryCheckpointIDs, 10)
		result, err = recovered.ApplyEntries(t.Context(), store, makeEntry(t, 12, makeProposal(12, create)))
		require.NoError(t, err)
		var limit *domain.ErrCheckpointLimitReached
		require.ErrorAs(t, result.Results[0].Error, &limit)

		handle, err := store.NewReadHandle()
		require.NoError(t, err)
		defer func() { require.NoError(t, handle.Close()) }()
		cron, err := query.ReadQueryCheckpointSchedule(handle)
		require.NoError(t, err)
		require.Equal(t, "0 0 31 2 *", cron)
	})
}

func TestQueryCheckpointSchedulerNeverFiringScheduleChanged(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var cron atomic.Value
		cron.Store("* * * * * *")
		var attempts atomic.Int32
		changed := signal.New()
		scheduler := NewQueryCheckpointScheduler(logging.Testing(), func() bool { return true }, func() string {
			return cron.Load().(string)
		}, func() error {
			attempts.Add(1)

			return nil
		}, changed)
		scheduler.Start()
		synctest.Wait()
		cron.Store("0 0 31 2 *")
		changed.Notify()
		synctest.Wait()
		before := attempts.Load()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		require.Equal(t, before, attempts.Load())
		cron.Store("* * * * * *")
		changed.Notify()
		synctest.Wait()
		time.Sleep(1500 * time.Millisecond)
		synctest.Wait()
		require.Greater(t, attempts.Load(), before)
		scheduler.Stop()
	})
}
