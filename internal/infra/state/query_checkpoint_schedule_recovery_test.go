package state

import (
	"sync"
	"sync/atomic"
	"testing"
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
	stopScheduler := sync.OnceFunc(scheduler.Stop)
	t.Cleanup(stopScheduler)
	require.Never(t, func() bool { return attempts.Load() > 0 }, 250*time.Millisecond, 5*time.Millisecond)
	stopScheduler()
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
}

func TestQueryCheckpointSchedulerNeverFiringScheduleChanged(t *testing.T) {
	t.Parallel()
	var cron atomic.Value
	cron.Store("* * * * * *")
	var attempts atomic.Int32
	changed := signal.New()
	read := make(chan string, 8)
	scheduler := NewQueryCheckpointScheduler(logging.Testing(), func() bool { return true }, func() string {
		current := cron.Load().(string)
		select {
		case read <- current:
		default:
		}

		return current
	}, func() error {
		attempts.Add(1)

		return nil
	}, changed)
	scheduler.Start()
	t.Cleanup(scheduler.Stop)
	select {
	case <-read:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler did not start")
	}
	cron.Store("0 0 31 2 *")
	changed.Notify()
	require.Eventually(t, func() bool {
		select {
		case value := <-read:
			return value == "0 0 31 2 *"
		default:
			return false
		}
	}, 5*time.Second, 5*time.Millisecond)
	before := attempts.Load()
	require.Never(t, func() bool { return attempts.Load() != before }, 1100*time.Millisecond, 5*time.Millisecond)
	cron.Store("* * * * * *")
	changed.Notify()
	require.Eventually(t, func() bool { return attempts.Load() > before }, 3*time.Second, 5*time.Millisecond)
}
