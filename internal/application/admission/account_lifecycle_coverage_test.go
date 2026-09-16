package admission

import (
	"context"
	"testing"
	stdtime "time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestReleaseLifecycleWhenFSMCompletesWaitsPastCallerCancellation(t *testing.T) {
	t.Parallel()

	future := futures.New[state.ApplyResult]()
	released := make(chan struct{})
	releaseLifecycleWhenFSMCompletes(future, func() { close(released) })

	select {
	case <-released:
		t.Fatal("lifecycle lock released before FSM completion")
	case <-stdtime.After(20 * stdtime.Millisecond):
	}

	future.Resolve(state.ApplyResult{}, nil)
	select {
	case <-released:
	case <-stdtime.After(stdtime.Second):
		t.Fatal("lifecycle lock was not released after FSM completion")
	}
}

func TestAccountTypeMutationSerializesAllLifecycleStripes(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)
	require.NoError(t, admission.ephemeralLifecycleLocks[0].Acquire(t.Context(), 1))

	done := make(chan func(), 1)
	go func() {
		release, err := admission.expandAccountLifecycleCoverage(t.Context(), plan.NewCoverage(), []*plan.Coverage{plan.NewCoverage()}, []*raftcmdpb.Order{
			ledgerApplyOrder(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{
				AddAccountType: &raftcmdpb.AddAccountTypeOrder{AccountType: &commonpb.AccountType{
					Name: "hold", Pattern: "hold:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
				}},
			}}),
		})
		require.NoError(t, err)
		done <- release
	}()

	select {
	case release := <-done:
		release()
		t.Fatal("account-type mutation did not wait for an existing lifecycle holder")
	case <-stdtime.After(20 * stdtime.Millisecond):
	}

	admission.ephemeralLifecycleLocks[0].Release(1)
	select {
	case release := <-done:
		release()
	case <-stdtime.After(stdtime.Second):
		t.Fatal("account-type mutation did not acquire lifecycle locks after release")
	}
}

func TestAccountTypeMutationCancellationReleasesAcquiredLifecycleStripes(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)
	require.NoError(t, admission.ephemeralLifecycleLocks[1].Acquire(t.Context(), 1))
	t.Cleanup(func() { admission.ephemeralLifecycleLocks[1].Release(1) })

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := admission.expandAccountLifecycleCoverage(ctx, plan.NewCoverage(), []*plan.Coverage{plan.NewCoverage()}, []*raftcmdpb.Order{
			ledgerApplyOrder(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{
				AddAccountType: &raftcmdpb.AddAccountTypeOrder{AccountType: &commonpb.AccountType{
					Name: "hold", Pattern: "hold:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
				}},
			}}),
		})
		done <- err
	}()

	require.Eventually(t, func() bool {
		if admission.ephemeralLifecycleLocks[0].TryAcquire(1) {
			admission.ephemeralLifecycleLocks[0].Release(1)

			return false
		}

		return true
	}, stdtime.Second, stdtime.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.True(t, admission.ephemeralLifecycleLocks[0].TryAcquire(1), "cancellation must release already acquired stripes")
	admission.ephemeralLifecycleLocks[0].Release(1)
}

func TestAccountLifecycleTypeSnapshotsCoverProposalTypeTransitions(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, attrs := createTestAdmission(t, store)
	info, err := attrs.Ledger.Get(store, domain.LedgerKey{Name: testLedgerName}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, info)
	info.AccountTypes = map[string]*commonpb.AccountType{
		"fallback": {
			Name:        "fallback",
			Pattern:     "users:{id}",
			Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
		},
		"specific": {
			Name:        "specific",
			Pattern:     "users:alice",
			Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL,
		},
	}
	batch := store.OpenWriteSession()
	_, err = attrs.Ledger.Set(batch, domain.LedgerKey{Name: testLedgerName}.Bytes(), info)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	orders := []*raftcmdpb.Order{
		ledgerApplyOrder(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveAccountType{
			RemoveAccountType: &raftcmdpb.RemoveAccountTypeOrder{Name: "specific"},
		}}),
		ledgerApplyOrder(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{
			AddAccountType: &raftcmdpb.AddAccountTypeOrder{AccountType: &commonpb.AccountType{
				Name:        "temporary",
				Pattern:     "temporary:{id}",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
			}},
		}}),
	}

	snapshots, err := admission.accountLifecycleTypeSnapshots(orders)
	require.NoError(t, err)
	require.True(t, accountMatchesEphemeralSnapshot("users:alice", snapshots[testLedgerName]),
		"removing a more-specific persistent type must expose the ephemeral fallback")
	require.True(t, accountMatchesEphemeralSnapshot("temporary:one", snapshots[testLedgerName]),
		"an ephemeral type introduced by the proposal must participate in coverage")
}

func TestAccountLifecycleTypeSnapshotsPreserveSkippedDuplicateAdd(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, attrs := createTestAdmission(t, store)
	info, err := attrs.Ledger.Get(store, domain.LedgerKey{Name: testLedgerName}.Bytes())
	require.NoError(t, err)
	info.AccountTypes = map[string]*commonpb.AccountType{
		"fallback": {Name: "fallback", Pattern: "users:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL},
	}
	batch := store.OpenWriteSession()
	_, err = attrs.Ledger.Set(batch, domain.LedgerKey{Name: testLedgerName}.Bytes(), info)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	snapshots, err := admission.accountLifecycleTypeSnapshots([]*raftcmdpb.Order{
		ledgerApplyOrder(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{
			AddAccountType: &raftcmdpb.AddAccountTypeOrder{AccountType: &commonpb.AccountType{
				Name: "fallback", Pattern: "users:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL,
			}},
		}}),
	})
	require.NoError(t, err)
	require.True(t, accountMatchesEphemeralSnapshot("users:alice", snapshots[testLedgerName]),
		"a duplicate add is skipped by apply and must not replace the effective type")
}

func ledgerApplyOrder(apply *raftcmdpb.LedgerApplyOrder) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger:  testLedgerName,
		Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: apply},
	}}}
}
