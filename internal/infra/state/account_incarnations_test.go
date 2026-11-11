package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestEnsureAccountIncarnationsCreatesOnePerNormalAccount(t *testing.T) {
	t.Parallel()
	machine, store, attrs := newTestMachine(t)
	b := machine.writeSet
	b.Reset(&commonpb.Timestamp{})
	account := domain.AccountKey{LedgerName: "ledger", Account: "normal:one"}
	for _, asset := range []string{"USD/2", "EUR/2"} {
		b.Derived.Volumes.Put(domain.NewVolumeKey(account.LedgerName, account.Account, asset, ""), &raftcmdpb.VolumePair{})
	}
	ephemeral := domain.AccountKey{LedgerName: account.LedgerName, Account: "ephemeral:one"}
	b.Derived.Volumes.Put(domain.NewVolumeKey(ephemeral.LedgerName, ephemeral.Account, "USD/2", ""), &raftcmdpb.VolumePair{})
	b.gatedLedgerTypes = gatedTypesFor(&commonpb.LedgerInfo{
		Name: account.LedgerName,
		AccountTypes: map[string]*commonpb.AccountType{
			"ephemeral": {Name: "ephemeral", Pattern: "ephemeral:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL},
		},
	})
	id, _ := attributes.MakeKey(account.Bytes())
	factory := NewScopeFactory(b, &raftcmdpb.ExecutionPlan{Attributes: []*raftcmdpb.AttributeCoverage{
		declareTestPlan(id, dal.SubAttrAccountIncarnation),
	}}, logging.FromContext(logging.TestingContext()), nil, 42)
	scope, err := factory.NewProposalScope()
	require.NoError(t, err)
	require.NoError(t, b.ensureAccountIncarnations(scope))
	require.Len(t, b.Derived.AccountIncarnations.DirtyValues(), 1)
	_, err = b.Derived.AccountIncarnations.Get(ephemeral)
	require.ErrorIs(t, err, domain.ErrNotFound)
	incarnation, err := b.Derived.AccountIncarnations.Get(account)
	require.NoError(t, err)
	require.Equal(t, uint64(42), incarnation.GetOpenedAtRaftIndex())
	require.NoError(t, b.ensureAccountIncarnations(scope))
	require.Len(t, b.Derived.AccountIncarnations.DirtyValues(), 1)

	updates, deletions, err := b.Derived.AccountIncarnations.Merge()
	require.NoError(t, err)
	require.Empty(t, deletions)
	batch := store.OpenWriteSession()
	require.NoError(t, mergeSimpleWithCache(attrs.AccountIncarnation, batch, 0, dal.SubAttrAccountIncarnation, updates))
	require.NoError(t, batch.Commit())
	machine.Registry.Cache.AccountIncarnations.Rotate()
	machine.Registry.Cache.AccountIncarnations.Rotate()
	_, inCache := machine.Registry.Cache.AccountIncarnations.Get(id)
	require.False(t, inCache)
	persisted, err := attrs.AccountIncarnation.Get(store, account.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(42), persisted.GetOpenedAtRaftIndex())
}
