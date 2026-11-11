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

func TestEnsureAccountIncarnationsOpensMetadataOnlyNormalAccount(t *testing.T) {
	t.Parallel()
	machine, _, _ := newTestMachine(t)
	b := machine.writeSet
	b.Reset(&commonpb.Timestamp{})
	account := domain.AccountKey{LedgerName: "ledger", Account: "normal:metadata-only"}
	b.Derived.AccountMetadata.Put(domain.MetadataKey{AccountKey: account, Key: "owner"}, commonpb.NewStringValue("alice"))
	b.gatedLedgerTypes = gatedTypesFor(&commonpb.LedgerInfo{Name: account.LedgerName})
	id, _ := attributes.MakeKey(account.Bytes())
	factory := NewScopeFactory(b, &raftcmdpb.ExecutionPlan{Attributes: []*raftcmdpb.AttributeCoverage{
		declareTestPlan(id, dal.SubAttrAccountIncarnation),
	}}, logging.FromContext(logging.TestingContext()), nil, 42)
	scope, err := factory.NewProposalScope()
	require.NoError(t, err)
	require.NoError(t, b.ensureAccountIncarnations(scope))
	incarnation, err := b.Derived.AccountIncarnations.Get(account)
	require.NoError(t, err)
	require.Equal(t, uint64(42), incarnation.GetOpenedAtRaftIndex())
	require.Equal(t, commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL, incarnation.GetPersistence())
}

func TestAssertAccountIncarnationsRejectsConcurrentOpening(t *testing.T) {
	t.Parallel()
	machine, _, _ := newTestMachine(t)
	account := domain.AccountKey{LedgerName: "ledger", Account: "normal:one"}
	id, tag := attributes.MakeKey(account.Bytes())
	coverage := declareTestPlan(id, dal.SubAttrAccountIncarnation)
	coverage.AssertAccountIncarnation = true
	plan := &raftcmdpb.ExecutionPlan{Attributes: []*raftcmdpb.AttributeCoverage{coverage}}

	// Admission observed no lifetime. Another proposal opens the account
	// before this one applies, so the old observation must be retried.
	require.NoError(t, machine.assertAccountIncarnations(plan))
	machine.Registry.Cache.AccountIncarnations.Put(id, attributes.Entry[*commonpb.AccountIncarnation]{
		Tag:  tag,
		Data: &commonpb.AccountIncarnation{OpenedAtRaftIndex: 41, Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL},
	})
	require.ErrorIs(t, machine.assertAccountIncarnations(plan), domain.ErrStaleInputsResolution)

	coverage.ExpectedIncarnationOpenedAtRaftIndex = 41
	require.NoError(t, machine.assertAccountIncarnations(plan))
	coverage.ExpectedIncarnationOpenedAtRaftIndex = 42
	require.ErrorIs(t, machine.assertAccountIncarnations(plan), domain.ErrStaleInputsResolution)
}

func TestApplyProposal_StaleAccountIncarnationIsNotAuditedOrFrozen(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	account := domain.AccountKey{LedgerName: "ledger", Account: "normal:one"}
	id, tag := attributes.MakeKey(account.Bytes())
	machine.Registry.Cache.AccountIncarnations.Put(id, attributes.Entry[*commonpb.AccountIncarnation]{
		Tag:  tag,
		Data: &commonpb.AccountIncarnation{OpenedAtRaftIndex: 41, Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL},
	})
	proposal := makeProposal(1, createLedgerOrder("candidate"))
	proposal.Idempotency = &commonpb.Idempotency{Key: "stale-incarnation"}
	coverage := declareTestPlan(id, dal.SubAttrAccountIncarnation)
	coverage.AssertAccountIncarnation = true // Admission observed no lifetime.
	proposal.ExecutionPlan.Attributes = append(proposal.ExecutionPlan.Attributes, coverage)
	beforeAudit := machine.State.NextAuditSequenceID
	beforeTimestamp := machine.State.LastAppliedTimestamp

	result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, proposal))
	require.NoError(t, err)
	require.ErrorIs(t, result.Results[0].Error, domain.ErrStaleInputsResolution)
	require.False(t, result.Results[0].AuditEntryWritten)
	require.Equal(t, beforeAudit, machine.State.NextAuditSequenceID)
	require.Equal(t, beforeTimestamp, machine.State.LastAppliedTimestamp)
	_, frozen := machine.Registry.Idempotency.Get("stale-incarnation")
	require.False(t, frozen)
}
