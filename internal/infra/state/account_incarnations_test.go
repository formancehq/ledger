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
	b.Reset(&commonpb.Timestamp{Data: 100})
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
	require.Equal(t, uint64(100), incarnation.GetInsertedAt().GetData())
	require.Equal(t, uint64(100), incarnation.GetUpdatedAt().GetData())
	require.NoError(t, b.ensureAccountIncarnations(scope))
	require.Len(t, b.Derived.AccountIncarnations.DirtyValues(), 1)

	updates, deletions, err := b.Derived.AccountIncarnations.Merge()
	require.NoError(t, err)
	require.Empty(t, deletions)
	batch := store.OpenWriteSession()
	require.NoError(t, mergeSimpleWithCache(attrs.AccountIncarnation, batch, 0, dal.SubAttrAccountIncarnation, updates))
	require.NoError(t, batch.Commit())

	b.Reset(&commonpb.Timestamp{Data: 200})
	b.gatedLedgerTypes = gatedTypesFor(&commonpb.LedgerInfo{Name: account.LedgerName})
	b.Derived.Volumes.Put(domain.NewVolumeKey(account.LedgerName, account.Account, "GBP/2", ""), &raftcmdpb.VolumePair{})
	factory = NewScopeFactory(b, &raftcmdpb.ExecutionPlan{Attributes: []*raftcmdpb.AttributeCoverage{
		declareTestPlan(id, dal.SubAttrAccountIncarnation),
	}}, logging.FromContext(logging.TestingContext()), nil, 43)
	scope, err = factory.NewProposalScope()
	require.NoError(t, err)
	require.NoError(t, b.ensureAccountIncarnations(scope))
	updated, err := b.Derived.AccountIncarnations.Get(account)
	require.NoError(t, err)
	require.Equal(t, uint64(42), updated.GetOpenedAtRaftIndex())
	require.Equal(t, uint64(100), updated.GetInsertedAt().GetData())
	require.Equal(t, uint64(200), updated.GetUpdatedAt().GetData())
	updates, deletions, err = b.Derived.AccountIncarnations.Merge()
	require.NoError(t, err)
	require.Empty(t, deletions)
	batch = store.OpenWriteSession()
	require.NoError(t, mergeSimpleWithCache(attrs.AccountIncarnation, batch, 0, dal.SubAttrAccountIncarnation, updates))
	require.NoError(t, batch.Commit())

	machine.Registry.Cache.AccountIncarnations.Rotate()
	machine.Registry.Cache.AccountIncarnations.Rotate()
	_, inCache := machine.Registry.Cache.AccountIncarnations.Get(id)
	require.False(t, inCache)
	persisted, err := attrs.AccountIncarnation.Get(store, account.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(42), persisted.GetOpenedAtRaftIndex())
	require.Equal(t, uint64(100), persisted.GetInsertedAt().GetData())
	require.Equal(t, uint64(200), persisted.GetUpdatedAt().GetData())
}

func TestEnsureAccountIncarnationsOpensMetadataOnlyNormalAccount(t *testing.T) {
	t.Parallel()
	machine, _, _ := newTestMachine(t)
	b := machine.writeSet
	b.Reset(&commonpb.Timestamp{Data: 100})
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
	require.Equal(t, uint64(100), incarnation.GetInsertedAt().GetData())
	require.Equal(t, uint64(100), incarnation.GetUpdatedAt().GetData())
}

func TestEnsureAccountIncarnationsIncludesWorld(t *testing.T) {
	t.Parallel()
	machine, _, _ := newTestMachine(t)
	b := machine.writeSet
	b.Reset(&commonpb.Timestamp{Data: 100})
	account := domain.AccountKey{LedgerName: "ledger", Account: "world"}
	b.Derived.Volumes.Put(domain.NewVolumeKey(account.LedgerName, account.Account, "USD/2", ""), &raftcmdpb.VolumePair{})
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
	require.Equal(t, uint64(100), incarnation.GetInsertedAt().GetData())
	require.Equal(t, uint64(100), incarnation.GetUpdatedAt().GetData())
}

func TestEnsureAccountIncarnationsUpdatesOnMetadataDeletion(t *testing.T) {
	t.Parallel()
	machine, _, _ := newTestMachine(t)
	b := machine.writeSet
	b.Reset(&commonpb.Timestamp{Data: 200})
	account := domain.AccountKey{LedgerName: "ledger", Account: "normal:one"}
	key := domain.MetadataKey{AccountKey: account, Key: "owner"}
	b.Derived.AccountMetadata.Delete(key)
	b.gatedLedgerTypes = gatedTypesFor(&commonpb.LedgerInfo{Name: account.LedgerName})
	id, tag := attributes.MakeKey(account.Bytes())
	machine.Registry.Cache.AccountIncarnations.Put(id, attributes.Entry[*commonpb.AccountIncarnation]{
		Tag: tag,
		Data: &commonpb.AccountIncarnation{
			OpenedAtRaftIndex: 42,
			Persistence:       commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL,
			InsertedAt:        &commonpb.Timestamp{Data: 100},
			UpdatedAt:         &commonpb.Timestamp{Data: 100},
		},
	})
	factory := NewScopeFactory(b, &raftcmdpb.ExecutionPlan{Attributes: []*raftcmdpb.AttributeCoverage{
		declareTestPlan(id, dal.SubAttrAccountIncarnation),
	}}, logging.FromContext(logging.TestingContext()), nil, 43)
	scope, err := factory.NewProposalScope()
	require.NoError(t, err)
	require.NoError(t, b.ensureAccountIncarnations(scope))
	updated, err := b.Derived.AccountIncarnations.Get(account)
	require.NoError(t, err)
	require.Equal(t, uint64(42), updated.GetOpenedAtRaftIndex())
	require.Equal(t, uint64(100), updated.GetInsertedAt().GetData())
	require.Equal(t, uint64(200), updated.GetUpdatedAt().GetData())
}

func TestApplyProposalWorldIncarnationTracksMetadataWrites(t *testing.T) {
	t.Parallel()
	machine, store, attrs := newTestMachine(t)
	const ledger = "world-metadata"
	created, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledger))))
	require.NoError(t, err)
	require.NoError(t, created.Results[0].Error)

	account := domain.AccountKey{LedgerName: ledger, Account: "world"}
	id, _ := attributes.MakeKey(account.Bytes())
	target := &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: "world"}}}
	apply := func(index uint64, action *raftcmdpb.LedgerApplyOrder, expected uint64) {
		t.Helper()
		order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger:  ledger,
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: action},
		}}}
		proposal := makeProposal(index, order)
		coverage := declareTestPlan(id, dal.SubAttrAccountIncarnation)
		coverage.AssertAccountIncarnation = true
		coverage.ExpectedIncarnationOpenedAtRaftIndex = expected
		proposal.ExecutionPlan.Attributes = append(proposal.ExecutionPlan.Attributes, coverage)
		result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, index, proposal))
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)
	}

	apply(2, &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{AddMetadata: &raftcmdpb.SaveMetadataOrder{
		Target: target, Metadata: map[string]*commonpb.MetadataValue{"owner": commonpb.NewStringValue("alice")},
	}}}, 0)
	opened, err := attrs.AccountIncarnation.Get(store, account.Bytes())
	require.NoError(t, err)
	require.NotNil(t, opened)
	require.Equal(t, uint64(2), opened.GetOpenedAtRaftIndex())
	require.Equal(t, opened.GetInsertedAt().GetData(), opened.GetUpdatedAt().GetData())

	apply(3, &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
		Target: target, Key: "owner",
	}}}, 2)
	updated, err := attrs.AccountIncarnation.Get(store, account.Bytes())
	require.NoError(t, err)
	require.NotNil(t, updated)
	require.Equal(t, uint64(2), updated.GetOpenedAtRaftIndex())
	require.Equal(t, opened.GetInsertedAt().GetData(), updated.GetInsertedAt().GetData())
	require.Greater(t, updated.GetUpdatedAt().GetData(), opened.GetUpdatedAt().GetData())
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
