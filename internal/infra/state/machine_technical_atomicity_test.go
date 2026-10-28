package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// unsealedTechnicalEntry preserves deliberately missing per-operation coverage;
// makeEntry would admit every declared key to every operation.
func unsealedTechnicalEntry(t *testing.T, index uint64, proposal *raftcmdpb.Proposal) *raftpb.Entry {
	t.Helper()
	data, err := proto.Marshal(proposal)
	require.NoError(t, err)

	return &raftpb.Entry{Index: new(index), Term: proto.Uint64(1), Type: new(raftpb.EntryNormal), Data: data}
}

func sinkCursorUpdate(name string, cursor uint64) *raftcmdpb.TechnicalUpdate {
	return &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_EventsSink{
		EventsSink: &raftcmdpb.EventsSinkUpdate{SinkName: name, Cursor: cursor},
	}}
}

func requireTechnicalCursor(t *testing.T, store *dal.Store, name string, want uint64) {
	t.Helper()
	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	got, err := query.ReadSinkCursor(reader, name)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestTechnicalAtomicity_CoverageRejectionDiscardsCursorPrefix(t *testing.T) {
	t.Parallel()
	for _, orderFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "later technical update", true: "later order"}[orderFailure], func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			seed := makeProposal(1, createLedgerOrder("atomic"))
			applied, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, seed))
			require.NoError(t, err)
			require.NoError(t, applied.Results[0].Error)
			beforeAudit := machine.State.NextAuditSequenceID
			beforeTime := machine.State.LastAppliedTimestamp

			previous := makeProposal(2)
			previous.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{sinkCursorUpdate("preserved", 7), sinkCursorUpdate("rejected", 11)}
			rejected := makeProposal(3)
			rejected.AtomicTechnicalUpdates = true
			rejected.Idempotency = &commonpb.Idempotency{Key: "retryable"}
			rejected.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{sinkCursorUpdate("rejected", 999)}
			id, _ := attributes.MakeKey((domain.LedgerKey{Name: "atomic"}).Bytes())
			rejected.ExecutionPlan.Attributes = []*raftcmdpb.AttributeCoverage{declareTestPlan(id, dal.SubAttrLedger)}
			if orderFailure {
				rejected.Orders = []*raftcmdpb.Order{createLedgerOrder("atomic")}
			} else {
				rejected.TechnicalUpdates = append(rejected.TechnicalUpdates, &raftcmdpb.TechnicalUpdate{
					Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "atomic"}},
				})
			}
			applied, err = machine.ApplyEntries(t.Context(), store,
				makeEntry(t, 2, previous), unsealedTechnicalEntry(t, 3, rejected))
			require.NoError(t, err)
			require.NoError(t, applied.Results[0].Error)
			var miss *ErrCoverageMiss
			require.ErrorAs(t, applied.Results[1].Error, &miss)
			require.Equal(t, "ledgers", miss.Attribute)
			require.False(t, applied.Results[1].AuditEntryWritten)
			require.Equal(t, beforeAudit, machine.State.NextAuditSequenceID)
			require.Equal(t, beforeTime, machine.State.LastAppliedTimestamp)
			_, frozen := machine.Registry.Idempotency.Get("retryable")
			require.False(t, frozen)
			requireTechnicalCursor(t, store, "preserved", 7)
			requireTechnicalCursor(t, store, "rejected", 11)
			require.Equal(t, uint64(3), machine.LastAppliedIndex())

			recovered := recoverMachineOnStore(t, store)
			require.Equal(t, uint64(3), recovered.LastAppliedIndex())
			require.Equal(t, beforeAudit, recovered.State.NextAuditSequenceID)
			requireTechnicalCursor(t, store, "rejected", 11)
		})
	}
}

func TestTechnicalAtomicity_LegacyPrefixRetained(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	proposal := makeProposal(1)
	proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{
		sinkCursorUpdate("legacy", 999),
		{Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "undeclared"}}},
	}
	result, err := machine.ApplyEntries(t.Context(), store, unsealedTechnicalEntry(t, 1, proposal))
	require.NoError(t, err)
	var miss *ErrCoverageMiss
	require.ErrorAs(t, result.Results[0].Error, &miss)
	requireTechnicalCursor(t, store, "legacy", 999)
	recovered := recoverMachineOnStore(t, store)
	require.Equal(t, uint64(1), recovered.LastAppliedIndex())
	requireTechnicalCursor(t, store, "legacy", 999)
}

func TestTechnicalAtomicity_AuditKeyFirstValueWins(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	seed := store.OpenWriteSession()
	require.NoError(t, seed.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}))
	require.NoError(t, seed.Commit())
	machine.State.AuditKey = ""
	machine.State.HashGenerator = nil
	proposal := makeProposal(1)
	proposal.AtomicTechnicalUpdates = true
	proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{
		{Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: []byte("fedcba9876543210fedcba9876543210")}},
		{Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: []byte("0123456789abcdef0123456789abcdef")}},
	}
	result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, proposal))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	key, err := query.ReadAuditKey(store)
	require.NoError(t, err)
	require.Equal(t, "fedcba9876543210fedcba9876543210", string(key))
	require.Equal(t, string(key), machine.State.AuditKey)
	require.Equal(t, string(key), recoverMachineOnStore(t, store).State.AuditKey)
}

func TestTechnicalAtomicity_OrderExitDiscardsTechnicalEffects(t *testing.T) {
	t.Parallel()
	for _, exit := range []string{"maintenance", "preload unavailable", "definitive failure", "success replay", "failure replay", "conflict", "expired outcome"} {
		t.Run(exit, func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			policy := machine.State.ClusterPolicy.CloneVT()
			policy.IdempotencyTtlMicros = 50
			machine.State.UpdateClusterPolicy(policy)
			seed := makeProposal(1, createLedgerOrder("existing"))
			seed.Date.Data = 50
			seed.Idempotency = &commonpb.Idempotency{Key: "identity"}
			result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, seed))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)
			if exit == "failure replay" {
				failure := makeProposal(2, createLedgerOrder("existing"))
				failure.Date.Data = 51
				failure.Idempotency = &commonpb.Idempotency{Key: "failure"}
				result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, failure))
				require.NoError(t, err)
				require.Error(t, result.Results[0].Error)
			}
			beforeAudit := machine.State.NextAuditSequenceID
			beforeTimestamp := machine.State.LastAppliedTimestamp
			proposal := makeProposal(3, createLedgerOrder("existing"))
			proposal.AtomicTechnicalUpdates = true
			proposal.Date.Data = 52
			proposal.Idempotency = &commonpb.Idempotency{Key: "fresh"}
			proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{
				sinkCursorUpdate("discarded", 999),
				{Kind: &raftcmdpb.TechnicalUpdate_IdempotencyEviction{IdempotencyEviction: &raftcmdpb.IdempotencyEviction{CutoffMicros: 200}}},
			}
			wantAudit, wantReplay := false, false
			switch exit {
			case "maintenance":
				machine.sharedState.SetMaintenanceMode(true)
			case "preload unavailable":
				proposal.Orders[0].Technical = &raftcmdpb.OrderTechnical{PreloadUnavailable: true}
			case "definitive failure":
				wantAudit = true
			case "success replay":
				proposal.Idempotency.Key = "identity"
				wantReplay = true
			case "failure replay":
				proposal.Idempotency.Key = "failure"
				wantReplay = true
			case "conflict":
				proposal.Idempotency.Key = "identity"
				proposal.Orders = []*raftcmdpb.Order{createLedgerOrder("different")}
				proposal.ExecutionPlan.Attributes = buildOrderDeclarations(proposal.GetOrders())
				wantAudit = true
			case "expired outcome":
				proposal.Idempotency.Key = "identity"
				proposal.Date.Data = 150
				wantAudit = true
			}
			appliedIndex := machine.LastAppliedIndex() + 1
			result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, appliedIndex, proposal))
			require.NoError(t, err)
			applied := result.Results[0]
			require.Equal(t, wantAudit, applied.AuditEntryWritten)
			require.Equal(t, wantReplay, applied.Replayed)
			switch exit {
			case "maintenance":
				require.ErrorIs(t, applied.Error, domain.ErrMaintenanceMode)
			case "preload unavailable":
				require.ErrorIs(t, applied.Error, domain.ErrPreloadUnavailable)
			case "conflict":
				var conflict *domain.ErrIdempotencyKeyConflict
				require.ErrorAs(t, applied.Error, &conflict)
			case "success replay":
				require.NoError(t, applied.Error)
			default:
				require.Error(t, applied.Error)
			}
			requireTechnicalCursor(t, store, "discarded", 0)
			require.Zero(t, machine.State.LastIdempotencyEvictionCutoff)
			expectedAudit := beforeAudit
			if wantAudit {
				expectedAudit++
				require.Equal(t, proposal.GetDate().GetData(), machine.State.LastAppliedTimestamp)
			} else {
				require.Equal(t, beforeTimestamp, machine.State.LastAppliedTimestamp)
			}
			require.Equal(t, expectedAudit, machine.State.NextAuditSequenceID)
			outcome, exists := machine.Registry.Idempotency.Get("identity")
			require.True(t, exists, "tentative eviction must not erase a committed live identity")
			if exit == "expired outcome" {
				require.NotNil(t, outcome.GetFailure(), "an HLC-expired identity executes fresh and freezes its failure")
				require.Equal(t, uint64(200), outcome.GetExpiresAt())
			} else {
				require.Nil(t, outcome.GetFailure())
				require.Equal(t, uint64(100), outcome.GetExpiresAt())
			}
			recovered := recoverMachineOnStore(t, store)
			require.Equal(t, appliedIndex, recovered.LastAppliedIndex())
			require.Equal(t, expectedAudit, recovered.State.NextAuditSequenceID)
			require.Equal(t, machine.State.LastAppliedTimestamp, recovered.State.LastAppliedTimestamp)
			require.Zero(t, recovered.State.LastIdempotencyEvictionCutoff)
			durable, exists := recovered.Registry.Idempotency.Get("identity")
			require.True(t, exists)
			require.True(t, proto.Equal(outcome, durable))
		})
	}
}

func TestTechnicalAtomicity_ConfigPublishesAfterOverlay(t *testing.T) {
	t.Parallel()
	for _, withOrders := range []bool{false, true} {
		t.Run(map[bool]string{false: "technical only", true: "with business orders"}[withOrders], func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, createLedgerOrder("mirror"))))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)
			proposal := makeProposal(2)
			proposal.AtomicTechnicalUpdates = true
			if withOrders {
				proposal = makeProposal(2, createLedgerOrder("created-before-reset"))
				proposal.AtomicTechnicalUpdates = true
			}
			id, _ := attributes.MakeKey((domain.LedgerKey{Name: "mirror"}).Bytes())
			proposal.ExecutionPlan.Attributes = append(proposal.ExecutionPlan.Attributes, declareTestPlan(id, dal.SubAttrLedger))
			proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{
				{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: 2000}}},
				{Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "mirror", SourceLogCount: 88}}},
				sinkCursorUpdate("published", 42),
			}
			result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, proposal))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)
			require.Equal(t, uint64(2000), machine.Registry.Cache.GenerationThreshold())
			requireTechnicalCursor(t, store, "published", 42)
			reader, err := store.NewReadHandle()
			require.NoError(t, err)
			head, err := query.ReadMirrorSourceHead(reader, "mirror")
			require.NoError(t, err)
			require.Equal(t, uint64(88), head, "configuration must not erase the ledger before MirrorSync preparation")
			require.NoError(t, reader.Close())
			recovered := recoverMachineOnStore(t, store)
			require.Equal(t, uint64(2000), recovered.Registry.Cache.GenerationThreshold())
			require.Equal(t, machine.Registry.Cache.BaseIndex, recovered.Registry.Cache.BaseIndex)
			require.Equal(t, machine.State.NextAuditSequenceID, recovered.State.NextAuditSequenceID)
		})
	}
}

func TestTechnicalAtomicity_MultipleConfigResetsPreserveDeletion(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, createLedgerOrder("deleted"))))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	proposal := makeProposal(2, deleteLedgerOrder("deleted"))
	proposal.AtomicTechnicalUpdates = true
	proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{
		{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: 2000}}},
		{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: 3000}}},
	}
	result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, proposal))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	require.Equal(t, uint64(3000), machine.Registry.Cache.GenerationThreshold())
	recovered := recoverMachineOnStore(t, store)
	require.NoError(t, NewRecovery(recovered, store).RestoreCacheFromStore())
	require.Equal(t, uint64(3000), recovered.Registry.Cache.GenerationThreshold())
	require.Equal(t, machine.Registry.Cache.BaseIndex, recovered.Registry.Cache.BaseIndex)
	// A reset before Merge would lose the parent's boundary tombstone. Check
	// the primary attribute row as well as the cache reconstructed from 0xFF.
	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	boundary, err := dal.ReadProto[*raftcmdpb.LedgerBoundaries](reader,
		dal.NewKeyBuilder().PutZonePrefix(dal.ZoneAttributes, dal.SubAttrBoundary).PutBytes((domain.LedgerKey{Name: "deleted"}).Bytes()).Build())
	require.NoError(t, err)
	require.Nil(t, boundary)
	_, _, err = recovered.Registry.Boundaries.GetKey(domain.LedgerKey{Name: "deleted"})
	require.ErrorIs(t, err, domain.ErrNotFound)
}
