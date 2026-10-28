package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func payloadProposal(t *testing.T, p *raftcmdpb.Proposal) *raftcmdpb.Proposal {
	t.Helper()
	sealProposal(p)
	wire := p.CloneVT()
	if len(p.GetOrders()) == 1 && p.GetOrders()[0].GetSystemScoped().GetSetClusterPolicy() != nil {
		wire.Payload = &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: p.GetOrders()[0].GetSystemScoped().GetSetClusterPolicy().GetPolicy()}}
	} else {
		wire.Payload = &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: wire.GetOrders(), TechnicalUpdates: wire.GetTechnicalUpdates()}}
	}
	wire.Orders = nil
	wire.TechnicalUpdates = nil

	return wire
}

func TestPayloadMirrorStatusCommitsOnlyWithIngestion(t *testing.T) {
	t.Parallel()
	for _, mode := range []commonpb.LedgerMode{commonpb.LedgerMode_LEDGER_MODE_MIRROR, commonpb.LedgerMode_LEDGER_MODE_NORMAL} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			create := createLedgerOrder("mirror")
			create.GetLedgerScoped().GetCreateLedger().Mode = mode
			result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, create)))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)
			ingest := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "mirror", Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
					V2LogId: 1, Data: &raftcmdpb.MirrorLogEntry_FillGap{FillGap: &raftcmdpb.MirrorFillGap{}},
				}}},
			}}}
			p := makeProposal(2, ingest)
			p.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "mirror", SourceLogCount: 12, ClearError: true}}}}
			result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, payloadProposal(t, p)))
			require.NoError(t, err)
			reader, err := store.NewDirectReadHandle()
			require.NoError(t, err)
			defer func() { require.NoError(t, reader.Close()) }()
			head, err := query.ReadMirrorSourceHead(reader, "mirror")
			require.NoError(t, err)
			if mode == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
				require.NoError(t, result.Results[0].Error)
				require.Equal(t, uint64(12), head)
			} else {
				var rejected *domain.ErrLedgerNotInMirrorMode
				require.ErrorAs(t, result.Results[0].Error, &rejected)
				require.Zero(t, head, "business rejection must discard technical status")
			}
		})
	}
}

func TestPayloadHistoryPreservesOutcomesWithNativeAuditAndRecovery(t *testing.T) {
	t.Parallel()
	legacy, oldStore, _ := newTestMachine(t)
	modern, newStore, _ := newTestMachine(t)
	policy := idempotencyTestPolicy(2, 10000)
	steps := []*raftcmdpb.Proposal{
		makeProposal(1, clusterPolicyOrder(policy)),
		makeProposal(2, createLedgerOrder("payload")),
		makeProposal(3, clusterPolicyOrder(policy.CloneVT())),
		makeProposal(4, clusterPolicyOrder(idempotencyTestPolicy(2, 20000))),
		makeProposal(5, clusterPolicyOrder(idempotencyTestPolicy(1, 10000))),
		makeProposal(6, clusterPolicyOrder(nil)),
		makeProposal(7, createLedgerOrder("payload")), // Audited business failure.
	}
	for i, p := range steps {
		index := uint64(i + 1)
		old, err := legacy.ApplyEntries(t.Context(), oldStore, makeEntry(t, index, p))
		require.NoError(t, err)
		wire := payloadProposal(t, p.CloneVT())
		entry := makeEntry(t, index, wire)
		before := wire.CloneVT()
		prepared, err := modern.PrepareDecodedEntries(t.Context(), newStore, DecodedEntry{Entry: entry, Proposal: wire})
		require.NoError(t, err)
		require.True(t, proto.Equal(before, wire), "apply must not mutate the wire proposal")
		require.NoError(t, modern.CommitPreparedBatch(t.Context(), prepared))
		fresh := prepared.Result
		require.Equal(t, old.Results[0].AuditEntryWritten, fresh.Results[0].AuditEntryWritten)
		if old.Results[0].Error != nil {
			require.EqualError(t, fresh.Results[0].Error, old.Results[0].Error.Error())
		} else {
			require.NoError(t, fresh.Results[0].Error)
		}
		require.Equal(t, legacy.State.LastAppliedTimestamp, modern.State.LastAppliedTimestamp)
		require.Equal(t, legacy.State.NextSequenceID, modern.State.NextSequenceID)
		require.True(t, proto.Equal(legacy.State.ClusterPolicy, modern.State.ClusterPolicy))
		oldReader, err := oldStore.NewDirectReadHandle()
		require.NoError(t, err)
		newReader, err := newStore.NewDirectReadHandle()
		require.NoError(t, err)
		oldAudit, err := query.ReadLastAuditEntry(oldReader)
		require.NoError(t, err)
		newAudit, err := query.ReadLastAuditEntry(newReader)
		require.NoError(t, err)
		require.NotEqual(t, oldAudit.GetHash(), newAudit.GetHash(), "native policy intent has its own hash preimage")
		oldAudit.Hash = nil
		newAudit.Hash = nil
		require.True(t, proto.Equal(oldAudit, newAudit), "audit outcome headers must be identical")
		oldItems, err := query.ReadAuditItems(t.Context(), oldReader, oldAudit.GetSequence())
		require.NoError(t, err)
		newItems, err := query.ReadAuditItems(t.Context(), newReader, newAudit.GetSequence())
		require.NoError(t, err)
		require.Len(t, newItems, len(oldItems))
		for j := range oldItems {
			if policyOrder := p.GetOrders()[0].GetSystemScoped().GetSetClusterPolicy(); policyOrder != nil && fresh.Results[0].AuditEntryWritten {
				require.Empty(t, newItems[j].GetSerializedOrder())
				require.NotEmpty(t, newItems[j].GetSerializedSetClusterPolicy())
				control := &raftcmdpb.SetClusterPolicyProposal{}
				require.NoError(t, control.UnmarshalVT(newItems[j].GetSerializedSetClusterPolicy()))
				require.True(t, proto.Equal(policyOrder.GetPolicy(), control.GetPolicy()))
				require.Equal(t, oldItems[j].GetLogSequence(), newItems[j].GetLogSequence())
			} else if len(newItems[j].GetSerializedSetClusterPolicy()) == 0 {
				require.True(t, proto.Equal(oldItems[j], newItems[j]))
			}
		}
		require.NoError(t, oldReader.Close())
		require.NoError(t, newReader.Close())
	}
	recovered := recoverMachineOnStore(t, newStore)
	require.True(t, proto.Equal(modern.State.ClusterPolicy, recovered.State.ClusterPolicy))
	require.Equal(t, modern.State.LastAppliedIndex, recovered.State.LastAppliedIndex)
	require.Equal(t, modern.State.LastAppliedTimestamp, recovered.State.LastAppliedTimestamp)
}

func TestPolicyPayloadMaintenanceAndExhaustionGuards(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	machine.sharedState.SetMaintenanceMode(true)
	p := payloadProposal(t, makeProposal(1, clusterPolicyOrder(idempotencyTestPolicy(2, 100))))
	result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, p))
	require.NoError(t, err)
	require.ErrorIs(t, result.Results[0].Error, domain.ErrMaintenanceMode)
	require.Equal(t, uint64(1), machine.State.ClusterPolicy.GetRevision())
	machine.sharedState.SetMaintenanceMode(false)
	machine.State.NextAuditSequenceID = ^uint64(0)
	_, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, p))
	var exhausted *domain.ErrSequenceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, domain.SequenceCounterAudit, exhausted.Counter)
	require.Equal(t, uint64(1), machine.State.ClusterPolicy.GetRevision())
}

func TestTechnicalPayloadBeforeAuditKeyAndWithoutAudit(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	machine.State.AuditKey = ""
	p := makeProposal(1)
	p.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: []byte("0123456789abcdef0123456789abcdef")}}}
	beforeTime := machine.State.LastAppliedTimestamp
	result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, payloadProposal(t, p)))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	require.False(t, result.Results[0].AuditEntryWritten)
	require.NotEmpty(t, machine.State.AuditKey)
	require.Equal(t, beforeTime, machine.State.LastAppliedTimestamp)
	require.Equal(t, uint64(1), machine.State.NextAuditSequenceID)
	// A technical write after initialization still cannot enter the audit path.
	p = makeProposal(2)
	p.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_EventsSink{EventsSink: &raftcmdpb.EventsSinkUpdate{SinkName: "sink", Cursor: 17}}}}
	result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, payloadProposal(t, p)))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	require.False(t, result.Results[0].AuditEntryWritten)
	require.Equal(t, beforeTime, machine.State.LastAppliedTimestamp)
}

func TestPayloadBatchIdempotency(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	apply := func(index uint64, ledger string) ApplyResult {
		p := makeProposal(index, createLedgerOrder(ledger))
		p.Idempotency = &commonpb.Idempotency{Key: "payload-key"}
		result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, index, payloadProposal(t, p)))
		require.NoError(t, err)

		return result.Results[0]
	}
	first := apply(1, "first")
	require.NoError(t, first.Error)
	replay := apply(2, "first")
	require.NoError(t, replay.Error)
	require.True(t, replay.Replayed)
	require.False(t, replay.AuditEntryWritten)
	conflict := apply(3, "different")
	require.Error(t, conflict.Error)
	require.False(t, conflict.Replayed, "different native batches must not hash as an empty order list")
	require.True(t, conflict.AuditEntryWritten)
}
