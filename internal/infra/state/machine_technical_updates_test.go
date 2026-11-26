package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestApplyMirrorSyncUpdate_KeysOffEnvelopeNotProjection covers EN-1522 gap
// A2: applyMirrorSyncUpdate must derive MirrorSyncWrite.LedgerName from the
// command envelope (update.GetLedgerName()), not from the loaded projection's
// mutable Name. A divergent LedgerInfo.name must not redirect the queued
// cursor/status write to another ledger's keys.
func TestApplyMirrorSyncUpdate_KeysOffEnvelopeNotProjection(t *testing.T) {
	t.Parallel()

	fsm, dataStore, _ := newTestMachine(t)

	const (
		gen0Byte byte = 0
		envelope      = "mirror-envelope"
	)

	// Seed the ledger UNDER the envelope key but with a DIVERGENT stored Name.
	envKey := domain.LedgerKey{Name: envelope}
	divergentInfo := &commonpb.LedgerInfo{Id: 1, Name: "divergent-projection"}

	seedBatch := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.Ledgers.PutWithCache(seedBatch, gen0Byte, envKey.Bytes(), divergentInfo)
	require.NoError(t, err)
	// Global row under the envelope key too: the divergence under test is the
	// payload's Name field, not a split between the two seeded rows.
	require.NoError(t, SaveLedger(seedBatch, envelope, divergentInfo))
	require.NoError(t, seedBatch.Commit())

	envID, _ := attributes.MakeKey(envKey.Bytes())
	executionPlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: fsm.Registry.Cache.BaseIndex.Gen0,
		Attributes: []*raftcmdpb.AttributeCoverage{
			declareTestPlan(envID, dal.SubAttrLedger),
		},
	}

	proposal := &raftcmdpb.Proposal{
		Id:            1,
		Date:          &commonpb.Timestamp{Data: 1700000000},
		ExecutionPlan: executionPlan,
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{
			{Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: envelope, SourceLogCount: 7}}},
		},
	}
	// Stamp coverage bits on the TU so its scope admits the declared ledger
	// (mirrors what makeEntry/admission does for production proposals).
	sealProposal(proposal)

	applyBatch := dataStore.OpenWriteSession()
	defer func() { _ = applyBatch.Cancel() }()

	fsm.writeSet.Reset(proposal.GetDate())
	buffer := fsm.writeSet
	scopeFactory := NewScopeFactory(buffer, executionPlan, fsm.logger, fsm.preloadMissCounter, proposal.GetId())

	require.NoError(t, fsm.applyTechnicalUpdates(scopeFactory, applyBatch, proposal.GetId(), proposal))

	require.Len(t, buffer.pendingMirrorSyncs, 1)
	require.Equal(t, envelope, buffer.pendingMirrorSyncs[0].LedgerName,
		"mirror-sync write must key off the envelope, not the divergent projection name")
}
