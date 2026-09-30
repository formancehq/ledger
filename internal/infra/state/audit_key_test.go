package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestAuditKeyInitializationFirstCommitWinsAndSurvivesRecovery(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	clearBatch := store.OpenWriteSession()
	require.NoError(t, clearBatch.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}))
	require.NoError(t, clearBatch.Commit())
	machine.State.AuditKey = ""
	machine.State.HashGenerator = nil
	first := []byte("fedcba9876543210fedcba9876543210")
	second := []byte("abcdef0123456789abcdef0123456789")
	for i, key := range [][]byte{first, second} {
		proposal := &raftcmdpb.Proposal{TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: key},
		}}}
		_, err := machine.ApplyEntries(context.Background(), store, makeEntry(t, uint64(i+1), proposal))
		require.NoError(t, err)
	}
	stored, err := query.ReadAuditKey(store)
	require.NoError(t, err)
	// A second leader's conflicting proposal cannot rotate the first key.
	require.Equal(t, first, stored)
	require.Equal(t, string(stored), machine.State.AuditKey)
	restarted := recoverMachineOnStore(t, store)
	require.Equal(t, string(stored), restarted.State.AuditKey)
}

func TestAuditKeySameCommittedProposalOnTwoNodes(t *testing.T) {
	t.Parallel()
	key := []byte("fedcba9876543210fedcba9876543210")
	var hashes [][]byte
	for range 2 {
		machine, store, _ := newTestMachine(t)
		clearBatch := store.OpenWriteSession()
		require.NoError(t, clearBatch.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}))
		require.NoError(t, clearBatch.Commit())
		machine.State.AuditKey = ""
		machine.State.HashGenerator = nil
		proposal := &raftcmdpb.Proposal{TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: key},
		}}}
		_, err := machine.ApplyEntries(context.Background(), store, makeEntry(t, 1, proposal))
		require.NoError(t, err)
		stored, err := query.ReadAuditKey(store)
		require.NoError(t, err)
		require.Equal(t, key, stored)
		require.Equal(t, string(key), machine.State.AuditKey)
		_, err = machine.ApplyEntries(context.Background(), store, makeEntry(t, 2, makeProposal(2, createLedgerOrder("ledger"))))
		require.NoError(t, err)
		hashes = append(hashes, append([]byte(nil), machine.State.LastAuditHash...))
	}
	require.Equal(t, hashes[0], hashes[1])
}

func TestAuditKeyRecoveryFailsClosed(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{name: "missing"},
		{name: "corrupt", value: []byte("short")},
		{name: "mismatched", value: []byte("abcdef0123456789abcdef0123456789")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			_, err := machine.ApplyEntries(context.Background(), store, makeEntry(t, 1, makeProposal(1, createLedgerOrder("ledger"))))
			require.NoError(t, err)
			batch := store.OpenWriteSession()
			if tc.value == nil {
				require.NoError(t, batch.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}))
			} else {
				require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}, tc.value))
			}
			require.NoError(t, batch.Commit())
			err = NewRecovery(machine, store).RecoverState()
			require.ErrorContains(t, err, "audit key")
		})
	}
}

// A malformed row is rejected even without any history; it must never be
// interpreted as a fresh cluster that may generate a replacement key.
func TestAuditKeyMalformedBeforeFirstAudit(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}, []byte("bad")))
	require.NoError(t, batch.Commit())
	_, err := query.ReadAuditKey(store)
	require.ErrorContains(t, err, "invalid audit key length")
}
