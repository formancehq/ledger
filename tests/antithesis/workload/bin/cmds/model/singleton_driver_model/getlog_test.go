package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// topLevelLog builds a response log with no Apply arm — the shape ledger
// metadata takes, which carries a sequence but no ledger-local id.
func topLevelLog(seq uint64) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_SavedLedgerMetadata{
			SavedLedgerMetadata: &commonpb.SavedLedgerMetadataLog{},
		}},
	}
}

func TestPickLogSequenceUsesTheModelsLearnedSequences(t *testing.T) {
	t.Parallel()

	state := committedStateWithSequences(t, 42)

	var sawLearned, sawUnassigned bool
	for range 200 {
		target, learned, ok := pickLogSequence(state)
		require.True(t, ok)
		if learned {
			require.Equal(t, committedLogTarget{ledger: "L", id: 1, sequence: 42}, target)
			sawLearned = true
		} else {
			require.Equal(t, uint64(42+unassignedSeqSlack), target.sequence, "the unassigned probe sits past every learned sequence")
			sawUnassigned = true
		}
	}
	require.True(t, sawLearned)
	require.True(t, sawUnassigned)

	_, _, ok := pickLogSequence(oracle.NewGlobalState())
	require.False(t, ok, "nothing to read back before a sequence is learned")
}

func TestCommittedLogMatchesTheModelsRow(t *testing.T) {
	t.Parallel()

	gs := committedStateWithSequences(t, 42)
	target := committedLogTarget{ledger: "L", id: 1, sequence: 42}
	got := servedRows(gs.Ledger("L"), "L", 1)[0]

	require.True(t, committedLogMatches(gs, target, got))

	other := got
	other.sequence = 43
	require.False(t, committedLogMatches(gs, target, other), "a different global sequence is another log")

	other = got
	other.id = 2
	require.False(t, committedLogMatches(gs, target, other), "a different ledger-local id is another log")

	other = got
	other.kind = "created_transaction"
	require.False(t, committedLogMatches(gs, target, other), "the served row is compared field by field")

	require.False(t, committedLogMatches(gs, committedLogTarget{ledger: "L", id: 2, sequence: 42}, got), "an id the stream does not hold at that sequence matches nothing")
	require.False(t, committedLogMatches(gs, committedLogTarget{ledger: "L", id: 1, sequence: 43}, got), "a sequence the stream never learned matches nothing")
}

// A ledger's deletion retires its per-ledger view, not its place in the global
// log stream: the server keeps serving the row by sequence, so the model's
// expectation for it must hold on a base that has folded the delete.
func TestCommittedLogSurvivesLedgerDeletion(t *testing.T) {
	t.Parallel()

	gs := committedStateWithSequences(t, 42)
	target := committedLogTarget{ledger: "L", id: 1, sequence: 42}
	got := servedRows(gs.Ledger("L"), "L", 1)[0]

	deleted := gs.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteLedger{
		DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"},
	}}))
	require.True(t, deleted.OK, deleted.Reason)
	require.Empty(t, deleted.State.Ledger("L").LogRows(), "the per-ledger view is gone")

	require.True(t, committedLogMatches(deleted.State, target, got))

	picked, learned, ok := pickLogSequence(deleted.State)
	require.True(t, ok)
	if learned {
		require.Equal(t, target, picked, "the deleted ledger's log stays a read target")
	}
}

func TestServerLogKindMatchesModelVocabulary(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.AddTypeReq("acc"),
		oracletest.TxReq("world", "acc:a", "USD", 1),
	)

	rows := ls.LogRows()
	require.Len(t, rows, 2)
	modelKind := rows[1].Kind

	served := &commonpb.Log{
		Sequence: 2,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: "L",
				Log: &commonpb.LedgerLog{Id: 2, Data: &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
						CreatedTransaction: &commonpb.CreatedTransaction{},
					},
				}},
			},
		}},
	}

	require.Equal(t, modelKind, serverLogKind(served))
	require.Equal(t, "other", serverLogKind(topLevelLog(3)))
}
