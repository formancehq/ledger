package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestGlobalState_LogStreamOutlivesItsLedger(t *testing.T) {
	t.Parallel()

	committed := NewGlobalState().Apply(bulkOf(oracletest.TxReqL("L", "world", "acc:1", "USD", 5)))
	require.True(t, committed.OK, committed.Reason)

	_, ok := committed.State.Log(7)
	require.False(t, ok, "nothing is filed before its sequence is learned")

	committed.State.LearnLogSequence("L", 1, 7)

	entry, ok := committed.State.Log(7)
	require.True(t, ok)
	require.Equal(t, "L", entry.Ledger)
	require.Equal(t, uint64(1), entry.ID)
	require.Equal(t, uint64(7), entry.Row.Sequence)
	require.Equal(t, "created_transaction", entry.Row.Kind)
	require.NotNil(t, entry.Tx)
	require.Equal(t, uint64(1), entry.Tx.Id())

	deleted := committed.State.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteLedger{
		DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"},
	}}))
	require.True(t, deleted.OK, deleted.Reason)
	require.Empty(t, deleted.State.Ledger("L").LogRows())

	kept, ok := deleted.State.Log(7)
	require.True(t, ok, "the global stream keeps a deleted ledger's rows, as the server does")
	require.Equal(t, entry, kept)

	var seqs []uint64
	for seq := range deleted.State.Logs() {
		seqs = append(seqs, seq)
	}
	require.Equal(t, []uint64{7}, seqs)
}

func TestGlobalState_LearnLogSequenceIsFillOnce(t *testing.T) {
	t.Parallel()

	committed := NewGlobalState().Apply(bulkOf(oracletest.TxReqL("L", "world", "acc:1", "USD", 5)))
	require.True(t, committed.OK, committed.Reason)

	committed.State.LearnLogSequence("L", 1, 7)
	committed.State.LearnLogSequence("L", 1, 8)

	_, ok := committed.State.Log(8)
	require.False(t, ok)
	require.Equal(t, uint64(7), committed.State.Ledger("L").LogRows()[0].Sequence)
}
