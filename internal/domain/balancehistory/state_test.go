package balancehistory

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestReducerStateRoundTrip(t *testing.T) {
	t.Parallel()

	reducer := NewReducer()
	_, err := reducer.Reduce(Position{AuditSequence: 1, OrderIndex: 0, LogSequence: 1}, &commonpb.Log{
		Sequence: 1,
		Payload:  &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger", Id: 7}}},
	})
	require.NoError(t, err)

	restored, err := NewReducerFromState(reducer.State())
	require.NoError(t, err)
	require.Equal(t, reducer.State(), restored.State())

	_, err = restored.Reduce(Position{AuditSequence: 2, OrderIndex: 0, LogSequence: 2}, &commonpb.Log{
		Sequence: 2,
		Payload:  &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: "ledger"}}},
	})
	require.NoError(t, err)
	require.Empty(t, restored.State().Active)
	require.Len(t, restored.State().Seen, 1)
}
