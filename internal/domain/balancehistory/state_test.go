package balancehistory

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestReducerEmptyStateIsCanonical(t *testing.T) {
	t.Parallel()

	state := NewReducer().State()
	require.Equal(t, State{}, state)
	require.Nil(t, state.Active)
	require.Nil(t, state.Seen)

	restored, err := NewReducerFromState(State{})
	require.NoError(t, err)
	require.Equal(t, State{}, restored.State())
}

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

func TestReducerStateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		state State
		want  string
	}{
		{
			name:  "invalid seen incarnation",
			state: State{Seen: []IncarnationState{{Name: "", ID: 1}}},
			want:  "invalid seen incarnation snapshot",
		},
		{
			name: "duplicate seen id",
			state: State{Seen: []IncarnationState{
				{Name: "first", ID: 1},
				{Name: "second", ID: 1},
			}},
			want: "duplicate seen incarnation id 1",
		},
		{
			name: "duplicate seen name",
			state: State{Seen: []IncarnationState{
				{Name: "ledger", ID: 1},
				{Name: "ledger", ID: 2},
			}},
			want: "duplicate seen ledger name",
		},
		{
			name:  "invalid active incarnation",
			state: State{Active: []IncarnationState{{Name: "ledger", ID: 0}}},
			want:  "invalid active incarnation snapshot",
		},
		{
			name: "active absent from seen",
			state: State{
				Seen:   []IncarnationState{{Name: "other", ID: 1}},
				Active: []IncarnationState{{Name: "ledger", ID: 1}},
			},
			want: "is absent from seen set",
		},
		{
			name: "enabled ledger is not active",
			state: State{
				Seen:    []IncarnationState{{Name: "ledger", ID: 1}},
				Enabled: []string{"ledger"},
			},
			want: "enabled ledger \"ledger\" is not active",
		},
		{
			name:  "invalid last position",
			state: State{HasLast: true, Last: Position{AuditSequence: 1}},
			want:  "invalid last reducer position",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewReducerFromState(test.state)
			require.ErrorContains(t, err, test.want)
		})
	}

	var reducer *Reducer
	require.Equal(t, State{}, reducer.State())
}
