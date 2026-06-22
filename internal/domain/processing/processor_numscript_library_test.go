package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestProcessSaveNumscript_RejectsInvalidNames pins the validator branch on
// the save path. Numscript names land in the `x-next-cursor` trailer of
// `numscripts list`, so they must be HTTP/2-header-safe (printable ASCII).
func TestProcessSaveNumscript_RejectsInvalidNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr domain.Describable
	}{
		{name: "empty", input: "", wantErr: domain.ErrNumscriptNameRequired},
		{name: "with newline", input: "trans\nfer", wantErr: domain.ErrNumscriptNameInvalidChar},
		{name: "with non-ASCII", input: "transférer", wantErr: domain.ErrNumscriptNameInvalidChar},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			// Validator must short-circuit before any store interaction —
			// gomock fails the test if PutNumscript/GetLedger is hit.

			order := &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "main",
						Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{
							SaveNumscript: &raftcmdpb.SaveNumscriptOrder{
								Name:    tt.input,
								Content: "send [USD 1] (source = @world allocate { @bob })"},
						},
					},
				},
			}

			result, err := processor.ProcessOrder(order, mockStore)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, result)
		})
	}
}

func TestProcessDeleteNumscript_RejectsInvalidNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr domain.Describable
	}{
		{name: "empty", input: "", wantErr: domain.ErrNumscriptNameRequired},
		{name: "with control byte", input: "name\x01", wantErr: domain.ErrNumscriptNameInvalidChar},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			order := &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "main",
						Payload: &raftcmdpb.LedgerScopedOrder_DeleteNumscript{
							DeleteNumscript: &raftcmdpb.DeleteNumscriptOrder{
								Name: tt.input},
						},
					},
				},
			}

			result, err := processor.ProcessOrder(order, mockStore)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, result)
		})
	}
}
