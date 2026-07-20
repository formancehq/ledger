package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

const validNumscriptContent = "send [USD 1] (source = @world destination = @x)"

func saveNumscriptOrder(ledger, name, content, version string) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger:  ledger,
		Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{SaveNumscript: &raftcmdpb.SaveNumscriptOrder{Name: name, Content: content, Version: version}},
	}}}
}

// TestProcessSaveNumscript_RejectsInvalidNames pins the validator branch.
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
			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			result, perr := processor.ProcessOrder(saveNumscriptOrder("main", tt.input, validNumscriptContent, "1.0.0"), mockStore)
			require.Error(t, perr)
			require.ErrorIs(t, perr, tt.wantErr)
			require.Nil(t, result)
		})
	}
}

// TestProcessSaveNumscript_RequiresExplicitSemver rejects "", "latest" and
// partial selectors — save is explicit-semver only.
func TestProcessSaveNumscript_RequiresExplicitSemver(t *testing.T) {
	t.Parallel()

	for _, version := range []string{"", "latest", "1", "1.2", "bogus"} {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			// Version is validated before any store interaction (after parse).
			result, perr := processor.ProcessOrder(saveNumscriptOrder("main", "pay", validNumscriptContent, version), mockStore)
			require.Nil(t, result)
			var e *domain.ErrNumscriptInvalidVersion
			require.ErrorAs(t, perr, &e)
		})
	}
}

// TestProcessSaveNumscript_DuplicateVersionRejected: an already-stored version
// is immutable.
func TestProcessSaveNumscript_DuplicateVersionRejected(t *testing.T) {
	t.Parallel()

	const ledger = "main"

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, (&commonpb.LedgerInfo{Name: ledger}).AsReader(), nil)
	mockStore.EXPECT().NumscriptVersionExists(ledger, "pay", "1.0.0").Return(true, nil)

	result, perr := processor.ProcessOrder(saveNumscriptOrder(ledger, "pay", validNumscriptContent, "1.0.0"), mockStore)
	require.Nil(t, result)
	var e *domain.ErrNumscriptVersionAlreadyExists
	require.ErrorAs(t, perr, &e)
}

// TestProcessSaveNumscript_KeepsGreatestPointer: saving a lower version after a
// greater one keeps the latest pointer at the greatest.
func TestProcessSaveNumscript_KeepsGreatestPointer(t *testing.T) {
	t.Parallel()

	const ledger = "main"

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, (&commonpb.LedgerInfo{Name: ledger}).AsReader(), nil)
	mockStore.EXPECT().NumscriptVersionExists(ledger, "pay", "1.0.0").Return(false, nil)
	mockStore.EXPECT().GetNumscriptLatestVersion(ledger, "pay").Return("2.0.0", nil)
	mockStore.EXPECT().PutNumscript(ledger, gomock.Any())
	mockStore.EXPECT().GetDate().Return((&commonpb.Timestamp{}).AsReader())
	// New version (1.0.0) is lower than the current greatest (2.0.0): the pointer
	// is restored to 2.0.0.
	mockStore.EXPECT().SetNumscriptLatestVersion(ledger, "pay", "2.0.0")

	result, perr := processor.ProcessOrder(saveNumscriptOrder(ledger, "pay", validNumscriptContent, "1.0.0"), mockStore)
	require.Nil(t, perr)
	require.NotNil(t, result.GetSavedNumscript())
}
