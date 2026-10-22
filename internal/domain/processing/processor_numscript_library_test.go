package processing

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

const validNumscriptContent = "send [USD 1] (source = @world destination = @x)"

func saveNumscriptOrder(ledger, name, content, version string) *raftcmdpb.Order {
	return saveNumscriptOrderWithDescription(ledger, name, content, version, "")
}

func saveNumscriptOrderWithDescription(ledger, name, content, version, description string) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: ledger,
		Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{SaveNumscript: &raftcmdpb.SaveNumscriptOrder{
			Name: name, Content: content, Version: version, Description: description,
		}},
	}}}
}

// TestProcessSaveNumscript_RejectsInvalidNames pins the validator branch.
func TestProcessSaveNumscript_RejectsInvalidNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr domain.SerializableError
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

	expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, (&ledgerpb.LedgerInfo{Name: ledger}).AsReader(), nil)
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

	expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, (&ledgerpb.LedgerInfo{Name: ledger}).AsReader(), nil)
	mockStore.EXPECT().NumscriptVersionExists(ledger, "pay", "1.0.0").Return(false, nil)
	mockStore.EXPECT().GetNumscriptLatestVersion(ledger, "pay").Return("2.0.0", nil)
	mockStore.EXPECT().PutNumscript(ledger, gomock.Any())
	mockStore.EXPECT().GetDate().Return((&ledgerpb.Timestamp{}).AsReader())
	// New version (1.0.0) is lower than the current greatest (2.0.0): the pointer
	// is restored to 2.0.0.
	mockStore.EXPECT().SetNumscriptLatestVersion(ledger, "pay", "2.0.0")

	result, perr := processor.ProcessOrder(saveNumscriptOrder(ledger, "pay", validNumscriptContent, "1.0.0"), mockStore)
	require.Nil(t, perr)
	require.NotNil(t, result.GetSavedNumscript())
}

// TestProcessSaveNumscript_StoresDescription: the description is persisted with
// the version and echoed in the audited log; an order without one (every order
// accepted before the field existed) stores and logs an empty description.
func TestProcessSaveNumscript_StoresDescription(t *testing.T) {
	t.Parallel()

	for _, description := range []string{"", "Pay out a user", "Line one\n\tLine two", strings.Repeat("é", domain.MaxNumscriptDescriptionLength/2)} {
		t.Run(description, func(t *testing.T) {
			t.Parallel()

			const ledger = "main"

			ctrl := gomock.NewController(t)
			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			var stored *commonpb.NumscriptInfo

			expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, (&commonpb.LedgerInfo{Name: ledger}).AsReader(), nil)
			mockStore.EXPECT().NumscriptVersionExists(ledger, "pay", "1.0.0").Return(false, nil)
			mockStore.EXPECT().GetNumscriptLatestVersion(ledger, "pay").Return("", nil)
			mockStore.EXPECT().PutNumscript(ledger, gomock.Any()).Do(func(_ string, info *commonpb.NumscriptInfo) { stored = info })
			mockStore.EXPECT().GetDate().Return((&commonpb.Timestamp{}).AsReader())

			result, perr := processor.ProcessOrder(saveNumscriptOrderWithDescription(ledger, "pay", validNumscriptContent, "1.0.0", description), mockStore)
			require.Nil(t, perr)
			require.Equal(t, description, stored.GetDescription())
			require.Equal(t, description, result.GetSavedNumscript().GetInfo().GetDescription())
		})
	}
}

// TestProcessSaveNumscript_RejectsLongDescription: the byte limit is checked
// before any store interaction.
func TestProcessSaveNumscript_RejectsLongDescription(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	description := strings.Repeat("a", domain.MaxNumscriptDescriptionLength+1)
	result, perr := processor.ProcessOrder(saveNumscriptOrderWithDescription("main", "pay", validNumscriptContent, "1.0.0", description), mockStore)
	require.Nil(t, result)
	require.ErrorIs(t, perr, domain.ErrNumscriptDescriptionTooLong)
}

// TestProcessSaveNumscript_RejectsControlCharacters: control characters other
// than newline and tab are rejected before any store interaction.
func TestProcessSaveNumscript_RejectsControlCharacters(t *testing.T) {
	t.Parallel()

	for name, description := range map[string]string{
		"escape":          "\x1b[2Jcleared",
		"bell":            "ring\x07",
		"carriage return": "real\rfake",
		"delete":          "a\x7fb",
		"c1 csi":          "a\u009bb",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			result, perr := processor.ProcessOrder(saveNumscriptOrderWithDescription("main", "pay", validNumscriptContent, "1.0.0", description), mockStore)
			require.Nil(t, result)
			require.ErrorIs(t, perr, domain.ErrNumscriptDescriptionInvalidChar)
		})
	}
}
