package processing

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestProcessCreateTransaction_NumscriptReference_ResolvesContent(t *testing.T) {
	t.Parallel()

	const (
		ledger  = "test-ledger"
		name    = "payment"
		content = `
			vars {
				account $destination
			}
			send [USD 100] (
				source = @world
				destination = $destination
			)
		`
	)

	tests := []struct {
		name            string
		selector        string
		resolvedVersion string
		resolveLatest   bool
	}{
		{name: "exact version", selector: "1.2.3", resolvedVersion: "1.2.3"},
		{name: "latest", selector: "latest", resolvedVersion: "2.0.0", resolveLatest: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			if tt.resolveLatest {
				mockStore.EXPECT().GetNumscriptLatestVersion(ledger, name).Return(tt.resolvedVersion, nil)
				mockStore.EXPECT().CheckCoverage(
					dal.SubAttrNumscriptContent,
					domain.NumscriptEntryKey{LedgerName: ledger, Name: name, Version: tt.resolvedVersion},
				).Return(nil)
			}

			mockStore.EXPECT().ResolveNumscriptContent(ledger, name, tt.resolvedVersion).Return(
				(&commonpb.NumscriptInfo{Content: content}).AsReader(),
				nil,
			)
			setupNumscriptVolumeMocks(mockStore)
			mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
			mockStore.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1234567890}).AsReader()).AnyTimes()
			expectPutTransactionState(t, mockStore, domain.TransactionKey{LedgerName: ledger, ID: 1}, nil)

			payload, processErr := processCreateTransaction(ledger, &raftcmdpb.CreateTransactionOrder{
				NumscriptReference: &raftcmdpb.NumscriptReference{
					Name:    name,
					Version: tt.selector,
					Vars:    map[string]string{"destination": "merchants:shop"},
				},
			}, &Context{
				Scope:          mockStore,
				Boundaries:     &raftcmdpb.LedgerBoundaries{NextTransactionId: 1},
				LedgerInfo:     &commonpb.LedgerInfo{Name: ledger, Id: 1},
				NumscriptCache: processor.numscriptCache,
			})
			require.NoError(t, processErr)
			require.NotNil(t, payload)

			postings := payload.GetCreatedTransaction().GetTransaction().GetPostings()
			require.Len(t, postings, 1)
			require.Equal(t, "world", postings[0].GetSource())
			require.Equal(t, "merchants:shop", postings[0].GetDestination())
			require.Equal(t, "USD", postings[0].GetAsset())
			require.Equal(t, int64(100), postings[0].GetAmount().ToBigInt().Int64())
		})
	}
}

func TestProcessCreateTransaction_NumscriptReference_RejectsResolutionFailures(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test-ledger"
		name   = "payment"
	)

	latestLookupErr := errors.New("latest lookup failed")
	coverageErr := errors.New("content key is outside proposal coverage")
	contentLookupErr := errors.New("content lookup failed")

	tests := []struct {
		name     string
		selector string
		setup    func(*MockScope)
		assert   func(*testing.T, domain.Describable)
	}{
		{
			name:     "latest lookup failure",
			selector: "latest",
			setup: func(mockStore *MockScope) {
				mockStore.EXPECT().GetNumscriptLatestVersion(ledger, name).Return("", latestLookupErr)
			},
			assert: func(t *testing.T, err domain.Describable) {
				t.Helper()
				var storageErr *domain.ErrStorageOperation
				require.ErrorAs(t, err, &storageErr)
				require.ErrorIs(t, err, latestLookupErr)
			},
		},
		{
			name:     "numscript not found",
			selector: "latest",
			setup: func(mockStore *MockScope) {
				mockStore.EXPECT().GetNumscriptLatestVersion(ledger, name).Return("", nil)
			},
			assert: func(t *testing.T, err domain.Describable) {
				t.Helper()
				var notFound *domain.ErrNumscriptNotFound
				require.ErrorAs(t, err, &notFound)
				require.Equal(t, name, notFound.Name)
				require.Empty(t, notFound.Version)
			},
		},
		{
			name:     "latest content coverage is stale",
			selector: "latest",
			setup: func(mockStore *MockScope) {
				mockStore.EXPECT().GetNumscriptLatestVersion(ledger, name).Return("2.0.0", nil)
				mockStore.EXPECT().CheckCoverage(
					dal.SubAttrNumscriptContent,
					domain.NumscriptEntryKey{LedgerName: ledger, Name: name, Version: "2.0.0"},
				).Return(coverageErr)
			},
			assert: func(t *testing.T, err domain.Describable) {
				t.Helper()
				require.ErrorIs(t, err, domain.ErrStaleProposal)
			},
		},
		{
			name:     "content lookup failure",
			selector: "1.2.3",
			setup: func(mockStore *MockScope) {
				mockStore.EXPECT().ResolveNumscriptContent(ledger, name, "1.2.3").Return(nil, contentLookupErr)
			},
			assert: func(t *testing.T, err domain.Describable) {
				t.Helper()
				var storageErr *domain.ErrStorageOperation
				require.ErrorAs(t, err, &storageErr)
				require.ErrorIs(t, err, contentLookupErr)
			},
		},
		{
			name:     "content not found",
			selector: "1.2.3",
			setup: func(mockStore *MockScope) {
				mockStore.EXPECT().ResolveNumscriptContent(ledger, name, "1.2.3").Return(nil, nil)
			},
			assert: func(t *testing.T, err domain.Describable) {
				t.Helper()
				var notFound *domain.ErrNumscriptNotFound
				require.ErrorAs(t, err, &notFound)
				require.Equal(t, name, notFound.Name)
				require.Equal(t, "1.2.3", notFound.Version)
			},
		},
		{
			// Only the literal "latest" resolves through the latest pointer.
			// Admission rejects an empty selector (NUMSCRIPT_INVALID_VERSION), so
			// it cannot reach apply; setting no GetNumscriptLatestVersion
			// expectation asserts the pointer is never consulted for it.
			name:     "empty selector is not resolved as latest",
			selector: "",
			setup: func(mockStore *MockScope) {
				mockStore.EXPECT().ResolveNumscriptContent(ledger, name, "").Return(nil, nil)
			},
			assert: func(t *testing.T, err domain.Describable) {
				t.Helper()
				var notFound *domain.ErrNumscriptNotFound
				require.ErrorAs(t, err, &notFound)
				require.Equal(t, name, notFound.Name)
				require.Empty(t, notFound.Version)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockStore := NewMockScope(ctrl)
			tt.setup(mockStore)

			payload, err := processCreateTransaction(ledger, &raftcmdpb.CreateTransactionOrder{
				NumscriptReference: &raftcmdpb.NumscriptReference{Name: name, Version: tt.selector},
			}, &Context{Scope: mockStore})
			require.Nil(t, payload)
			require.Error(t, err)
			tt.assert(t, err)
		})
	}
}
