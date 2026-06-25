package processing

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// addAccountTypeOrderWithDefaults builds an Order for adding an account type
// with optional default_metadata.
func addAccountTypeOrderWithDefaults(ledger, name, pattern string, defaultMetadata map[string]*commonpb.MetadataValue) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{
							AddAccountType: &raftcmdpb.AddAccountTypeOrder{
								AccountType: &commonpb.AccountType{
									Name:            name,
									Pattern:         pattern,
									DefaultMetadata: defaultMetadata,
								},
							},
						},
					},
				},
			},
		},
	}
}

// ledgerInfoWithID returns a minimal LedgerInfo for mock returns.
func ledgerInfoWithID(name string, id uint32) *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{
		Name: name,
		Id:   id,
	}
}

// TestAddAccountType_WithDefaultMetadata_PopulatedLedger verifies that adding
// an account type carrying default_metadata to a ledger that already has
// transactions (NextTransactionId > 1) returns ErrDefaultMetadataOnPopulatedLedger.
func TestAddAccountType_WithDefaultMetadata_PopulatedLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	const ledger = "test-ledger"
	info := ledgerInfoWithID(ledger, 1)

	// processApply outer mocks: GetLedger (AnyTimes covers inner loadLedger
	// call too, but processApply returns early on error before PutBoundaries).
	mockStore.EXPECT().GetLedger(ledger).Return(info, nil).AnyTimes()
	mockStore.EXPECT().GetBoundaries(ledger).Return((&raftcmdpb.LedgerBoundaries{NextLogId: 1}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()

	// processAddAccountType: loadBoundaries (inner) returns populated ledger.
	// Note: processApply calls loadBoundaries first (using the outer mock above),
	// then processAddAccountType calls loadBoundaries again internally.
	// The second GetBoundaries call comes from inside processAddAccountType.
	populatedBoundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5}
	mockStore.EXPECT().GetBoundaries(ledger).Return(populatedBoundaries.AsReader(), nil)

	// processApply does NOT call PutBoundaries when processAddAccountType
	// returns an error (it short-circuits before the PutBoundaries call).

	order := addAccountTypeOrderWithDefaults(ledger, "user", "users:{id}", map[string]*commonpb.MetadataValue{
		"tier": commonpb.NewStringValue("standard"),
	})

	result, descErr := processor.ProcessOrder(order, mockStore)
	require.Nil(t, result)
	require.NotNil(t, descErr)

	// Must be exactly ErrDefaultMetadataOnPopulatedLedger.
	var target *domain.ErrDefaultMetadataOnPopulatedLedger
	require.True(t, errors.As(descErr.(error), &target),
		"expected ErrDefaultMetadataOnPopulatedLedger, got %T: %v", descErr, descErr)
	require.Equal(t, ledger, target.Ledger)
	require.Equal(t, "user", target.TypeName)
}

// TestAddAccountType_WithDefaultMetadata_FreshLedger verifies that adding an
// account type with default_metadata to a fresh ledger (NextTransactionId == 1)
// succeeds and returns the AddedAccountType log.
func TestAddAccountType_WithDefaultMetadata_FreshLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	const ledger = "test-ledger"
	info := ledgerInfoWithID(ledger, 1)

	// processApply outer mocks.
	mockStore.EXPECT().GetLedger(ledger).Return(info, nil).AnyTimes()
	mockStore.EXPECT().GetBoundaries(ledger).Return((&raftcmdpb.LedgerBoundaries{NextLogId: 1}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	mockStore.EXPECT().PutBoundaries(ledger, gomock.Any())

	// processAddAccountType inner: loadBoundaries for the default_metadata guard.
	// Fresh ledger: NextTransactionId == 1.
	freshBoundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1}
	mockStore.EXPECT().GetBoundaries(ledger).Return(freshBoundaries.AsReader(), nil)

	// processAddAccountType updates ledger info.
	mockStore.EXPECT().PutLedger(ledger, gomock.Any())

	order := addAccountTypeOrderWithDefaults(ledger, "user", "users:{id}", map[string]*commonpb.MetadataValue{
		"tier": commonpb.NewStringValue("standard"),
	})

	result, descErr := processor.ProcessOrder(order, mockStore)
	require.Nil(t, descErr)
	require.NotNil(t, result)

	added := result.GetApply().GetLog().GetData().GetAddedAccountType()
	require.NotNil(t, added)
	require.Equal(t, "user", added.GetAccountType().GetName())
	require.Equal(t, "users:{id}", added.GetAccountType().GetPattern())
	require.Equal(t, commonpb.NewStringValue("standard"), added.GetAccountType().GetDefaultMetadata()["tier"])
}

// TestAddAccountType_WithoutDefaultMetadata_PopulatedLedger verifies that the
// guard is NOT triggered when the account type has no default_metadata, even
// when the ledger is populated. The inner GetBoundaries call inside
// processAddAccountType must NOT happen — only the outer processApply call does.
func TestAddAccountType_WithoutDefaultMetadata_PopulatedLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	const ledger = "test-ledger"
	info := ledgerInfoWithID(ledger, 1)

	// processApply outer mocks — exactly one GetBoundaries call (from processApply).
	// If processAddAccountType calls GetBoundaries a second time the strict mock fails.
	mockStore.EXPECT().GetLedger(ledger).Return(info, nil).AnyTimes()
	mockStore.EXPECT().GetBoundaries(ledger).Return((&raftcmdpb.LedgerBoundaries{NextLogId: 1}).AsReader(), nil).Times(1)
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	mockStore.EXPECT().PutBoundaries(ledger, gomock.Any())
	mockStore.EXPECT().PutLedger(ledger, gomock.Any())

	// No DefaultMetadata — inner guard path is not taken.
	order := addAccountTypeOrderWithDefaults(ledger, "user", "users:{id}", nil)

	result, descErr := processor.ProcessOrder(order, mockStore)
	require.Nil(t, descErr)
	require.NotNil(t, result)

	added := result.GetApply().GetLog().GetData().GetAddedAccountType()
	require.NotNil(t, added)
	require.Equal(t, "user", added.GetAccountType().GetName())
}
