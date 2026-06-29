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
// transactions and account metadata now SUCCEEDS (EN-1276 is no longer
// create-only). The universal existence marker protects pre-existing accounts
// from backfill, so attaching defaults at any time is safe; processAddAccountType
// no longer reads boundaries (no inner GetBoundaries call).
func TestAddAccountType_WithDefaultMetadata_PopulatedLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	const ledger = "test-ledger"
	info := ledgerInfoWithID(ledger, 1)

	// The create-only guard that read boundaries a second time is gone. Populated
	// boundaries are irrelevant to the outcome now — adding defaults succeeds regardless.
	expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, info.AsReader(), nil).AnyTimes()
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: ledger},
		(&raftcmdpb.LedgerBoundaries{NextLogId: 1, NextTransactionId: 5, MetadataCount: 3}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: ledger}, nil)
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: ledger}, nil)

	order := addAccountTypeOrderWithDefaults(ledger, "user", "users:{id}", map[string]*commonpb.MetadataValue{
		"tier": commonpb.NewStringValue("standard"),
	})

	result, descErr := processor.ProcessOrder(order, mockStore)
	require.Nil(t, descErr)
	require.NotNil(t, result)

	added := result.GetApply().GetLog().GetData().GetAddedAccountType()
	require.NotNil(t, added)
	require.Equal(t, "user", added.GetAccountType().GetName())
	require.Equal(t, commonpb.NewStringValue("standard"), added.GetAccountType().GetDefaultMetadata()["tier"])
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
	expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, info.AsReader(), nil).AnyTimes()
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: ledger}, (&raftcmdpb.LedgerBoundaries{NextLogId: 1}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: ledger}, nil)

	// processAddAccountType updates ledger info.
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: ledger}, nil)

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

// TestAddAccountType_DefaultMetadata_NullByteKeyRejected verifies that a
// default_metadata key containing a null byte is rejected by validateDefaultMetadata
// (which now delegates to domain.ValidateMetadataKey) before any boundaries read.
// Null bytes would corrupt the null-terminated Pebble canonical-key layout, and
// admission does not validate AddAccountType default_metadata, so this is the only gate.
func TestAddAccountType_DefaultMetadata_NullByteKeyRejected(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	const ledger = "test-ledger"
	info := ledgerInfoWithID(ledger, 1)

	// validateDefaultMetadata fails before the inner loadBoundaries, so only the
	// outer processApply GetBoundaries call fires and no Put* happens.
	expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, info.AsReader(), nil).AnyTimes()
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: ledger}, (&raftcmdpb.LedgerBoundaries{NextLogId: 1}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()

	order := addAccountTypeOrderWithDefaults(ledger, "user", "users:{id}", map[string]*commonpb.MetadataValue{
		"ti\x00er": commonpb.NewStringValue("standard"),
	})

	result, descErr := processor.ProcessOrder(order, mockStore)
	require.Nil(t, result)
	require.NotNil(t, descErr)
	require.True(t, errors.Is(descErr.(error), domain.ErrMetadataKeyContainsNullByte),
		"expected ErrMetadataKeyContainsNullByte, got %T: %v", descErr, descErr)
}

// TestAddAccountType_DefaultMetadata_NullByteValueRejected verifies that a
// default_metadata string value containing a null byte is rejected, wrapped in
// ErrMetadataKeyValidation so the offending key reaches operator logs.
func TestAddAccountType_DefaultMetadata_NullByteValueRejected(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	const ledger = "test-ledger"
	info := ledgerInfoWithID(ledger, 1)

	expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, info.AsReader(), nil).AnyTimes()
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: ledger}, (&raftcmdpb.LedgerBoundaries{NextLogId: 1}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()

	order := addAccountTypeOrderWithDefaults(ledger, "user", "users:{id}", map[string]*commonpb.MetadataValue{
		"tier": commonpb.NewStringValue("stand\x00ard"),
	})

	result, descErr := processor.ProcessOrder(order, mockStore)
	require.Nil(t, result)
	require.NotNil(t, descErr)

	var target *domain.ErrMetadataKeyValidation
	require.True(t, errors.As(descErr.(error), &target),
		"expected ErrMetadataKeyValidation, got %T: %v", descErr, descErr)
	require.Equal(t, "tier", target.Key)
	require.True(t, errors.Is(target.Cause.(error), domain.ErrMetadataValueContainsNullByte))
}

// TestAddAccountType_WithoutDefaultMetadata_PopulatedLedger verifies that adding
// an account type with no default_metadata to a populated ledger succeeds and
// reads boundaries only once (the outer processApply call) — processAddAccountType
// itself never reads boundaries.
func TestAddAccountType_WithoutDefaultMetadata_PopulatedLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	const ledger = "test-ledger"
	info := ledgerInfoWithID(ledger, 1)

	// processApply outer mocks — processAddAccountType itself no longer reads
	// boundaries (the create-only guard is gone), only processApply does.
	expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, info.AsReader(), nil).AnyTimes()
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: ledger}, (&raftcmdpb.LedgerBoundaries{NextLogId: 1}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: ledger}, nil)
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: ledger}, nil)

	// No DefaultMetadata — inner guard path is not taken.
	order := addAccountTypeOrderWithDefaults(ledger, "user", "users:{id}", nil)

	result, descErr := processor.ProcessOrder(order, mockStore)
	require.Nil(t, descErr)
	require.NotNil(t, result)

	added := result.GetApply().GetLog().GetData().GetAddedAccountType()
	require.NotNil(t, added)
	require.Equal(t, "user", added.GetAccountType().GetName())
}
