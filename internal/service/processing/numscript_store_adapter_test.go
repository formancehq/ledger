package processing

import (
	"context"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	numscriptlib "github.com/formancehq/numscript"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetBalances_ForceMode(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:    mockStore,
		ledgerID: 1,
		force:    true,
	}

	query := numscriptlib.BalanceQuery{
		"bank": {"USD", "EUR"},
	}

	balances, err := adapter.GetBalances(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, balances)

	// In force mode, all balances should be MaxForceBalance
	require.NotNil(t, balances["bank"]["USD"])
	require.NotNil(t, balances["bank"]["EUR"])
	require.True(t, balances["bank"]["USD"].Sign() > 0)
}

func TestGetBalances_PreloadedVolumes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:    mockStore,
		ledgerID: 1,
		force:    false,
	}

	volumeKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "bank"},
		Asset:      "USD",
	}

	// Input=1000, Output=300, Balance=700
	mockStore.EXPECT().GetVolume(volumeKey).Return(&raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(1000),
		OutputKnown: commonpb.NewUint256FromUint64(300),
	}, nil)

	query := numscriptlib.BalanceQuery{
		"bank": {"USD"},
	}

	balances, err := adapter.GetBalances(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, balances)
	require.Equal(t, int64(700), balances["bank"]["USD"].Int64())
}

func TestGetBalances_DiffOnlyVolume(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:    mockStore,
		ledgerID: 1,
		force:    false,
	}

	volumeKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "bank"},
		Asset:      "USD",
	}

	// Only diff values present (no Known)
	mockStore.EXPECT().GetVolume(volumeKey).Return(&raftcmdpb.VolumePair{
		InputDiff:  commonpb.NewUint256FromUint64(500),
		OutputDiff: commonpb.NewUint256FromUint64(100),
	}, nil)

	query := numscriptlib.BalanceQuery{
		"bank": {"USD"},
	}

	balances, err := adapter.GetBalances(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, balances)
	require.Equal(t, int64(400), balances["bank"]["USD"].Int64())
}

func TestGetBalances_NotPreloaded(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:    mockStore,
		ledgerID: 1,
		force:    false,
	}

	volumeKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "bank"},
		Asset:      "USD",
	}

	// Volume exists but has no input values (not preloaded)
	mockStore.EXPECT().GetVolume(volumeKey).Return(&raftcmdpb.VolumePair{}, nil)

	query := numscriptlib.BalanceQuery{
		"bank": {"USD"},
	}

	_, err := adapter.GetBalances(context.Background(), query)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not preloaded")
}

func TestGetBalances_VolumeNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:    mockStore,
		ledgerID: 1,
		force:    false,
	}

	volumeKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "bank"},
		Asset:      "USD",
	}

	// Volume not found at all
	mockStore.EXPECT().GetVolume(volumeKey).Return(nil, dal.ErrNotFound)

	query := numscriptlib.BalanceQuery{
		"bank": {"USD"},
	}

	_, err := adapter.GetBalances(context.Background(), query)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not preloaded")
}

func TestGetAccountsMetadata_Basic(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:    mockStore,
		ledgerID: 1,
		force:    false,
	}

	metaKey := dal.MetadataKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:001"},
		Key:        "status",
	}

	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(commonpb.NewStringValue("active"), nil)

	query := numscriptlib.MetadataQuery{
		"users:001": {"status"},
	}

	result, err := adapter.GetAccountsMetadata(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "active", result["users:001"]["status"])
}

func TestGetAccountsMetadata_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:    mockStore,
		ledgerID: 1,
		force:    false,
	}

	metaKey := dal.MetadataKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:001"},
		Key:        "status",
	}

	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(nil, dal.ErrNotFound)

	query := numscriptlib.MetadataQuery{
		"users:001": {"status"},
	}

	result, err := adapter.GetAccountsMetadata(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, result)
	// Key not found, should have empty metadata
	require.Empty(t, result["users:001"])
}

func TestGetAccountsMetadata_WithSchemaConversion(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerID:   1,
		force:      false,
		ledgerName: "test-ledger",
	}

	metaKey := dal.MetadataKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:001"},
		Key:        "age",
	}

	// Return a string value that should be converted to int64 per schema
	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(commonpb.NewStringValue("25"), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{
		Name: "test-ledger",
		Id:   1,
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"age": {
					Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE,
				},
			},
		},
	}, true)
	// Schema conversion writes back the converted value
	mockStore.EXPECT().PutAccountMetadata(metaKey, gomock.Any())

	query := numscriptlib.MetadataQuery{
		"users:001": {"age"},
	}

	result, err := adapter.GetAccountsMetadata(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result["users:001"]["age"])
}

func TestGetAccountsMetadata_NoSchemaLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerID:   1,
		force:      false,
		ledgerName: "test-ledger",
	}

	metaKey := dal.MetadataKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:001"},
		Key:        "age",
	}

	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(commonpb.NewStringValue("25"), nil)
	// Ledger not found -- no schema conversion
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)

	query := numscriptlib.MetadataQuery{
		"users:001": {"age"},
	}

	result, err := adapter.GetAccountsMetadata(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "25", result["users:001"]["age"])
}
