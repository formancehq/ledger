package processing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestGetBalances_ForceMode(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      true,
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
	require.Positive(t, balances["bank"]["USD"].Sign())
}

func TestGetBalances_PreloadedVolumes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	volumeKey := domain.NewVolumeKey("test", "bank", "USD")

	// Input=1000, Output=300, Balance=700
	mockStore.EXPECT().GetVolume(volumeKey).Return((&raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(1000),
		Output: commonpb.NewUint256FromUint64(300),
	}).AsReader(), nil)

	query := numscriptlib.BalanceQuery{
		"bank": {"USD"},
	}

	balances, err := adapter.GetBalances(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, balances)
	require.Equal(t, int64(700), balances["bank"]["USD"].Int64())
}

func TestGetBalances_NotPreloaded(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	volumeKey := domain.NewVolumeKey("test", "bank", "USD")

	// Volume exists but has no input values (not preloaded)
	mockStore.EXPECT().GetVolume(volumeKey).Return((&raftcmdpb.VolumePair{}).AsReader(), nil)

	query := numscriptlib.BalanceQuery{
		"bank": {"USD"},
	}

	_, err := adapter.GetBalances(context.Background(), query)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not preloaded")
}

func TestGetBalances_VolumeNotFound_ReturnsError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	volumeKey := domain.NewVolumeKey("test", "bank", "USD")

	// Volume not found — the adapter returns ErrBalanceNotPreloaded
	mockStore.EXPECT().GetVolume(volumeKey).Return(nil, domain.ErrNotFound)

	query := numscriptlib.BalanceQuery{
		"bank": {"USD"},
	}

	_, err := adapter.GetBalances(context.Background(), query)
	require.Error(t, err)

	var preloadErr *domain.ErrBalanceNotPreloaded
	require.ErrorAs(t, err, &preloadErr)
	require.Equal(t, "bank", preloadErr.Account)
	require.Equal(t, "USD", preloadErr.Asset)
}

func TestGetAccountsMetadata_Basic(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:001"},
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
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:001"},
		Key:        "status",
	}

	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(nil, domain.ErrNotFound)

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
		ledgerName: "test",
		force:      false,
		schema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"age": {
					Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE,
				},
			},
		},
	}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:001"},
		Key:        "age",
	}

	// Return a string value that should be converted to int64 per schema
	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(commonpb.NewStringValue("25"), nil)
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
		ledgerName: "test",
		force:      false,
	}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:001"},
		Key:        "age",
	}

	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(commonpb.NewStringValue("25"), nil)

	query := numscriptlib.MetadataQuery{
		"users:001": {"age"},
	}

	result, err := adapter.GetAccountsMetadata(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "25", result["users:001"]["age"])
}
