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

	mockStore := NewMockScope(ctrl)
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

	mockStore := NewMockScope(ctrl)
	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	volumeKey := domain.NewVolumeKey("test", "bank", "USD")

	// Input=1000, Output=300, Balance=700
	expectGetVolume(mockStore, volumeKey, (&raftcmdpb.VolumePair{
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

	mockStore := NewMockScope(ctrl)
	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	volumeKey := domain.NewVolumeKey("test", "bank", "USD")

	// Volume exists but has no input values (not preloaded)
	expectGetVolume(mockStore, volumeKey, (&raftcmdpb.VolumePair{}).AsReader(), nil)

	query := numscriptlib.BalanceQuery{
		"bank": {"USD"},
	}

	_, err := adapter.GetBalances(context.Background(), query)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not preloaded")
}

// TestGetBalances_VolumeNotFound_TreatedAsZero pins the EN-1378 contract:
// a declared-but-absent volume key (Scope.GetVolume → domain.ErrNotFound)
// is treated as a fresh zero balance by the numscript balance adapter, not
// as an admission failure. The coverage gate (one layer up) is what catches
// "admission forgot to declare"; ErrNotFound is the legitimate signal once
// admission has stopped injecting zero-VolumePair AttributeValue plans.
func TestGetBalances_VolumeNotFound_TreatedAsZero(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	volumeKey := domain.NewVolumeKey("test", "bank", "USD")

	expectGetVolume(mockStore, volumeKey, nil, domain.ErrNotFound)

	query := numscriptlib.BalanceQuery{
		"bank": {"USD"},
	}

	balances, err := adapter.GetBalances(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, balances["bank"])
	require.Equal(t, "0", balances["bank"]["USD"].String())
}

func TestGetAccountsMetadata_Basic(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:001"},
		Key:        "status",
	}

	expectGetAccountMetadata(mockStore, metaKey, commonpb.NewStringValue("active"), nil)

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

	mockStore := NewMockScope(ctrl)
	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:001"},
		Key:        "status",
	}

	expectGetAccountMetadata(mockStore, metaKey, nil, domain.ErrNotFound)

	query := numscriptlib.MetadataQuery{
		"users:001": {"status"},
	}

	result, err := adapter.GetAccountsMetadata(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, result)
	// Key not found, should have empty metadata
	require.Empty(t, result["users:001"])
}

// TestGetAccountsMetadata_PreservesVerbatimAcrossDeclaredType pins that
// Numscript sees the raw client bytes regardless of the field's declared
// type. Coercing the value for the script (e.g. STRING "030" under a
// UINT64 declaration → "30") would let a retype silently change
// transaction outcomes, breaking the lossless contract.
func TestGetAccountsMetadata_PreservesVerbatimAcrossDeclaredType(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:001"},
		Key:        "age",
	}

	// "030" stored verbatim; even if a declared UINT64 type existed on
	// this field, the adapter must not project it through commonpb's
	// converter — Numscript must observe "030".
	expectGetAccountMetadata(mockStore, metaKey, commonpb.NewStringValue("030"), nil)

	query := numscriptlib.MetadataQuery{
		"users:001": {"age"},
	}

	result, err := adapter.GetAccountsMetadata(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "030", result["users:001"]["age"],
		"declared_type must not influence the value Numscript sees")
}

func TestGetAccountsMetadata_NoSchemaLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	adapter := &numscriptStoreAdapter{
		store:      mockStore,
		ledgerName: "test",
		force:      false,
	}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:001"},
		Key:        "age",
	}

	expectGetAccountMetadata(mockStore, metaKey, commonpb.NewStringValue("25"), nil)

	query := numscriptlib.MetadataQuery{
		"users:001": {"age"},
	}

	result, err := adapter.GetAccountsMetadata(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "25", result["users:001"]["age"])
}
