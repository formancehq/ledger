package processing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// rowFor returns the (account, asset) balance row from a Balances slice.
func rowFor(t *testing.T, balances numscriptlib.Balances, account, asset string) numscriptlib.BalanceRow {
	t.Helper()
	for _, row := range balances {
		if row.Account == account && row.Asset == asset {
			return row
		}
	}
	t.Fatalf("no balance row for %s/%s", account, asset)

	return numscriptlib.BalanceRow{}
}

func metaValue(metas numscriptlib.AccountsMetadata, account, key string) (string, bool) {
	for _, row := range metas {
		if row.Account == account && row.Key == key {
			return row.Value, true
		}
	}

	return "", false
}

func newScopeStore(mockStore *MockScope, force bool) *numscript.Store {
	return numscript.NewStore(&scopeValueSource{store: mockStore, ledgerName: "test"}, force)
}

func TestGetBalances_ForceMode(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	store := newScopeStore(mockStore, true)

	query := numscriptlib.BalanceQuery{
		{Account: "bank", Asset: "USD"},
		{Account: "bank", Asset: "EUR"},
	}

	balances, err := store.GetBalances(context.Background(), query)
	require.NoError(t, err)

	// In force mode, all balances should be MaxForceBalance (no store reads).
	require.Positive(t, rowFor(t, balances, "bank", "USD").Amount.Sign())
	require.Positive(t, rowFor(t, balances, "bank", "EUR").Amount.Sign())
}

func TestGetBalances_PreloadedVolumes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	store := newScopeStore(mockStore, false)

	volumeKey := domain.NewVolumeKey("test", "bank", "USD")

	// Input=1000, Output=300, Balance=700
	expectGetVolume(mockStore, volumeKey, (&raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(1000),
		Output: commonpb.NewUint256FromUint64(300),
	}).AsReader(), nil)

	query := numscriptlib.BalanceQuery{{Account: "bank", Asset: "USD"}}

	balances, err := store.GetBalances(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, int64(700), rowFor(t, balances, "bank", "USD").Amount.Int64())
}

func TestGetBalances_NotPreloaded(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	store := newScopeStore(mockStore, false)

	volumeKey := domain.NewVolumeKey("test", "bank", "USD")

	// Volume exists but has no input values (not preloaded)
	expectGetVolume(mockStore, volumeKey, (&raftcmdpb.VolumePair{}).AsReader(), nil)

	query := numscriptlib.BalanceQuery{{Account: "bank", Asset: "USD"}}

	_, err := store.GetBalances(context.Background(), query)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not preloaded")
}

// TestGetBalances_VolumeNotFound_TreatedAsZero pins the EN-1378 contract:
// a declared-but-absent volume key (Scope.GetVolume → domain.ErrNotFound)
// is treated as a fresh zero balance, not as an admission failure.
func TestGetBalances_VolumeNotFound_TreatedAsZero(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	store := newScopeStore(mockStore, false)

	volumeKey := domain.NewVolumeKey("test", "bank", "USD")

	expectGetVolume(mockStore, volumeKey, nil, domain.ErrNotFound)

	query := numscriptlib.BalanceQuery{{Account: "bank", Asset: "USD"}}

	balances, err := store.GetBalances(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, "0", rowFor(t, balances, "bank", "USD").Amount.String())
}

func TestGetAccountsMetadata_Basic(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	store := newScopeStore(mockStore, false)

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:001"},
		Key:        "status",
	}

	expectGetAccountMetadata(mockStore, metaKey, commonpb.NewStringValue("active"), nil)

	query := numscriptlib.MetadataQuery{{Account: "users:001", Keys: []string{"status"}}}

	result, err := store.GetAccountsMetadata(context.Background(), query)
	require.NoError(t, err)
	value, ok := metaValue(result, "users:001", "status")
	require.True(t, ok)
	require.Equal(t, "active", value)
}

func TestGetAccountsMetadata_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	store := newScopeStore(mockStore, false)

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:001"},
		Key:        "status",
	}

	expectGetAccountMetadata(mockStore, metaKey, nil, domain.ErrNotFound)

	query := numscriptlib.MetadataQuery{{Account: "users:001", Keys: []string{"status"}}}

	result, err := store.GetAccountsMetadata(context.Background(), query)
	require.NoError(t, err)
	_, ok := metaValue(result, "users:001", "status")
	require.False(t, ok, "absent metadata must not appear in the result")
}

// TestGetAccountsMetadata_PreservesVerbatimAcrossDeclaredType pins that
// Numscript sees the raw client bytes regardless of the field's declared type.
func TestGetAccountsMetadata_PreservesVerbatimAcrossDeclaredType(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	store := newScopeStore(mockStore, false)

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:001"},
		Key:        "age",
	}

	expectGetAccountMetadata(mockStore, metaKey, commonpb.NewStringValue("030"), nil)

	query := numscriptlib.MetadataQuery{{Account: "users:001", Keys: []string{"age"}}}

	result, err := store.GetAccountsMetadata(context.Background(), query)
	require.NoError(t, err)
	value, ok := metaValue(result, "users:001", "age")
	require.True(t, ok)
	require.Equal(t, "030", value, "declared_type must not influence the value Numscript sees")
}
