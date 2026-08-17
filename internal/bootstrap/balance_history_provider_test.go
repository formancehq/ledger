package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

func TestBalanceHistoryVolumeViewProviderGatesBeforeOpeningStore(t *testing.T) {
	t.Parallel()

	provider := newBalanceHistoryVolumeViewProvider(nil, nil)
	_, err := provider.Open(context.Background(), "ledger", ctrl.HistoricalBalanceSelector{
		At:          1,
		Temporality: balancehistorystore.TemporalityEffective,
	}, 0)

	var building *balancehistorystore.ErrBuilding
	require.ErrorAs(t, err, &building)
}

func TestLocalProviderUsesAuditDerivedLedgerConfiguration(t *testing.T) {
	t.Parallel()

	store := newBalanceHistoryProviderTestStore(t)
	require.NoError(t, store.ResetForConfiguration([]string{"enabled"}))
	_, err := store.Publish(balancehistorystore.Publication{
		Coverage: balancehistorystore.Coverage{SourceComplete: true},
	})
	require.NoError(t, err)
	require.NoError(t, store.CompleteRebuild(0, 0))

	local := ctrl.NewLocalVolumeViewProvider(store)
	selector := ctrl.HistoricalBalanceSelector{At: 1, Temporality: balancehistorystore.TemporalityEffective}
	view, err := local.Open(context.Background(), "enabled", selector, 0)
	require.NoError(t, err)
	require.NoError(t, view.Close())

	_, err = local.Open(context.Background(), "disabled", selector, 0)
	var missing *balancehistorystore.ErrSourceMissing
	require.True(t, errors.As(err, &missing))
	require.Contains(t, missing.Detail, `ledger "disabled"`)
}

func TestBalanceHistoryStatusDistinguishesDisabledBuildingAndReady(t *testing.T) {
	t.Parallel()

	store := newBalanceHistoryProviderTestStore(t)
	provider := newBalanceHistoryVolumeViewProvider(store, nil)

	status, err := provider.Status(context.Background(), "disabled")
	require.NoError(t, err)
	require.Equal(t, servicepb.GetHistoricalBalancesStatusResponse_STATE_DISABLED, status.GetState())

	require.NoError(t, store.ResetForConfiguration([]string{"enabled"}))
	status, err = provider.Status(context.Background(), "enabled")
	require.NoError(t, err)
	require.Equal(t, servicepb.GetHistoricalBalancesStatusResponse_STATE_BUILDING, status.GetState())

	_, err = store.Publish(balancehistorystore.Publication{Coverage: balancehistorystore.Coverage{SourceComplete: true}})
	require.NoError(t, err)
	require.NoError(t, store.CompleteRebuild(0, 0))
	local := ctrl.NewLocalVolumeViewProvider(store)
	status, err = local.Status(context.Background(), "enabled")
	require.NoError(t, err)
	require.Equal(t, servicepb.GetHistoricalBalancesStatusResponse_STATE_READY, status.GetState())

	require.NoError(t, store.MarkSourceMissing("archived source unavailable"))
	status, err = provider.Status(context.Background(), "enabled")
	require.NoError(t, err)
	require.Equal(t, servicepb.GetHistoricalBalancesStatusResponse_STATE_ERROR, status.GetState())
	require.Contains(t, status.GetError(), "archived source unavailable")

	require.NoError(t, store.ResetForSourceRepair())
	status, err = provider.Status(context.Background(), "enabled")
	require.NoError(t, err)
	require.Equal(t, servicepb.GetHistoricalBalancesStatusResponse_STATE_ERROR, status.GetState())
	require.Contains(t, status.GetError(), "archived source unavailable")
}

func newBalanceHistoryProviderTestStore(t *testing.T) *balancehistorystore.Store {
	t.Helper()

	store, err := balancehistorystore.New(t.TempDir(), logging.NopZap(), balancehistorystore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	return store
}
