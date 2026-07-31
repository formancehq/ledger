package bootstrap

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	appbalancehistory "github.com/formancehq/ledger/v3/internal/application/balancehistory"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	historydomain "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestBalanceHistoryVolumeViewProviderFailsClosedWhenDisabled(t *testing.T) {
	t.Parallel()

	provider := newBalanceHistoryVolumeViewProvider(nil, nil, Config{
		BalanceHistoryConfig: BalanceHistoryConfig{Enabled: false},
	})
	_, err := provider.Open(context.Background(), "ledger", 1, ctrl.PointInTimeSelector{
		At:   1,
		Axis: balancehistorystore.AxisEffective,
	}, 0)

	var missing *balancehistorystore.ErrSourceMissing
	require.Error(t, err)
	require.True(t, errors.As(err, &missing))
	require.Contains(t, missing.Detail, "not enabled by configuration")
}

func TestBalanceHistoryVolumeViewProviderGatesBeforeOpeningStore(t *testing.T) {
	t.Parallel()

	provider := newBalanceHistoryVolumeViewProvider(nil, nil, Config{
		BalanceHistoryConfig: BalanceHistoryConfig{
			Enabled: true,
			Ledgers: []string{"canary"},
		},
	})
	selector := ctrl.PointInTimeSelector{At: 1, Axis: balancehistorystore.AxisEffective}

	_, err := provider.Open(context.Background(), "not-canary", 1, selector, 0)
	var missing *balancehistorystore.ErrSourceMissing
	require.ErrorAs(t, err, &missing)
	require.Contains(t, missing.Detail, `ledger "not-canary"`)

	_, err = provider.Open(context.Background(), "canary", 1, selector, 0)
	var building *balancehistorystore.ErrBuilding
	require.ErrorAs(t, err, &building)
}

func TestBalanceHistoryVolumeViewProviderRejectsPersistedReadyStoreUntilBuilderReady(t *testing.T) {
	t.Parallel()

	store := newBalanceHistoryProviderTestStore(t)
	publishBalanceHistoryProviderTestEffects(t, store)
	local := ctrl.NewLocalVolumeViewProvider(store)
	selector := ctrl.PointInTimeSelector{At: 10, Axis: balancehistorystore.AxisEffective}

	view, err := local.Open(context.Background(), "ledger", 7, selector, 1)
	require.NoError(t, err, "the persisted store itself is READY")
	require.NoError(t, view.Close())

	builder := appbalancehistory.NewBuilder(
		nil,
		store,
		nil,
		nil,
		"provider-not-ready",
		logging.NopZap(),
		nil,
		1,
		2,
		time.Millisecond,
		time.Millisecond,
	)
	provider := newBalanceHistoryVolumeViewProvider(store, builder, Config{
		BalanceHistoryConfig: BalanceHistoryConfig{Enabled: true},
	})

	_, err = provider.Open(context.Background(), "ledger", 7, selector, 1)
	var building *balancehistorystore.ErrBuilding
	require.ErrorAs(t, err, &building)
}

func TestBalanceHistoryVolumeViewProviderEmptyAllowlistAllowsEveryLedger(t *testing.T) {
	t.Parallel()

	store := newBalanceHistoryProviderTestStore(t)
	publishBalanceHistoryProviderTestEffects(t, store)
	provider := newBalanceHistoryVolumeViewProvider(store, newReadyBalanceHistoryProviderTestBuilder(t), Config{
		BalanceHistoryConfig: BalanceHistoryConfig{Enabled: true},
	})

	view, err := provider.Open(context.Background(), "any-ledger", 7, ctrl.PointInTimeSelector{
		At:   10,
		Axis: balancehistorystore.AxisEffective,
	}, 1)
	require.NoError(t, err)
	require.NoError(t, view.Close())
}

func TestBalanceHistoryVolumeViewProviderSameNameRecreationUsesLedgerIDIsolation(t *testing.T) {
	t.Parallel()

	store := newBalanceHistoryProviderTestStore(t)
	publishBalanceHistoryProviderTestEffects(t, store)
	provider := newBalanceHistoryVolumeViewProvider(store, newReadyBalanceHistoryProviderTestBuilder(t), Config{
		BalanceHistoryConfig: BalanceHistoryConfig{
			Enabled: true,
			Ledgers: []string{"canary"},
		},
	})
	selector := ctrl.PointInTimeSelector{At: 10, Axis: balancehistorystore.AxisEffective}

	for _, test := range []struct {
		ledgerID uint32
		input    string
	}{
		{ledgerID: 7, input: "5"},
		{ledgerID: 8, input: "9"},
	} {
		view, err := provider.Open(context.Background(), "canary", test.ledgerID, selector, 1)
		require.NoError(t, err)

		result, err := view.Aggregate(context.Background(), nil, query.AggregateOptions{})
		require.NoError(t, err)
		require.Len(t, result.GetVolumes(), 1)
		require.Equal(t, test.input, result.GetVolumes()[0].GetInput().ToBigInt().String())
		require.NoError(t, view.Close())
	}
}

func newReadyBalanceHistoryProviderTestBuilder(t *testing.T) *appbalancehistory.Builder {
	t.Helper()

	root := t.TempDir()
	logger := logging.NopZap()
	meterProvider := metric.NewMeterProvider()
	primary, err := dal.NewStore(
		filepath.Join(root, "primary"),
		logger,
		meterProvider.Meter("test.primary"),
		dal.DefaultConfig(),
	)
	require.NoError(t, err)
	store, err := balancehistorystore.New(
		filepath.Join(root, "history"),
		logger,
		balancehistorystore.DefaultConfig(),
	)
	require.NoError(t, err)
	_, err = store.Publish(balancehistorystore.Publication{
		Coverage: balancehistorystore.Coverage{SourceComplete: true},
	})
	require.NoError(t, err)

	builder := appbalancehistory.NewBuilder(
		appbalancehistory.NewHotSource(primary),
		store,
		nil,
		nil,
		"provider-ready",
		logger,
		meterProvider.Meter("test.builder"),
		1,
		2,
		time.Millisecond,
		time.Millisecond,
	)
	builder.Start()
	require.Eventually(t, builder.Ready, time.Second, time.Millisecond)
	t.Cleanup(func() {
		require.NoError(t, builder.Stop())
		require.NoError(t, store.Close())
		require.NoError(t, primary.Close())
		require.NoError(t, meterProvider.Shutdown(context.Background()))
	})

	return builder
}

func newBalanceHistoryProviderTestStore(t *testing.T) *balancehistorystore.Store {
	t.Helper()

	store, err := balancehistorystore.New(t.TempDir(), logging.NopZap(), balancehistorystore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	return store
}

func publishBalanceHistoryProviderTestEffects(t *testing.T, store *balancehistorystore.Store) {
	t.Helper()

	_, err := store.Publish(balancehistorystore.Publication{
		Effects: []historydomain.Effect{
			{
				LedgerID: 7, AuditSequence: 1, LogSequence: 1,
				EffectiveAt: 10, InsertedAt: 20, Account: "account", AssetBase: "USD",
				Input: historydomain.AmountFromUint64(5),
			},
			{
				LedgerID: 8, AuditSequence: 1, LogSequence: 1,
				EffectiveAt: 10, InsertedAt: 20, Account: "account", AssetBase: "USD",
				Input: historydomain.AmountFromUint64(9),
			},
		},
		Coverage: balancehistorystore.Coverage{
			AuditSequence:  1,
			LogSequence:    1,
			SourceComplete: true,
		},
	})
	require.NoError(t, err)
}
