package query_test

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	historydomain "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

func newHistoryView(t *testing.T, effects []historydomain.Effect) *balancehistorystore.View {
	t.Helper()

	store, err := balancehistorystore.New(t.TempDir(), logging.NopZap(), balancehistorystore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	_, err = store.Publish(balancehistorystore.Publication{
		Effects: effects,
		Coverage: balancehistorystore.Coverage{
			AuditSequence:  1,
			LogSequence:    1,
			SourceComplete: true,
		},
	})
	require.NoError(t, err)

	view, err := store.OpenView(1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })

	return view
}

func historyInput(account, base, color string, precision uint8, value uint64) historydomain.Effect {
	return historydomain.Effect{
		LedgerName: "default", AuditSequence: 1, LogSequence: 1,
		EffectiveAt: 10, InsertedAt: 20,
		Account: account, AssetBase: base, AssetPrecision: precision, Color: color,
		Input: historydomain.AmountFromUint64(value),
	}
}

func TestAggregateHistoricalVolumesReusesLiveSemantics(t *testing.T) {
	t.Parallel()

	view := newHistoryView(t, []historydomain.Effect{
		historyInput("users:a", "USD", "RED", 2, 100),
		historyInput("users:b", "USD", "BLUE", 2, 200),
		historyInput("users:c", "USD", "RED", 3, 1000),
	})

	filtered, err := query.AggregateHistoricalVolumesSelected(
		context.Background(), view, "default", balancehistorystore.TemporalityEffective, 10,
		[]string{"users:a"}, nil, nil, query.AggregateOptions{},
	)
	require.NoError(t, err)
	require.Len(t, filtered.GetVolumes(), 1)
	require.Equal(t, "USD/2", filtered.GetVolumes()[0].GetAsset())
	require.Equal(t, "RED", filtered.GetVolumes()[0].GetColor())
	require.Equal(t, "100", filtered.GetVolumes()[0].GetInput().ToBigInt().String())

	grouped, err := query.AggregateHistoricalVolumesSelected(
		context.Background(), view, "default", balancehistorystore.TemporalityEffective, 10, nil, nil, nil,
		query.AggregateOptions{GroupByPrefixes: []string{"users:"}, UseMaxPrecision: true, CollapseColors: true},
	)
	require.NoError(t, err)
	require.Len(t, grouped.GetGroups(), 1)
	require.Len(t, grouped.GetGroups()[0].GetVolumes(), 1)
	require.Equal(t, "USD/3", grouped.GetGroups()[0].GetVolumes()[0].GetAsset())
	require.Empty(t, grouped.GetGroups()[0].GetVolumes()[0].GetColor())
	// 100@/2 + 200@/2 rescale to 3000@/3, plus 1000@/3.
	require.Equal(t, "4000", grouped.GetGroups()[0].GetVolumes()[0].GetInput().ToBigInt().String())
}

func TestAggregateHistoricalVolumesSurfacesUint256Overflow(t *testing.T) {
	t.Parallel()

	maximum := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	var amount historydomain.Amount
	maximum.FillBytes(amount[:])

	view := newHistoryView(t, []historydomain.Effect{
		{
			LedgerName: "default", AuditSequence: 1, LogSequence: 1, EffectiveAt: 10, InsertedAt: 10,
			Account: "a", AssetBase: "USD", Input: amount,
		},
		{
			LedgerName: "default", AuditSequence: 1, LogSequence: 1, EffectiveAt: 10, InsertedAt: 10,
			Account: "b", AssetBase: "USD", Input: historydomain.AmountFromUint64(1),
		},
	})

	_, err := query.AggregateHistoricalVolumesSelected(
		context.Background(), view, "default", balancehistorystore.TemporalityEffective, 10,
		nil, nil, nil, query.AggregateOptions{},
	)
	var overflow *query.ErrAggregateOverflow
	require.True(t, errors.As(err, &overflow))
	require.Equal(t, "accumulate", overflow.Stage)
	require.Equal(t, "input", overflow.Side)
}

func TestAggregateHistoricalVolumesByPrefixFiltersHistoricalAccountsAndPreservesOptions(t *testing.T) {
	t.Parallel()

	view := newHistoryView(t, []historydomain.Effect{
		historyInput("users:archived-a", "USD", "RED", 2, 100),
		historyInput("users:archived-b", "USD", "BLUE", 3, 2000),
		historyInput("merchants:current", "USD", "RED", 2, 900),
	})

	result, err := query.AggregateHistoricalVolumesSelected(
		context.Background(), view, "default", balancehistorystore.TemporalityEffective, 10,
		nil, []string{"users:"}, nil,
		query.AggregateOptions{
			GroupByPrefixes: []string{"users:"},
			UseMaxPrecision: true,
			CollapseColors:  true,
		},
	)
	require.NoError(t, err)
	require.Len(t, result.GetGroups(), 1)
	require.Len(t, result.GetGroups()[0].GetVolumes(), 1)
	require.Equal(t, "USD/3", result.GetGroups()[0].GetVolumes()[0].GetAsset())
	require.Empty(t, result.GetGroups()[0].GetVolumes()[0].GetColor())
	// 100@/2 rescales to 1000@/3, then adds 2000@/3. The merchant
	// row is excluded even though it exists in the same historical view.
	require.Equal(t, "3000", result.GetGroups()[0].GetVolumes()[0].GetInput().ToBigInt().String())
}

func TestAggregateHistoricalVolumesMatchingSupportsMixedSelection(t *testing.T) {
	t.Parallel()

	view := newHistoryView(t, []historydomain.Effect{
		historyInput("archived:gone", "USD", "", 0, 2),
		historyInput("current:metadata-match", "USD", "", 0, 3),
		historyInput("current:unmatched", "USD", "", 0, 5),
	})

	result, err := query.AggregateHistoricalVolumesSelected(
		context.Background(), view, "default", balancehistorystore.TemporalityEffective, 10,
		[]string{"current:metadata-match", "archived:gone"}, []string{"archived:"},
		func(account string) bool {
			return account == "archived:gone" || account == "current:metadata-match"
		},
		query.AggregateOptions{},
	)
	require.NoError(t, err)
	require.Len(t, result.GetVolumes(), 1)
	require.Equal(t, "5", result.GetVolumes()[0].GetInput().ToBigInt().String())
}

func TestAggregateHistoricalVolumesMatchingHonorsCancellation(t *testing.T) {
	t.Parallel()

	view := newHistoryView(t, []historydomain.Effect{historyInput("a", "USD", "", 0, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := query.AggregateHistoricalVolumesSelected(
		ctx, view, "default", balancehistorystore.TemporalityEffective, 10,
		nil, nil, func(string) bool { return true }, query.AggregateOptions{},
	)
	require.ErrorIs(t, err, context.Canceled)
}
