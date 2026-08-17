package ctrl

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	historydomain "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

func TestLocalVolumeViewProviderPinsManifestAndToken(t *testing.T) {
	t.Parallel()

	store, err := balancehistorystore.New(t.TempDir(), logging.NopZap(), balancehistorystore.DefaultConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	require.NoError(t, store.ResetForConfiguration([]string{"ledger"}))

	_, err = store.Publish(balancehistorystore.Publication{
		Effects: []historydomain.Effect{{
			LedgerName: "ledger", AuditSequence: 1, LogSequence: 1,
			EffectiveAt: 10, InsertedAt: 20, Account: "a", AssetBase: "USD",
			Input: historydomain.AmountFromUint64(5),
		}},
		Coverage: balancehistorystore.Coverage{
			AuditSequence: 1, LogSequence: 1, SourceComplete: true,
		},
	})
	require.NoError(t, err)
	require.NoError(t, store.CompleteRebuild(1, 1))

	provider := NewLocalVolumeViewProvider(store)
	view, err := provider.Open(context.Background(), "ledger", HistoricalBalanceSelector{At: 10, Temporality: balancehistorystore.TemporalityEffective}, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()

	token := view.Token()
	require.Equal(t, "ledger", token.Ledger)
	require.Equal(t, uint64(1), token.AuditWatermark)
	require.Equal(t, uint64(1), token.LogWatermark)
	require.NotEmpty(t, token.Token)

	result, err := view.Aggregate(context.Background(), nil, query.AggregateOptions{})
	require.NoError(t, err)
	require.Len(t, result.GetVolumes(), 1)
	require.Equal(t, "5", result.GetVolumes()[0].GetInput().ToBigInt().String())

	// Publishing a later correction does not mutate the already-open view.
	_, err = store.Publish(balancehistorystore.Publication{
		Effects: []historydomain.Effect{{
			LedgerName: "ledger", AuditSequence: 2, LogSequence: 2,
			EffectiveAt: 5, InsertedAt: 30, Account: "a", AssetBase: "USD",
			Input: historydomain.AmountFromUint64(2),
		}},
		Coverage: balancehistorystore.Coverage{
			AuditSequence: 2, LogSequence: 2, SourceComplete: true,
		},
	})
	require.NoError(t, err)

	result, err = view.Aggregate(context.Background(), nil, query.AggregateOptions{})
	require.NoError(t, err)
	require.Equal(t, "5", result.GetVolumes()[0].GetInput().ToBigInt().String())
}

func TestHistoricalVolumeViewAggregateHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	store, err := balancehistorystore.New(t.TempDir(), logging.NopZap(), balancehistorystore.DefaultConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	require.NoError(t, store.ResetForConfiguration([]string{"ledger"}))

	_, err = store.Publish(balancehistorystore.Publication{
		Effects: []historydomain.Effect{{
			LedgerName: "ledger", AuditSequence: 1, LogSequence: 1,
			EffectiveAt: 10, InsertedAt: 20, Account: "a", AssetBase: "USD",
			Input: historydomain.AmountFromUint64(5),
		}},
		Coverage: balancehistorystore.Coverage{
			AuditSequence: 1, LogSequence: 1, SourceComplete: true,
		},
	})
	require.NoError(t, err)
	require.NoError(t, store.CompleteRebuild(1, 1))

	view, err := NewLocalVolumeViewProvider(store).Open(
		context.Background(),
		"ledger",
		HistoricalBalanceSelector{At: 10, Temporality: balancehistorystore.TemporalityEffective},
		1,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = view.Aggregate(ctx, nil, query.AggregateOptions{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestLocalVolumeViewProviderFailsClosedWhenUnconfigured(t *testing.T) {
	t.Parallel()

	_, err := (*LocalVolumeViewProvider)(nil).Open(
		context.Background(), "ledger",
		HistoricalBalanceSelector{Temporality: balancehistorystore.TemporalityEffective},
		0,
	)
	var missing *balancehistorystore.ErrSourceMissing
	require.True(t, errors.As(err, &missing))
}

func hardcodedAddressPrefix(prefix string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: prefix},
	}}}
}

func hardcodedAddressExact(address string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: address},
	}}}
}

func currentMetadataFilter() *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field:     &commonpb.FieldRef{Metadata: "category"},
		Condition: &commonpb.FieldCondition_StringCond{StringCond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "vip"}}},
	}}}
}

func TestPrepareTemporalAccountSelectionUsesHistoricalAddressUniverse(t *testing.T) {
	t.Parallel()

	compileCalls := 0
	selection, err := prepareTemporalAccountSelection(
		context.Background(),
		hardcodedAddressPrefix("users:"),
		func(context.Context, *commonpb.QueryFilter) ([]string, error) {
			compileCalls++

			return nil, nil
		},
	)
	require.NoError(t, err)
	require.Zero(t, compileCalls, "a direct address filter must not depend on the current read store")
	require.Nil(t, selection.accounts)
	require.Equal(t, []string{"users:"}, selection.accountPrefixes)
	require.True(t, selection.match("users:historical-but-deleted"))
	require.False(t, selection.match("merchants:current"))
}

func TestPrepareTemporalAccountSelectionComposesHistoricalAddressAndCurrentMetadata(t *testing.T) {
	t.Parallel()

	metadata := currentMetadataFilter()
	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{
		hardcodedAddressPrefix("archived:"),
		metadata,
	}}}}

	selection, err := prepareTemporalAccountSelection(
		context.Background(), filter,
		func(_ context.Context, got *commonpb.QueryFilter) ([]string, error) {
			require.Same(t, metadata, got)

			return []string{"current:matched"}, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"current:matched"}, selection.accounts)
	require.Equal(t, []string{"archived:"}, selection.accountPrefixes)
	require.True(t, selection.match("archived:gone"), "the address arm includes an account absent from current state")
	require.True(t, selection.match("current:matched"), "the metadata arm uses its current-state match set")
	require.False(t, selection.match("current:unmatched"))
}

func TestPrepareTemporalAccountSelectionBoundsMixedAndByCurrentMatches(t *testing.T) {
	t.Parallel()

	metadata := currentMetadataFilter()
	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{
		hardcodedAddressPrefix("users:"),
		metadata,
	}}}}

	selection, err := prepareTemporalAccountSelection(
		context.Background(), filter,
		func(context.Context, *commonpb.QueryFilter) ([]string, error) {
			return []string{"other:matched", "users:matched", "users:matched"}, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"other:matched", "users:matched"}, selection.accounts)
	require.False(t, selection.match("other:matched"))
	require.True(t, selection.match("users:matched"))
	require.False(t, selection.match("users:historical-but-no-current-metadata"))
}

func TestPrepareTemporalAccountSelectionSupportsHistoricalExactAndNotAddress(t *testing.T) {
	t.Parallel()

	exact, err := prepareTemporalAccountSelection(
		context.Background(), hardcodedAddressExact("archived:one"),
		func(context.Context, *commonpb.QueryFilter) ([]string, error) {
			require.FailNow(t, "current compiler must not be called")

			return nil, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"archived:one"}, exact.accounts)
	require.True(t, exact.match("archived:one"))

	notFilter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{
		Filter: hardcodedAddressPrefix("excluded:"),
	}}}
	not, err := prepareTemporalAccountSelection(
		context.Background(), notFilter,
		func(context.Context, *commonpb.QueryFilter) ([]string, error) {
			require.FailNow(t, "current compiler must not be called")

			return nil, nil
		},
	)
	require.NoError(t, err)
	require.Nil(t, not.accounts)
	require.True(t, not.match("historical:gone"))
	require.False(t, not.match("excluded:gone"))
}

func TestPrepareTemporalAccountSelectionRejectsMixedNot(t *testing.T) {
	t.Parallel()

	mixed := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{
		hardcodedAddressPrefix("users:"),
		currentMetadataFilter(),
	}}}}
	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: mixed}}}

	_, err := prepareTemporalAccountSelection(
		context.Background(), filter,
		func(context.Context, *commonpb.QueryFilter) ([]string, error) { return nil, nil },
	)
	var unsupported *balancehistorystore.ErrUnsupportedTemporalFilter
	require.ErrorAs(t, err, &unsupported)
	require.Equal(t, "mixed-not", unsupported.Category)
}

func TestPrepareTemporalAccountSelectionEnforcesFilterDepthLimit(t *testing.T) {
	t.Parallel()

	filter := hardcodedAddressExact("a")
	for range domain.MaxFilterDepth {
		filter = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: filter}}}
	}

	_, err := prepareTemporalAccountSelection(
		context.Background(), filter,
		func(context.Context, *commonpb.QueryFilter) ([]string, error) { return nil, nil },
	)
	require.ErrorIs(t, err, domain.ErrFilterTooDeep)
}

func TestLocalVolumeViewProviderValidatesTemporalityAndClosedViews(t *testing.T) {
	t.Parallel()

	store, err := balancehistorystore.New(t.TempDir(), logging.NopZap(), balancehistorystore.DefaultConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	provider := NewLocalVolumeViewProvider(store)
	_, err = provider.Open(
		context.Background(),
		"ledger",
		HistoricalBalanceSelector{Temporality: balancehistorystore.Temporality(99)},
		0,
	)
	require.ErrorContains(t, err, "invalid historical-balance temporality")

	view := (*HistoricalVolumeView)(nil)
	_, err = view.Aggregate(context.Background(), nil, query.AggregateOptions{})
	require.ErrorContains(t, err, "historical volume view is closed")
	require.NoError(t, view.Close())

	closed := &HistoricalVolumeView{}
	_, err = closed.Aggregate(context.Background(), nil, query.AggregateOptions{})
	require.ErrorContains(t, err, "historical volume view is closed")
	require.NoError(t, closed.Close())
}

func TestBuildTemporalAccountFilterPlanRejectsUnsupportedShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filter   *commonpb.QueryFilter
		category string
	}{
		{name: "nil filter", category: "nil-subfilter"},
		{
			name: "nil address",
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{
				Address: nil,
			}},
			category: "invalid-address",
		},
		{
			name: "empty address match",
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{},
			}},
			category: "invalid-address",
		},
		{
			name: "parameterized exact",
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_ParamExact{ParamExact: "account"},
			}}},
			category: "parameterized-address",
		},
		{
			name: "parameterized prefix",
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_ParamPrefix{ParamPrefix: "account"},
			}}},
			category: "parameterized-address",
		},
		{
			name: "nil not",
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{
				Not: nil,
			}},
			category: "nil-subfilter",
		},
		{
			name:     "unknown condition",
			filter:   &commonpb.QueryFilter{},
			category: "account-condition",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := buildTemporalAccountFilterPlan(test.filter, 0)
			var unsupported *balancehistorystore.ErrUnsupportedTemporalFilter
			require.ErrorAs(t, err, &unsupported)
			require.Equal(t, test.category, unsupported.Category)
		})
	}
}

func TestPrepareTemporalAccountSelectionCurrentAndEmptyPlans(t *testing.T) {
	t.Parallel()

	compileErr := errors.New("compile failed")
	_, err := prepareTemporalAccountSelection(
		context.Background(),
		currentMetadataFilter(),
		func(context.Context, *commonpb.QueryFilter) ([]string, error) {
			return nil, compileErr
		},
	)
	require.ErrorIs(t, err, compileErr)

	selection, err := prepareTemporalAccountSelection(
		context.Background(),
		currentMetadataFilter(),
		func(context.Context, *commonpb.QueryFilter) ([]string, error) {
			return []string{"b", "a", "b"}, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, selection.accounts)
	require.Nil(t, selection.match)

	for _, filter := range []*commonpb.QueryFilter{
		{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{}}},
		{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{}}},
	} {
		empty, emptyErr := prepareTemporalAccountSelection(
			context.Background(),
			filter,
			func(context.Context, *commonpb.QueryFilter) ([]string, error) { return nil, nil },
		)
		require.NoError(t, emptyErr)
		require.Empty(t, empty.accounts)
		require.False(t, empty.match("anything"))
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = prepareTemporalAccountSelection(
		canceled,
		currentMetadataFilter(),
		func(context.Context, *commonpb.QueryFilter) ([]string, error) { return nil, nil },
	)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTemporalAccountCandidatesComposeBooleanPlans(t *testing.T) {
	t.Parallel()

	exactA, err := buildTemporalAccountFilterPlan(hardcodedAddressExact("a"), 0)
	require.NoError(t, err)
	exactB, err := buildTemporalAccountFilterPlan(hardcodedAddressExact("b"), 0)
	require.NoError(t, err)
	prefix, err := buildTemporalAccountFilterPlan(hardcodedAddressPrefix("users:"), 0)
	require.NoError(t, err)
	notPrefix := &temporalAccountFilterPlan{
		kind:     temporalFilterAddress,
		operator: temporalFilterNot,
		children: []*temporalAccountFilterPlan{prefix},
	}

	intersection := &temporalAccountFilterPlan{
		kind:     temporalFilterAddress,
		operator: temporalFilterAnd,
		children: []*temporalAccountFilterPlan{
			{kind: temporalFilterCurrent, operator: temporalFilterLeaf, currentAccounts: []string{"b", "a"}},
			{kind: temporalFilterCurrent, operator: temporalFilterLeaf, currentAccounts: []string{"b", "c"}},
		},
	}
	candidates, bounded := intersection.candidates()
	require.True(t, bounded)
	require.Equal(t, []string{"b"}, candidates.accounts)

	andWithPrefix := &temporalAccountFilterPlan{
		kind:     temporalFilterAddress,
		operator: temporalFilterAnd,
		children: []*temporalAccountFilterPlan{prefix, exactA},
	}
	candidates, bounded = andWithPrefix.candidates()
	require.True(t, bounded)
	require.Equal(t, []string{"a"}, candidates.accounts)
	require.Empty(t, candidates.prefixes)

	union := &temporalAccountFilterPlan{
		kind:     temporalFilterAddress,
		operator: temporalFilterOr,
		children: []*temporalAccountFilterPlan{exactB, exactA, exactA, prefix},
	}
	candidates, bounded = union.candidates()
	require.True(t, bounded)
	require.Equal(t, []string{"a", "b"}, candidates.accounts)
	require.Equal(t, []string{"users:"}, candidates.prefixes)

	unbounded := &temporalAccountFilterPlan{
		kind:     temporalFilterAddress,
		operator: temporalFilterOr,
		children: []*temporalAccountFilterPlan{exactA, notPrefix},
	}
	_, bounded = unbounded.candidates()
	require.False(t, bounded)

	require.False(t, (&temporalAccountFilterPlan{operator: temporalFilterOperator(99)}).matches("a"))
	_, bounded = (&temporalAccountFilterPlan{operator: temporalFilterOperator(99)}).candidates()
	require.False(t, bounded)
}

func TestAccountSetHelpersAreSortedAndIndependent(t *testing.T) {
	t.Parallel()

	input := []string{"b", "a", "b", "c"}
	require.Equal(t, []string{"a", "b", "c"}, deduplicateSortedAccounts(input))
	require.Equal(t, []string{"b", "a", "b", "c"}, input)
	require.Empty(t, deduplicateSortedAccounts(nil))
	require.Equal(
		t,
		[]string{"b", "d"},
		intersectSortedAccounts([]string{"d", "b", "a", "b"}, []string{"c", "d", "b"}),
	)
}
