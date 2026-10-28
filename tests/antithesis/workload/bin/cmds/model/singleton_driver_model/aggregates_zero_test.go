package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestModelAggregateRetainsZeroPostingBucket(t *testing.T) {
	t.Parallel()

	// applyPosting writes both cells even for amount zero. The result stage
	// retains those buckets; UseMaxPrecision does not change their presence.
	ls := buildLedger(t,
		oracletest.TxReqColoredL("L", "world", "a:1", "COIN", "", 233465405),
		oracletest.TxReqColoredL("L", "world", "a:1", "COIN", "A", 347155),
		oracletest.TxReqColoredL("L", "world", "a:1", "EUR/2", "A", 749939022715868),
		oracletest.TxReqColoredL("L", "world", "a:1", "COIN", "B", 0),
	)
	_, present := ls.Volumes().Get(oracle.VolumeKey{Address: "a:1", Asset: "COIN", Color: "B"})
	require.True(t, present, "zero posting creates a real volume cell")

	for _, useMaxPrecision := range []bool{false, true} {
		got := modelAggregate(ls, nil, aggOptions{useMaxPrecision: useMaxPrecision})
		require.Equal(t, "COIN|=in:233465405,out:233465405 COIN|A=in:347155,out:347155 COIN|B=in:0,out:0 EUR/2|A=in:749939022715868,out:749939022715868", renderAgg(got))

		delete(got, assetColor{Asset: "COIN", Color: "B"})
		require.False(t, aggEqual(modelAggregate(ls, nil, aggOptions{useMaxPrecision: useMaxPrecision}), got), "a missing stored zero bucket must fail")
		got[assetColor{Asset: "COIN", Color: "B"}] = &aggPair{}
		got[assetColor{Asset: "COIN", Color: "invented"}] = &aggPair{}
		require.False(t, aggEqual(modelAggregate(ls, nil, aggOptions{useMaxPrecision: useMaxPrecision}), got), "an invented zero bucket must fail")
	}
}

func TestModelAggregateZeroCellsParticipateInOptions(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReqColoredL("L", "world", "a:1", "COIN", "A", 7),
		oracletest.TxReqColoredL("L", "world", "a:1", "COIN", "B", 0),
		oracletest.TxReqColoredL("L", "world", "a:1", "COIN/2", "C", 0),
	)
	for _, tc := range []struct {
		name string
		opts aggOptions
		want string
	}{
		{"raw", aggOptions{}, "COIN/2|C=in:0,out:0 COIN|A=in:7,out:0 COIN|B=in:0,out:0"},
		{"precision", aggOptions{useMaxPrecision: true}, "COIN/2|A=in:700,out:0 COIN/2|B=in:0,out:0 COIN/2|C=in:0,out:0"},
		{"colors", aggOptions{collapseColors: true}, "COIN/2|=in:0,out:0 COIN|=in:7,out:0"},
		{"both", aggOptions{useMaxPrecision: true, collapseColors: true}, "COIN/2|=in:700,out:0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, renderAgg(modelAggregate(ls, filterAddrPrefix("a:"), tc.opts)))
			tc.opts.groupByPrefixes = []string{"a:", "absent:"}
			groups := modelAggregateGroups(ls, nil, tc.opts)
			require.Len(t, groups, 2)
			require.Equal(t, tc.want, renderAgg(groups[0].sums))
			require.Empty(t, groups[1].sums)
		})
	}
}

func TestModelAggregateDoesNotInventPurgedZeroCells(t *testing.T) {
	t.Parallel()

	for _, persistence := range []commonpb.AccountTypePersistence{
		commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
		commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
	} {
		t.Run(persistence.String(), func(t *testing.T) {
			t.Parallel()
			ls := buildLedger(t,
				oracletest.AddTypeReqP("e", persistence),
				oracletest.TxReqColoredL("L", "world", "e:1", "COIN", "B", 0),
			)
			_, present := ls.Volumes().Get(oracle.VolumeKey{Address: "e:1", Asset: "COIN", Color: "B"})
			require.False(t, present, "purge deletes the cell rather than retaining a zero pair")
			opts := aggOptions{useMaxPrecision: true, groupByPrefixes: []string{"e:"}}
			require.Empty(t, modelAggregate(ls, filterAddrPrefix("e:"), opts))
			groups := modelAggregateGroups(ls, nil, opts)
			require.Len(t, groups, 1)
			require.Empty(t, groups[0].sums)
		})
	}
}
