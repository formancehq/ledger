package query

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func uintCond(lo, hi uint64, minExcl, maxExcl bool) *commonpb.UintCondition {
	return &commonpb.UintCondition{Min: &lo, Max: &hi, MinExclusive: minExcl, MaxExclusive: maxExcl}
}

// TestResolveBounds_DegenerateRangesAreEmpty pins the crossed-bounds rule: max
// is the exclusive upper, so adjusted bounds that meet or cross resolve to the
// empty match. They must never reach Pebble as iterator bounds — LowerBound
// above UpperBound is an iterator invariant violation (mergingIter panics
// under the invariants/race build tags), not an empty scan.
func TestResolveBounds_DegenerateRangesAreEmpty(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		cond  *commonpb.UintCondition
		empty bool
	}{
		{"exclusive-exclusive same value", uintCond(7, 7, true, true), true},
		{"inclusive-exclusive same value", uintCond(7, 7, false, true), true},
		{"crossed", uintCond(9, 3, false, false), true},
		{"exclusive min meets inclusive max", uintCond(7, 7, true, false), true},
		{"single value", uintCond(7, 7, false, false), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, err := resolveUintBounds(tc.cond, nil)
			require.NoError(t, err)
			require.Equal(t, tc.empty, b.empty)

			ic := &commonpb.IntCondition{
				Min: new(int64(tc.cond.GetMin())), Max: new(int64(tc.cond.GetMax())),
				MinExclusive: tc.cond.GetMinExclusive(), MaxExclusive: tc.cond.GetMaxExclusive(),
			}
			ib, err := resolveIntBounds(ic, nil)
			require.NoError(t, err)
			require.Equal(t, tc.empty, ib.empty, "int resolver must agree")
		})
	}
}

// TestCompile_NotOverDegenerateRange_YieldsUniverse pins the composition that
// panicked under the invariants build: not(logId[(x,x)]) on LOGS. The
// degenerate child resolves to the empty match, and its complement is the
// whole universe — every log comes back.
func TestCompile_NotOverDegenerateRange_YieldsUniverse(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"

	logger := logging.FromContext(logging.TestingContext())
	store, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()
	for logID := uint64(1); logID <= 3; logID++ {
		seq := make([]byte, 8)
		binary.BigEndian.PutUint64(seq, logID+100)
		require.NoError(t, batch.SetBytes(readstore.LedgerLogKey(kb, ledgerName, logID), seq))
	}
	require.NoError(t, batch.Commit())

	reader := store.DB()
	info := &commonpb.LedgerInfo{Name: ledgerName}

	two := uint64(2)
	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{
		Filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{LogId: &commonpb.LogIdCondition{
			Cond: &commonpb.UintCondition{Min: &two, Max: &two, MinExclusive: true, MaxExclusive: true},
		}}},
	}}}

	iter, err := Compile(
		reader, dal.NewKeyBuilder(), filter,
		commonpb.QueryTarget_QUERY_TARGET_LOGS, ledgerName,
		nil, nil, info, nil, nil, nil, reader, 0)
	require.NoError(t, err)

	t.Cleanup(iter.Close)

	var got []uint64
	for iter.Next() {
		got = append(got, binary.BigEndian.Uint64(iter.Current()))
	}
	require.NoError(t, iter.Err())

	require.Equal(t, []uint64{1, 2, 3}, got,
		"not(empty range) must yield the whole universe")
}

// TestConsumerGuards_DegenerateRangeShortCircuits pins the empty-bounds guard
// in each range consumer that feeds Pebble iterator bounds: a degenerate
// condition must come back as the canonical empty iterator before any
// existence probe or range construction. The zero compileCtx is part of the
// pin — reaching past the guard would dereference the absent readers.
func TestConsumerGuards_DegenerateRangeShortCircuits(t *testing.T) {
	t.Parallel()

	ctx := &compileCtx{}
	cond := uintCond(7, 7, true, true)

	it, err := compileTxIDCondition(ctx, cond)
	require.NoError(t, err)
	require.False(t, it.Next(), "txID guard must yield the empty iterator")

	it, err = compileTimestampRangeCondition(ctx, cond, []byte("p"), "tstmp", 0)
	require.NoError(t, err)
	require.False(t, it.Next(), "timestamp guard must yield the empty iterator")

	it, err = compileLogIdCondition(ctx, cond)
	require.NoError(t, err)
	require.False(t, it.Next(), "logId guard must yield the empty iterator")
}
