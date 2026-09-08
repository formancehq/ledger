package query

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func drainLogIDs(t *testing.T, iter readstore.EntityIterator) []uint64 {
	t.Helper()
	defer iter.Close()

	var out []uint64
	for iter.Next() {
		out = append(out, binary.BigEndian.Uint64(iter.Current()))
	}
	require.NoError(t, iter.Err())

	return out
}

// oracleUintMatch is an independent oracle for a UintCondition: it applies the
// raw proto bounds with plain uint64 comparisons, deliberately NOT going through
// resolveUintBounds. Comparing the compiled iterator against it pins the range
// semantics (Math half-open after exclusivity adjustment) without deriving the
// expected value the same way the compiler does.
func oracleUintMatch(cond *commonpb.UintCondition, id uint64) bool {
	if cond.Min != nil {
		v := cond.GetMin()
		if cond.GetMinExclusive() {
			if id <= v {
				return false
			}
		} else if id < v {
			return false
		}
	}

	if cond.Max != nil {
		v := cond.GetMax()
		if cond.GetMaxExclusive() {
			if id >= v {
				return false
			}
		} else if id > v {
			return false
		}
	}

	return true
}

// TestCompileLogIdCondition_RangesAgainstUintOracle checks every log-ID range
// shape (missing/empty/inverted bounds, zero, MaxUint64) against the
// independent uint64-set oracle over a universe that includes a MaxUint64 log.
// This pins the deliberate correction: an inclusive-Max / unbounded range must
// include MaxUint64, while an exclusive-Max range must exclude it.
func TestCompileLogIdCondition_RangesAgainstUintOracle(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"
	universe := []uint64{0, 1, 5, math.MaxUint64 - 1, math.MaxUint64}

	logger := logging.FromContext(logging.TestingContext())
	store, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()
	for _, id := range universe {
		seq := make([]byte, 8)
		binary.BigEndian.PutUint64(seq, id+100)
		require.NoError(t, batch.SetBytes(readstore.LedgerLogKey(kb, ledgerName, id), seq))
	}
	require.NoError(t, batch.Commit())

	ctx := &compileCtx{
		kb:          kb,
		indexReader: store.DB(),
		ledgerName:  ledgerName,
	}

	u64 := func(v uint64) *uint64 { return &v }

	cases := []struct {
		name string
		cond *commonpb.UintCondition
	}{
		{"missing bounds", &commonpb.UintCondition{}},
		{"missing max", &commonpb.UintCondition{Min: u64(1)}},
		{"missing min", &commonpb.UintCondition{Max: u64(5)}},
		{"zero inclusive min", &commonpb.UintCondition{Min: u64(0)}},
		{"zero exclusive min", &commonpb.UintCondition{Min: u64(0), MinExclusive: true}},
		{"inclusive range", &commonpb.UintCondition{Min: u64(1), Max: u64(5)}},
		{"exclusive range", &commonpb.UintCondition{Min: u64(1), Max: u64(5), MinExclusive: true, MaxExclusive: true}},
		{"inverted bounds", &commonpb.UintCondition{Min: u64(9), Max: u64(3)}},
		{"empty crossing exclusive", &commonpb.UintCondition{Min: u64(5), Max: u64(5), MinExclusive: true, MaxExclusive: true}},
		{"singleton equality", &commonpb.UintCondition{Min: u64(1), Max: u64(1)}},
		{"inclusive MaxUint64", &commonpb.UintCondition{Min: u64(0), Max: u64(math.MaxUint64)}},
		{"exclusive MaxUint64", &commonpb.UintCondition{Min: u64(0), Max: u64(math.MaxUint64), MaxExclusive: true}},
		{"exact MaxUint64 equality", &commonpb.UintCondition{Min: u64(math.MaxUint64), Max: u64(math.MaxUint64)}},
		{"min at MaxUint64 unbounded above", &commonpb.UintCondition{Min: u64(math.MaxUint64)}},
		{"min exclusive MaxUint64-1", &commonpb.UintCondition{Min: u64(math.MaxUint64 - 1), MinExclusive: true}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			iter, err := compileLogIdCondition(ctx, tc.cond)
			require.NoError(t, err)

			got := drainLogIDs(t, iter)

			var want []uint64
			for _, id := range universe {
				if oracleUintMatch(tc.cond, id) {
					want = append(want, id)
				}
			}

			require.Equal(t, want, got)
		})
	}
}

// TestCompileLogIdCondition_RangeStreamsWithoutMaterializing pins the perf
// requirement: a log-ID range must be served by the streaming leaf and must not
// allocate a complete-range ID slice (the materialization counters stay zero).
func TestCompileLogIdCondition_RangeStreamsWithoutMaterializing(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"

	logger := logging.FromContext(logging.TestingContext())
	store, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()
	for id := range uint64(50) {
		seq := make([]byte, 8)
		binary.BigEndian.PutUint64(seq, id+1000)
		require.NoError(t, batch.SetBytes(readstore.LedgerLogKey(kb, ledgerName, id), seq))
	}
	require.NoError(t, batch.Commit())

	profile := &QueryProfile{}
	ctx := &compileCtx{
		kb:          kb,
		indexReader: store.DB(),
		ledgerName:  ledgerName,
		profile:     profile,
	}

	lo, hi := uint64(10), uint64(30)

	iter, err := compileLogIdCondition(ctx, &commonpb.UintCondition{Min: &lo, Max: &hi})
	require.NoError(t, err)
	defer iter.Close()

	var got []uint64
	for iter.Next() {
		got = append(got, binary.BigEndian.Uint64(iter.Current()))
	}
	require.NoError(t, iter.Err())

	// [lo, hi] inclusive (both bounds default to non-exclusive).
	var want []uint64
	for id := lo; id <= hi; id++ {
		want = append(want, id)
	}
	require.Equal(t, want, got)

	require.Zero(t, profile.MaterializedRanges)
	require.Zero(t, profile.MaterializedItems)
	require.NotNil(t, profile.Root)
	require.Equal(t, "LedgerLogRange", profile.Root.Kind)
}
