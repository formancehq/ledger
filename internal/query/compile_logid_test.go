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

// u64Bytes encodes v as its 8-byte big-endian entity key.
func u64Bytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)

	return b
}

// seedLogIDStore creates a store seeded with ledger-log IDs [0, rows) for
// ledgerName and returns it together with the key builder used for iteration.
func seedLogIDStore(tb testing.TB, ledgerName string, rows int) (*readstore.Store, *dal.KeyBuilder) {
	tb.Helper()

	logger := logging.FromContext(logging.TestingContext())
	store, err := readstore.New(tb.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()
	for id := range rows {
		require.NoError(tb, batch.SetBytes(readstore.LedgerLogKey(kb, ledgerName, uint64(id)), nil))
	}
	require.NoError(tb, batch.Commit())

	return store, kb
}

// compileLogIDRange compiles an inclusive [lo, hi] log-ID range against store
// and returns the tracked iterator plus its profile for counter inspection.
func compileLogIDRange(tb testing.TB, store *readstore.Store, kb *dal.KeyBuilder, ledgerName string, lo, hi uint64) (readstore.EntityIterator, *QueryProfile) {
	tb.Helper()

	profile := &QueryProfile{}
	ctx := &compileCtx{
		kb:          kb,
		indexReader: store.DB(),
		ledgerName:  ledgerName,
		profile:     profile,
	}

	iter, err := compileLogIdCondition(ctx, &commonpb.UintCondition{Min: &lo, Max: &hi})
	require.NoError(tb, err)

	return iter, profile
}

// assertNoMaterialization pins the streaming-leaf contract on the profile: the
// page came straight from LedgerLogRange and never materialized a range.
func assertNoMaterialization(t *testing.T, profile *QueryProfile) {
	t.Helper()

	require.NotNil(t, profile.Root)
	require.Equal(t, "LedgerLogRange", profile.Root.Kind)
	require.Zero(t, profile.Root.MaterializedRanges)
	require.Zero(t, profile.Root.MaterializedItems)
}

// TestCompileLogIdCondition_PaginationKeepsWorkBounded is the pagination-level
// regression for EN-1967: pulling a first or final 100-ID page through the
// production PaginateForward path must cost a bounded number of Next/Seek calls
// (independent of the total row count) and must never materialize the range.
// The counter-only checks would still pass if a future leaf eagerly buffered the
// whole range without updating those counters; the exact Next/Seek bounds pin
// that the page is served straight from the streaming leaf.
func TestCompileLogIdCondition_PaginationKeepsWorkBounded(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"
	const pageSize = uint32(100)

	sizes := []struct {
		name string
		rows uint64
	}{
		{"1k", 1_000},
		{"10k", 10_000},
		{"50k", 50_000},
	}

	for _, sz := range sizes {
		t.Run(sz.name, func(t *testing.T) {
			t.Parallel()

			store, kb := seedLogIDStore(t, ledgerName, int(sz.rows))
			last := sz.rows - 1

			t.Run("first page", func(t *testing.T) {
				iter, profile := compileLogIDRange(t, store, kb, ledgerName, 0, last)
				defer iter.Close()

				items, hasMore, err := readstore.PaginateForward(iter, pageSize, nil)
				require.NoError(t, err)

				require.Len(t, items, 100)
				require.True(t, hasMore, "a %d-row range spans more than one page", sz.rows)
				require.Equal(t, uint64(0), binary.BigEndian.Uint64(items[0]))
				require.Equal(t, uint64(99), binary.BigEndian.Uint64(items[99]))

				// 100 collected items plus one boundary probe = 101 Next calls,
				// regardless of total row count. No seek is needed from the head.
				require.Equal(t, int64(pageSize+1), profile.Root.NextCalls)
				require.Zero(t, profile.Root.SeekCalls)

				assertNoMaterialization(t, profile)
			})

			t.Run("last page", func(t *testing.T) {
				iter, profile := compileLogIDRange(t, store, kb, ledgerName, 0, last)
				defer iter.Close()

				// after = the entity immediately before the final 100.
				after := u64Bytes(sz.rows - uint64(pageSize) - 1)

				items, hasMore, err := readstore.PaginateForward(iter, pageSize, after)
				require.NoError(t, err)

				require.Len(t, items, 100)
				require.False(t, hasMore)
				require.Equal(t, sz.rows-100, binary.BigEndian.Uint64(items[0]))
				require.Equal(t, last, binary.BigEndian.Uint64(items[99]))

				// One seek to the cursor, one skip of the cursor entity, then the
				// 100 collected items with a final exhaustion probe = 101 Next calls.
				require.Equal(t, int64(pageSize+1), profile.Root.NextCalls)
				require.Equal(t, int64(1), profile.Root.SeekCalls)

				assertNoMaterialization(t, profile)
			})
		})
	}
}

// BenchmarkCompileLogIdCondition_Pagination measures the per-request cost that
// EN-1967 targets: compiling an inclusive log-ID range and pulling one 100-ID
// page (first or final) through PaginateForward. It reports allocations so a
// future regression back to materialize-and-sort shows up here as well.
func BenchmarkCompileLogIdCondition_Pagination(b *testing.B) {
	const ledgerName = "ledger1"
	const pageSize = uint32(100)

	sizes := []struct {
		name string
		rows uint64
	}{
		{"1k", 1_000},
		{"10k", 10_000},
		{"50k", 50_000},
	}

	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			store, kb := seedLogIDStore(b, ledgerName, int(sz.rows))
			last := sz.rows - 1

			b.Run("first page", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for b.Loop() {
					iter, _ := compileLogIDRange(b, store, kb, ledgerName, 0, last)
					_, _, err := readstore.PaginateForward(iter, pageSize, nil)
					if err != nil {
						b.Fatal(err)
					}
					iter.Close()
				}
			})

			b.Run("last page", func(b *testing.B) {
				after := u64Bytes(sz.rows - uint64(pageSize) - 1)
				b.ReportAllocs()
				b.ResetTimer()

				for b.Loop() {
					iter, _ := compileLogIDRange(b, store, kb, ledgerName, 0, last)
					_, _, err := readstore.PaginateForward(iter, pageSize, after)
					if err != nil {
						b.Fatal(err)
					}
					iter.Close()
				}
			})
		})
	}
}
