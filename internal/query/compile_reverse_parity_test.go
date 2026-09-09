package query_test

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Descending pagination parity (EN-1966).
//
// The oracle is the ascending drain reversed. For every filter shape, a FULL
// traversal in descending PAGES — each page resuming from the previous page's
// last id, exactly as the controller does — must equal that oracle: no
// duplicates, no omissions, at every page size and at the boundaries.
//
// Paging is what makes this more than a drain comparison: it exercises the
// cursor seek on every leaf and composite in the tree, which is where a
// mis-implemented descending Seek silently drops or repeats rows.

const parityLedger = "ledger1"

func parityInfo() *commonpb.LedgerInfo { return &commonpb.LedgerInfo{Name: parityLedger} }

// parityResolver reports a live v1 keyspace with NO type binding, so
// compileFieldCondition validates against the declared schema rather than the
// retype-window binding. That keeps the fixture about direction, not about
// EN-1724's rewrite window, which has its own tests.
func parityResolver() readstore.IndexVersionResolver {
	return func(string) (readstore.ResolvedIndexVersion, bool, error) {
		return readstore.ResolvedIndexVersion{Version: 1}, true, nil
	}
}

// parityRegistry declares every index the parity filters touch as READY.
func parityRegistry() staticIndexLookup {
	reg := staticIndexLookup{}
	for _, id := range []*commonpb.IndexID{
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
	} {
		reg[indexes.KeyFor(parityLedger, id)] = &commonpb.Index{Ledger: parityLedger, Id: id}
	}

	for _, key := range []string{"colour", "size"} {
		id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
		reg[indexes.KeyFor(parityLedger, id)] = &commonpb.Index{Ledger: parityLedger, Id: id}
	}

	return reg
}

func paritySchema() map[string]*commonpb.MetadataFieldSchema {
	return map[string]*commonpb.MetadataFieldSchema{
		"colour": {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		"size":   {Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
	}
}

// parityStore seeds one read store with accounts carrying colour/size
// metadata, written as index EVENTS at a sequence below the pin the tests use.
func parityStore(t *testing.T) *readstore.Store {
	t.Helper()

	logger := logging.FromContext(logging.TestingContext())

	store, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()

	// 12 accounts so several page sizes divide the set unevenly.
	for i := range 12 {
		account := fmt.Sprintf("accounts:%02d", i)

		colour := "red"
		if i%3 == 0 {
			colour = "blue"
		}

		require.NoError(t, batch.SetBytes(readstore.MetadataIndexEventKeyV(
			kb, parityLedger, readstore.NamespaceAccount, "colour", 1,
			readstore.EncodeString(nil, colour), []byte(account),
			uint64(10+i), readstore.MetadataEventAdd), nil))

		require.NoError(t, batch.SetBytes(readstore.MetadataIndexEventKeyV(
			kb, parityLedger, readstore.NamespaceAccount, "size", 1,
			readstore.EncodeUint64(nil, uint64(i%4)), []byte(account),
			uint64(10+i), readstore.MetadataEventAdd), nil))

		// Existence rows so exists / NOT compositions have a universe.
		require.NoError(t, batch.SetBytes(readstore.EntityExistsEventKeyV(
			kb, parityLedger, readstore.NamespaceAccount, "colour", 1,
			false, []byte(account), uint64(10+i), readstore.MetadataEventAdd), nil))
	}

	require.NoError(t, batch.Commit())

	return store
}

func accountFieldFilter(key string, cond *commonpb.FieldCondition) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: cond}}
}

func stringFieldFilter(key, value string) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field: &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_StringCond{StringCond: &commonpb.StringCondition{
			Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value},
		}},
	})
}

func uintRangeFieldFilter(key string, minV, maxV uint64) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field: &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_UintCond{UintCond: &commonpb.UintCondition{
			Min: &minV, Max: &maxV,
		}},
	})
}

func uintEqualFieldFilter(key string, v uint64) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field: &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_UintCond{UintCond: &commonpb.UintCondition{
			Min: &v, Max: &v,
		}},
	})
}

func existsFieldFilter(key string) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field:     &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
	})
}

func andFilter(fs ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: fs}}}
}

func orFilter(fs ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: fs}}}
}

func notFilter(f *commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: f}}}
}

const parityPin = 1000

// ascendingReference drains the ascending tree — the oracle's source.
func ascendingReference(t *testing.T, store *readstore.Store, filter *commonpb.QueryFilter) []string {
	t.Helper()

	reader := store.DB()

	iter, err := query.Compile(
		reader, dal.NewKeyBuilder(), filter,
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger,
		nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
	require.NoError(t, err)

	defer iter.Close()

	var out []string
	for iter.Next() {
		out = append(out, string(iter.Current()))
	}

	require.NoError(t, iter.Err())

	return out
}

// descendingByPages walks the whole result descending, one page at a time,
// resuming from the previous page's last entity — the controller's own loop.
func descendingByPages(t *testing.T, store *readstore.Store, filter *commonpb.QueryFilter, pageSize uint32) []string {
	t.Helper()

	reader := store.DB()

	var (
		out    []string
		before []byte
	)

	// A generous page budget: any traversal needing more than this is looping.
	for range 100 {
		iter, err := query.CompileReverse(
			reader, dal.NewKeyBuilder(), filter,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger,
			nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
		require.NoError(t, err)

		items, _, pErr := readstore.PaginateReverse(iter, pageSize, before)
		iter.Close()
		require.NoError(t, pErr)

		if len(items) == 0 {
			return out
		}

		for _, it := range items {
			out = append(out, string(it))
		}

		before = items[len(items)-1]
	}

	t.Fatal("descending traversal did not terminate: a page is repeating its cursor")

	return nil
}

func parityFilters() []struct {
	name   string
	filter *commonpb.QueryFilter
} {
	return []struct {
		name   string
		filter *commonpb.QueryFilter
	}{
		{"universe", nil},
		{"string equality (streaming leaf)", stringFieldFilter("colour", "red")},
		{"uint equality (streaming leaf)", uintEqualFieldFilter("size", 2)},
		{"uint range (materializing fallback)", uintRangeFieldFilter("size", 1, 2)},
		{"exists (streaming leaf)", existsFieldFilter("colour")},
		{"and of two streaming leaves", andFilter(stringFieldFilter("colour", "red"), uintEqualFieldFilter("size", 1))},
		{"or of two streaming leaves", orFilter(stringFieldFilter("colour", "blue"), uintEqualFieldFilter("size", 3))},
		{"not of a streaming leaf", notFilter(stringFieldFilter("colour", "red"))},
		{"and containing a materializing range", andFilter(stringFieldFilter("colour", "red"), uintRangeFieldFilter("size", 0, 2))},
		{"nested and(or, not)", andFilter(
			orFilter(stringFieldFilter("colour", "red"), stringFieldFilter("colour", "blue")),
			notFilter(uintEqualFieldFilter("size", 0)),
		)},
		{"nested not(not)", notFilter(notFilter(stringFieldFilter("colour", "blue")))},
		{"empty result", stringFieldFilter("colour", "chartreuse")},
	}
}

// TestDescendingParity_FullTraversal is the acceptance oracle: for every
// filter shape and page size, the paged descending traversal equals the
// reversed ascending reference.
func TestDescendingParity_FullTraversal(t *testing.T) {
	t.Parallel()

	store := parityStore(t)

	for _, tc := range parityFilters() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			want := ascendingReference(t, store, tc.filter)
			slices.Reverse(want)

			// Page sizes around, at, and past the result size, so boundary
			// and final pages are all exercised.
			for _, pageSize := range []uint32{1, 2, 3, 5, 12, 50} {
				got := descendingByPages(t, store, tc.filter, pageSize)

				require.Equal(t, want, got,
					"pageSize=%d: descending traversal must equal the reversed ascending reference", pageSize)

				require.Equal(t, len(want), len(got),
					"pageSize=%d: no duplicates and no omissions", pageSize)
			}
		})
	}
}

// TestDescendingParity_CursorBoundaries pins the two ends of the cursor: a
// cursor at the highest entity must exclude it and return the rest, and a
// cursor at the lowest must return nothing.
func TestDescendingParity_CursorBoundaries(t *testing.T) {
	t.Parallel()

	store := parityStore(t)
	reader := store.DB()
	filter := stringFieldFilter("colour", "red")

	want := ascendingReference(t, store, filter)
	slices.Reverse(want)
	require.NotEmpty(t, want)

	page := func(before []byte, size uint32) []string {
		iter, err := query.CompileReverse(
			reader, dal.NewKeyBuilder(), filter,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger,
			nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
		require.NoError(t, err)

		defer iter.Close()

		items, _, pErr := readstore.PaginateReverse(iter, size, before)
		require.NoError(t, pErr)

		out := make([]string, 0, len(items))
		for _, it := range items {
			out = append(out, string(it))
		}

		return out
	}

	require.Equal(t, want, page(nil, 50), "no cursor returns the whole set descending")

	require.Equal(t, want[1:], page([]byte(want[0]), 50),
		"a cursor at the highest entity excludes it")

	require.Empty(t, page([]byte(want[len(want)-1]), 50),
		"a cursor at the lowest entity returns nothing")
}

// TestDescendingParity_NoFullMaterialization is the performance contract in
// test form. A streaming shape must not materialize anything to serve a
// descending page, and a fallback shape must materialize its ONE range — not
// two, and not a second copy for the reversal.
func TestDescendingParity_NoFullMaterialization(t *testing.T) {
	t.Parallel()

	store := parityStore(t)
	reader := store.DB()

	profileFor := func(filter *commonpb.QueryFilter) *query.QueryProfile {
		profile := &query.QueryProfile{}

		iter, err := query.CompileReverse(
			reader, dal.NewKeyBuilder(), filter,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger,
			nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), profile, reader, parityPin)
		require.NoError(t, err)

		defer iter.Close()

		items, _, pErr := readstore.PaginateReverse(iter, 2, nil)
		require.NoError(t, pErr)
		require.Len(t, items, 2)

		return profile
	}

	streaming := profileFor(andFilter(stringFieldFilter("colour", "red"), existsFieldFilter("colour")))
	require.Zero(t, streaming.MaterializedRanges,
		"a composition of streaming leaves must serve a descending page without materializing")
	require.Zero(t, streaming.MaterializedItems)

	fallback := profileFor(uintRangeFieldFilter("size", 1, 2))
	require.Equal(t, 1, fallback.MaterializedRanges,
		"the value-ordered range keeps its ONE materialization")

	ascProfile := &query.QueryProfile{}
	ascIter, err := query.Compile(
		reader, dal.NewKeyBuilder(), uintRangeFieldFilter("size", 1, 2),
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger,
		nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), ascProfile, reader, parityPin)
	require.NoError(t, err)

	for ascIter.Next() {
	}

	require.NoError(t, ascIter.Err())
	ascIter.Close()

	require.Equal(t, ascProfile.MaterializedItems, fallback.MaterializedItems,
		"descending must reuse the ascending materialization, not double it")
}

// TestDescendingParity_Uint64Extremes covers the fixed-width entity
// boundaries: id 0 and MaxUint64 must page like any other id.
func TestDescendingParity_Uint64Extremes(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())

	store, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()

	ids := []uint64{0, 1, 1 << 32, ^uint64(0) - 1, ^uint64(0)}
	for i, id := range ids {
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, id)
		require.NoError(t, batch.SetBytes(readstore.LedgerLogKey(kb, parityLedger, id), nil))
		_ = i
	}

	require.NoError(t, batch.Commit())

	reader := store.DB()

	prefix := readstore.LedgerLogPrefix(dal.NewKeyBuilder(), parityLedger)

	rev, err := readstore.NewReversePrefixIterator(reader, prefix, len(prefix), 8)
	require.NoError(t, err)

	defer rev.Close()

	var before []byte

	var got []uint64

	for range 20 {
		items, _, pErr := readstore.PaginateReverse(rev, 2, before)
		require.NoError(t, pErr)

		if len(items) == 0 {
			break
		}

		for _, it := range items {
			got = append(got, binary.BigEndian.Uint64(it))
		}

		before = items[len(items)-1]
	}

	want := slices.Clone(ids)
	slices.Reverse(want)

	require.Equal(t, want, got,
		"descending paging must cross id 0 and MaxUint64 without dropping or repeating")
}
