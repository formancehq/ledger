package query

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// A compilation binds each index to one served version. A retype promotion
// flushed mid-compile flips the live resolver from the retained version to the
// promoted one; validation, range merging and leaf compilation must still all
// see the version the first resolution picked.
func TestCompile_PromotionFlushedMidCompileKeepsOneVersionBinding(t *testing.T) {
	t.Parallel()

	const (
		ledgerName = "ledger1"
		pin        = 10
	)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "n")
	canonical := indexes.Canonical(id)
	registry := staticIndexLookup{indexes.KeyFor(ledgerName, id): {Ledger: ledgerName, Id: id}}
	schema := map[string]*commonpb.MetadataFieldSchema{
		"n": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
	}

	// v2 (INT64) is committed but its promotion is unflushed, so pin 10 is
	// served from v1 (UINT64) until the flush, and from v2 after it.
	retyped := readstore.IndexVersionState{
		CurrentVersion:             2,
		HighWater:                  2,
		ActivationSequence:         8,
		CurrentType:                commonpb.MetadataType_METADATA_TYPE_INT64,
		CurrentTypeDeclared:        true,
		PreviousVersion:            1,
		PreviousType:               commonpb.MetadataType_METADATA_TYPE_UINT64,
		PreviousTypeDeclared:       true,
		PreviousActivationSequence: 3,
		PreviousValidThrough:       12,
	}

	intLeaf := func(cond *commonpb.IntCondition) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
			Field:     &commonpb.FieldRef{Metadata: "n"},
			Condition: &commonpb.FieldCondition_IntCond{IntCond: cond},
		}}}
	}
	gte := func(v int64) *commonpb.QueryFilter { return intLeaf(&commonpb.IntCondition{Min: &v}) }
	lte := func(v int64) *commonpb.QueryFilter { return intLeaf(&commonpb.IntCondition{Max: &v}) }
	and := func(fs ...*commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: fs}}}
	}

	// compileDuringFlush compiles over a snapshot holding the in-flight
	// promotion and flushes it right after the first version resolution.
	compileDuringFlush := func(t *testing.T, filter *commonpb.QueryFilter, reverse bool) []string {
		t.Helper()

		store, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
		require.NoError(t, err)

		t.Cleanup(func() { _ = store.Close() })

		kb := dal.NewKeyBuilder()
		batch := store.NewBatch()
		for i, account := range []string{"accounts:a", "accounts:b"} {
			require.NoError(t, batch.SetBytes(readstore.MetadataIndexEventKeyV(
				kb, ledgerName, readstore.NamespaceAccount, "n", 1,
				readstore.EncodeUint64(nil, uint64(i)), []byte(account),
				uint64(4+i), readstore.MetadataEventAdd), nil))
		}
		require.NoError(t, batch.Commit())

		store.MarkPromotion(ledgerName, canonical, 2)

		batch = store.NewBatch()
		require.NoError(t, store.WriteIndexVersionState(batch, ledgerName, canonical, retyped))
		require.NoError(t, batch.Commit())

		snap := store.NewSnapshot()
		t.Cleanup(func() { _ = snap.Close() })

		live := store.PinnedVersionResolver(snap, ledgerName, pin)
		var first *readstore.ResolvedIndexVersion
		resolveThenFlush := func(c string) (readstore.ResolvedIndexVersion, bool, error) {
			resolved, ok, err := live(c)
			if first == nil {
				first = &resolved
				require.NoError(t, store.FlushPromotions())
			}

			return resolved, ok, err
		}

		options := CompileOptions{
			IndexReader:     snap,
			MainReader:      snap,
			KeyBuilder:      dal.NewKeyBuilder(),
			Target:          commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			LedgerName:      ledgerName,
			Schema:          schema,
			LedgerInfo:      &commonpb.LedgerInfo{Name: ledgerName},
			IndexRegistry:   registry,
			IndexVersionFor: resolveThenFlush,
			Pin:             pin,
		}

		var out []string
		if reverse {
			iter, err := CompileReverse(filter, options)
			require.NoError(t, err)

			defer iter.Close()

			items, _, err := readstore.PaginateReverse(iter, 100, nil)
			require.NoError(t, err)

			for _, item := range items {
				out = append(out, string(item))
			}
			slices.Reverse(out)
		} else {
			iter, err := Compile(filter, options)
			require.NoError(t, err)

			defer iter.Close()

			for iter.Next() {
				out = append(out, string(iter.Current()))
			}
			require.NoError(t, iter.Err())
		}

		require.NotNil(t, first)
		require.Equal(t, uint32(1), first.Version, "the first resolution must see the promotion in flight")

		after, _, err := live(canonical)
		require.NoError(t, err)
		require.Equal(t, uint32(2), after.Version, "the flush must have switched the live resolver to v2")

		return out
	}

	cases := []struct {
		name   string
		filter *commonpb.QueryFilter
		want   []string
	}{
		{name: "lower bounds", filter: and(gte(0), gte(1)), want: []string{"accounts:b"}},
		{name: "closed range", filter: and(gte(0), lte(1)), want: []string{"accounts:a", "accounts:b"}},
	}

	for _, reverse := range []bool{false, true} {
		direction := "ascending"
		if reverse {
			direction = "descending"
		}

		for _, tc := range cases {
			t.Run(direction+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				require.Equal(t, tc.want, compileDuringFlush(t, tc.filter, reverse))
			})
		}
	}
}
