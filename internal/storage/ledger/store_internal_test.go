package ledger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v5/pkg/query"

	"github.com/formancehq/ledger/internal/storage/common"
)

// eachShape runs fn against the three concrete paginated-query shapes carrying
// the given options, so the type switches in the helpers stay honest.
func eachShape(opts common.ResourceQuery[any], fn func(common.PaginatedQuery[any])) {
	initial := common.InitialPaginatedQuery[any]{Options: opts}
	fn(initial)
	fn(common.ColumnPaginatedQuery[any]{InitialPaginatedQuery: initial})
	fn(common.OffsetPaginatedQuery[any]{InitialPaginatedQuery: initial})
}

func TestQueryFiltersOnJSONB(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		builder query.Builder
		expect  bool
	}{
		{"no filter", nil, false},
		{"metadata key", query.Match("metadata[source_wallet_id]", "w1"), true},
		{"metadata existence", query.Match("metadata", "source_wallet_id"), true},
		{"account", query.Match("account", "wallet:main"), true},
		{"source", query.Match("source", "world"), true},
		{"destination", query.Match("destination", "bank"), true},
		{"id only", query.Gte("id", 1), false},
		{"timestamp only", query.Gte("timestamp", "2026-01-01T00:00:00Z"), false},
		{"reference only", query.Match("reference", "ref-1"), false},
		{"jsonb nested under and/not", query.And(
			query.Gte("id", 1),
			query.Not(query.Match("metadata[source_wallet_id]", "w1")),
		), true},
		{"plain nested under or", query.Or(
			query.Gte("id", 1),
			query.Match("reference", "ref-1"),
		), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eachShape(common.ResourceQuery[any]{Builder: tc.builder}, func(q common.PaginatedQuery[any]) {
				require.Equalf(t, tc.expect, queryFiltersOnJSONB(q), "shape %T", q)
			})
		})
	}
}

func TestWithOwnExpand(t *testing.T) {
	t.Parallel()

	caller := []string{"volumes", "effectiveVolumes"}

	eachShape(common.ResourceQuery[any]{Expand: caller}, func(q common.PaginatedQuery[any]) {
		var got []string
		switch v := withOwnExpand(q).(type) {
		case common.OffsetPaginatedQuery[any]:
			got = v.Options.Expand
		case common.ColumnPaginatedQuery[any]:
			got = v.Options.Expand
		case common.InitialPaginatedQuery[any]:
			got = v.Options.Expand
		default:
			t.Fatalf("unexpected shape %T", v)
		}

		require.Equal(t, caller, got, "the copy must carry the same values")

		// The repository sorts Expand in place; writing to the copy must not
		// reach the caller's slice.
		got[0] = "mutated"
		require.Equal(t, []string{"volumes", "effectiveVolumes"}, caller,
			"shape %T shares its Expand backing array with the caller", q)
	})
}

func TestWithOwnExpandUnknownShapePassesThrough(t *testing.T) {
	t.Parallel()

	require.Nil(t, withOwnExpand(nil))
}

func TestSetTestHookBeforePaginateSelectWithoutContainer(t *testing.T) {
	t.Parallel()

	// New allocates the container; a Store value built without it must still be
	// safe to call rather than dereferencing nil.
	require.NotPanics(t, func() {
		(&Store{}).SetTestHookBeforePaginateSelect(func(context.Context, bun.Tx, bool) error {
			return nil
		})
	})
}
