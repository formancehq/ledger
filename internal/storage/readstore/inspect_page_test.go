package readstore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// inspectPageFixture indexes values v1..v5 under key "k". Each value has two
// live entities, except v3 whose only entity was deleted at seq 30 and v4
// whose second entity was added after the horizon (seq 40).
func inspectPageFixture(t *testing.T) (InspectParams, func(string) []byte) {
	t.Helper()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	encoded := func(v string) []byte {
		return EncodeMetadataValue(nil, &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: v}})
	}
	put := func(value, entity string, seq uint64, op byte) {
		require.NoError(t, s.DB().Set(
			MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, encoded(value), []byte(entity), seq, op),
			nil,
			pebble.NoSync,
		))
	}

	for _, v := range []string{"v1", "v2", "v5"} {
		put(v, "a:1", 10, MetadataEventAdd)
		put(v, "a:2", 10, MetadataEventAdd)
	}

	put("v3", "a:1", 10, MetadataEventAdd)
	put("v3", "a:1", 30, MetadataEventDel)
	put("v4", "a:1", 10, MetadataEventAdd)
	put("v4", "a:2", 40, MetadataEventAdd)

	return InspectParams{
		Reader:          s.DB(),
		KB:              kb,
		LedgerName:      "l",
		Namespace:       NamespaceAccount,
		MetadataKey:     "k",
		Version:         1,
		HorizonSequence: 35,
		PageSize:        2,
	}, encoded
}

func values(r *InspectResult) []string {
	out := make([]string, 0, len(r.Values))
	for _, v := range r.Values {
		out = append(out, v.GetStringValue())
	}

	return out
}

func TestInspectDistinctValues_Pages(t *testing.T) {
	t.Parallel()

	base, encoded := inspectPageFixture(t)

	for _, tc := range []struct {
		name     string
		cursor   string
		backward bool
		values   []string
		hasMore  bool
		first    string
		last     string
	}{
		{name: "forward first page", values: []string{"v1", "v2"}, hasMore: true, first: "v1", last: "v2"},
		{name: "forward past v2 skips the dead v3", cursor: "v2", values: []string{"v4", "v5"}, first: "v4", last: "v5"},
		{name: "backward from the end", backward: true, values: []string{"v5", "v4"}, hasMore: true, first: "v5", last: "v4"},
		{name: "backward before v4 skips the dead v3", cursor: "v4", backward: true, values: []string{"v2", "v1"}, first: "v2", last: "v1"},
		{name: "backward before v1 is empty", cursor: "v1", backward: true, values: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			params := base
			params.KB = dal.NewKeyBuilder() // a KeyBuilder is not safe for concurrent use
			params.Mode = InspectDistinctValuesMode
			params.Backward = tc.backward

			if tc.cursor != "" {
				params.CursorBytes = encoded(tc.cursor)
			}

			r, err := InspectIndex(params)
			require.NoError(t, err)
			require.Equal(t, tc.values, values(r))
			require.Equal(t, tc.hasMore, r.HasMore)

			if tc.first == "" {
				require.Nil(t, r.FirstValue)
				require.Nil(t, r.LastValue)

				return
			}

			require.Equal(t, encoded(tc.first), r.FirstValue)
			require.Equal(t, encoded(tc.last), r.LastValue)
		})
	}
}

func TestInspectFacets_BackwardCountsAtHorizon(t *testing.T) {
	t.Parallel()

	params, encoded := inspectPageFixture(t)
	params.Mode = InspectFacetsMode
	params.Backward = true
	params.PageSize = 3

	r, err := InspectIndex(params)
	require.NoError(t, err)
	require.Len(t, r.Facets, 3)
	require.Equal(t, "v5", r.Facets[0].Value.GetStringValue())
	require.Equal(t, uint64(2), r.Facets[0].Count)
	require.Equal(t, "v4", r.Facets[1].Value.GetStringValue())
	require.Equal(t, uint64(1), r.Facets[1].Count, "the entity added after the horizon is not counted")
	require.Equal(t, "v2", r.Facets[2].Value.GetStringValue())
	require.True(t, r.HasMore)
	require.Equal(t, encoded("v5"), r.FirstValue)
	require.Equal(t, encoded("v2"), r.LastValue)
}
