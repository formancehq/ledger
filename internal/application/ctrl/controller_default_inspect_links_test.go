package ctrl

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
)

func stringValues(vs ...string) []*commonpb.MetadataValue {
	out := make([]*commonpb.MetadataValue, 0, len(vs))
	for _, v := range vs {
		out = append(out, &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: v}})
	}

	return out
}

func decodeInspectToken(t *testing.T, token string) pagecursor.Cursor {
	t.Helper()

	c, err := pagecursor.Decode(token)
	require.NoError(t, err)

	return c
}

func TestInspectIndexResponseLinks(t *testing.T) {
	t.Parallel()

	key := func(v string) string { return base64Encoding.EncodeToString([]byte(v)) }

	t.Run("forward page", func(t *testing.T) {
		t.Parallel()

		resp := toInspectIndexResponse(readstore.InspectDistinctValuesMode, pagecursor.Cursor{Key: key("a")}, &readstore.InspectResult{
			Values:     stringValues("b", "c"),
			HasMore:    true,
			FirstValue: []byte("b"),
			LastValue:  []byte("c"),
		})

		dv := resp.GetDistinctValues()
		require.True(t, dv.GetHasMore())
		require.Equal(t, pagecursor.Cursor{Key: key("c")}, decodeInspectToken(t, dv.GetNextCursor()))
		require.Equal(t, pagecursor.Cursor{Key: key("b"), Back: true}, decodeInspectToken(t, dv.GetPreviousCursor()))
	})

	t.Run("back page is returned ascending", func(t *testing.T) {
		t.Parallel()

		// Scanned descending from before "d": c then b, with a further value.
		resp := toInspectIndexResponse(readstore.InspectFacetsMode, pagecursor.Cursor{Key: key("d"), Back: true}, &readstore.InspectResult{
			Facets: []readstore.InspectFacetEntry{
				{Value: stringValues("c")[0], Count: 1},
				{Value: stringValues("b")[0], Count: 2},
			},
			HasMore:    true,
			FirstValue: []byte("c"),
			LastValue:  []byte("b"),
		})

		f := resp.GetFacets()
		require.Equal(t, "b", f.GetFacets()[0].GetValue().GetStringValue())
		require.Equal(t, "c", f.GetFacets()[1].GetValue().GetStringValue())
		require.Equal(t, pagecursor.Cursor{Key: key("c")}, decodeInspectToken(t, f.GetNextCursor()))
		require.Equal(t, pagecursor.Cursor{Key: key("b"), Back: true}, decodeInspectToken(t, f.GetPreviousCursor()))
		require.True(t, f.GetHasMore(), "a back page always has a next page")
	})

	t.Run("first page has no previous", func(t *testing.T) {
		t.Parallel()

		resp := toInspectIndexResponse(readstore.InspectDistinctValuesMode, pagecursor.Cursor{}, &readstore.InspectResult{
			Values:     stringValues("a"),
			FirstValue: []byte("a"),
			LastValue:  []byte("a"),
		})

		dv := resp.GetDistinctValues()
		require.Empty(t, dv.GetPreviousCursor())
		require.Empty(t, dv.GetNextCursor())
		require.False(t, dv.GetHasMore())
	})
}
