package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithProfile_ContextPropagation(t *testing.T) {
	t.Parallel()

	ctx, profile := query.WithProfile(context.Background())
	require.NotNil(t, profile)

	extracted := query.ProfileFromContext(ctx)
	require.Same(t, profile, extracted)
}

func TestProfileFromContext_NilWhenNotSet(t *testing.T) {
	t.Parallel()

	profile := query.ProfileFromContext(context.Background())
	require.Nil(t, profile)
}

func TestQueryProfile_TotalDuration(t *testing.T) {
	t.Parallel()

	p := &query.QueryProfile{
		IndexDuration:      3 * time.Millisecond,
		EnrichmentDuration: 7 * time.Millisecond,
	}
	require.Equal(t, 10*time.Millisecond, p.TotalDuration())
}

func TestQueryProfile_ToProto(t *testing.T) {
	t.Parallel()

	p := &query.QueryProfile{
		IndexDuration:      5 * time.Millisecond,
		EnrichmentDuration: 2 * time.Millisecond,
		ItemsCollected:     42,
		EnrichedCount:      10,
		MaterializedRanges: 3,
		MaterializedItems:  15,
		Root: &query.IteratorStats{
			Label:     "PrefixIterator(exist:ledger:a:)",
			Kind:      "Prefix",
			Bucket:    "exist",
			NextCalls: 100,
			SeekCalls: 1,
		},
	}

	pb := p.ToProto()
	require.NotNil(t, pb)
	assert.Equal(t, int64(5000), pb.IndexDurationUs)
	assert.Equal(t, int64(2000), pb.EnrichmentDurationUs)
	assert.Equal(t, int32(42), pb.ItemsCollected)
	assert.Equal(t, int32(10), pb.EnrichedCount)
	assert.Equal(t, int32(3), pb.MaterializedRanges)
	assert.Equal(t, int32(15), pb.MaterializedItems)

	require.NotNil(t, pb.RootIterator)
	assert.Equal(t, "PrefixIterator(exist:ledger:a:)", pb.RootIterator.Label)
	assert.Equal(t, "Prefix", pb.RootIterator.Kind)
	assert.Equal(t, "exist", pb.RootIterator.Bucket)
	assert.Equal(t, int64(100), pb.RootIterator.NextCalls)
	assert.Equal(t, int64(1), pb.RootIterator.SeekCalls)
}

func TestQueryProfile_ToProto_NilProfile(t *testing.T) {
	t.Parallel()

	var p *query.QueryProfile
	require.Nil(t, p.ToProto())
}

func TestQueryProfile_ToProto_NoRoot(t *testing.T) {
	t.Parallel()

	p := &query.QueryProfile{
		ItemsCollected: 5,
	}
	pb := p.ToProto()
	require.NotNil(t, pb)
	assert.Nil(t, pb.RootIterator)
	assert.Equal(t, int32(5), pb.ItemsCollected)
}

func TestIteratorStats_ToProto_WithChildren(t *testing.T) {
	t.Parallel()

	stats := &query.IteratorStats{
		Label: "AndIterator",
		Kind:  "And",
		Children: []*query.IteratorStats{
			{
				Label:     "PrefixIterator(midx:ledger:a:role:admin)",
				Kind:      "Prefix",
				Bucket:    "midx",
				NextCalls: 10,
				SeekCalls: 1,
			},
			{
				Label:     "PrefixIterator(exist:ledger:a:)",
				Kind:      "Prefix",
				Bucket:    "exist",
				NextCalls: 20,
				SeekCalls: 2,
			},
		},
	}

	pb := stats.ToProto()
	require.NotNil(t, pb)
	assert.Equal(t, "AndIterator", pb.Label)
	require.Len(t, pb.Children, 2)
	assert.Equal(t, int64(10), pb.Children[0].NextCalls)
	assert.Equal(t, int64(20), pb.Children[1].NextCalls)
}

func TestIteratorStats_ToProto_Nil(t *testing.T) {
	t.Parallel()

	var s *query.IteratorStats
	require.Nil(t, s.ToProto())
}

func TestIteratorStats_String(t *testing.T) {
	t.Parallel()

	stats := &query.IteratorStats{
		Label:     "AndIterator",
		Kind:      "And",
		NextCalls: 5,
		SeekCalls: 1,
		Children: []*query.IteratorStats{
			{
				Label:     "PrefixIterator(midx:ledger:a:role:admin)",
				Kind:      "Prefix",
				Bucket:    "midx",
				NextCalls: 10,
				SeekCalls: 1,
			},
		},
	}

	s := stats.String()
	assert.Contains(t, s, "AndIterator")
	assert.Contains(t, s, "next=5")
	assert.Contains(t, s, "seek=1")
	assert.Contains(t, s, "PrefixIterator(midx:ledger:a:role:admin)")
	assert.Contains(t, s, "bucket=midx")
}

func TestIteratorStats_String_Nil(t *testing.T) {
	t.Parallel()

	var s *query.IteratorStats
	assert.Equal(t, "<nil>", s.String())
}

func TestQueryProfile_EmitToSpan_NilSafe(t *testing.T) {
	t.Parallel()

	// Should not panic on nil profile
	var p *query.QueryProfile
	p.EmitToSpan(nil) //nolint:staticcheck
}

func TestQueryProfile_LogTo_NilSafe(t *testing.T) {
	t.Parallel()

	// Should not panic on nil profile
	var p *query.QueryProfile
	p.LogTo(nil) //nolint:staticcheck
}
