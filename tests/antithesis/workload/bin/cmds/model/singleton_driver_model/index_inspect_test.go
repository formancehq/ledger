package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func inspectValue(s string) *commonpb.MetadataValue {
	return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: s}}
}

func TestInspectPageViolation_DistinctValues(t *testing.T) {
	t.Parallel()

	mode := servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES
	page := func(vs []*commonpb.MetadataValue, hasMore bool, next string) *servicepb.InspectIndexResponse {
		return &servicepb.InspectIndexResponse{Result: &servicepb.InspectIndexResponse_DistinctValues{
			DistinctValues: &servicepb.InspectDistinctValues{Values: vs, HasMore: hasMore, NextCursor: next},
		}}
	}

	require.Empty(t, inspectPageViolation(page([]*commonpb.MetadataValue{inspectValue("a"), inspectValue("b")}, false, ""), mode, 2))
	require.Equal(t, "page longer than requested", inspectPageViolation(page([]*commonpb.MetadataValue{inspectValue("a"), inspectValue("b")}, false, ""), mode, 1))
	require.Equal(t, "value served twice", inspectPageViolation(page([]*commonpb.MetadataValue{inspectValue("a"), inspectValue("a")}, false, ""), mode, 5))
	require.Equal(t, "more to serve but no resume cursor", inspectPageViolation(page(nil, true, ""), mode, 5))
	require.Equal(t, "resume cursor on an exhausted scan", inspectPageViolation(page(nil, false, "k"), mode, 5))
	require.Equal(t, "distinct-values mode answered another arm", inspectPageViolation(&servicepb.InspectIndexResponse{}, mode, 5))
}

func TestInspectPageViolation_FacetsAndSummary(t *testing.T) {
	t.Parallel()

	facets := servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS
	withFacets := func(fs []*servicepb.InspectFacet) *servicepb.InspectIndexResponse {
		return &servicepb.InspectIndexResponse{Result: &servicepb.InspectIndexResponse_Facets{
			Facets: &servicepb.InspectFacets{Facets: fs},
		}}
	}

	require.Empty(t, inspectPageViolation(withFacets([]*servicepb.InspectFacet{{Value: inspectValue("a"), Count: 2}}), facets, 5))
	require.Equal(t, "facet counts no entity", inspectPageViolation(withFacets([]*servicepb.InspectFacet{{Value: inspectValue("a")}}), facets, 5))
	require.Equal(t, "value served twice", inspectPageViolation(withFacets([]*servicepb.InspectFacet{
		{Value: inspectValue("a"), Count: 1}, {Value: inspectValue("a"), Count: 1},
	}), facets, 5))

	summary := servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY
	withSummary := func(cardinality, withKey uint64) *servicepb.InspectIndexResponse {
		return &servicepb.InspectIndexResponse{Result: &servicepb.InspectIndexResponse_Summary{
			Summary: &servicepb.InspectSummary{Cardinality: cardinality, EntitiesWithKey: withKey},
		}}
	}

	require.Empty(t, inspectPageViolation(withSummary(2, 5), summary, 5))
	// The index and the entity-exists keyspace are separate projections: a
	// deleted key leaves value groups live that no entity carries, so neither
	// counter bounds the other.
	require.Empty(t, inspectPageViolation(withSummary(6, 5), summary, 5))
	require.Equal(t, "summary mode answered another arm", inspectPageViolation(&servicepb.InspectIndexResponse{}, summary, 5))
}
