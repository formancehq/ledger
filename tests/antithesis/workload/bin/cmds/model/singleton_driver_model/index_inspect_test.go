package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
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

// modelInspectCounts must reproduce the split that made the naive
// `cardinality <= entities_with_key` relation false: a value the declared type
// cannot hold is still a group in the value index, but its entity is counted as
// null rather than as carrying the key.
func TestModelInspectCountsSplitsNullsFromCarriers(t *testing.T) {
	t.Parallel()

	const key = "rank"

	state := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*servicepb.Request{
		actions.CreateLedgerAction("L", nil),
		{Type: &servicepb.Request_SetMetadataFieldType{SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
			Ledger:     "L",
			TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:        key,
			Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
		}}},
		oracletest.AddTypeReq("acc"),
		oracletest.TxReq("world", "acc:1", "USD", 10),
		oracletest.TxReq("world", "acc:2", "USD", 10),
		oracletest.TxReq("world", "acc:3", "USD", 10),
		oracletest.TxReq("world", "acc:4", "USD", 10),
		// Two carriers sharing a value, so cardinality and the carrier count differ.
		actions.SaveAccountMetadataAction("L", "acc:1", map[string]string{key: "7"}),
		actions.SaveAccountMetadataAction("L", "acc:2", map[string]string{key: "7"}),
		// Two carriers whose text the declared uint64 cannot hold, each keeping
		// its own null group because the encoder carries the original.
		actions.SaveAccountMetadataAction("L", "acc:3", map[string]string{key: "abc"}),
		actions.SaveAccountMetadataAction("L", "acc:4", map[string]string{key: "xyz"}),
	}})
	require.True(t, state.OK, state.Reason)

	ls := state.State.Ledger("L")
	counts := modelInspectCounts(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, key, commonpb.MetadataType_METADATA_TYPE_UINT64)

	require.Equal(t, uint64(2), counts.entitiesWithKey, "only the two convertible accounts carry the key")
	require.Equal(t, uint64(2), counts.entitiesWithNull, "the two unconvertible accounts are counted apart")
	require.Equal(t, uint64(3), counts.cardinality, "one group for 7, one per unconvertible original")

	require.Greater(t, counts.cardinality, counts.entitiesWithKey,
		"cardinality does not bound entities_with_key — the relation the first model run refuted")
	require.LessOrEqual(t, counts.cardinality, counts.entitiesWithKey+counts.entitiesWithNull,
		"every entity contributes one value, so the two buckets together do bound it")
}

func TestModelInspectCountsIgnoresOtherKeysAndTargets(t *testing.T) {
	t.Parallel()

	state := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*servicepb.Request{
		actions.CreateLedgerAction("L", nil),
		oracletest.AddTypeReq("acc"),
		oracletest.TxReq("world", "acc:1", "USD", 10),
		actions.SaveAccountMetadataAction("L", "acc:1", map[string]string{"rank": "1", "other": "2"}),
	}})
	require.True(t, state.OK, state.Reason)

	ls := state.State.Ledger("L")

	counts := modelInspectCounts(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, "rank", commonpb.MetadataType_METADATA_TYPE_STRING)
	require.Equal(t, inspectCounts{cardinality: 1, entitiesWithKey: 1}, counts)

	// The same key on the other target is a different index entirely.
	require.Equal(t, inspectCounts{},
		modelInspectCounts(ls, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "rank", commonpb.MetadataType_METADATA_TYPE_STRING))

	require.Equal(t, inspectCounts{},
		modelInspectCounts(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, "never-set", commonpb.MetadataType_METADATA_TYPE_STRING))
}
