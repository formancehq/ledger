package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// Every index the generator churns needs its own sonde, or a run can serve
// pages from some of them and still look fully covered.
func TestCoverageMessages_OneSondePerWorkloadIndex(t *testing.T) {
	t.Parallel()

	msgs := coverageMessages()

	for _, wi := range workloadIndexes() {
		require.Contains(t, msgs, coverageIndexMessage(wi.canonical),
			"index %s is churned but has no coverage sonde", wi.canonical)
	}

	require.Len(t, msgs, len(workloadIndexes())+3,
		"the two metadata sondes and the retype sonde, and nothing else")
}

// Antithesis keys properties by message, so two sondes sharing a name collapse
// into one signal and a starved index hides behind a covered one.
func TestCoverageMessages_AreUniqueAndPrefixed(t *testing.T) {
	t.Parallel()

	seen := map[string]struct{}{}

	for _, msg := range coverageMessages() {
		require.True(t, strings.HasPrefix(msg, coveragePrefix),
			"%q escapes the prefix run_model_test.sh keys its gate on", msg)

		_, dup := seen[msg]
		require.False(t, dup, "duplicate sonde name %q", msg)
		seen[msg] = struct{}{}
	}
}

// The two entity targets must not collapse onto one another's sonde.
func TestCoverageMetadataMessage_DistinguishesTargets(t *testing.T) {
	t.Parallel()

	require.NotEqual(t,
		coverageMetadataMessage(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS),
		coverageMetadataMessage(commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS))
}

// The metadata sonde is satisfied by a metadata-field leaf and nothing else: a
// builtin-index leaf is served by a sonde of its own, and an index-free one by
// none.
func TestFilterNeedsMetadataIndex(t *testing.T) {
	t.Parallel()

	meta := filterMetaExists("k1")
	require.True(t, filterNeedsMetadataIndex(meta))
	require.True(t, filterNeedsMetadataIndex(filterAnd(meta, filterReverted(true))),
		"a metadata leaf anywhere in the tree counts")

	require.False(t, filterNeedsMetadataIndex(filterReverted(true)))
	require.False(t, filterNeedsMetadataIndex(filterReference("ref-1")),
		"a builtin index is not a metadata-field index")
	require.False(t, filterNeedsMetadataIndex(nil))
}

// The retype sonde reads the committed model, so it must see a window the fold
// opened — and only for an index the query actually needed.
func TestRetypeWindowOpenFor(t *testing.T) {
	t.Parallel()

	const key = "k1"

	target := commonpb.TargetType_TARGET_TYPE_ACCOUNT
	canon := metadataCanonical(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, key)
	needed := map[string]struct{}{canon: {}}

	c := NewChecker([]string{"L"}, nil)
	require.False(t, c.retypeWindowOpenFor("L", needed), "no window on a fresh state")

	// A window opens only once the index exists over a declared field and the
	// declaration then changes: the served version may still be bound to the
	// superseded type.
	apply := func(reqs ...*servicepb.Request) {
		t.Helper()

		res := c.modelState.Apply(oracle.Bulk{Requests: reqs})
		require.True(t, res.OK, "setup bulk rejected: %s", res.Reason)
		c.modelState = res.State
	}

	apply(oracletest.SetFieldTypeReq(target, key, commonpb.MetadataType_METADATA_TYPE_STRING))
	apply(oracletest.CreateIndexReq(indexes.MetadataID(target, key)))
	require.False(t, c.retypeWindowOpenFor("L", needed), "declaring and indexing opens no window")

	apply(oracletest.SetFieldTypeReq(target, key, commonpb.MetadataType_METADATA_TYPE_INT64))
	require.True(t, c.retypeWindowOpenFor("L", needed), "the retype opens it")

	// A window open somewhere in the ledger is not the claim: only a query that
	// needed THAT index observed the superseded binding. A second indexed field
	// that was never retyped must read as closed.
	const other = "k2"

	apply(oracletest.SetFieldTypeReq(target, other, commonpb.MetadataType_METADATA_TYPE_STRING))
	apply(oracletest.CreateIndexReq(indexes.MetadataID(target, other)))

	require.False(t, c.retypeWindowOpenFor("L", map[string]struct{}{
		metadataCanonical(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, other): {},
	}), "another field's open window says nothing about this one")

	require.False(t, c.retypeWindowOpenFor("L", map[string]struct{}{assetIndexCanonical: {}}),
		"a query that did not need the retyped index proves nothing about the window")
}

// A sonde needs an accepted outcome, a returned row, and the filter to have
// needed that index. Any one missing and it stays unsatisfied — an empty page
// in particular proves the index was consulted, never that it can produce a
// matching record, and the generator emits unmatchable filters on purpose.
func TestCoverageHits_NeedsAcceptedNonEmptyAndNeeded(t *testing.T) {
	t.Parallel()

	accounts := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	assetSonde := coverageIndexMessage(assetIndexCanonical)
	needed := map[string]struct{}{assetIndexCanonical: {}}
	filter := filterMetaExists("k1")

	hits := coverageHits(accounts, filter, needed, true, 3, true)
	require.True(t, hits[assetSonde], "accepted, non-empty, and needed")
	require.True(t, hits[coverageMetadataMessage(accounts)])
	require.True(t, hits[coverageRetypeMessage])

	require.False(t, coverageHits(accounts, filter, needed, true, 0, true)[assetSonde],
		"a verified but empty page must not satisfy a sonde")
	require.False(t, coverageHits(accounts, filter, needed, false, 3, true)[assetSonde],
		"an outcome the oracle rejected proves nothing")

	other := map[string]struct{}{logDateIndexCanonical: {}}
	require.False(t, coverageHits(accounts, filter, other, true, 3, true)[assetSonde],
		"a page served through another index cannot vouch for this one")

	require.False(t, coverageHits(accounts, filter, needed, true, 3, false)[coverageRetypeMessage],
		"no open window, nothing to prove")
	require.False(t, coverageHits(accounts, filterReverted(true), needed, true, 3, true)[coverageMetadataMessage(accounts)],
		"no metadata leaf, no metadata-index claim")
}

// Every registered sonde must be decided on every call, or one could never be
// evaluated false and Antithesis would get no gradient for it.
func TestCoverageHits_DecidesEveryEntitySonde(t *testing.T) {
	t.Parallel()

	accounts := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS

	hits := coverageHits(accounts, filterMetaExists("k1"), nil, true, 1, false)
	for _, msg := range coverageMessages() {
		if msg == coverageMetadataMessage(commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS) {
			continue // the other target's sonde is decided by its own queries
		}

		_, decided := hits[msg]
		require.True(t, decided, "%q was not evaluated", msg)
	}
}
