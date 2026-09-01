package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func TestIndexNeeds(t *testing.T) {
	t.Parallel()

	assertNeeds := func(asset, other bool, gotAsset, gotOther bool) {
		t.Helper()
		require.Equal(t, asset, gotAsset, "asset need")
		require.Equal(t, other, gotOther, "other need")
	}

	// nil / universe: no index.
	a, o := indexNeeds(nil, accounts)
	assertNeeds(false, false, a, o)

	// has-asset → asset index only.
	a, o = indexNeeds(filterHasAsset("USD", 2), accounts)
	assertNeeds(true, false, a, o)

	// metadata field → a non-asset index on either target.
	a, o = indexNeeds(filterMetaExists("k"), accounts)
	assertNeeds(false, true, a, o)
	a, o = indexNeeds(filterMetaExists("k"), txns)
	assertNeeds(false, true, a, o)

	// index-free leaves.
	a, o = indexNeeds(filterReverted(true), txns)
	assertNeeds(false, false, a, o)
	a, o = indexNeeds(filterTxIDRange(1, 5), txns)
	assertNeeds(false, false, a, o)
	a, o = indexNeeds(filterAddrPrefix("acc:"), accounts)
	assertNeeds(false, false, a, o)

	// address is index-backed (non-asset) on transactions.
	a, o = indexNeeds(filterAddrPrefix("acc:"), txns)
	assertNeeds(false, true, a, o)

	// Combinators OR their children's needs.
	a, o = indexNeeds(filterAnd(filterHasAsset("USD", 2), filterAddrPrefix("acc:")), accounts)
	assertNeeds(true, false, a, o) // asset-only: the other operand is index-free
	a, o = indexNeeds(filterAnd(filterHasAsset("USD", 2), filterMetaExists("k")), accounts)
	assertNeeds(true, true, a, o) // needs both → not asset-only
	a, o = indexNeeds(filterNot(filterHasAsset("USD", 2)), accounts)
	assertNeeds(true, false, a, o)
}

func TestAccountHasAsset(t *testing.T) {
	t.Parallel()

	// world → acc:1 in USD/2 and world → acc:2 in COIN (precision 0).
	ls := buildLedger(t,
		oracletest.TxReq("world", "acc:1", "USD/2", 5),
		oracletest.TxReq("world", "acc:2", "COIN", 5),
	)

	require.True(t, accountHasAsset(ls, "acc:1", "USD", 2))
	require.False(t, accountHasAsset(ls, "acc:1", "USD", 4)) // precision must match
	require.False(t, accountHasAsset(ls, "acc:1", "EUR", 2))
	require.True(t, accountHasAsset(ls, "acc:2", "COIN", 0))
	require.False(t, accountHasAsset(ls, "acc:2", "COIN", 2))
	// A source is credited on the output side, so it holds the asset too.
	require.True(t, accountHasAsset(ls, "world", "USD", 2))
	require.True(t, accountHasAsset(ls, "world", "COIN", 0))
	require.False(t, accountHasAsset(ls, "acc:absent", "USD", 2))
}

// TestGenAccountAssetFilterAssetOnly asserts every filter the asset generator
// produces is a bare has-asset leaf that is asset-only (needs the account-by-asset
// index and no other) and valid on the accounts target — the properties the
// lifecycle path relies on.
func TestGenAccountAssetFilterAssetOnly(t *testing.T) {
	t.Parallel()

	for i := 0; i < 500; i++ {
		f := genAccountAssetFilter()
		a, o := indexNeeds(f, accounts)
		require.True(t, a, "must need the asset index: %s", describeFilter(f))
		require.False(t, o, "must not need any other index: %s", describeFilter(f))
		require.False(t, filterInvalidForTarget(f, accounts), "must be valid on accounts: %s", describeFilter(f))

		_, _, ok := hasAssetTarget(f)
		require.True(t, ok, "must be a bare has-asset leaf: %s", describeFilter(f))
	}
}

// TestAssetWindow checks the ever-touched account set and the has-asset page
// window: every account a USD/2 posting touched (both sides, including the
// source world) appears, ordered by address, with the exclusive cursor and
// reverse applied — and an account that only touched a different asset does not.
func TestAssetWindow(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReq("world", "acc:1", "USD/2", 5),
		oracletest.TxReq("world", "acc:2", "USD/2", 5),
		oracletest.TxReq("world", "acc:3", "EUR/2", 5),
	)

	// Ever-touched USD/2: acc:1, acc:2, world (source of both). acc:3 only EUR.
	require.Equal(t, []string{"acc:1", "acc:2", "world"}, ls.EverAssetAccounts("USD", 2))
	require.Equal(t, []string{"acc:3", "world"}, ls.EverAssetAccounts("EUR", 2))
	require.Empty(t, ls.EverAssetAccounts("USD", 4)) // precision must match

	// Full window, forward.
	require.Equal(t, []string{"acc:1", "acc:2", "world"},
		assetWindow(ls, "USD", 2, "", 10, false))

	// pageSize cap.
	require.Equal(t, []string{"acc:1", "acc:2"},
		assetWindow(ls, "USD", 2, "", 2, false))

	// Forward cursor is exclusive (drops <= cursor).
	require.Equal(t, []string{"acc:2", "world"},
		assetWindow(ls, "USD", 2, "acc:1", 10, false))

	// Reverse order + exclusive cursor (drops >= cursor).
	require.Equal(t, []string{"acc:2", "acc:1"},
		assetWindow(ls, "USD", 2, "world", 10, true))
}

// A multi-leaf filter can carry both an absent needed index and a
// kind-mismatched leaf; the surfaced rejection follows the compiler's walk
// order, so both honest error classes must be legal — but never results, and
// never a rejection class the filter cannot produce.
func TestIndexedQueryOutcomeLegal_MismatchAndAbsentCoexist(t *testing.T) {
	t.Parallel()

	acct := commonpb.TargetType_TARGET_TYPE_ACCOUNT

	// k0 declared UINT64 with an active index; k3 never declared, no index.
	gs := buildGlobal(t,
		oracletest.SetFieldTypeReq(acct, "k0", commonpb.MetadataType_METADATA_TYPE_UINT64),
		oracletest.CreateIndexReq(indexes.MetadataID(acct, "k0")),
	)
	gs.SetIndexActive("L", indexes.Canonical(indexes.MetadataID(acct, "k0")))
	ls := gs.Ledger("L")

	neg := int64(-5)
	mismatched := filterFieldInt("k0", &neg, nil) // negative bound on unsigned: kind mismatch
	absent := filterFieldExists("k3", false)

	both := filterOr(mismatched, absent)
	needed := map[string]struct{}{}
	neededIndexCanonicals(both, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, needed)

	noWindow := func(oracle.LedgerState) bool { return false }

	require.True(t, indexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, both, needed, indexedErrCompilation, noWindow),
		"the mismatched leaf makes a compilation rejection legal even with an absent sibling index")
	require.True(t, indexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, both, needed, indexedErrNotReady, noWindow),
		"the absent index keeps a not-ready rejection legal too")
	require.False(t, indexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, both, needed, indexedErrNone, noWindow),
		"results are never legal while a leaf mismatches")

	// Mismatch alone (all needed indexes active): compilation legal, not-ready illegal.
	alone := filterOr(mismatched, mismatched)
	neededAlone := map[string]struct{}{}
	neededIndexCanonicals(alone, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, neededAlone)

	require.True(t, indexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, alone, neededAlone, indexedErrCompilation, noWindow))
	require.False(t, indexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, alone, neededAlone, indexedErrNotReady, noWindow),
		"with every needed index active, a not-ready rejection is unexplained")
}

// fakeStatusClient serves a canned GetIndexStatus response, standing in for one
// node of PerNodeConns. Every other client method panics via the embedded nil
// interface — the poller must not call anything else.
type fakeStatusClient struct {
	servicepb.BucketServiceClient
	resp *servicepb.GetIndexStatusResponse
}

func (f fakeStatusClient) GetIndexStatus(context.Context, *servicepb.GetIndexStatusRequest, ...grpc.CallOption) (*servicepb.GetIndexStatusResponse, error) {
	return f.resp, nil
}

// TestReconcileIndexes_IncarnationGuard pins the readiness poll's binding to
// the sampled incarnation: a node's report counts toward promotion only once
// its indexer has folded past the index's create frontier, and a verdict is
// discarded when the frontier moved while the RPCs were in flight (a
// drop+recreate reused the canonical). Without either gate, a report about a
// dead incarnation promotes its still-building successor and manufactures a
// false "rejected while active" finding.
func TestReconcileIndexes_IncarnationGuard(t *testing.T) {
	t.Parallel()

	id := assetIndexID()
	canon := assetIndexCanonical

	newTracked := func(createSeq uint64) *Checker {
		c := NewChecker([]string{"L"}, nil)
		res := c.modelState.Apply(oracle.Bulk{Requests: []*servicepb.Request{oracletest.CreateIndexReq(id)}})
		require.True(t, res.OK)
		c.modelState = res.State
		c.recordIndexCreates(
			oracle.Bulk{Requests: []*servicepb.Request{oracletest.CreateIndexReq(id)}},
			&servicepb.ApplyResponse{Logs: []*commonpb.Log{{Sequence: createSeq}}},
		)

		return c
	}

	statusConns := func(lastIndexed uint64, version uint32) internal.PerNodeConns {
		return internal.PerNodeConns{{Bucket: fakeStatusClient{resp: &servicepb.GetIndexStatusResponse{
			LastIndexedSequence: lastIndexed,
			Indexes:             []*servicepb.IndexEntry{{Ledger: "L", Index: &commonpb.Index{Id: id}, CurrentVersion: version}},
		}}}}
	}

	active := func(c *Checker) bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		_, a := c.modelState.Ledger("L").IndexState(canon)

		return a
	}

	// Node folded past the create and serves the index live: promote.
	c := newTracked(10)
	reconcileIndexes(context.Background(), c, statusConns(12, 1))
	require.True(t, active(c), "a report about this incarnation promotes")

	// Node still folding toward the create: its live version describes the
	// previous same-canonical incarnation and must not promote this one.
	c = newTracked(10)
	reconcileIndexes(context.Background(), c, statusConns(9, 1))
	require.False(t, active(c), "a pre-create fold cursor must not promote")

	// Frontier moved while the poll was in flight (drop+recreate): the
	// all-ready verdict was gathered against frontier 10, the recreate moved
	// it to 20 before the apply — the verdict belongs to the dead incarnation
	// and is discarded. Drives the apply step directly, since the window sits
	// between reconcileIndexes' own snapshot and its apply.
	c = newTracked(10)
	c.mu.Lock()
	c.indexCreateSeq["L"][canon] = 20 // recreate committed mid-poll
	c.applyIndexReadiness("L", map[string]uint64{canon: 10}, map[string]bool{canon: true})
	c.mu.Unlock()
	require.False(t, active(c), "a moved create frontier discards the verdict")
}
