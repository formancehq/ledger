package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

var listTargets = []commonpb.QueryTarget{
	commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
	commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
	commonpb.QueryTarget_QUERY_TARGET_LOGS,
}

// Every rolled shape must survive the wire unchanged in what the write-time
// validation says about it, and every model function a query or a save goes
// through must take it without panicking. A refused filter never reaches the
// window functions, so each target's window only sees what that target accepts.
func TestAnyFilterShapeIsTotal(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReq("world", "acc:1", "USD/2", 1),
		oracletest.AddAccountMetaReq("acc:1", "k0", commonpb.NewStringValue("x")),
	)
	kinds := map[string]bool{}

	for range 5000 {
		f := mutateFilter(genAccountFilter(nil))
		if oneIn(2) {
			f = genAnyFilter(0)
		}

		raw, err := f.MarshalVT()
		require.NoError(t, err)
		wire := &commonpb.QueryFilter{}
		require.NoError(t, wire.UnmarshalVT(raw))

		for _, target := range listTargets {
			local := domain.ValidateFilterForTarget(f, target)
			remote := domain.ValidateFilterForTarget(wire, target)
			require.Equal(t, local == nil, remote == nil, "filter %s target %v", describeFilter(f), target)
			kinds[map[bool]string{true: "accepted", false: "refused"}[local == nil]] = true

			neededIndexCanonicals(f, target, map[string]struct{}{})
			if classifyRejectedFilter(f, target).rejected {
				continue
			}

			switch target {
			case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
				accountWindow(ls, f, "", 10, oneIn(2))
			case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
				transactionWindowRows(ls, f, 0, oneIn(2))
			default:
				logWindowRows(ls, "L", f, 0, oneIn(2))
			}
		}

		describeFilter(f)
		collectParams(f)
		substituteParams(f, preparedParams{"p0": uintParam(1), "p1": stringParam("a")})
		parameterizeFilter(f, preparedParams{})
		neededLogIndexes(f)
	}

	require.True(t, kinds["accepted"] && kinds["refused"], "the walker must roll both accepted and refused shapes")
}

func TestClassifyRejectedFilter(t *testing.T) {
	t.Parallel()

	accounts := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	k0 := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0"))
	broken := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{}}}
	param := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_ParamPrefix{ParamPrefix: "p0"},
	}}}

	require.False(t, classifyRejectedFilter(filterAnd(filterMetaExists("k0"), filterAddrPrefix("a:")), accounts).rejected)

	rf := classifyRejectedFilter(filterAnd(filterMetaExists("k0"), broken), accounts)
	require.True(t, rf.rejected)
	require.Contains(t, rf.prior, k0, "an index compiled before the refused leaf may answer not-ready first")

	rf = classifyRejectedFilter(filterAnd(broken, filterMetaExists("k0")), accounts)
	require.True(t, rf.rejected)
	require.Empty(t, rf.prior, "nothing is compiled before the refused leaf")

	require.True(t, classifyRejectedFilter(param, accounts).rejected, "a list query never binds a parameter")
	require.True(t, classifyRejectedFilter(&commonpb.QueryFilter{}, accounts).rejected)
	require.True(t, classifyRejectedFilter(filterHasAsset("USD", 256), accounts).rejected)
	require.True(t, classifyRejectedFilter(filterNot(filterReverted(true)), accounts).rejected)
}

// A has-asset leaf selects accounts whose volume rows are gone; every other arm,
// and the complement a Not takes, selects only current accounts.
func TestHasAssetComposesOverCurrentAccounts(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t, oracletest.TxReq("world", "acc:1", "USD/2", 1))

	require.True(t, matchAccountFilterIn(ls, filterHasAsset("USD", 2), "gone:1", false) == ls.HasEverAsset("gone:1", "USD", 2))
	require.False(t, matchAccountFilterIn(ls, filterNot(filterHasAsset("EUR", 2)), "gone:1", false))
	require.False(t, matchAccountFilterIn(ls, filterAddrPrefix("gone:"), "gone:1", false))
	require.False(t, matchAccountFilterIn(ls, filterAnd(), "gone:1", false))
	require.True(t, matchAccountFilterIn(ls, filterAnd(), "acc:1", true))
	require.Equal(t, []string{"acc:1"}, accountWindow(ls, filterAnd(filterHasAsset("USD", 2), filterAddrPrefix("acc:")), "", 10, false))
	require.Equal(t, []string{"world"}, accountWindow(ls, filterAnd(filterHasAsset("USD", 2), filterNot(filterAddrPrefix("acc:"))), "", 10, false))
}

// A leaf compiled before the refused one may answer not-ready while its index
// is not active, or while its key's retype window is open even though its index
// is active — but only when the refusal names that index.
func TestRefusedFilterAcceptsAWindowedEarlierLeaf(t *testing.T) {
	t.Parallel()

	accounts := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0")
	canon := indexes.Canonical(id)

	gs := oracle.NewGlobalState()
	for _, req := range []*servicepb.Request{
		oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0", commonpb.MetadataType_METADATA_TYPE_STRING),
		oracletest.CreateIndexReq(id),
	} {
		res := gs.Apply(oracle.Bulk{Requests: []*servicepb.Request{req}})
		require.True(t, res.OK, res.Reason)
		gs = res.State
	}

	broken := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{}}}
	rf := classifyRejectedFilter(filterAnd(filterMetaExists("k0"), broken), accounts)
	require.Equal(t, priorIndex{label: `metadata["k0"] on accounts`, metadata: true}, rf.prior[canon])

	building := func(index string) error {
		st, err := status.New(codes.Unavailable, "index is building").WithDetails(&errdetails.ErrorInfo{
			Domain: "ledger", Reason: "INDEX_BUILDING", Metadata: map[string]string{"index": index},
		})
		require.NoError(t, err)

		return st.Err()
	}
	own := building(`metadata["k0"] on accounts`)
	sibling := building(`metadata["k9"] on accounts`)

	require.True(t, refusedFilterNotReadyLegal(gs.Ledger("L"), rf, accounts, own), "the not-yet-active index may refuse")
	require.False(t, refusedFilterNotReadyLegal(gs.Ledger("L"), rf, accounts, sibling), "a not-active prior does not excuse a refusal naming another index")
	require.False(t, refusedFilterNotReadyLegal(gs.Ledger("L"), rf, accounts, building("has asset")), "nor one naming a builtin")

	gs.SetIndexActive("L", canon)
	require.False(t, refusedFilterNotReadyLegal(gs.Ledger("L"), rf, accounts, own), "active and no window: the refusal is illegal")

	res := gs.Apply(oracle.Bulk{Requests: []*servicepb.Request{
		oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0", commonpb.MetadataType_METADATA_TYPE_INT64),
	}})
	require.True(t, res.OK, res.Reason)
	windowed := res.State.Ledger("L")
	_, open := windowed.RetypeWindow(canon)
	require.True(t, open)

	require.True(t, refusedFilterNotReadyLegal(windowed, rf, accounts, own), "the windowed index may refuse")
	require.False(t, refusedFilterNotReadyLegal(windowed, rf, accounts, sibling), "a refusal naming another index stays illegal")
}
