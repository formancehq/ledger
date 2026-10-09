package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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
