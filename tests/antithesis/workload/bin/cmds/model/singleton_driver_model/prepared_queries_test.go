package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// roundTripFilters covers every parameterizable arm: address prefix and exact,
// a reference string, a ledger string, a uint range (two-sided, so each bound
// rolls independently), a log-id range, and each FieldCondition value kind.
func roundTripFilters() map[string]*commonpb.QueryFilter {
	lo, hi := int64(-4), int64(9)
	ulo, uhi := uint64(3), uint64(77)

	return map[string]*commonpb.QueryFilter{
		"address prefix": filterAddrPrefix("acc:"),
		"address exact":  filterAddrExact("acc:1"),
		"reference":      filterReference("ref-1"),
		"tx id range":    filterTxIDRange(2, 20),
		"date range": filterDateRange(
			commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, 100, 900),
		"field string": filterFieldString("k1", "v1"),
		"field int":    filterFieldInt("k2", &lo, &hi),
		"field uint":   filterFieldUint("k3", &ulo, &uhi),
		"field bool":   filterFieldBool("k4", true),
		"field exists": filterFieldExists("k5", false),
		"reverted":     filterReverted(true),
		"has asset":    filterHasAsset("USD", 2),
		"nested": filterAnd(
			filterOr(filterAddrPrefix("a:"), filterNot(filterReference("r"))),
			filterFieldUint("k6", &ulo, &uhi),
		),
	}
}

// TestParameterizeSubstituteRoundTrip pins the property the whole prepared-query
// validation rests on: substitution is the exact inverse of parameterization, so
// a stored filter bound with its parameters evaluates identically to the
// concrete filter it was derived from — and the ad-hoc evaluators are reused
// unchanged.
//
// The subset that gets parameterized is random, so each filter is round-tripped
// repeatedly to cover the different subsets.
func TestParameterizeSubstituteRoundTrip(t *testing.T) {
	t.Parallel()

	for name, build := range roundTripFilters() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			for range 32 {
				concrete := build.CloneVT()

				params := preparedParams{}
				stored := parameterizeFilter(concrete, params)

				// Every reference the stored filter carries must be one the
				// parameterization bound — no dangling names.
				for _, need := range collectParams(stored) {
					require.Contains(t, params, need.name)
				}

				back, ok := substituteParams(stored, params)
				require.True(t, ok)
				require.Truef(t, concrete.EqualVT(back),
					"round trip diverged\n stored: %s\n  back:  %s\n  want:  %s",
					describeFilter(stored), describeFilter(back), describeFilter(concrete))
			}
		})
	}
}

// TestParameterizeIsIdentityWithoutParams pins that a filter with no
// parameterizable leaf is returned unchanged and binds nothing.
func TestParameterizeIsIdentityWithoutParams(t *testing.T) {
	t.Parallel()

	for _, f := range []*commonpb.QueryFilter{
		filterReverted(true),
		filterHasAsset("USD", 2),
		filterFieldExists("k1", true),
		nil,
	} {
		params := preparedParams{}
		out := parameterizeFilter(f, params)

		require.Empty(t, params)
		require.Empty(t, collectParams(out))
		require.True(t, f.EqualVT(out))
	}
}

// TestSubstituteParamsRejectsUnresolvable pins the compiler's resolution rules:
// a reference to a parameter that was not supplied, or one supplied with the
// wrong value type, is a rejection — which is what lets a candidate base whose
// stored filter needs a missing parameter predict an error instead of a window.
func TestSubstituteParamsRejectsUnresolvable(t *testing.T) {
	t.Parallel()

	addrParam := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{
		Address: &commonpb.AddressMatch{Match: &commonpb.AddressMatch_ParamPrefix{ParamPrefix: "p0"}},
	}}

	uintParamFilter := filterTxIDRange(0, 0)
	uintParamFilter.GetBuiltinUint().GetCond().Min = nil
	uintParamFilter.GetBuiltinUint().GetCond().ParamMin = "p0"

	for _, tc := range []struct {
		name   string
		filter *commonpb.QueryFilter
		params preparedParams
	}{
		{"missing string param", addrParam, preparedParams{}},
		{"wrong type for string param", addrParam, preparedParams{"p0": uintParam(7)}},
		{"missing uint param", uintParamFilter, preparedParams{}},
		{"wrong type for uint param", uintParamFilter, preparedParams{"p0": stringParam("7")}},
		{"unrelated param supplied", addrParam, preparedParams{"other": stringParam("x")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, ok := substituteParams(tc.filter, tc.params)
			require.False(t, ok)
		})
	}
}

// TestSubstituteParamsResolvesEveryValueKind pins each ParameterValue arm
// against the condition kind that consumes it.
func TestSubstituteParamsResolvesEveryValueKind(t *testing.T) {
	t.Parallel()

	lo, hi := int64(0), int64(0)
	ulo, uhi := uint64(0), uint64(0)

	intFilter := filterFieldInt("k", &lo, &hi)
	intFilter.GetField().GetIntCond().Min = nil
	intFilter.GetField().GetIntCond().ParamMin = "pi"

	uintFilter := filterFieldUint("k", &ulo, &uhi)
	uintFilter.GetField().GetUintCond().Max = nil
	uintFilter.GetField().GetUintCond().ParamMax = "pu"

	boolFilter := filterFieldBool("k", false)
	boolFilter.GetField().GetBoolCond().Value = &commonpb.BoolCondition_Param{Param: "pb"}

	strFilter := filterFieldString("k", "")
	strFilter.GetField().GetStringCond().Value = &commonpb.StringCondition_Param{Param: "ps"}

	params := preparedParams{
		"pi": intParam(-3),
		"pu": uintParam(42),
		"pb": boolParam(true),
		"ps": stringParam("hello"),
	}

	gotInt, ok := substituteParams(intFilter, params)
	require.True(t, ok)
	require.Equal(t, int64(-3), gotInt.GetField().GetIntCond().GetMin())
	require.Empty(t, gotInt.GetField().GetIntCond().GetParamMin())

	gotUint, ok := substituteParams(uintFilter, params)
	require.True(t, ok)
	require.Equal(t, uint64(42), gotUint.GetField().GetUintCond().GetMax())
	require.Empty(t, gotUint.GetField().GetUintCond().GetParamMax())

	gotBool, ok := substituteParams(boolFilter, params)
	require.True(t, ok)
	require.True(t, gotBool.GetField().GetBoolCond().GetHardcoded())

	gotStr, ok := substituteParams(strFilter, params)
	require.True(t, ok)
	require.Equal(t, "hello", gotStr.GetField().GetStringCond().GetHardcoded())
}
