package query_test

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestCompileIDResolution(t *testing.T) {
	t.Parallel()
	store := parityStore(t)
	for _, tc := range []struct {
		name   string
		target commonpb.QueryTarget
		cond   *commonpb.UintCondition
		want   []string
	}{
		{"transaction point", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, &commonpb.UintCondition{ParamMin: "point", ParamMax: "point"}, []string{"103"}},
		{"absent transaction point", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, &commonpb.UintCondition{Min: new(uint64(99)), Max: new(uint64(99))}, []string{}},
		{"transaction exclusive range", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, &commonpb.UintCondition{Min: new(uint64(101)), Max: new(uint64(105)), MinExclusive: true, MaxExclusive: true}, []string{"102", "103", "104"}},
		{"log point", commonpb.QueryTarget_QUERY_TARGET_LOGS, &commonpb.UintCondition{ParamMin: "point", ParamMax: "point"}, []string{"203"}},
		{"absent log point", commonpb.QueryTarget_QUERY_TARGET_LOGS, &commonpb.UintCondition{Min: new(uint64(199)), Max: new(uint64(199))}, []string{}},
		{"log exclusive range", commonpb.QueryTarget_QUERY_TARGET_LOGS, &commonpb.UintCondition{Min: new(uint64(201)), Max: new(uint64(205)), MinExclusive: true, MaxExclusive: true}, []string{"202", "203", "204"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			options := leafResolutionOptions(store, tc.target)
			point := uint64(103)
			filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, Cond: tc.cond}}}
			if tc.target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
				point = 203
				filter = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{LogId: &commonpb.LogIdCondition{Cond: tc.cond}}}
			}
			options.Parameters = map[string]*commonpb.ParameterValue{"point": {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: point}}}
			assertLeafResolutionEntities(t, filter, options, tc.want)
		})
	}
}
