package commonpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsTextualFilterParamName(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"n", "ref", "_x", "x_1", "andy", "Trueish"} {
		require.True(t, IsTextualFilterParamName(name), name)
	}

	invalid := []string{"", "1n", "a-b", "a.b", "a b", "n or reverted == true", "USD/2", "é"}
	invalid = append(invalid, FilterGrammarKeywords...)
	for _, name := range invalid {
		require.False(t, IsTextualFilterParamName(name), name)
	}
}

// TestFilterLeafParamNamesCoversEveryParamField pins that the collector reads
// each condition kind that can carry a parameter, so the admission gate cannot
// miss one.
func TestFilterLeafParamNamesCoversEveryParamField(t *testing.T) {
	t.Parallel()

	uc := &UintCondition{ParamMin: "lo", ParamMax: "hi"}
	cases := map[string]*QueryFilter{
		"string field":   {Filter: &QueryFilter_Field{Field: &FieldCondition{Condition: &FieldCondition_StringCond{StringCond: &StringCondition{Value: &StringCondition_Param{Param: "p"}}}}}},
		"bool field":     {Filter: &QueryFilter_Field{Field: &FieldCondition{Condition: &FieldCondition_BoolCond{BoolCond: &BoolCondition{Value: &BoolCondition_Param{Param: "p"}}}}}},
		"int field":      {Filter: &QueryFilter_Field{Field: &FieldCondition{Condition: &FieldCondition_IntCond{IntCond: &IntCondition{ParamMin: "lo", ParamMax: "hi"}}}}},
		"uint field":     {Filter: &QueryFilter_Field{Field: &FieldCondition{Condition: &FieldCondition_UintCond{UintCond: uc}}}},
		"address exact":  {Filter: &QueryFilter_Address{Address: &AddressMatch{Match: &AddressMatch_ParamExact{ParamExact: "p"}}}},
		"address prefix": {Filter: &QueryFilter_Address{Address: &AddressMatch{Match: &AddressMatch_ParamPrefix{ParamPrefix: "p"}}}},
		"reference":      {Filter: &QueryFilter_Reference{Reference: &ReferenceCondition{Cond: &StringCondition{Value: &StringCondition_Param{Param: "p"}}}}},
		"ledger":         {Filter: &QueryFilter_Ledger{Ledger: &LedgerCondition{Cond: &StringCondition{Value: &StringCondition_Param{Param: "p"}}}}},
		"builtin uint":   {Filter: &QueryFilter_BuiltinUint{BuiltinUint: &BuiltinUintCondition{Cond: uc}}},
		"log builtin":    {Filter: &QueryFilter_LogBuiltinUint{LogBuiltinUint: &LogBuiltinUintCondition{Cond: uc}}},
		"log id":         {Filter: &QueryFilter_LogId{LogId: &LogIdCondition{Cond: uc}}},
	}

	for name, f := range cases {
		require.NotEmpty(t, FilterLeafParamNames(f), name)
	}
}

// TestFilterLeafParamNamesReportsEmptyParamArms pins that a param oneof arm set
// with an empty name is still reported, so admission can refuse it.
func TestFilterLeafParamNamesReportsEmptyParamArms(t *testing.T) {
	t.Parallel()

	cases := map[string]*QueryFilter{
		"string":  {Filter: &QueryFilter_Reference{Reference: &ReferenceCondition{Cond: &StringCondition{Value: &StringCondition_Param{}}}}},
		"bool":    {Filter: &QueryFilter_Field{Field: &FieldCondition{Condition: &FieldCondition_BoolCond{BoolCond: &BoolCondition{Value: &BoolCondition_Param{}}}}}},
		"address": {Filter: &QueryFilter_Address{Address: &AddressMatch{Match: &AddressMatch_ParamPrefix{}}}},
	}

	for name, f := range cases {
		require.Equal(t, []string{""}, FilterLeafParamNames(f), name)
	}

	literal := &QueryFilter{Filter: &QueryFilter_LogId{LogId: &LogIdCondition{Cond: &UintCondition{Min: new(uint64(1))}}}}
	require.Empty(t, FilterLeafParamNames(literal))
}
