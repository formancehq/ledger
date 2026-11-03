package query_test

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Pin identities independently of either compiler: both directions will share
// the resolver, so agreement alone would miss a wrong key or parameter value.
func TestCompileMetadataResolution(t *testing.T) {
	t.Parallel()
	store := parityStore(t)
	for _, tc := range []struct {
		name   string
		filter *commonpb.QueryFilter
		params map[string]*commonpb.ParameterValue
		want   []string
	}{
		{"string literal", stringFieldFilter("colour", "blue"), nil, []string{"accounts:00", "accounts:03", "accounts:06", "accounts:09"}},
		{"string parameter", accountFieldFilter("colour", &commonpb.FieldCondition{Field: &commonpb.FieldRef{Metadata: "colour"}, Condition: &commonpb.FieldCondition_StringCond{StringCond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Param{Param: "colour"}}}}), map[string]*commonpb.ParameterValue{"colour": {Value: &commonpb.ParameterValue_StringValue{StringValue: "blue"}}}, []string{"accounts:00", "accounts:03", "accounts:06", "accounts:09"}},
		{"bool false literal", boolFieldFilter("flag", false), nil, []string{"accounts:01", "accounts:03", "accounts:05", "accounts:07", "accounts:09", "accounts:11"}},
		{"bool false parameter", accountFieldFilter("flag", &commonpb.FieldCondition{Field: &commonpb.FieldRef{Metadata: "flag"}, Condition: &commonpb.FieldCondition_BoolCond{BoolCond: &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Param{Param: "flag"}}}}), map[string]*commonpb.ParameterValue{"flag": {Value: &commonpb.ParameterValue_BoolValue{BoolValue: false}}}, []string{"accounts:01", "accounts:03", "accounts:05", "accounts:07", "accounts:09", "accounts:11"}},
		{"exists non-null", existsFieldFilter("note"), nil, []string{"accounts:00", "accounts:02", "accounts:03", "accounts:04", "accounts:06", "accounts:07", "accounts:08", "accounts:10", "accounts:11"}},
		{"exists including null", existsWithNullFieldFilter("note"), nil, []string{"accounts:00", "accounts:01", "accounts:02", "accounts:03", "accounts:04", "accounts:05", "accounts:06", "accounts:07", "accounts:08", "accounts:09", "accounts:10", "accounts:11"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			options := leafResolutionOptions(store, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
			options.Schema = paritySchema()
			options.Parameters = tc.params
			assertLeafResolutionEntities(t, tc.filter, options, tc.want)
		})
	}
}
