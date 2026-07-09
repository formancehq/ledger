package commonpb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func u64(v uint64) *uint64 { return new(v) }
func i64(v int64) *int64   { return new(v) }

// TestQueryFilterRoundTrip marshals each proto variant to JSON and unmarshals
// it back, asserting the proto is preserved. It covers every arm of the
// QueryFilter oneof plus every nested condition, so a new proto arm without a
// codec update surfaces as a failing round-trip (marshal errors loudly).
func TestQueryFilterRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		filter *QueryFilter
	}{
		{
			name: "field/string hardcoded",
			filter: &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{
				Field:     &FieldRef{Metadata: "tier"},
				Condition: &FieldCondition_StringCond{StringCond: &StringCondition{Value: &StringCondition_Hardcoded{Hardcoded: "gold"}}},
			}}},
		},
		{
			name: "field/string param",
			filter: &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{
				Field:     &FieldRef{Metadata: "tier"},
				Condition: &FieldCondition_StringCond{StringCond: &StringCondition{Value: &StringCondition_Param{Param: "p"}}},
			}}},
		},
		{
			name: "field/int range exclusive + params",
			filter: &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{
				Field: &FieldRef{Metadata: "score"},
				Condition: &FieldCondition_IntCond{IntCond: &IntCondition{
					Min: i64(-5), Max: i64(10), MinExclusive: true, MaxExclusive: true, ParamMin: "lo", ParamMax: "hi",
				}},
			}}},
		},
		{
			name: "field/uint range (large value stays lossless)",
			filter: &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{
				Field: &FieldRef{Metadata: "count"},
				Condition: &FieldCondition_UintCond{UintCond: &UintCondition{
					Min: u64(18446744073709551615), Max: u64(9007199254740993),
				}},
			}}},
		},
		{
			name: "field/bool hardcoded",
			filter: &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{
				Field:     &FieldRef{Metadata: "vip"},
				Condition: &FieldCondition_BoolCond{BoolCond: &BoolCondition{Value: &BoolCondition_Hardcoded{Hardcoded: true}}},
			}}},
		},
		{
			name: "field/bool param",
			filter: &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{
				Field:     &FieldRef{Metadata: "vip"},
				Condition: &FieldCondition_BoolCond{BoolCond: &BoolCondition{Value: &BoolCondition_Param{Param: "b"}}},
			}}},
		},
		{
			name: "field/exists includeNull",
			filter: &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{
				Field:     &FieldRef{Metadata: "opt"},
				Condition: &FieldCondition_ExistsCond{ExistsCond: &ExistsCondition{IncludeNull: true}},
			}}},
		},
		{
			name:   "address/hardcoded prefix with role",
			filter: &QueryFilter{Filter: &QueryFilter_Address{Address: &AddressMatch{Match: &AddressMatch_HardcodedPrefix{HardcodedPrefix: "acc:"}, Role: AddressRole_ADDRESS_ROLE_SOURCE}}},
		},
		{
			name:   "address/hardcoded exact",
			filter: &QueryFilter{Filter: &QueryFilter_Address{Address: &AddressMatch{Match: &AddressMatch_HardcodedExact{HardcodedExact: "acc:1"}}}},
		},
		{
			name:   "address/param prefix destination",
			filter: &QueryFilter{Filter: &QueryFilter_Address{Address: &AddressMatch{Match: &AddressMatch_ParamPrefix{ParamPrefix: "p"}, Role: AddressRole_ADDRESS_ROLE_DESTINATION}}},
		},
		{
			name:   "address/param exact",
			filter: &QueryFilter{Filter: &QueryFilter_Address{Address: &AddressMatch{Match: &AddressMatch_ParamExact{ParamExact: "p"}}}},
		},
		{
			name:   "reference",
			filter: &QueryFilter{Filter: &QueryFilter_Reference{Reference: &ReferenceCondition{Cond: &StringCondition{Value: &StringCondition_Hardcoded{Hardcoded: "ref-1"}}}}},
		},
		{
			name:   "ledger",
			filter: &QueryFilter{Filter: &QueryFilter_Ledger{Ledger: &LedgerCondition{Cond: &StringCondition{Value: &StringCondition_Hardcoded{Hardcoded: "main"}}}}},
		},
		{
			name:   "logId range",
			filter: &QueryFilter{Filter: &QueryFilter_LogId{LogId: &LogIdCondition{Cond: &UintCondition{Min: u64(1), Max: u64(100)}}}},
		},
		{
			name:   "builtinUint/timestamp",
			filter: &QueryFilter{Filter: &QueryFilter_BuiltinUint{BuiltinUint: &BuiltinUintCondition{Field: TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, Cond: &UintCondition{Min: u64(1700000000)}}}},
		},
		{
			name:   "builtinUint/revertedAt",
			filter: &QueryFilter{Filter: &QueryFilter_BuiltinUint{BuiltinUint: &BuiltinUintCondition{Field: TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT, Cond: &UintCondition{Max: u64(2000000000)}}}},
		},
		{
			name:   "logBuiltinUint/date",
			filter: &QueryFilter{Filter: &QueryFilter_LogBuiltinUint{LogBuiltinUint: &LogBuiltinUintCondition{Field: LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, Cond: &UintCondition{Min: u64(1)}}}},
		},
		{
			name:   "accountHasAsset",
			filter: &QueryFilter{Filter: &QueryFilter_AccountHasAsset{AccountHasAsset: &AccountHasAssetCondition{AssetBase: "USD", Precision: 2}}},
		},
		{
			name:   "reverted",
			filter: &QueryFilter{Filter: &QueryFilter_Reverted{Reverted: &RevertedCondition{Value: true}}},
		},
		{
			name: "and/or/not nested combinators",
			filter: &QueryFilter{Filter: &QueryFilter_And{And: &AndFilter{Filters: []*QueryFilter{
				{Filter: &QueryFilter_Or{Or: &OrFilter{Filters: []*QueryFilter{
					{Filter: &QueryFilter_Reverted{Reverted: &RevertedCondition{Value: false}}},
					{Filter: &QueryFilter_Address{Address: &AddressMatch{Match: &AddressMatch_HardcodedPrefix{HardcodedPrefix: "world"}}}},
				}}}},
				{Filter: &QueryFilter_Not{Not: &NotFilter{Filter: &QueryFilter{
					Filter: &QueryFilter_AccountHasAsset{AccountHasAsset: &AccountHasAssetCondition{AssetBase: "EUR"}},
				}}}},
			}}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := json.Marshal(tc.filter)
			require.NoError(t, err)

			// The marshalled JSON must not leak any protobuf-internal name.
			for _, leak := range []string{
				`"filters"`, `"stringCond"`, `"intCond"`, `"uintCond"`, `"boolCond"`,
				`"existsCond"`, `"hardcoded"`, `"hardcodedPrefix"`, `"hardcodedExact"`,
				`"paramPrefix"`, `"paramExact"`, `"builtinUint":{`, "TX_BUILTIN_INDEX",
				"LOG_BUILTIN_INDEX", "ADDRESS_ROLE",
			} {
				require.NotContains(t, string(data), leak, "protojson-internal name leaked: %s", leak)
			}

			got := &QueryFilter{}
			require.NoError(t, json.Unmarshal(data, got))
			require.True(t, proto.Equal(tc.filter, got), "round-trip mismatch\n json: %s\n want: %v\n got:  %v", data, tc.filter, got)
		})
	}
}

// TestQueryFilterUnmarshalErrors covers the loud-failure paths.
func TestQueryFilterUnmarshalErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{"empty object", `{}`, "exactly one of"},
		{"two combinators", `{"and":[],"or":[]}`, "exactly one of"},
		{"combinator + match", `{"not":{"match":{"type":"reverted","value":true}},"match":{"type":"reverted","value":true}}`, "exactly one of"},
		{"match missing type", `{"match":{}}`, "missing discriminator"},
		{"match unknown type", `{"match":{"type":"nope"}}`, "unknown condition type"},
		{"field missing metadata", `{"match":{"type":"field","condition":{"type":"exists"}}}`, "metadata is required"},
		{"field missing condition", `{"match":{"type":"field","metadata":"x"}}`, "condition is required"},
		{"field unknown condition type", `{"match":{"type":"field","metadata":"x","condition":{"type":"nope"}}}`, "unknown type"},
		{"address missing operator", `{"match":{"type":"address","value":"x"}}`, "operator is required"},
		{"address unknown operator", `{"match":{"type":"address","operator":"contains"}}`, "unknown operator"},
		{"address unknown role", `{"match":{"type":"address","operator":"exact","value":"x","role":"middle"}}`, "unknown role"},
		{"builtinUint unknown field", `{"match":{"type":"builtinUint","field":"bogus"}}`, "unknown field"},
		{"accountHasAsset missing assetBase", `{"match":{"type":"accountHasAsset"}}`, "assetBase is required"},
		{"string cond both equals and param", `{"match":{"type":"field","metadata":"x","condition":{"type":"string","equals":"a","param":"b"}}}`, "only one of equals or param"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := json.Unmarshal([]byte(tc.raw), &QueryFilter{})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestPreparedQueryMarshalJSON asserts the response side emits the canonical
// flat filter and the string target enum, with no protojson leakage.
func TestPreparedQueryMarshalJSON(t *testing.T) {
	t.Parallel()

	pq := &PreparedQuery{
		Name:   "vip",
		Target: QueryTarget_QUERY_TARGET_TRANSACTIONS,
		Filter: &QueryFilter{Filter: &QueryFilter_Reference{Reference: &ReferenceCondition{
			Cond: &StringCondition{Value: &StringCondition_Hardcoded{Hardcoded: "ref-1"}},
		}}},
	}

	data, err := json.Marshal(pq)
	require.NoError(t, err)

	var out struct {
		Name   string          `json:"name"`
		Target string          `json:"target"`
		Filter json.RawMessage `json:"filter"`
	}
	require.NoError(t, json.Unmarshal(data, &out))
	require.Equal(t, "vip", out.Name)
	require.Equal(t, "TRANSACTIONS", out.Target)

	// Filter must decode back through the canonical codec.
	rt := &QueryFilter{}
	require.NoError(t, json.Unmarshal(out.Filter, rt))
	require.True(t, proto.Equal(pq.GetFilter(), rt))
}
