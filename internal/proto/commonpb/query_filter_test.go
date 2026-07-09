package commonpb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func u64(v uint64) *uint64 {
	c := v

	return &c
}

func i64(v int64) *int64 {
	c := v

	return &c
}

// TestQueryFilterRoundTrip marshals each proto variant to the v2-aligned DSL and
// unmarshals it back, asserting the proto is preserved. It covers every arm of
// the QueryFilter oneof plus nested conditions, params, and closed ranges, so a
// new proto arm without a codec update surfaces as a failing round-trip.
func TestQueryFilterRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		filter *QueryFilter
		// wantJSON, when set, asserts the exact marshalled shape.
		wantJSON string
	}{
		{
			name:     "metadata string ==",
			filter:   fieldFilter("tier", &FieldCondition_StringCond{StringCond: hardcoded("gold")}),
			wantJSON: `{"$match":{"metadata[tier]":"gold"}}`,
		},
		{
			name:     "metadata string param",
			filter:   fieldFilter("tier", &FieldCondition_StringCond{StringCond: param("p")}),
			wantJSON: `{"$match":{"metadata[tier]":{"$param":"p"}}}`,
		},
		{
			name:     "metadata bool",
			filter:   fieldFilter("vip", &FieldCondition_BoolCond{BoolCond: &BoolCondition{Value: &BoolCondition_Hardcoded{Hardcoded: true}}}),
			wantJSON: `{"$match":{"metadata[vip]":true}}`,
		},
		{
			name:     "metadata exists",
			filter:   fieldFilter("opt", &FieldCondition_ExistsCond{ExistsCond: &ExistsCondition{}}),
			wantJSON: `{"$exists":{"metadata":"opt"}}`,
		},
		{
			name:     "metadata int single lower bound (gte)",
			filter:   fieldFilter("score", &FieldCondition_IntCond{IntCond: &IntCondition{Min: i64(10)}}),
			wantJSON: `{"$gte":{"metadata[score]":10}}`,
		},
		{
			name:   "metadata int closed range exclusive",
			filter: fieldFilter("score", &FieldCondition_IntCond{IntCond: &IntCondition{Min: i64(-5), Max: i64(10), MinExclusive: true, MaxExclusive: true}}),
		},
		{
			name:   "metadata int range with params",
			filter: fieldFilter("score", &FieldCondition_IntCond{IntCond: &IntCondition{ParamMin: "lo", ParamMax: "hi"}}),
		},
		{
			name:     "metadata uint single bound (stays uint, lossless)",
			filter:   fieldFilter("count", &FieldCondition_UintCond{UintCond: &UintCondition{Min: u64(9007199254740993)}}),
			wantJSON: `{"$gte":{"metadata[count]":"9007199254740993"}}`,
		},
		{
			name:   "metadata uint closed range",
			filter: fieldFilter("count", &FieldCondition_UintCond{UintCond: &UintCondition{Min: u64(1), Max: u64(9)}}),
		},
		{
			name:     "address hardcoded prefix",
			filter:   addressFilter(&AddressMatch_HardcodedPrefix{HardcodedPrefix: "users:"}, AddressRole_ADDRESS_ROLE_ANY),
			wantJSON: `{"$match":{"address":"users:"}}`,
		},
		{
			name:     "address hardcoded exact",
			filter:   addressFilter(&AddressMatch_HardcodedExact{HardcodedExact: "acc:1"}, AddressRole_ADDRESS_ROLE_ANY),
			wantJSON: `{"$match":{"address":"acc:1"}}`,
		},
		{
			name:     "source hardcoded prefix",
			filter:   addressFilter(&AddressMatch_HardcodedPrefix{HardcodedPrefix: "banks:"}, AddressRole_ADDRESS_ROLE_SOURCE),
			wantJSON: `{"$match":{"source":"banks:"}}`,
		},
		{
			name:     "destination param exact",
			filter:   addressFilter(&AddressMatch_ParamExact{ParamExact: "dst"}, AddressRole_ADDRESS_ROLE_DESTINATION),
			wantJSON: `{"$match":{"destination":{"$param":"dst"}}}`,
		},
		{
			name:     "address param prefix (like)",
			filter:   addressFilter(&AddressMatch_ParamPrefix{ParamPrefix: "acc"}, AddressRole_ADDRESS_ROLE_ANY),
			wantJSON: `{"$like":{"address":{"$param":"acc"}}}`,
		},
		{
			// A hardcoded prefix without a trailing ":" cannot use the $match
			// trailing-colon convention (it would look like an exact match), so it
			// is carried via $like to stay lossless.
			name:     "address hardcoded prefix without colon (like)",
			filter:   addressFilter(&AddressMatch_HardcodedPrefix{HardcodedPrefix: "users"}, AddressRole_ADDRESS_ROLE_ANY),
			wantJSON: `{"$like":{"address":"users"}}`,
		},
		{
			name:     "reference",
			filter:   &QueryFilter{Filter: &QueryFilter_Reference{Reference: &ReferenceCondition{Cond: hardcoded("ref-1")}}},
			wantJSON: `{"$match":{"reference":"ref-1"}}`,
		},
		{
			name:     "ledger",
			filter:   &QueryFilter{Filter: &QueryFilter_Ledger{Ledger: &LedgerCondition{Cond: hardcoded("main")}}},
			wantJSON: `{"$match":{"ledger":"main"}}`,
		},
		{
			name:     "reverted true",
			filter:   &QueryFilter{Filter: &QueryFilter_Reverted{Reverted: &RevertedCondition{Value: true}}},
			wantJSON: `{"$match":{"reverted":true}}`,
		},
		{
			name:     "reverted false",
			filter:   &QueryFilter{Filter: &QueryFilter_Reverted{Reverted: &RevertedCondition{Value: false}}},
			wantJSON: `{"$match":{"reverted":false}}`,
		},
		{
			name:     "accountHasAsset with precision",
			filter:   &QueryFilter{Filter: &QueryFilter_AccountHasAsset{AccountHasAsset: &AccountHasAssetCondition{AssetBase: "USD", Precision: 2}}},
			wantJSON: `{"$exists":{"asset":"USD/2"}}`,
		},
		{
			name:     "accountHasAsset no precision",
			filter:   &QueryFilter{Filter: &QueryFilter_AccountHasAsset{AccountHasAsset: &AccountHasAssetCondition{AssetBase: "USD"}}},
			wantJSON: `{"$exists":{"asset":"USD"}}`,
		},
		{
			name:     "logId single upper bound (lt)",
			filter:   &QueryFilter{Filter: &QueryFilter_LogId{LogId: &LogIdCondition{Cond: &UintCondition{Max: u64(100), MaxExclusive: true}}}},
			wantJSON: `{"$lt":{"logId":"100"}}`,
		},
		{
			name:   "builtinUint timestamp closed range",
			filter: &QueryFilter{Filter: &QueryFilter_BuiltinUint{BuiltinUint: &BuiltinUintCondition{Field: TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, Cond: &UintCondition{Min: u64(1700000000), Max: u64(1800000000)}}}},
		},
		{
			name:     "builtinUint id single (gte, large uint stays lossless)",
			filter:   &QueryFilter{Filter: &QueryFilter_BuiltinUint{BuiltinUint: &BuiltinUintCondition{Field: TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, Cond: &UintCondition{Min: u64(9007199254740993)}}}},
			wantJSON: `{"$gte":{"id":"9007199254740993"}}`,
		},
		{
			name:     "builtinUint revertedAt param bound",
			filter:   &QueryFilter{Filter: &QueryFilter_BuiltinUint{BuiltinUint: &BuiltinUintCondition{Field: TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT, Cond: &UintCondition{ParamMax: "before"}}}},
			wantJSON: `{"$lte":{"revertedAt":{"$param":"before"}}}`,
		},
		{
			name:   "logBuiltinUint date closed range",
			filter: &QueryFilter{Filter: &QueryFilter_LogBuiltinUint{LogBuiltinUint: &LogBuiltinUintCondition{Field: LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, Cond: &UintCondition{Min: u64(1), Max: u64(999)}}}},
		},
		{
			name: "nested and/or/not",
			filter: &QueryFilter{Filter: &QueryFilter_And{And: &AndFilter{Filters: []*QueryFilter{
				{Filter: &QueryFilter_Or{Or: &OrFilter{Filters: []*QueryFilter{
					{Filter: &QueryFilter_Reverted{Reverted: &RevertedCondition{Value: false}}},
					addressFilter(&AddressMatch_HardcodedPrefix{HardcodedPrefix: "world:"}, AddressRole_ADDRESS_ROLE_ANY),
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

			if tc.wantJSON != "" {
				require.JSONEq(t, tc.wantJSON, string(data))
			}

			// No protobuf-internal name may leak.
			for _, leak := range []string{
				`"filters"`, `"stringCond"`, `"intCond"`, `"uintCond"`, `"boolCond"`,
				`"existsCond"`, `"hardcoded"`, `"hardcodedPrefix"`, `"hardcodedExact"`,
				`"paramPrefix"`, `"paramExact"`, "TX_BUILTIN_INDEX", "LOG_BUILTIN_INDEX",
				"ADDRESS_ROLE", `"assetBase"`,
			} {
				require.NotContains(t, string(data), leak, "protobuf-internal name leaked: %s", leak)
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
		{"empty object", `{}`, "empty object"},
		{"two operators", `{"$and":[],"$or":[]}`, "exactly one operator"},
		{"unknown operator", `{"$bogus":{}}`, "unknown operator"},
		{"empty and", `{"$and":[]}`, "must contain at least one filter"},
		{"empty or", `{"$or":[]}`, "must contain at least one filter"},
		{"and not array", `{"$and":{}}`, "expected an array"},
		{"match empty body", `{"$match":{}}`, "expected exactly one field"},
		{"match two fields", `{"$match":{"a":1,"b":2}}`, "expected exactly one field"},
		{"match unsupported field", `{"$match":{"bogus":"x"}}`, "unsupported field"},
		{"exists unsupported field", `{"$exists":{"bogus":"x"}}`, "unsupported field"},
		{"exists metadata empty key", `{"$exists":{"metadata":""}}`, "key is required"},
		{"exists asset empty", `{"$exists":{"asset":""}}`, "assetRef is required"},
		{"exists with param", `{"$exists":{"metadata":{"$param":"p"}}}`, "must be a literal"},
		{"param empty name", `{"$match":{"reference":{"$param":""}}}`, "name is required"},
		{"param not sole key", `{"$match":{"reference":{"$param":"p","x":1}}}`, "must be the only key"},
		{"range unsupported field", `{"$gt":{"bogus":1}}`, "unsupported field"},
		{"range on reference rejected", `{"$gt":{"reference":"x"}}`, "unsupported field"},
		{"range on address rejected", `{"$gt":{"sourceAddress":"1"}}`, "unsupported field"},
		{"in unsupported", `{"$in":{"address":["a"]}}`, "not supported"},
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

// TestQueryFilterClosedRangeFolding asserts that an $and of two same-field bounds
// folds into a single range condition (and non-range $and stays an AND).
func TestQueryFilterClosedRangeFolding(t *testing.T) {
	t.Parallel()

	// Closed timestamp range -> single BuiltinUintCondition.
	got := &QueryFilter{}
	require.NoError(t, json.Unmarshal([]byte(`{"$and":[{"$gte":{"timestamp":"1"}},{"$lte":{"timestamp":"9"}}]}`), got))
	bu := got.GetBuiltinUint()
	require.NotNil(t, bu, "closed range must fold into a single BuiltinUintCondition")
	require.Equal(t, TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, bu.GetField())
	require.Equal(t, uint64(1), bu.GetCond().GetMin())
	require.Equal(t, uint64(9), bu.GetCond().GetMax())

	// Different fields -> stays an AND (no fold).
	got2 := &QueryFilter{}
	require.NoError(t, json.Unmarshal([]byte(`{"$and":[{"$gte":{"timestamp":"1"}},{"$lte":{"id":"9"}}]}`), got2))
	require.NotNil(t, got2.GetAnd(), "different-field bounds must stay an AND")
	require.Len(t, got2.GetAnd().GetFilters(), 2)
}

// --- test helpers ---

func fieldFilter(key string, cond isFieldCondition_Condition) *QueryFilter {
	return &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{
		Field:     &FieldRef{Metadata: key},
		Condition: cond,
	}}}
}

func addressFilter(match isAddressMatch_Match, role AddressRole) *QueryFilter {
	return &QueryFilter{Filter: &QueryFilter_Address{Address: &AddressMatch{Match: match, Role: role}}}
}

func hardcoded(v string) *StringCondition {
	return &StringCondition{Value: &StringCondition_Hardcoded{Hardcoded: v}}
}

func param(name string) *StringCondition {
	return &StringCondition{Value: &StringCondition_Param{Param: name}}
}

// TestPreparedQueryMarshalJSON asserts the response side emits the v2 filter and
// the string target enum, with no protojson leakage.
func TestPreparedQueryMarshalJSON(t *testing.T) {
	t.Parallel()

	pq := &PreparedQuery{
		Name:   "vip",
		Target: QueryTarget_QUERY_TARGET_TRANSACTIONS,
		Filter: &QueryFilter{Filter: &QueryFilter_Reference{Reference: &ReferenceCondition{
			Cond: hardcoded("ref-1"),
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
	require.JSONEq(t, `{"$match":{"reference":"ref-1"}}`, string(out.Filter))

	rt := &QueryFilter{}
	require.NoError(t, json.Unmarshal(out.Filter, rt))
	require.True(t, proto.Equal(pq.GetFilter(), rt))
}
