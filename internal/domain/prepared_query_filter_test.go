package domain_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func mustFilter(t *testing.T, raw string) *commonpb.QueryFilter {
	t.Helper()

	f := &commonpb.QueryFilter{}
	require.NoError(t, json.Unmarshal([]byte(raw), f))

	return f
}

func TestIsPreparedQueryExecutableTarget(t *testing.T) {
	t.Parallel()

	require.True(t, domain.IsPreparedQueryExecutableTarget(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS))
	require.True(t, domain.IsPreparedQueryExecutableTarget(commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS))
	// LOGS is executable as a prepared query since EN-1503 (query.EnrichLogs).
	require.True(t, domain.IsPreparedQueryExecutableTarget(commonpb.QueryTarget_QUERY_TARGET_LOGS))
	// AUDIT never is (no cursor field, no public target JSON mapping).
	require.False(t, domain.IsPreparedQueryExecutableTarget(commonpb.QueryTarget_QUERY_TARGET_AUDIT))
	require.False(t, domain.IsPreparedQueryExecutableTarget(commonpb.QueryTarget(999)))
}

func TestValidateFilterForTarget(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		raw     string
		target  commonpb.QueryTarget
		wantErr string
	}{
		{
			name:   "nil filter is nothing to validate",
			raw:    "", // handled below as a nil filter
			target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		},
		{
			name:   "metadata condition valid on accounts",
			raw:    `{"$exists":{"metadata":"x"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		},
		{
			name:    "transaction-only reference rejected on accounts",
			raw:     `{"$match":{"reference":"r"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "accounts",
		},
		{
			name:   "transaction-only reference valid on transactions",
			raw:    `{"$match":{"reference":"r"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		},
		{
			name:    "log-only logId rejected on accounts",
			raw:     `{"$gt":{"logId":"5"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "accounts",
		},
		{
			name:    "log-only logId rejected on transactions",
			raw:     `{"$gt":{"logId":"5"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			wantErr: "transactions",
		},
		{
			name:   "log-only logId valid on logs",
			raw:    `{"$gt":{"logId":"5"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_LOGS,
		},
		{
			name:   "ledger condition valid on logs",
			raw:    `{"$match":{"ledger":"main"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_LOGS,
		},
		{
			name:    "address rejected on logs (no account→log translation)",
			raw:     `{"$match":{"address":"world"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_LOGS,
			wantErr: "logs",
		},
		{
			name:    "metadata rejected on logs (no log-metadata index)",
			raw:     `{"$match":{"metadata[k]":"v"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_LOGS,
			wantErr: "logs",
		},
		{
			name:    "metadata $exists rejected on logs (no log-metadata index)",
			raw:     `{"$exists":{"metadata":"k"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_LOGS,
			wantErr: "logs",
		},
		{
			name:    "invalid condition nested in $and is rejected",
			raw:     `{"$and":[{"$exists":{"metadata":"x"}},{"$gt":{"logId":"5"}}]}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "accounts",
		},
		{
			name:   "combinator with all-valid children passes",
			raw:    `{"$or":[{"$exists":{"metadata":"x"}},{"$match":{"address":"world"}}]}`,
			target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var f *commonpb.QueryFilter
			if tc.raw != "" {
				f = mustFilter(t, tc.raw)
			}

			err := domain.ValidateFilterForTarget(f, tc.target)
			if tc.wantErr == "" {
				require.Nil(t, err)

				return
			}

			require.NotNil(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestValidateFilterForTarget_RejectsDeeplyNestedFilter is the regression for
// the EN-1503 review P1: ValidateFilterForTarget runs at write time (admission
// + FSM) on an untrusted gRPC filter, before query.Compile's depth guard can
// fire. Without its own bound, a hostile deeply-nested $and/$or/$not tree would
// blow the Go stack here — an unrecoverable, fatal DoS. Build a chain deeper
// than domain.MaxFilterDepth and assert it is rejected loudly rather than
// recursing. Both CreatePreparedQuery and UpdatePreparedQuery route their
// untrusted filter through this same function, so one guard covers both paths.
func TestValidateFilterForTarget_RejectsDeeplyNestedFilter(t *testing.T) {
	t.Parallel()

	build := func(wrap func(child *commonpb.QueryFilter) *commonpb.QueryFilter) *commonpb.QueryFilter {
		// Innermost leaf is a valid LOGS condition so, absent the depth guard,
		// the walk would traverse the whole chain and succeed.
		var f = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_LogId{
				LogId: &commonpb.LogIdCondition{Cond: &commonpb.UintCondition{}},
			},
		}

		for range domain.MaxFilterDepth + 5 {
			f = wrap(f)
		}

		return f
	}

	andWrap := func(child *commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{
			And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{child}},
		}}
	}
	orWrap := func(child *commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{
			Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{child}},
		}}
	}
	notWrap := func(child *commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{
			Not: &commonpb.NotFilter{Filter: child},
		}}
	}

	for name, wrap := range map[string]func(*commonpb.QueryFilter) *commonpb.QueryFilter{
		"and": andWrap,
		"or":  orWrap,
		"not": notWrap,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := domain.ValidateFilterForTarget(build(wrap), commonpb.QueryTarget_QUERY_TARGET_LOGS)
			require.NotNil(t, err, "deeply-nested %s filter must trip the depth guard", name)
			require.Contains(t, err.Error(), "nesting depth")
		})
	}

	// Boundary must match query.Compile node-for-node: Compile checks
	// `depth >= MaxFilterDepth` on entry of every node (combinators AND the
	// leaf), so with N combinators wrapping a leaf the leaf is entered at
	// depth==N. The deepest tree both accept has N == MaxFilterDepth-1
	// combinators (leaf entered at depth MaxFilterDepth-1, still under the cap);
	// N == MaxFilterDepth is rejected (leaf entered at depth MaxFilterDepth).
	// A shallower write-time bound would persist prepared queries that fail to
	// compile at execute time — the off-by-one this pins against.
	nestedLogId := func(combinators int) *commonpb.QueryFilter {
		var f = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_LogId{
				LogId: &commonpb.LogIdCondition{Cond: &commonpb.UintCondition{}},
			},
		}
		for range combinators {
			f = andWrap(f)
		}

		return f
	}

	require.Nil(t, domain.ValidateFilterForTarget(nestedLogId(domain.MaxFilterDepth-1),
		commonpb.QueryTarget_QUERY_TARGET_LOGS),
		"MaxFilterDepth-1 combinators must be accepted (matches query.Compile)")

	atCap := domain.ValidateFilterForTarget(nestedLogId(domain.MaxFilterDepth),
		commonpb.QueryTarget_QUERY_TARGET_LOGS)
	require.NotNil(t, atCap,
		"MaxFilterDepth combinators must be rejected (matches query.Compile)")
	require.Contains(t, atCap.Error(), "nesting depth")
	require.ErrorIs(t, atCap, domain.ErrFilterTooDeep)
}

// TestValidateFilterForTarget_RejectsHasAssetPrecisionOverflow pins the
// write-time half of the account-by-asset precision bound: a prepared query
// whose has-asset precision does not fit the index's one-byte cell can never
// compile, so it must be rejected before it is stored.
func TestValidateFilterForTarget_RejectsHasAssetPrecisionOverflow(t *testing.T) {
	t.Parallel()

	hasAsset := func(precision uint32) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_AccountHasAsset{
			AccountHasAsset: &commonpb.AccountHasAssetCondition{AssetBase: "USD", Precision: precision},
		}}
	}
	notWrap := func(child *commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: child}}}
	}
	accounts := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS

	require.Nil(t, domain.ValidateFilterForTarget(hasAsset(domain.MaxHasAssetPrecision), accounts))

	for name, f := range map[string]*commonpb.QueryFilter{
		"bare":   hasAsset(domain.MaxHasAssetPrecision + 1),
		"nested": notWrap(hasAsset(1038)),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := domain.ValidateFilterForTarget(f, accounts)
			require.NotNil(t, err)

			var compileErr *domain.ErrFilterCompilation
			require.ErrorAs(t, err, &compileErr)
			require.Contains(t, err.Error(), "has asset precision")
		})
	}
}

// TestValidateFilterForTarget_RejectsMalformedLeaves pins the write-time half of
// domain.ValidateFilterLeaf: each leaf shape below fails query.Compile whatever
// the schema, parameters or index state, so a prepared query carrying one must
// be rejected before it is stored. The well-formed twin of each case passes.
func TestValidateFilterForTarget_RejectsMalformedLeaves(t *testing.T) {
	t.Parallel()

	accounts := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	transactions := commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	logs := commonpb.QueryTarget_QUERY_TARGET_LOGS
	one := uint64(1)
	uintCond := &commonpb.UintCondition{Min: &one}
	stringCond := &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "x"}}

	field := func(fc *commonpb.FieldCondition) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: fc}}
	}
	ref := &commonpb.FieldRef{Metadata: "colour"}
	reference := func(c *commonpb.StringCondition) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{Reference: &commonpb.ReferenceCondition{Cond: c}}}
	}
	ledger := func(c *commonpb.StringCondition) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Ledger{Ledger: &commonpb.LedgerCondition{Cond: c}}}
	}
	builtinUint := func(f commonpb.TransactionBuiltinIndex, c *commonpb.UintCondition) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{Field: f, Cond: c}}}
	}
	logBuiltinUint := func(f commonpb.LogBuiltinIndex, c *commonpb.UintCondition) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{LogBuiltinUint: &commonpb.LogBuiltinUintCondition{Field: f, Cond: c}}}
	}
	address := func(m *commonpb.AddressMatch) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: m}}
	}

	for _, tc := range []struct {
		name      string
		target    commonpb.QueryTarget
		malformed *commonpb.QueryFilter
		valid     *commonpb.QueryFilter
		want      string
	}{
		{
			name:      "reference without condition",
			target:    transactions,
			malformed: reference(nil),
			valid:     reference(stringCond),
			want:      "reference condition has no value",
		},
		{
			name:      "reference with empty string condition",
			target:    transactions,
			malformed: reference(&commonpb.StringCondition{}),
			valid:     reference(stringCond),
			want:      "string condition has no value",
		},
		{
			name:      "ledger without condition",
			target:    logs,
			malformed: ledger(nil),
			valid:     ledger(stringCond),
			want:      "ledger condition has no value",
		},
		{
			name:      "ledger with empty string condition",
			target:    logs,
			malformed: ledger(&commonpb.StringCondition{}),
			valid:     ledger(stringCond),
			want:      "string condition has no value",
		},
		{
			name:      "builtin uint without condition",
			target:    transactions,
			malformed: builtinUint(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, nil),
			valid:     builtinUint(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, uintCond),
			want:      "builtin uint condition has no value",
		},
		{
			name:      "builtin uint on the reference field",
			target:    transactions,
			malformed: builtinUint(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE, uintCond),
			valid:     builtinUint(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, uintCond),
			want:      "unsupported builtin uint field: TX_BUILTIN_INDEX_REFERENCE",
		},
		{
			name:      "builtin uint on the address field",
			target:    transactions,
			malformed: builtinUint(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS, uintCond),
			valid:     builtinUint(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT, uintCond),
			want:      "unsupported builtin uint field: TX_BUILTIN_INDEX_ADDRESS",
		},
		{
			name:      "log builtin uint without condition",
			target:    logs,
			malformed: logBuiltinUint(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, nil),
			valid:     logBuiltinUint(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, uintCond),
			want:      "log builtin uint condition has no value",
		},
		{
			name:      "log builtin uint with unspecified field",
			target:    logs,
			malformed: logBuiltinUint(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_UNSPECIFIED, uintCond),
			valid:     logBuiltinUint(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, uintCond),
			want:      "unsupported log builtin uint field: LOG_BUILTIN_INDEX_UNSPECIFIED",
		},
		{
			name:      "address without match",
			target:    accounts,
			malformed: address(&commonpb.AddressMatch{}),
			valid:     address(&commonpb.AddressMatch{Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "users:"}}),
			want:      "address condition has no match",
		},
		{
			name:   "field without field reference",
			target: accounts,
			malformed: field(&commonpb.FieldCondition{
				Condition: &commonpb.FieldCondition_StringCond{StringCond: stringCond},
			}),
			valid: field(&commonpb.FieldCondition{
				Field:     ref,
				Condition: &commonpb.FieldCondition_StringCond{StringCond: stringCond},
			}),
			want: "field condition has no field reference",
		},
		{
			name:      "field without condition",
			target:    accounts,
			malformed: field(&commonpb.FieldCondition{Field: ref}),
			valid: field(&commonpb.FieldCondition{
				Field:     ref,
				Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
			}),
			want: "field condition has no condition",
		},
		{
			name:   "field with empty string condition",
			target: accounts,
			malformed: field(&commonpb.FieldCondition{
				Field:     ref,
				Condition: &commonpb.FieldCondition_StringCond{StringCond: &commonpb.StringCondition{}},
			}),
			valid: field(&commonpb.FieldCondition{
				Field:     ref,
				Condition: &commonpb.FieldCondition_StringCond{StringCond: stringCond},
			}),
			want: "string condition has no value",
		},
		{
			name:   "field with empty bool condition",
			target: accounts,
			malformed: field(&commonpb.FieldCondition{
				Field:     ref,
				Condition: &commonpb.FieldCondition_BoolCond{BoolCond: &commonpb.BoolCondition{}},
			}),
			valid: field(&commonpb.FieldCondition{
				Field:     ref,
				Condition: &commonpb.FieldCondition_BoolCond{BoolCond: &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Param{Param: "flag"}}},
			}),
			want: "bool condition has no value",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Nil(t, domain.ValidateFilterForTarget(tc.valid, tc.target))

			for name, f := range map[string]*commonpb.QueryFilter{
				"bare":   tc.malformed,
				"nested": {Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{tc.valid, tc.malformed}}}},
			} {
				err := domain.ValidateFilterForTarget(f, tc.target)
				require.NotNil(t, err, name)

				var compileErr *domain.ErrFilterCompilation
				require.ErrorAs(t, err, &compileErr, name)
				require.Equal(t, tc.want, compileErr.Detail, name)
			}
		})
	}
}
