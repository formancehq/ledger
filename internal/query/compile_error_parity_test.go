package query_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Compilation-error parity (EN-1966).
//
// The set oracle in compile_reverse_parity_test.go proves the two entry points
// agree on which entities a filter selects. It says nothing about the filters
// neither direction may compile at all, and those refusals are the other half
// of the shared-semantics claim: a filter accepted descending but refused
// ascending (or refused with a different message) is a direction-dependent API
// contract, which is exactly what having two compilers risks.
//
// Every case below must fail in BOTH directions with the IDENTICAL error, and
// each one enters through a shared refusal path — the early target guard,
// rejectInvalidCondition, the depth guard, resolveFieldMetadataCtx's schema
// and coercion checks, requireIndexReady, or a leaf's own "condition has no
// value" arm.

// txRevertedAtRangeFilter names an index parityRegistry deliberately does NOT
// declare, so both directions must refuse it at the readiness gate.
func txRevertedAtRangeFilter(minV, maxV uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
		BuiltinUint: &commonpb.BuiltinUintCondition{
			Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT,
			Cond:  &commonpb.UintCondition{Min: &minV, Max: &maxV},
		},
	}}
}

// nestedAndFilter builds `depth` nested AND nodes around one leaf, to drive
// the depth guard from either side.
func nestedAndFilter(depth int, leaf *commonpb.QueryFilter) *commonpb.QueryFilter {
	f := leaf
	for range depth {
		f = andFilter(f)
	}

	return f
}

func TestCompileErrorParity(t *testing.T) {
	t.Parallel()

	store := parityStore(t)
	reader := store.DB()

	accounts := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	transactions := commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	logs := commonpb.QueryTarget_QUERY_TARGET_LOGS

	for _, tc := range []struct {
		name   string
		target commonpb.QueryTarget
		filter *commonpb.QueryFilter
		// wantSubstring pins WHICH refusal fired, so a case cannot pass on
		// some unrelated error that happens to match in both directions.
		wantSubstring string
	}{
		{
			name:          "unsupported target",
			target:        commonpb.QueryTarget_QUERY_TARGET_AUDIT,
			filter:        nil,
			wantSubstring: "unsupported query target",
		},
		{
			name:          "reverted is not valid on accounts",
			target:        accounts,
			filter:        revertedFilter(true),
			wantSubstring: "is not valid on target accounts",
		},
		{
			name:          "reference is not valid on logs",
			target:        logs,
			filter:        referenceFilter(parityReferenceA),
			wantSubstring: "is not valid on target logs",
		},
		{
			name:          "has asset is not valid on transactions",
			target:        transactions,
			filter:        hasAssetFilter(parityAsset, uint32(parityAssetPrecision)),
			wantSubstring: "is not valid on target transactions",
		},
		{
			name:          "metadata is not valid on logs",
			target:        logs,
			filter:        stringFieldFilter("colour", "red"),
			wantSubstring: "is not valid on target logs",
		},
		{
			name:          "nested past the depth guard",
			target:        accounts,
			filter:        nestedAndFilter(query.MaxFilterDepth+1, stringFieldFilter("colour", "red")),
			wantSubstring: "exceeds maximum nesting depth",
		},
		{
			name:          "metadata field absent from the schema",
			target:        accounts,
			filter:        stringFieldFilter("unknown", "red"),
			wantSubstring: "unknown",
		},
		{
			name:          "condition type does not match the declared field type",
			target:        accounts,
			filter:        stringFieldFilter("size", "not-a-number"),
			wantSubstring: "size",
		},
		{
			name:          "index not ready",
			target:        transactions,
			filter:        txRevertedAtRangeFilter(1, 2),
			wantSubstring: "reverted_at",
		},
		{
			name:   "reference condition has no value",
			target: transactions,
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{
				Reference: &commonpb.ReferenceCondition{},
			}},
			wantSubstring: "reference condition has no value",
		},
		{
			name:   "bool condition has no value",
			target: accounts,
			filter: accountFieldFilter("flag", &commonpb.FieldCondition{
				Field:     &commonpb.FieldRef{Metadata: "flag"},
				Condition: &commonpb.FieldCondition_BoolCond{BoolCond: &commonpb.BoolCondition{}},
			}),
			wantSubstring: "bool condition has no value",
		},
		{
			name:   "unsupported log builtin field",
			target: logs,
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{
				LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
					Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_UNSPECIFIED,
					Cond:  &commonpb.UintCondition{},
				},
			}},
			wantSubstring: "log builtin uint",
		},
		{
			name:   "ledger condition has no value",
			target: logs,
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Ledger{
				Ledger: &commonpb.LedgerCondition{},
			}},
			wantSubstring: "ledger condition has no value",
		},
		{
			name:          "has asset precision out of range",
			target:        accounts,
			filter:        hasAssetFilter(parityAsset, 1_000),
			wantSubstring: "precision",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ascIter, ascErr := query.Compile(
				reader, dal.NewKeyBuilder(), tc.filter,
				tc.target, parityLedger,
				nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
			if ascErr == nil {
				ascIter.Close()
			}

			descIter, descErr := query.CompileReverse(
				reader, dal.NewKeyBuilder(), tc.filter,
				tc.target, parityLedger,
				nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
			if descErr == nil {
				descIter.Close()
			}

			require.Error(t, ascErr, "the ascending compiler must refuse this filter")
			require.Error(t, descErr, "the descending compiler must refuse the same filter")

			require.Contains(t, strings.ToLower(ascErr.Error()), strings.ToLower(tc.wantSubstring),
				"the ascending refusal must come from the expected check, not an unrelated one")

			require.Equal(t, ascErr.Error(), descErr.Error(),
				"both directions must refuse with the identical message: a filter the API rejects one way and rejects differently the other way is a direction-dependent contract")
		})
	}
}

// TestCompileErrorParity_AcceptanceIsAlsoShared is the positive half: every
// filter shape the paged oracle drives must COMPILE in both directions.
// Without it, a descending compiler that refused a valid filter outright would
// leave TestCompileErrorParity green (it only inspects refusals) and the
// traversal oracle would fail with a confusing empty-result diff instead of
// naming the refusal.
func TestCompileErrorParity_AcceptanceIsAlsoShared(t *testing.T) {
	t.Parallel()

	store := parityStore(t)
	reader := store.DB()

	for _, tc := range parityCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ascIter, ascErr := query.Compile(
				reader, dal.NewKeyBuilder(), tc.filter,
				tc.target, parityLedger,
				nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
			require.NoError(t, ascErr)

			ascIter.Close()

			descIter, descErr := query.CompileReverse(
				reader, dal.NewKeyBuilder(), tc.filter,
				tc.target, parityLedger,
				nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
			require.NoError(t, descErr, "a filter the ascending compiler accepts must compile descending too")

			descIter.Close()
		})
	}
}
