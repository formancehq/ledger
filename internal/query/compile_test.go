package query

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// TestCompile_RejectsDeeplyNestedFilter is the regression for #341 /
// Review-2 L-19. A hostile gRPC client can hand-craft a deeply-nested
// QueryFilter and ship it through prepared queries; without a depth
// guard the compile() recursion blows the Go stack — a fatal,
// unrecoverable abort. Build a chain of nested Or wrappers deeper
// than MaxFilterDepth and assert that compile() returns
// ErrFilterTooDeep instead of recursing.
func TestCompile_RejectsDeeplyNestedFilter(t *testing.T) {
	t.Parallel()

	// Universe iterator at the leaf — every wrapper level is an Or
	// with a single child, so compile dispatches Or → compile(child)
	// without needing a Pebble reader (the depth check fires before
	// we reach a leaf when the chain is deeper than MaxFilterDepth).
	var leaf *ledgerpb.QueryFilter // nil = universe; would reach compileUniverse if we got there.
	filter := leaf

	for range MaxFilterDepth + 5 {
		filter = &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Or{
				Or: &ledgerpb.OrFilter{Filters: []*ledgerpb.QueryFilter{filter}},
			},
		}
	}

	ctx := &compileCtx{
		target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
	}

	_, err := compile(ctx, filter)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrFilterTooDeep),
		"deeply-nested QueryFilter must trip the depth guard, got: %v", err)
}

// ledgerFilter builds a LOGS-target LedgerCondition (exact match on name).
func ledgerFilter(name string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Ledger{
			Ledger: &ledgerpb.LedgerCondition{
				Cond: &ledgerpb.StringCondition{
					Value: &ledgerpb.StringCondition_Hardcoded{Hardcoded: name},
				},
			},
		},
	}
}

// TestCompile_LedgerConditionOtherLedgerIsEmpty is the regression for the
// EN-1503 review: a LOGS-target LedgerCondition naming a ledger other than the
// one the query executes against must compile to an empty (unsatisfiable)
// result, not silently fall through to the universe of the executing ledger's
// logs. The mismatch branch returns before touching any Pebble reader, so no
// store setup is needed.
func TestCompile_LedgerConditionOtherLedgerIsEmpty(t *testing.T) {
	t.Parallel()

	ctx := &compileCtx{
		target:     ledgerpb.QueryTarget_QUERY_TARGET_LOGS,
		ledgerName: "ledger-a",
	}

	iter, err := compile(ctx, ledgerFilter("ledger-b"))
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Close()

	// Empty iterator: a filter naming a different ledger can match nothing.
	require.False(t, iter.Next(), "LedgerCondition on a different ledger must yield no rows")
	require.NoError(t, iter.Err())
}

// TestCompile_LedgerConditionMissingValue asserts a LedgerCondition carrying no
// StringCondition value fails loudly rather than silently matching everything.
func TestCompile_LedgerConditionMissingValue(t *testing.T) {
	t.Parallel()

	ctx := &compileCtx{
		target:     ledgerpb.QueryTarget_QUERY_TARGET_LOGS,
		ledgerName: "ledger-a",
	}

	filter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Ledger{Ledger: &ledgerpb.LedgerCondition{}},
	}

	_, err := compile(ctx, filter)
	require.Error(t, err)
}

// TestCompile_DepthBoundMatchesValidator pins that the execute-time guard in
// compile() and the write-time guard in domain.ValidateFilterForTarget agree at
// the exact boundary — a filter accepted at write time must compile, and one
// rejected at write time must fail to compile. A drift here would let a prepared
// query be persisted that then fails only at execute time (or vice versa). Both
// count every node (combinators AND the leaf), so N combinators wrapping a leaf
// enters the leaf at depth N: N == MaxFilterDepth-1 is the deepest both accept,
// N == MaxFilterDepth is rejected by both.
func TestCompile_DepthBoundMatchesValidator(t *testing.T) {
	t.Parallel()

	// Leaf is a metadata field condition (valid on ACCOUNTS, needs no reader —
	// the depth guard fires or the leaf validity check passes before any store
	// access on the accepted path). Wrap in single-child Or combinators.
	nested := func(combinators int) *ledgerpb.QueryFilter {
		f := &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_Field{
			Field: fieldCondition("k", &ledgerpb.ExistsCondition{}),
		}}
		for range combinators {
			f = &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_Or{
				Or: &ledgerpb.OrFilter{Filters: []*ledgerpb.QueryFilter{f}},
			}}
		}

		return f
	}

	target := ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS

	// N == MaxFilterDepth-1: both accept (compile reaches the leaf; validator
	// returns nil).
	deepestOK := nested(MaxFilterDepth - 1)
	require.Nil(t, domain.ValidateFilterForTarget(deepestOK, target),
		"validator must accept MaxFilterDepth-1 combinators")
	ctx := &compileCtx{target: target}
	_, err := compile(ctx, deepestOK)
	require.False(t, errors.Is(err, ErrFilterTooDeep),
		"compile must not trip the depth guard at MaxFilterDepth-1 combinators (got: %v)", err)

	// N == MaxFilterDepth: both reject with the depth error.
	tooDeep := nested(MaxFilterDepth)
	valErr := domain.ValidateFilterForTarget(tooDeep, target)
	require.NotNil(t, valErr, "validator must reject MaxFilterDepth combinators")
	require.Contains(t, valErr.Error(), "nesting depth")
	ctx = &compileCtx{target: target}
	_, err = compile(ctx, tooDeep)
	require.True(t, errors.Is(err, ErrFilterTooDeep),
		"compile must trip the depth guard at MaxFilterDepth combinators (got: %v)", err)
}

func fieldCondition(metaKey string, cond any) *ledgerpb.FieldCondition {
	fc := &ledgerpb.FieldCondition{
		Field: &ledgerpb.FieldRef{Metadata: metaKey},
	}

	switch c := cond.(type) {
	case *ledgerpb.IntCondition:
		fc.Condition = &ledgerpb.FieldCondition_IntCond{IntCond: c}
	case *ledgerpb.UintCondition:
		fc.Condition = &ledgerpb.FieldCondition_UintCond{UintCond: c}
	case *ledgerpb.StringCondition:
		fc.Condition = &ledgerpb.FieldCondition_StringCond{StringCond: c}
	case *ledgerpb.BoolCondition:
		fc.Condition = &ledgerpb.FieldCondition_BoolCond{BoolCond: c}
	case *ledgerpb.ExistsCondition:
		fc.Condition = &ledgerpb.FieldCondition_ExistsCond{ExistsCond: c}
	}

	return fc
}

func TestValidateAndCoerceCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fc        *ledgerpb.FieldCondition
		schema    *ledgerpb.MetadataFieldSchema
		wantErr   string
		checkCond func(t *testing.T, fc *ledgerpb.FieldCondition)
	}{
		{
			name:   "int schema + IntCondition → OK",
			fc:     fieldCondition("age", &ledgerpb.IntCondition{Min: new(int64(10)), Max: new(int64(99))}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_INT64},
		},
		{
			name:    "int schema + StringCondition → error",
			fc:      fieldCondition("age", &ledgerpb.StringCondition{Value: &ledgerpb.StringCondition_Hardcoded{Hardcoded: "hello"}}),
			schema:  &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_INT64},
			wantErr: `field "age" is declared as METADATA_TYPE_INT64, cannot use string condition`,
		},
		{
			name:    "int schema + BoolCondition → error",
			fc:      fieldCondition("age", &ledgerpb.BoolCondition{Value: &ledgerpb.BoolCondition_Hardcoded{Hardcoded: true}}),
			schema:  &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_INT64},
			wantErr: `field "age" is declared as METADATA_TYPE_INT64, cannot use bool condition`,
		},
		{
			name:    "int schema + UintCondition → error",
			fc:      fieldCondition("age", &ledgerpb.UintCondition{Min: new(uint64(10))}),
			schema:  &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_INT64},
			wantErr: `field "age" is declared as METADATA_TYPE_INT64, cannot use unsigned integer condition`,
		},
		{
			name:   "uint schema + IntCondition (positive) → coerced to UintCondition",
			fc:     fieldCondition("counter", &ledgerpb.IntCondition{Min: new(int64(5)), Max: new(int64(100))}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64},
			checkCond: func(t *testing.T, fc *ledgerpb.FieldCondition) {
				t.Helper()

				uc, ok := fc.GetCondition().(*ledgerpb.FieldCondition_UintCond)
				require.True(t, ok, "expected UintCondition after coercion")
				require.NotNil(t, uc.UintCond.Min)
				assert.Equal(t, uint64(5), uc.UintCond.GetMin())
				require.NotNil(t, uc.UintCond.Max)
				assert.Equal(t, uint64(100), uc.UintCond.GetMax())
			},
		},
		{
			name:   "uint schema + IntCondition with params → coerced preserving params",
			fc:     fieldCondition("counter", &ledgerpb.IntCondition{ParamMin: "lo", ParamMax: "hi", MinExclusive: true}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64},
			checkCond: func(t *testing.T, fc *ledgerpb.FieldCondition) {
				t.Helper()

				uc, ok := fc.GetCondition().(*ledgerpb.FieldCondition_UintCond)
				require.True(t, ok, "expected UintCondition after coercion")
				assert.Equal(t, "lo", uc.UintCond.GetParamMin())
				assert.Equal(t, "hi", uc.UintCond.GetParamMax())
				assert.True(t, uc.UintCond.GetMinExclusive())
			},
		},
		{
			name:    "uint schema + IntCondition (negative min) → error",
			fc:      fieldCondition("counter", &ledgerpb.IntCondition{Min: new(int64(-1))}),
			schema:  &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64},
			wantErr: `field "counter" is unsigned, cannot use negative min bound -1`,
		},
		{
			name:    "uint schema + IntCondition (negative max) → error",
			fc:      fieldCondition("counter", &ledgerpb.IntCondition{Max: new(int64(-5))}),
			schema:  &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64},
			wantErr: `field "counter" is unsigned, cannot use negative max bound -5`,
		},
		{
			name:   "uint schema + UintCondition → OK",
			fc:     fieldCondition("counter", &ledgerpb.UintCondition{Min: new(uint64(10))}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64},
		},
		{
			name:    "uint schema + StringCondition → error",
			fc:      fieldCondition("counter", &ledgerpb.StringCondition{Value: &ledgerpb.StringCondition_Hardcoded{Hardcoded: "hello"}}),
			schema:  &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64},
			wantErr: `field "counter" is declared as METADATA_TYPE_UINT64, cannot use string condition`,
		},
		{
			name:   "string schema + StringCondition → OK",
			fc:     fieldCondition("name", &ledgerpb.StringCondition{Value: &ledgerpb.StringCondition_Hardcoded{Hardcoded: "alice"}}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_STRING},
		},
		{
			name:    "string schema + IntCondition → error",
			fc:      fieldCondition("name", &ledgerpb.IntCondition{Min: new(int64(5))}),
			schema:  &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_STRING},
			wantErr: `field "name" is declared as METADATA_TYPE_STRING, cannot use integer condition`,
		},
		{
			name:    "string schema + BoolCondition → error",
			fc:      fieldCondition("name", &ledgerpb.BoolCondition{Value: &ledgerpb.BoolCondition_Hardcoded{Hardcoded: true}}),
			schema:  &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_STRING},
			wantErr: `field "name" is declared as METADATA_TYPE_STRING, cannot use bool condition`,
		},
		{
			name:    "string schema + UintCondition → error",
			fc:      fieldCondition("name", &ledgerpb.UintCondition{Min: new(uint64(1))}),
			schema:  &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_STRING},
			wantErr: `field "name" is declared as METADATA_TYPE_STRING, cannot use unsigned integer condition`,
		},
		{
			name:   "bool schema + BoolCondition → OK",
			fc:     fieldCondition("active", &ledgerpb.BoolCondition{Value: &ledgerpb.BoolCondition_Hardcoded{Hardcoded: true}}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_BOOL},
		},
		{
			name:    "bool schema + StringCondition → error",
			fc:      fieldCondition("active", &ledgerpb.StringCondition{Value: &ledgerpb.StringCondition_Hardcoded{Hardcoded: "true"}}),
			schema:  &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_BOOL},
			wantErr: `field "active" is declared as METADATA_TYPE_BOOL, cannot use string condition`,
		},
		{
			name:    "bool schema + IntCondition → error",
			fc:      fieldCondition("active", &ledgerpb.IntCondition{Min: new(int64(1))}),
			schema:  &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_BOOL},
			wantErr: `field "active" is declared as METADATA_TYPE_BOOL, cannot use integer condition`,
		},
		{
			name:   "ExistsCondition + int schema → OK",
			fc:     fieldCondition("age", &ledgerpb.ExistsCondition{}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_INT64},
		},
		{
			name:   "ExistsCondition + string schema → OK",
			fc:     fieldCondition("name", &ledgerpb.ExistsCondition{}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_STRING},
		},
		{
			name:   "ExistsCondition + bool schema → OK",
			fc:     fieldCondition("active", &ledgerpb.ExistsCondition{}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_BOOL},
		},
		{
			name:   "int8 schema + IntCondition → OK",
			fc:     fieldCondition("level", &ledgerpb.IntCondition{Min: new(int64(-128)), Max: new(int64(127))}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_INT8},
		},
		{
			name:   "uint16 schema + IntCondition → coerced",
			fc:     fieldCondition("port", &ledgerpb.IntCondition{Min: new(int64(80))}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT16},
			checkCond: func(t *testing.T, fc *ledgerpb.FieldCondition) {
				t.Helper()

				uc, ok := fc.GetCondition().(*ledgerpb.FieldCondition_UintCond)
				require.True(t, ok, "expected UintCondition after coercion")
				require.NotNil(t, uc.UintCond.Min)
				assert.Equal(t, uint64(80), uc.UintCond.GetMin())
			},
		},
		{
			name:   "uint schema + IntCondition with zero min → coerced",
			fc:     fieldCondition("counter", &ledgerpb.IntCondition{Min: new(int64(0))}),
			schema: &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64},
			checkCond: func(t *testing.T, fc *ledgerpb.FieldCondition) {
				t.Helper()

				uc, ok := fc.GetCondition().(*ledgerpb.FieldCondition_UintCond)
				require.True(t, ok, "expected UintCondition after coercion")
				require.NotNil(t, uc.UintCond.Min)
				assert.Equal(t, uint64(0), uc.UintCond.GetMin())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := validateAndCoerceCondition(tt.fc, tt.schema)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.checkCond != nil {
				tt.checkCond(t, got)
			}
		})
	}
}

func TestCoerceIntToUint_ExclusiveFlags(t *testing.T) {
	t.Parallel()

	// Verify that exclusivity flags are preserved through coercion
	fc := fieldCondition("x", &ledgerpb.IntCondition{
		Min:          new(int64(10)),
		Max:          new(int64(20)),
		MinExclusive: true,
		MaxExclusive: true,
	})
	schema := &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT32}

	got, err := validateAndCoerceCondition(fc, schema)
	require.NoError(t, err)

	uc, ok := got.GetCondition().(*ledgerpb.FieldCondition_UintCond)
	require.True(t, ok)
	assert.True(t, uc.UintCond.GetMinExclusive())
	assert.True(t, uc.UintCond.GetMaxExclusive())
	require.NotNil(t, uc.UintCond.Min)
	assert.Equal(t, uint64(10), uc.UintCond.GetMin())
	require.NotNil(t, uc.UintCond.Max)
	assert.Equal(t, uint64(20), uc.UintCond.GetMax())
}

func TestCoerceIntToUint_NoMinNoMax(t *testing.T) {
	t.Parallel()

	// IntCondition with no bounds (just params) should coerce cleanly
	fc := fieldCondition("x", &ledgerpb.IntCondition{})
	schema := &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64}

	got, err := validateAndCoerceCondition(fc, schema)
	require.NoError(t, err)

	uc, ok := got.GetCondition().(*ledgerpb.FieldCondition_UintCond)
	require.True(t, ok)
	assert.Nil(t, uc.UintCond.Min)
	assert.Nil(t, uc.UintCond.Max)
}

func TestCoerceIntToUint_FieldRefPreserved(t *testing.T) {
	t.Parallel()

	fc := fieldCondition("myfield", &ledgerpb.IntCondition{Min: new(int64(0))})
	schema := &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64}

	got, err := validateAndCoerceCondition(fc, schema)
	require.NoError(t, err)
	assert.Equal(t, "myfield", got.GetField().GetMetadata())
}

func TestValidateCondition_BoolSchemaRejectsUint(t *testing.T) {
	t.Parallel()

	fc := fieldCondition("active", &ledgerpb.UintCondition{Min: new(uint64(1))})
	schema := &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_BOOL}

	_, err := validateAndCoerceCondition(fc, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot use unsigned integer condition")
}

func TestValidateCondition_UintSchemaRejectsBool(t *testing.T) {
	t.Parallel()

	fc := fieldCondition("counter", &ledgerpb.BoolCondition{Value: &ledgerpb.BoolCondition_Hardcoded{Hardcoded: true}})
	schema := &ledgerpb.MetadataFieldSchema{Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64}

	_, err := validateAndCoerceCondition(fc, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot use bool condition")
}

func TestResolveIntBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cond       *ledgerpb.IntCondition
		params     map[string]*ledgerpb.ParameterValue
		wantMin    int64
		wantMax    int64
		wantHasMin bool
		wantHasMax bool
		wantEq     bool
		wantEmpty  bool
		wantErr    bool
	}{
		{
			name:       "equality: min == max, both inclusive",
			cond:       &ledgerpb.IntCondition{Min: new(int64(25)), Max: new(int64(25))},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "range: min < max",
			cond:       &ledgerpb.IntCondition{Min: new(int64(10)), Max: new(int64(20))},
			wantMin:    10,
			wantMax:    21,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     false,
		},
		{
			name:       "min exclusive: min=24 exclusive → effective 25, max=25 inclusive → 26",
			cond:       &ledgerpb.IntCondition{Min: new(int64(24)), Max: new(int64(25)), MinExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "max exclusive: min=25, max=26 exclusive → equality on 25",
			cond:       &ledgerpb.IntCondition{Min: new(int64(25)), Max: new(int64(26)), MaxExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "both exclusive: min=24 excl, max=26 excl → range [25, 26) = equality",
			cond:       &ledgerpb.IntCondition{Min: new(int64(24)), Max: new(int64(26)), MinExclusive: true, MaxExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "only min",
			cond:       &ledgerpb.IntCondition{Min: new(int64(5))},
			wantMin:    5,
			wantHasMin: true,
			wantHasMax: false,
			wantEq:     false,
		},
		{
			name:       "only max",
			cond:       &ledgerpb.IntCondition{Max: new(int64(100))},
			wantMax:    101,
			wantHasMin: false,
			wantHasMax: true,
			wantEq:     false,
		},
		{
			name:       "no bounds",
			cond:       &ledgerpb.IntCondition{},
			wantHasMin: false,
			wantHasMax: false,
			wantEq:     false,
		},
		{
			name:       "param equality: same param for min and max",
			cond:       &ledgerpb.IntCondition{ParamMin: "val", ParamMax: "val"},
			params:     map[string]*ledgerpb.ParameterValue{"val": {Value: &ledgerpb.ParameterValue_Int64Value{Int64Value: 42}}},
			wantMin:    42,
			wantMax:    43,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:    "param error: missing param",
			cond:    &ledgerpb.IntCondition{ParamMin: "missing"},
			wantErr: true,
		},
		{
			name:      "overflow: min exclusive at MaxInt64 → empty",
			cond:      &ledgerpb.IntCondition{Min: new(int64(math.MaxInt64)), MinExclusive: true},
			wantEmpty: true,
		},
		{
			name:       "overflow: max inclusive at MaxInt64 → unbounded above",
			cond:       &ledgerpb.IntCondition{Min: new(int64(0)), Max: new(int64(math.MaxInt64))},
			wantMin:    0,
			wantHasMin: true,
			wantHasMax: false,
		},
		{
			name:      "overflow: param min exclusive at MaxInt64 → empty",
			cond:      &ledgerpb.IntCondition{ParamMin: "v", MinExclusive: true},
			params:    map[string]*ledgerpb.ParameterValue{"v": {Value: &ledgerpb.ParameterValue_Int64Value{Int64Value: math.MaxInt64}}},
			wantEmpty: true,
		},
		{
			name:       "overflow: param max inclusive at MaxInt64 → unbounded above",
			cond:       &ledgerpb.IntCondition{Min: new(int64(0)), ParamMax: "v"},
			params:     map[string]*ledgerpb.ParameterValue{"v": {Value: &ledgerpb.ParameterValue_Int64Value{Int64Value: math.MaxInt64}}},
			wantMin:    0,
			wantHasMin: true,
			wantHasMax: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveIntBounds(tt.cond, tt.params)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantEmpty, got.empty, "empty")
			assert.Equal(t, tt.wantHasMin, got.hasMin, "hasMin")
			assert.Equal(t, tt.wantHasMax, got.hasMax, "hasMax")

			if tt.wantHasMin {
				assert.Equal(t, tt.wantMin, got.min, "min")
			}

			if tt.wantHasMax {
				assert.Equal(t, tt.wantMax, got.max, "max")
			}

			assert.Equal(t, tt.wantEq, got.isEquality(), "isEquality")
		})
	}
}

func TestResolveUintBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cond       *ledgerpb.UintCondition
		params     map[string]*ledgerpb.ParameterValue
		wantMin    uint64
		wantMax    uint64
		wantHasMin bool
		wantHasMax bool
		wantEq     bool
		wantEmpty  bool
		wantErr    bool
	}{
		{
			name:       "equality: min == max, both inclusive",
			cond:       &ledgerpb.UintCondition{Min: new(uint64(25)), Max: new(uint64(25))},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "range: min < max",
			cond:       &ledgerpb.UintCondition{Min: new(uint64(10)), Max: new(uint64(20))},
			wantMin:    10,
			wantMax:    21,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     false,
		},
		{
			name:       "min exclusive: min=24 exclusive, max=25 → equality on 25",
			cond:       &ledgerpb.UintCondition{Min: new(uint64(24)), Max: new(uint64(25)), MinExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "max exclusive: min=25, max=26 exclusive → equality on 25",
			cond:       &ledgerpb.UintCondition{Min: new(uint64(25)), Max: new(uint64(26)), MaxExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "only min",
			cond:       &ledgerpb.UintCondition{Min: new(uint64(5))},
			wantMin:    5,
			wantHasMin: true,
			wantHasMax: false,
			wantEq:     false,
		},
		{
			name:       "no bounds",
			cond:       &ledgerpb.UintCondition{},
			wantHasMin: false,
			wantHasMax: false,
			wantEq:     false,
		},
		{
			name:       "param equality",
			cond:       &ledgerpb.UintCondition{ParamMin: "v", ParamMax: "v"},
			params:     map[string]*ledgerpb.ParameterValue{"v": {Value: &ledgerpb.ParameterValue_Uint64Value{Uint64Value: 100}}},
			wantMin:    100,
			wantMax:    101,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:    "param error: missing param",
			cond:    &ledgerpb.UintCondition{ParamMax: "missing"},
			wantErr: true,
		},
		{
			name:      "overflow: min exclusive at MaxUint64 → empty",
			cond:      &ledgerpb.UintCondition{Min: new(uint64(math.MaxUint64)), MinExclusive: true},
			wantEmpty: true,
		},
		{
			name:       "overflow: max inclusive at MaxUint64 → unbounded above",
			cond:       &ledgerpb.UintCondition{Min: new(uint64(0)), Max: new(uint64(math.MaxUint64))},
			wantMin:    0,
			wantHasMin: true,
			wantHasMax: false,
		},
		{
			name:      "overflow: param min exclusive at MaxUint64 → empty",
			cond:      &ledgerpb.UintCondition{ParamMin: "v", MinExclusive: true},
			params:    map[string]*ledgerpb.ParameterValue{"v": {Value: &ledgerpb.ParameterValue_Uint64Value{Uint64Value: math.MaxUint64}}},
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveUintBounds(tt.cond, tt.params)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantEmpty, got.empty, "empty")
			assert.Equal(t, tt.wantHasMin, got.hasMin, "hasMin")
			assert.Equal(t, tt.wantHasMax, got.hasMax, "hasMax")

			if tt.wantHasMin {
				assert.Equal(t, tt.wantMin, got.min, "min")
			}

			if tt.wantHasMax {
				assert.Equal(t, tt.wantMax, got.max, "max")
			}

			assert.Equal(t, tt.wantEq, got.isEquality(), "isEquality")
		})
	}
}

func TestSchemaFieldsForTarget(t *testing.T) {
	t.Parallel()

	t.Run("nil schema", func(t *testing.T) {
		t.Parallel()

		result := SchemaFieldsForTarget(nil, ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		assert.Nil(t, result)
	})

	t.Run("accounts target", func(t *testing.T) {
		t.Parallel()

		schema := &ledgerpb.MetadataSchema{
			AccountFields: map[string]*ledgerpb.MetadataFieldSchema{
				"name": {Type: ledgerpb.MetadataType_METADATA_TYPE_STRING},
			},
			TransactionFields: map[string]*ledgerpb.MetadataFieldSchema{
				"ref": {Type: ledgerpb.MetadataType_METADATA_TYPE_STRING},
			},
		}
		result := SchemaFieldsForTarget(schema, ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		require.Len(t, result, 1)
		assert.Contains(t, result, "name")
	})

	t.Run("transactions target", func(t *testing.T) {
		t.Parallel()

		schema := &ledgerpb.MetadataSchema{
			AccountFields: map[string]*ledgerpb.MetadataFieldSchema{
				"name": {Type: ledgerpb.MetadataType_METADATA_TYPE_STRING},
			},
			TransactionFields: map[string]*ledgerpb.MetadataFieldSchema{
				"ref": {Type: ledgerpb.MetadataType_METADATA_TYPE_STRING},
			},
		}
		result := SchemaFieldsForTarget(schema, ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
		require.Len(t, result, 1)
		assert.Contains(t, result, "ref")
	})
}

// TestBuiltinCompilers_GateOnLocalReadiness pins the F4 fix: every
// builtin compiler (reference, timestamp, inserted_at, log_date, plus
// the transaction-side address index) must refuse with
// ErrIndexBuilding when the local replica's IndexVersionState reports
// CurrentVersion == 0 — i.e. the initial backfill has not yet flipped
// into a live keyspace. Pre-fix only the metadata compiler gated on
// this signal, so a query mid-backfill silently scanned a partially
// populated builtin index.
func TestBuiltinCompilers_GateOnLocalReadiness(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"

	// indexResolverZero simulates a replica whose initial backfill
	// has not yet completed. Real production wiring uses
	// Store.SnapshotVersionResolver against the iteration snapshot.
	indexResolverZero := func(string) (readstore.ResolvedIndexVersion, bool, error) {
		return readstore.ResolvedIndexVersion{BindingKnown: true}, true, nil
	}

	info := &ledgerpb.LedgerInfo{Name: ledgerName}

	// indexRegistry declares every builtin index via the bucket-scoped
	// Lookup interface (post-PR#453 architecture). Per-replica readiness
	// lives in IndexVersionState (modelled here via the resolver) — see
	// EN-1323.
	indexRegistry := staticIndexLookup{}
	for _, id := range []*ledgerpb.IndexID{
		indexes.TxBuiltinID(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
		indexes.TxBuiltinID(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
		indexes.TxBuiltinID(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT),
		indexes.TxBuiltinID(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS),
		indexes.LogBuiltinID(ledgerpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
	} {
		indexRegistry[indexes.KeyFor(ledgerName, id)] = &ledgerpb.Index{Ledger: ledgerName, Id: id}
	}

	tcs := []struct {
		name   string
		target ledgerpb.QueryTarget
		filter *ledgerpb.QueryFilter
	}{
		{
			name:   "reference",
			target: ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			filter: &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_Reference{
				Reference: &ledgerpb.ReferenceCondition{
					Cond: &ledgerpb.StringCondition{Value: &ledgerpb.StringCondition_Hardcoded{Hardcoded: "x"}},
				},
			}},
		},
		{
			name:   "builtin-uint:timestamp",
			target: ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			filter: &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_BuiltinUint{
				BuiltinUint: &ledgerpb.BuiltinUintCondition{
					Field: ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
					Cond:  &ledgerpb.UintCondition{},
				},
			}},
		},
		{
			name:   "builtin-uint:inserted_at",
			target: ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			filter: &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_BuiltinUint{
				BuiltinUint: &ledgerpb.BuiltinUintCondition{
					Field: ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT,
					Cond:  &ledgerpb.UintCondition{},
				},
			}},
		},
		{
			name:   "address (transactions target)",
			target: ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			filter: &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_Address{
				Address: &ledgerpb.AddressMatch{
					Role:  ledgerpb.AddressRole_ADDRESS_ROLE_ANY,
					Match: &ledgerpb.AddressMatch_HardcodedExact{HardcodedExact: "alice"},
				},
			}},
		},
		{
			name:   "log-builtin-uint:date",
			target: ledgerpb.QueryTarget_QUERY_TARGET_LOGS,
			filter: &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_LogBuiltinUint{
				LogBuiltinUint: &ledgerpb.LogBuiltinUintCondition{
					Field: ledgerpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
					Cond:  &ledgerpb.UintCondition{},
				},
			}},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := Compile(
				nil, nil, tc.filter, tc.target, ledgerName,
				nil, nil, info, indexRegistry, indexResolverZero, nil, nil, 0)
			require.Error(t, err, "compiler must refuse when CurrentVersion=0")

			var building *domain.ErrIndexBuilding
			require.ErrorAs(t, err, &building,
				"compiler must return ErrIndexBuilding when local IndexVersionState has CurrentVersion=0 — pre-fix builtin compilers silently scanned a partial keyspace and returned incomplete results (got %T: %v)", err, err)
		})
	}
}

// TestRequireIndexReady_SurfacesPebbleError pins the CLAUDE.md
// invariant #7 corollary on the gate side: a Pebble I/O failure in
// the version resolver MUST bubble up — never get masqueraded as
// ErrIndexBuilding. The pre-fix code would mistake an unreadable
// PEBBLE_GET for "still building", looping clients indefinitely on a
// disk that's actually broken.
func TestRequireIndexReady_SurfacesPebbleError(t *testing.T) {
	t.Parallel()

	pebbleErr := errors.New("simulated pebble corruption")
	info := &ledgerpb.LedgerInfo{Name: "ledger1"}

	refID := indexes.TxBuiltinID(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	indexRegistry := staticIndexLookup{
		indexes.KeyFor("ledger1", refID): {Ledger: "ledger1", Id: refID},
	}

	ctx := &compileCtx{
		target:        ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		indexRegistry: indexRegistry,
		ledgerName:    "ledger1",
		info:          info,
		indexVersionFor: func(string) (readstore.ResolvedIndexVersion, bool, error) {
			return readstore.ResolvedIndexVersion{}, false, pebbleErr
		},
	}

	_, err := requireIndexReady(ctx,
		indexes.TxBuiltinID(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
		"reference")
	require.Error(t, err)
	require.ErrorIs(t, err, pebbleErr,
		"requireIndexReady must wrap and propagate the Pebble error rather than swallow it as ErrIndexBuilding (got %v)", err)

	var building *domain.ErrIndexBuilding
	require.False(t, errors.As(err, &building),
		"a Pebble I/O failure must NOT be reported as ErrIndexBuilding — that would mask a real outage as a transient readiness state")
}

// TestCompile_RejectsUnsupportedTarget is the regression for flemzord's P2 on
// #1563 (EN-1503). A prepared query stored via gRPC (which validates only the
// name) can carry an unsupported/unknown QueryTarget. Before the fix,
// compileUniverse's default arm returned an empty iterator for such a target,
// which executeList turned into an empty-but-successful page BEFORE reaching its
// own fail-loud switch — a silent success that masks the invariant violation.
// Compile must now reject an unsupported target loudly at the earliest point,
// for both the filtered and unfiltered (universe) paths.
func TestCompile_RejectsUnsupportedTarget(t *testing.T) {
	t.Parallel()

	// An enum value outside the wired set (ACCOUNTS/TRANSACTIONS/LOGS).
	const badTarget = ledgerpb.QueryTarget(9999)

	info := &ledgerpb.LedgerInfo{Name: "ledger1"}

	cases := []struct {
		name   string
		filter *ledgerpb.QueryFilter
	}{
		{
			name:   "nil filter (universe path)",
			filter: nil,
		},
		{
			name: "field filter",
			filter: &ledgerpb.QueryFilter{
				Filter: &ledgerpb.QueryFilter_Field{
					Field: fieldCondition("x", &ledgerpb.ExistsCondition{}),
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			iter, err := Compile(
				nil, nil, tc.filter, badTarget, "ledger1",
				nil, nil, info, nil, nil, nil, nil, 0)
			require.Error(t, err, "unsupported target must fail loudly, not return an (empty) iterator")
			require.Nil(t, iter)

			var compileErr *domain.ErrFilterCompilation
			require.ErrorAs(t, err, &compileErr,
				"unsupported target must surface as a filter-compilation error (got %T: %v)", err, err)
			require.Contains(t, err.Error(), "unsupported query target")
		})
	}
}

// TestIsSupportedTarget pins the exact allow-list so a new QueryTarget enum
// value is rejected until its iteration + enrichment paths are wired.
func TestIsSupportedTarget(t *testing.T) {
	t.Parallel()

	require.True(t, isSupportedTarget(ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS))
	require.True(t, isSupportedTarget(ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS))
	require.True(t, isSupportedTarget(ledgerpb.QueryTarget_QUERY_TARGET_LOGS))
	// No zero sentinel exists (ACCOUNTS == 0), so an unsupported value can only
	// be an out-of-range enum — the shape a corrupt/forward-compat stored proto
	// would take.
	require.False(t, isSupportedTarget(ledgerpb.QueryTarget(9999)))
}

// staticIndexLookup is an in-memory indexes.Lookup for unit tests that
// need to populate the bucket-scoped Index registry without spinning up
// a Pebble store. Keyed exactly like the production registry.
type staticIndexLookup map[domain.IndexKey]*ledgerpb.Index

func (s staticIndexLookup) Get(key domain.IndexKey) (ledgerpb.IndexReader, error) {
	idx, ok := s[key]
	if !ok {
		return nil, domain.ErrNotFound
	}

	return idx.AsReader(), nil
}

// An absent per-replica record means the index was REMOVED, not that it is
// still building: checkIndexed only passes because the registry lists the
// index at this read's pin, and alignment puts the fold cursor at or beyond
// that pin, so the builder must already have written the record when it
// folded the CreateIndex log. Reporting "building" would tell the client to
// wait for a readiness that will never come.
func TestCompile_AbsentVersionRecordIsRemovedNotBuilding(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"

	id := indexes.MetadataID(ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, "k2")
	indexRegistry := staticIndexLookup{
		indexes.KeyFor(ledgerName, id): {Ledger: ledgerName, Id: id},
	}

	info := &ledgerpb.LedgerInfo{Name: ledgerName}
	schema := map[string]*ledgerpb.MetadataFieldSchema{
		"k2": {Type: ledgerpb.MetadataType_METADATA_TYPE_INT64},
	}

	filter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field:     &ledgerpb.FieldRef{Metadata: "k2"},
				Condition: &ledgerpb.FieldCondition_ExistsCond{ExistsCond: &ledgerpb.ExistsCondition{}},
			},
		},
	}

	for _, tc := range []struct {
		name   string
		primed bool
		assert func(t *testing.T, err error)
	}{
		{
			name:   "record absent — removed",
			primed: false,
			assert: func(t *testing.T, err error) {
				var notFound *domain.ErrIndexNotFound
				require.ErrorAs(t, err, &notFound)
			},
		},
		{
			name:   "record present at version 0 — still building",
			primed: true,
			assert: func(t *testing.T, err error) {
				var building *domain.ErrIndexBuilding
				require.ErrorAs(t, err, &building)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := Compile(
				nil, nil, filter,
				ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
				nil, schema, info, indexRegistry,
				func(string) (readstore.ResolvedIndexVersion, bool, error) {
					return readstore.ResolvedIndexVersion{BindingKnown: true}, tc.primed, nil
				},
				nil, nil, 0,
			)
			require.Error(t, err)
			tc.assert(t, err)
		})
	}
}
