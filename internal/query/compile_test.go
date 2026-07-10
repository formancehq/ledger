package query

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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
	var leaf *commonpb.QueryFilter // nil = universe; would reach compileUniverse if we got there.
	filter := leaf

	for range MaxFilterDepth + 5 {
		filter = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Or{
				Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{filter}},
			},
		}
	}

	ctx := &compileCtx{
		target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
	}

	_, err := compile(ctx, filter)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrFilterTooDeep),
		"deeply-nested QueryFilter must trip the depth guard, got: %v", err)
}

func fieldCondition(metaKey string, cond any) *commonpb.FieldCondition {
	fc := &commonpb.FieldCondition{
		Field: &commonpb.FieldRef{Metadata: metaKey},
	}

	switch c := cond.(type) {
	case *commonpb.IntCondition:
		fc.Condition = &commonpb.FieldCondition_IntCond{IntCond: c}
	case *commonpb.UintCondition:
		fc.Condition = &commonpb.FieldCondition_UintCond{UintCond: c}
	case *commonpb.StringCondition:
		fc.Condition = &commonpb.FieldCondition_StringCond{StringCond: c}
	case *commonpb.BoolCondition:
		fc.Condition = &commonpb.FieldCondition_BoolCond{BoolCond: c}
	case *commonpb.ExistsCondition:
		fc.Condition = &commonpb.FieldCondition_ExistsCond{ExistsCond: c}
	}

	return fc
}

func TestValidateAndCoerceCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fc        *commonpb.FieldCondition
		schema    *commonpb.MetadataFieldSchema
		wantErr   string
		checkCond func(t *testing.T, fc *commonpb.FieldCondition)
	}{
		{
			name:   "int schema + IntCondition → OK",
			fc:     fieldCondition("age", &commonpb.IntCondition{Min: new(int64(10)), Max: new(int64(99))}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_INT64},
		},
		{
			name:    "int schema + StringCondition → error",
			fc:      fieldCondition("age", &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "hello"}}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			wantErr: `field "age" is declared as METADATA_TYPE_INT64, cannot use string condition`,
		},
		{
			name:    "int schema + BoolCondition → error",
			fc:      fieldCondition("age", &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: true}}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			wantErr: `field "age" is declared as METADATA_TYPE_INT64, cannot use bool condition`,
		},
		{
			name:    "int schema + UintCondition → error",
			fc:      fieldCondition("age", &commonpb.UintCondition{Min: new(uint64(10))}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			wantErr: `field "age" is declared as METADATA_TYPE_INT64, cannot use unsigned integer condition`,
		},
		{
			name:   "uint schema + IntCondition (positive) → coerced to UintCondition",
			fc:     fieldCondition("counter", &commonpb.IntCondition{Min: new(int64(5)), Max: new(int64(100))}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			checkCond: func(t *testing.T, fc *commonpb.FieldCondition) {
				t.Helper()

				uc, ok := fc.GetCondition().(*commonpb.FieldCondition_UintCond)
				require.True(t, ok, "expected UintCondition after coercion")
				require.NotNil(t, uc.UintCond.Min)
				assert.Equal(t, uint64(5), uc.UintCond.GetMin())
				require.NotNil(t, uc.UintCond.Max)
				assert.Equal(t, uint64(100), uc.UintCond.GetMax())
			},
		},
		{
			name:   "uint schema + IntCondition with params → coerced preserving params",
			fc:     fieldCondition("counter", &commonpb.IntCondition{ParamMin: "lo", ParamMax: "hi", MinExclusive: true}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			checkCond: func(t *testing.T, fc *commonpb.FieldCondition) {
				t.Helper()

				uc, ok := fc.GetCondition().(*commonpb.FieldCondition_UintCond)
				require.True(t, ok, "expected UintCondition after coercion")
				assert.Equal(t, "lo", uc.UintCond.GetParamMin())
				assert.Equal(t, "hi", uc.UintCond.GetParamMax())
				assert.True(t, uc.UintCond.GetMinExclusive())
			},
		},
		{
			name:    "uint schema + IntCondition (negative min) → error",
			fc:      fieldCondition("counter", &commonpb.IntCondition{Min: new(int64(-1))}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			wantErr: `field "counter" is unsigned, cannot use negative min bound -1`,
		},
		{
			name:    "uint schema + IntCondition (negative max) → error",
			fc:      fieldCondition("counter", &commonpb.IntCondition{Max: new(int64(-5))}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			wantErr: `field "counter" is unsigned, cannot use negative max bound -5`,
		},
		{
			name:   "uint schema + UintCondition → OK",
			fc:     fieldCondition("counter", &commonpb.UintCondition{Min: new(uint64(10))}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
		},
		{
			name:    "uint schema + StringCondition → error",
			fc:      fieldCondition("counter", &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "hello"}}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			wantErr: `field "counter" is declared as METADATA_TYPE_UINT64, cannot use string condition`,
		},
		{
			name:   "string schema + StringCondition → OK",
			fc:     fieldCondition("name", &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "alice"}}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		},
		{
			name:    "string schema + IntCondition → error",
			fc:      fieldCondition("name", &commonpb.IntCondition{Min: new(int64(5))}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_STRING},
			wantErr: `field "name" is declared as METADATA_TYPE_STRING, cannot use integer condition`,
		},
		{
			name:    "string schema + BoolCondition → error",
			fc:      fieldCondition("name", &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: true}}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_STRING},
			wantErr: `field "name" is declared as METADATA_TYPE_STRING, cannot use bool condition`,
		},
		{
			name:    "string schema + UintCondition → error",
			fc:      fieldCondition("name", &commonpb.UintCondition{Min: new(uint64(1))}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_STRING},
			wantErr: `field "name" is declared as METADATA_TYPE_STRING, cannot use unsigned integer condition`,
		},
		{
			name:   "bool schema + BoolCondition → OK",
			fc:     fieldCondition("active", &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: true}}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_BOOL},
		},
		{
			name:    "bool schema + StringCondition → error",
			fc:      fieldCondition("active", &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "true"}}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_BOOL},
			wantErr: `field "active" is declared as METADATA_TYPE_BOOL, cannot use string condition`,
		},
		{
			name:    "bool schema + IntCondition → error",
			fc:      fieldCondition("active", &commonpb.IntCondition{Min: new(int64(1))}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_BOOL},
			wantErr: `field "active" is declared as METADATA_TYPE_BOOL, cannot use integer condition`,
		},
		{
			name:   "ExistsCondition + int schema → OK",
			fc:     fieldCondition("age", &commonpb.ExistsCondition{}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_INT64},
		},
		{
			name:   "ExistsCondition + string schema → OK",
			fc:     fieldCondition("name", &commonpb.ExistsCondition{}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		},
		{
			name:   "ExistsCondition + bool schema → OK",
			fc:     fieldCondition("active", &commonpb.ExistsCondition{}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_BOOL},
		},
		{
			name:   "int8 schema + IntCondition → OK",
			fc:     fieldCondition("level", &commonpb.IntCondition{Min: new(int64(-128)), Max: new(int64(127))}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_INT8},
		},
		{
			name:   "uint16 schema + IntCondition → coerced",
			fc:     fieldCondition("port", &commonpb.IntCondition{Min: new(int64(80))}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT16},
			checkCond: func(t *testing.T, fc *commonpb.FieldCondition) {
				t.Helper()

				uc, ok := fc.GetCondition().(*commonpb.FieldCondition_UintCond)
				require.True(t, ok, "expected UintCondition after coercion")
				require.NotNil(t, uc.UintCond.Min)
				assert.Equal(t, uint64(80), uc.UintCond.GetMin())
			},
		},
		{
			name:   "uint schema + IntCondition with zero min → coerced",
			fc:     fieldCondition("counter", &commonpb.IntCondition{Min: new(int64(0))}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			checkCond: func(t *testing.T, fc *commonpb.FieldCondition) {
				t.Helper()

				uc, ok := fc.GetCondition().(*commonpb.FieldCondition_UintCond)
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
	fc := fieldCondition("x", &commonpb.IntCondition{
		Min:          new(int64(10)),
		Max:          new(int64(20)),
		MinExclusive: true,
		MaxExclusive: true,
	})
	schema := &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT32}

	got, err := validateAndCoerceCondition(fc, schema)
	require.NoError(t, err)

	uc, ok := got.GetCondition().(*commonpb.FieldCondition_UintCond)
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
	fc := fieldCondition("x", &commonpb.IntCondition{})
	schema := &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64}

	got, err := validateAndCoerceCondition(fc, schema)
	require.NoError(t, err)

	uc, ok := got.GetCondition().(*commonpb.FieldCondition_UintCond)
	require.True(t, ok)
	assert.Nil(t, uc.UintCond.Min)
	assert.Nil(t, uc.UintCond.Max)
}

func TestCoerceIntToUint_FieldRefPreserved(t *testing.T) {
	t.Parallel()

	fc := fieldCondition("myfield", &commonpb.IntCondition{Min: new(int64(0))})
	schema := &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64}

	got, err := validateAndCoerceCondition(fc, schema)
	require.NoError(t, err)
	assert.Equal(t, "myfield", got.GetField().GetMetadata())
}

func TestValidateCondition_BoolSchemaRejectsUint(t *testing.T) {
	t.Parallel()

	fc := fieldCondition("active", &commonpb.UintCondition{Min: new(uint64(1))})
	schema := &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_BOOL}

	_, err := validateAndCoerceCondition(fc, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot use unsigned integer condition")
}

func TestValidateCondition_UintSchemaRejectsBool(t *testing.T) {
	t.Parallel()

	fc := fieldCondition("counter", &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: true}})
	schema := &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64}

	_, err := validateAndCoerceCondition(fc, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot use bool condition")
}

func TestResolveIntBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cond       *commonpb.IntCondition
		params     map[string]*commonpb.ParameterValue
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
			cond:       &commonpb.IntCondition{Min: new(int64(25)), Max: new(int64(25))},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "range: min < max",
			cond:       &commonpb.IntCondition{Min: new(int64(10)), Max: new(int64(20))},
			wantMin:    10,
			wantMax:    21,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     false,
		},
		{
			name:       "min exclusive: min=24 exclusive → effective 25, max=25 inclusive → 26",
			cond:       &commonpb.IntCondition{Min: new(int64(24)), Max: new(int64(25)), MinExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "max exclusive: min=25, max=26 exclusive → equality on 25",
			cond:       &commonpb.IntCondition{Min: new(int64(25)), Max: new(int64(26)), MaxExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "both exclusive: min=24 excl, max=26 excl → range [25, 26) = equality",
			cond:       &commonpb.IntCondition{Min: new(int64(24)), Max: new(int64(26)), MinExclusive: true, MaxExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "only min",
			cond:       &commonpb.IntCondition{Min: new(int64(5))},
			wantMin:    5,
			wantHasMin: true,
			wantHasMax: false,
			wantEq:     false,
		},
		{
			name:       "only max",
			cond:       &commonpb.IntCondition{Max: new(int64(100))},
			wantMax:    101,
			wantHasMin: false,
			wantHasMax: true,
			wantEq:     false,
		},
		{
			name:       "no bounds",
			cond:       &commonpb.IntCondition{},
			wantHasMin: false,
			wantHasMax: false,
			wantEq:     false,
		},
		{
			name:       "param equality: same param for min and max",
			cond:       &commonpb.IntCondition{ParamMin: "val", ParamMax: "val"},
			params:     map[string]*commonpb.ParameterValue{"val": {Value: &commonpb.ParameterValue_Int64Value{Int64Value: 42}}},
			wantMin:    42,
			wantMax:    43,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:    "param error: missing param",
			cond:    &commonpb.IntCondition{ParamMin: "missing"},
			wantErr: true,
		},
		{
			name:      "overflow: min exclusive at MaxInt64 → empty",
			cond:      &commonpb.IntCondition{Min: new(int64(math.MaxInt64)), MinExclusive: true},
			wantEmpty: true,
		},
		{
			name:       "overflow: max inclusive at MaxInt64 → unbounded above",
			cond:       &commonpb.IntCondition{Min: new(int64(0)), Max: new(int64(math.MaxInt64))},
			wantMin:    0,
			wantHasMin: true,
			wantHasMax: false,
		},
		{
			name:      "overflow: param min exclusive at MaxInt64 → empty",
			cond:      &commonpb.IntCondition{ParamMin: "v", MinExclusive: true},
			params:    map[string]*commonpb.ParameterValue{"v": {Value: &commonpb.ParameterValue_Int64Value{Int64Value: math.MaxInt64}}},
			wantEmpty: true,
		},
		{
			name:       "overflow: param max inclusive at MaxInt64 → unbounded above",
			cond:       &commonpb.IntCondition{Min: new(int64(0)), ParamMax: "v"},
			params:     map[string]*commonpb.ParameterValue{"v": {Value: &commonpb.ParameterValue_Int64Value{Int64Value: math.MaxInt64}}},
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
		cond       *commonpb.UintCondition
		params     map[string]*commonpb.ParameterValue
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
			cond:       &commonpb.UintCondition{Min: new(uint64(25)), Max: new(uint64(25))},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "range: min < max",
			cond:       &commonpb.UintCondition{Min: new(uint64(10)), Max: new(uint64(20))},
			wantMin:    10,
			wantMax:    21,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     false,
		},
		{
			name:       "min exclusive: min=24 exclusive, max=25 → equality on 25",
			cond:       &commonpb.UintCondition{Min: new(uint64(24)), Max: new(uint64(25)), MinExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "max exclusive: min=25, max=26 exclusive → equality on 25",
			cond:       &commonpb.UintCondition{Min: new(uint64(25)), Max: new(uint64(26)), MaxExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "only min",
			cond:       &commonpb.UintCondition{Min: new(uint64(5))},
			wantMin:    5,
			wantHasMin: true,
			wantHasMax: false,
			wantEq:     false,
		},
		{
			name:       "no bounds",
			cond:       &commonpb.UintCondition{},
			wantHasMin: false,
			wantHasMax: false,
			wantEq:     false,
		},
		{
			name:       "param equality",
			cond:       &commonpb.UintCondition{ParamMin: "v", ParamMax: "v"},
			params:     map[string]*commonpb.ParameterValue{"v": {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 100}}},
			wantMin:    100,
			wantMax:    101,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:    "param error: missing param",
			cond:    &commonpb.UintCondition{ParamMax: "missing"},
			wantErr: true,
		},
		{
			name:      "overflow: min exclusive at MaxUint64 → empty",
			cond:      &commonpb.UintCondition{Min: new(uint64(math.MaxUint64)), MinExclusive: true},
			wantEmpty: true,
		},
		{
			name:       "overflow: max inclusive at MaxUint64 → unbounded above",
			cond:       &commonpb.UintCondition{Min: new(uint64(0)), Max: new(uint64(math.MaxUint64))},
			wantMin:    0,
			wantHasMin: true,
			wantHasMax: false,
		},
		{
			name:      "overflow: param min exclusive at MaxUint64 → empty",
			cond:      &commonpb.UintCondition{ParamMin: "v", MinExclusive: true},
			params:    map[string]*commonpb.ParameterValue{"v": {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: math.MaxUint64}}},
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

		result := SchemaFieldsForTarget(nil, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		assert.Nil(t, result)
	})

	t.Run("accounts target", func(t *testing.T) {
		t.Parallel()

		schema := &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"name": {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
			},
			TransactionFields: map[string]*commonpb.MetadataFieldSchema{
				"ref": {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
			},
		}
		result := SchemaFieldsForTarget(schema, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		require.Len(t, result, 1)
		assert.Contains(t, result, "name")
	})

	t.Run("transactions target", func(t *testing.T) {
		t.Parallel()

		schema := &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"name": {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
			},
			TransactionFields: map[string]*commonpb.MetadataFieldSchema{
				"ref": {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
			},
		}
		result := SchemaFieldsForTarget(schema, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
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
	// readstore.SnapshotVersionResolver against the iteration snapshot.
	indexResolverZero := func(string) (uint32, error) { return 0, nil }

	info := &commonpb.LedgerInfo{Name: ledgerName}

	// indexRegistry declares every builtin index via the bucket-scoped
	// Lookup interface (post-PR#453 architecture). Per-replica readiness
	// lives in IndexVersionState (modelled here via the resolver), not
	// on Index.BuildStatus — see EN-1323.
	indexRegistry := staticIndexLookup{}
	for _, id := range []*commonpb.IndexID{
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS),
		indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
	} {
		indexRegistry[indexes.KeyFor(ledgerName, id)] = &commonpb.Index{Ledger: ledgerName, Id: id}
	}

	tcs := []struct {
		name   string
		target commonpb.QueryTarget
		filter *commonpb.QueryFilter
	}{
		{
			name:   "reference",
			target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{
				Reference: &commonpb.ReferenceCondition{
					Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "x"}},
				},
			}},
		},
		{
			name:   "builtin-uint:timestamp",
			target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
				BuiltinUint: &commonpb.BuiltinUintCondition{
					Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
					Cond:  &commonpb.UintCondition{},
				},
			}},
		},
		{
			name:   "builtin-uint:inserted_at",
			target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
				BuiltinUint: &commonpb.BuiltinUintCondition{
					Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT,
					Cond:  &commonpb.UintCondition{},
				},
			}},
		},
		{
			name:   "address (transactions target)",
			target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Role:  commonpb.AddressRole_ADDRESS_ROLE_ANY,
					Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "alice"},
				},
			}},
		},
		{
			name:   "log-builtin-uint:date",
			target: commonpb.QueryTarget_QUERY_TARGET_LOGS,
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{
				LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
					Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
					Cond:  &commonpb.UintCondition{},
				},
			}},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := Compile(
				nil, nil, tc.filter, tc.target, ledgerName,
				nil, nil, info, indexRegistry, indexResolverZero, nil, nil)
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
	info := &commonpb.LedgerInfo{Name: "ledger1"}

	refID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	indexRegistry := staticIndexLookup{
		indexes.KeyFor("ledger1", refID): {Ledger: "ledger1", Id: refID},
	}

	ctx := &compileCtx{
		target:          commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		indexRegistry:   indexRegistry,
		ledgerName:      "ledger1",
		info:            info,
		indexVersionFor: func(string) (uint32, error) { return 0, pebbleErr },
	}

	_, err := requireIndexReady(ctx,
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
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
	const badTarget = commonpb.QueryTarget(9999)

	info := &commonpb.LedgerInfo{Name: "ledger1"}

	cases := []struct {
		name   string
		filter *commonpb.QueryFilter
	}{
		{
			name:   "nil filter (universe path)",
			filter: nil,
		},
		{
			name: "field filter",
			filter: &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: fieldCondition("x", &commonpb.ExistsCondition{}),
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			iter, err := Compile(
				nil, nil, tc.filter, badTarget, "ledger1",
				nil, nil, info, nil, nil, nil, nil)
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

	require.True(t, isSupportedTarget(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS))
	require.True(t, isSupportedTarget(commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS))
	require.True(t, isSupportedTarget(commonpb.QueryTarget_QUERY_TARGET_LOGS))
	// No zero sentinel exists (ACCOUNTS == 0), so an unsupported value can only
	// be an out-of-range enum — the shape a corrupt/forward-compat stored proto
	// would take.
	require.False(t, isSupportedTarget(commonpb.QueryTarget(9999)))
}

// staticIndexLookup is an in-memory indexes.Lookup for unit tests that
// need to populate the bucket-scoped Index registry without spinning up
// a Pebble store. Keyed exactly like the production registry.
type staticIndexLookup map[domain.IndexKey]*commonpb.Index

func (s staticIndexLookup) Get(key domain.IndexKey) (commonpb.IndexReader, error) {
	idx, ok := s[key]
	if !ok {
		return nil, domain.ErrNotFound
	}

	return idx.AsReader(), nil
}
