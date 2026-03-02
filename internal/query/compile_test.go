package query

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptr[T any](v T) *T { return &v }

func fieldCondition(metaKey string, cond interface{}) *commonpb.FieldCondition {
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
			fc:     fieldCondition("age", &commonpb.IntCondition{Min: ptr(int64(10)), Max: ptr(int64(99))}),
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
			fc:      fieldCondition("age", &commonpb.UintCondition{Min: ptr(uint64(10))}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			wantErr: `field "age" is declared as METADATA_TYPE_INT64, cannot use unsigned integer condition`,
		},
		{
			name:   "uint schema + IntCondition (positive) → coerced to UintCondition",
			fc:     fieldCondition("counter", &commonpb.IntCondition{Min: ptr(int64(5)), Max: ptr(int64(100))}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			checkCond: func(t *testing.T, fc *commonpb.FieldCondition) {
				t.Helper()
				uc, ok := fc.Condition.(*commonpb.FieldCondition_UintCond)
				require.True(t, ok, "expected UintCondition after coercion")
				require.NotNil(t, uc.UintCond.Min)
				assert.Equal(t, uint64(5), *uc.UintCond.Min)
				require.NotNil(t, uc.UintCond.Max)
				assert.Equal(t, uint64(100), *uc.UintCond.Max)
			},
		},
		{
			name:   "uint schema + IntCondition with params → coerced preserving params",
			fc:     fieldCondition("counter", &commonpb.IntCondition{ParamMin: "lo", ParamMax: "hi", MinExclusive: true}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			checkCond: func(t *testing.T, fc *commonpb.FieldCondition) {
				t.Helper()
				uc, ok := fc.Condition.(*commonpb.FieldCondition_UintCond)
				require.True(t, ok, "expected UintCondition after coercion")
				assert.Equal(t, "lo", uc.UintCond.ParamMin)
				assert.Equal(t, "hi", uc.UintCond.ParamMax)
				assert.True(t, uc.UintCond.MinExclusive)
			},
		},
		{
			name:    "uint schema + IntCondition (negative min) → error",
			fc:      fieldCondition("counter", &commonpb.IntCondition{Min: ptr(int64(-1))}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			wantErr: `field "counter" is unsigned, cannot use negative min bound -1`,
		},
		{
			name:    "uint schema + IntCondition (negative max) → error",
			fc:      fieldCondition("counter", &commonpb.IntCondition{Max: ptr(int64(-5))}),
			schema:  &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			wantErr: `field "counter" is unsigned, cannot use negative max bound -5`,
		},
		{
			name:   "uint schema + UintCondition → OK",
			fc:     fieldCondition("counter", &commonpb.UintCondition{Min: ptr(uint64(10))}),
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
			fc:      fieldCondition("name", &commonpb.IntCondition{Min: ptr(int64(5))}),
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
			fc:      fieldCondition("name", &commonpb.UintCondition{Min: ptr(uint64(1))}),
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
			fc:      fieldCondition("active", &commonpb.IntCondition{Min: ptr(int64(1))}),
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
			fc:     fieldCondition("level", &commonpb.IntCondition{Min: ptr(int64(-128)), Max: ptr(int64(127))}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_INT8},
		},
		{
			name:   "uint16 schema + IntCondition → coerced",
			fc:     fieldCondition("port", &commonpb.IntCondition{Min: ptr(int64(80))}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT16},
			checkCond: func(t *testing.T, fc *commonpb.FieldCondition) {
				t.Helper()
				uc, ok := fc.Condition.(*commonpb.FieldCondition_UintCond)
				require.True(t, ok, "expected UintCondition after coercion")
				require.NotNil(t, uc.UintCond.Min)
				assert.Equal(t, uint64(80), *uc.UintCond.Min)
			},
		},
		{
			name:   "uint schema + IntCondition with zero min → coerced",
			fc:     fieldCondition("counter", &commonpb.IntCondition{Min: ptr(int64(0))}),
			schema: &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			checkCond: func(t *testing.T, fc *commonpb.FieldCondition) {
				t.Helper()
				uc, ok := fc.Condition.(*commonpb.FieldCondition_UintCond)
				require.True(t, ok, "expected UintCondition after coercion")
				require.NotNil(t, uc.UintCond.Min)
				assert.Equal(t, uint64(0), *uc.UintCond.Min)
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
		Min:          ptr(int64(10)),
		Max:          ptr(int64(20)),
		MinExclusive: true,
		MaxExclusive: true,
	})
	schema := &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT32}

	got, err := validateAndCoerceCondition(fc, schema)
	require.NoError(t, err)

	uc, ok := got.Condition.(*commonpb.FieldCondition_UintCond)
	require.True(t, ok)
	assert.True(t, uc.UintCond.MinExclusive)
	assert.True(t, uc.UintCond.MaxExclusive)
	require.NotNil(t, uc.UintCond.Min)
	assert.Equal(t, uint64(10), *uc.UintCond.Min)
	require.NotNil(t, uc.UintCond.Max)
	assert.Equal(t, uint64(20), *uc.UintCond.Max)
}

func TestCoerceIntToUint_NoMinNoMax(t *testing.T) {
	t.Parallel()

	// IntCondition with no bounds (just params) should coerce cleanly
	fc := fieldCondition("x", &commonpb.IntCondition{})
	schema := &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64}

	got, err := validateAndCoerceCondition(fc, schema)
	require.NoError(t, err)

	uc, ok := got.Condition.(*commonpb.FieldCondition_UintCond)
	require.True(t, ok)
	assert.Nil(t, uc.UintCond.Min)
	assert.Nil(t, uc.UintCond.Max)
}

func TestCoerceIntToUint_FieldRefPreserved(t *testing.T) {
	t.Parallel()

	fc := fieldCondition("myfield", &commonpb.IntCondition{Min: ptr(int64(0))})
	schema := &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64}

	got, err := validateAndCoerceCondition(fc, schema)
	require.NoError(t, err)
	assert.Equal(t, "myfield", got.Field.Metadata)
}

func TestValidateCondition_BoolSchemaRejectsUint(t *testing.T) {
	t.Parallel()

	fc := fieldCondition("active", &commonpb.UintCondition{Min: ptr(uint64(1))})
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
		params     map[string]string
		wantMin    int64
		wantMax    int64
		wantHasMin bool
		wantHasMax bool
		wantEq     bool
		wantErr    bool
	}{
		{
			name:       "equality: min == max, both inclusive",
			cond:       &commonpb.IntCondition{Min: ptr(int64(25)), Max: ptr(int64(25))},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "range: min < max",
			cond:       &commonpb.IntCondition{Min: ptr(int64(10)), Max: ptr(int64(20))},
			wantMin:    10,
			wantMax:    21,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     false,
		},
		{
			name:       "min exclusive: min=24 exclusive → effective 25, max=25 inclusive → 26",
			cond:       &commonpb.IntCondition{Min: ptr(int64(24)), Max: ptr(int64(25)), MinExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "max exclusive: min=25, max=26 exclusive → equality on 25",
			cond:       &commonpb.IntCondition{Min: ptr(int64(25)), Max: ptr(int64(26)), MaxExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "both exclusive: min=24 excl, max=26 excl → range [25, 26) = equality",
			cond:       &commonpb.IntCondition{Min: ptr(int64(24)), Max: ptr(int64(26)), MinExclusive: true, MaxExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "only min",
			cond:       &commonpb.IntCondition{Min: ptr(int64(5))},
			wantMin:    5,
			wantHasMin: true,
			wantHasMax: false,
			wantEq:     false,
		},
		{
			name:       "only max",
			cond:       &commonpb.IntCondition{Max: ptr(int64(100))},
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
			params:     map[string]string{"val": "42"},
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
		params     map[string]string
		wantMin    uint64
		wantMax    uint64
		wantHasMin bool
		wantHasMax bool
		wantEq     bool
		wantErr    bool
	}{
		{
			name:       "equality: min == max, both inclusive",
			cond:       &commonpb.UintCondition{Min: ptr(uint64(25)), Max: ptr(uint64(25))},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "range: min < max",
			cond:       &commonpb.UintCondition{Min: ptr(uint64(10)), Max: ptr(uint64(20))},
			wantMin:    10,
			wantMax:    21,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     false,
		},
		{
			name:       "min exclusive: min=24 exclusive, max=25 → equality on 25",
			cond:       &commonpb.UintCondition{Min: ptr(uint64(24)), Max: ptr(uint64(25)), MinExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "max exclusive: min=25, max=26 exclusive → equality on 25",
			cond:       &commonpb.UintCondition{Min: ptr(uint64(25)), Max: ptr(uint64(26)), MaxExclusive: true},
			wantMin:    25,
			wantMax:    26,
			wantHasMin: true,
			wantHasMax: true,
			wantEq:     true,
		},
		{
			name:       "only min",
			cond:       &commonpb.UintCondition{Min: ptr(uint64(5))},
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
			params:     map[string]string{"v": "100"},
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
