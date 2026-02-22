package commonpb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStringValue(t *testing.T) {
	t.Parallel()
	v := NewStringValue("hello")
	require.NotNil(t, v)
	sv, ok := v.Type.(*MetadataValue_StringValue)
	require.True(t, ok)
	assert.Equal(t, "hello", sv.StringValue)
}

func TestNewIntValue(t *testing.T) {
	t.Parallel()
	v := NewIntValue(-42)
	require.NotNil(t, v)
	iv, ok := v.Type.(*MetadataValue_IntValue)
	require.True(t, ok)
	assert.Equal(t, int64(-42), iv.IntValue)
}

func TestNewUintValue(t *testing.T) {
	t.Parallel()
	v := NewUintValue(42)
	require.NotNil(t, v)
	uv, ok := v.Type.(*MetadataValue_UintValue)
	require.True(t, ok)
	assert.Equal(t, uint64(42), uv.UintValue)
}

func TestNewBoolValue(t *testing.T) {
	t.Parallel()
	v := NewBoolValue(true)
	require.NotNil(t, v)
	bv, ok := v.Type.(*MetadataValue_BoolValue)
	require.True(t, ok)
	assert.True(t, bv.BoolValue)
}

func TestNewNullValue(t *testing.T) {
	t.Parallel()
	v := NewNullValue("original")
	require.NotNil(t, v)
	nv, ok := v.Type.(*MetadataValue_NullValue)
	require.True(t, ok)
	require.NotNil(t, nv.NullValue)
	assert.Equal(t, "original", nv.NullValue.Original)
}

func TestMetadataValueToString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    *MetadataValue
		expected string
	}{
		{"nil", nil, ""},
		{"string", NewStringValue("hello"), "hello"},
		{"int positive", NewIntValue(42), "42"},
		{"int negative", NewIntValue(-42), "-42"},
		{"int zero", NewIntValue(0), "0"},
		{"uint", NewUintValue(42), "42"},
		{"uint zero", NewUintValue(0), "0"},
		{"bool true", NewBoolValue(true), "true"},
		{"bool false", NewBoolValue(false), "false"},
		{"null", NewNullValue("original"), "original"},
		{"null empty", NewNullValue(""), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, MetadataValueToString(tt.value))
		})
	}
}

func TestTypeMatches(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    *MetadataValue
		target   MetadataType
		expected bool
	}{
		{"string matches string", NewStringValue("x"), MetadataType_METADATA_TYPE_STRING, true},
		{"string doesn't match int", NewStringValue("x"), MetadataType_METADATA_TYPE_INT64, false},
		{"int matches int", NewIntValue(1), MetadataType_METADATA_TYPE_INT64, true},
		{"int doesn't match string", NewIntValue(1), MetadataType_METADATA_TYPE_STRING, false},
		{"uint matches uint", NewUintValue(1), MetadataType_METADATA_TYPE_UINT64, true},
		{"uint doesn't match bool", NewUintValue(1), MetadataType_METADATA_TYPE_BOOL, false},
		{"bool matches bool", NewBoolValue(true), MetadataType_METADATA_TYPE_BOOL, true},
		{"bool doesn't match uint", NewBoolValue(true), MetadataType_METADATA_TYPE_UINT64, false},
		{"nil value", nil, MetadataType_METADATA_TYPE_STRING, false},

		// Sub-64-bit signed: value stored as int_value, range-checked.
		{"int8 in range", NewIntValue(127), MetadataType_METADATA_TYPE_INT8, true},
		{"int8 min", NewIntValue(-128), MetadataType_METADATA_TYPE_INT8, true},
		{"int8 overflow", NewIntValue(128), MetadataType_METADATA_TYPE_INT8, false},
		{"int8 underflow", NewIntValue(-129), MetadataType_METADATA_TYPE_INT8, false},
		{"int8 wrong storage", NewUintValue(1), MetadataType_METADATA_TYPE_INT8, false},

		{"int16 in range", NewIntValue(32767), MetadataType_METADATA_TYPE_INT16, true},
		{"int16 min", NewIntValue(-32768), MetadataType_METADATA_TYPE_INT16, true},
		{"int16 overflow", NewIntValue(32768), MetadataType_METADATA_TYPE_INT16, false},

		{"int32 in range", NewIntValue(math.MaxInt32), MetadataType_METADATA_TYPE_INT32, true},
		{"int32 overflow", NewIntValue(math.MaxInt32 + 1), MetadataType_METADATA_TYPE_INT32, false},

		// Sub-64-bit unsigned: value stored as uint_value, range-checked.
		{"uint8 in range", NewUintValue(255), MetadataType_METADATA_TYPE_UINT8, true},
		{"uint8 overflow", NewUintValue(256), MetadataType_METADATA_TYPE_UINT8, false},
		{"uint8 wrong storage", NewIntValue(1), MetadataType_METADATA_TYPE_UINT8, false},

		{"uint16 in range", NewUintValue(65535), MetadataType_METADATA_TYPE_UINT16, true},
		{"uint16 overflow", NewUintValue(65536), MetadataType_METADATA_TYPE_UINT16, false},

		{"uint32 in range", NewUintValue(math.MaxUint32), MetadataType_METADATA_TYPE_UINT32, true},
		{"uint32 overflow", NewUintValue(math.MaxUint32 + 1), MetadataType_METADATA_TYPE_UINT32, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, TypeMatches(tt.value, tt.target))
		})
	}
}

func TestConvertFromString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		target   MetadataType
		expected *MetadataValue
	}{
		// to string (identity)
		{"string to string", "hello", MetadataType_METADATA_TYPE_STRING, NewStringValue("hello")},
		// to int64
		{"string to int64 positive", "42", MetadataType_METADATA_TYPE_INT64, NewIntValue(42)},
		{"string to int64 negative", "-42", MetadataType_METADATA_TYPE_INT64, NewIntValue(-42)},
		{"string to int64 zero", "0", MetadataType_METADATA_TYPE_INT64, NewIntValue(0)},
		{"string to int64 invalid", "hello", MetadataType_METADATA_TYPE_INT64, NewNullValue("hello")},
		{"string to int64 float", "3.14", MetadataType_METADATA_TYPE_INT64, NewNullValue("3.14")},
		// to uint64
		{"string to uint64 positive", "42", MetadataType_METADATA_TYPE_UINT64, NewUintValue(42)},
		{"string to uint64 zero", "0", MetadataType_METADATA_TYPE_UINT64, NewUintValue(0)},
		{"string to uint64 negative", "-1", MetadataType_METADATA_TYPE_UINT64, NewNullValue("-1")},
		{"string to uint64 invalid", "hello", MetadataType_METADATA_TYPE_UINT64, NewNullValue("hello")},
		// to bool
		{"string to bool true", "true", MetadataType_METADATA_TYPE_BOOL, NewBoolValue(true)},
		{"string to bool TRUE", "TRUE", MetadataType_METADATA_TYPE_BOOL, NewBoolValue(true)},
		{"string to bool 1", "1", MetadataType_METADATA_TYPE_BOOL, NewBoolValue(true)},
		{"string to bool false", "false", MetadataType_METADATA_TYPE_BOOL, NewBoolValue(false)},
		{"string to bool FALSE", "FALSE", MetadataType_METADATA_TYPE_BOOL, NewBoolValue(false)},
		{"string to bool 0", "0", MetadataType_METADATA_TYPE_BOOL, NewBoolValue(false)},
		{"string to bool invalid", "maybe", MetadataType_METADATA_TYPE_BOOL, NewNullValue("maybe")},

		// to int8
		{"string to int8 valid", "42", MetadataType_METADATA_TYPE_INT8, NewIntValue(42)},
		{"string to int8 max", "127", MetadataType_METADATA_TYPE_INT8, NewIntValue(127)},
		{"string to int8 min", "-128", MetadataType_METADATA_TYPE_INT8, NewIntValue(-128)},
		{"string to int8 overflow", "128", MetadataType_METADATA_TYPE_INT8, NewNullValue("128")},
		{"string to int8 underflow", "-129", MetadataType_METADATA_TYPE_INT8, NewNullValue("-129")},
		// to int16
		{"string to int16 valid", "1000", MetadataType_METADATA_TYPE_INT16, NewIntValue(1000)},
		{"string to int16 overflow", "32768", MetadataType_METADATA_TYPE_INT16, NewNullValue("32768")},
		// to int32
		{"string to int32 valid", "100000", MetadataType_METADATA_TYPE_INT32, NewIntValue(100000)},
		{"string to int32 overflow", "2147483648", MetadataType_METADATA_TYPE_INT32, NewNullValue("2147483648")},
		// to uint8
		{"string to uint8 valid", "200", MetadataType_METADATA_TYPE_UINT8, NewUintValue(200)},
		{"string to uint8 max", "255", MetadataType_METADATA_TYPE_UINT8, NewUintValue(255)},
		{"string to uint8 overflow", "256", MetadataType_METADATA_TYPE_UINT8, NewNullValue("256")},
		{"string to uint8 negative", "-1", MetadataType_METADATA_TYPE_UINT8, NewNullValue("-1")},
		// to uint16
		{"string to uint16 valid", "60000", MetadataType_METADATA_TYPE_UINT16, NewUintValue(60000)},
		{"string to uint16 overflow", "65536", MetadataType_METADATA_TYPE_UINT16, NewNullValue("65536")},
		// to uint32
		{"string to uint32 valid", "3000000000", MetadataType_METADATA_TYPE_UINT32, NewUintValue(3000000000)},
		{"string to uint32 overflow", "4294967296", MetadataType_METADATA_TYPE_UINT32, NewNullValue("4294967296")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ConvertMetadataValue(NewStringValue(tt.input), tt.target)
			assert.Equal(t, tt.expected.Type, result.Type)
		})
	}
}

func TestConvertFromInt64(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    int64
		target   MetadataType
		expected *MetadataValue
	}{
		// to string
		{"int64 to string positive", 42, MetadataType_METADATA_TYPE_STRING, NewStringValue("42")},
		{"int64 to string negative", -42, MetadataType_METADATA_TYPE_STRING, NewStringValue("-42")},
		{"int64 to string zero", 0, MetadataType_METADATA_TYPE_STRING, NewStringValue("0")},
		// to int64 (identity)
		{"int64 to int64", 42, MetadataType_METADATA_TYPE_INT64, NewIntValue(42)},
		// to uint64
		{"int64 to uint64 positive", 42, MetadataType_METADATA_TYPE_UINT64, NewUintValue(42)},
		{"int64 to uint64 zero", 0, MetadataType_METADATA_TYPE_UINT64, NewUintValue(0)},
		{"int64 to uint64 negative", -1, MetadataType_METADATA_TYPE_UINT64, NewNullValue("-1")},
		// to bool
		{"int64 to bool non-zero", 1, MetadataType_METADATA_TYPE_BOOL, NewBoolValue(true)},
		{"int64 to bool negative", -1, MetadataType_METADATA_TYPE_BOOL, NewBoolValue(true)},
		{"int64 to bool zero", 0, MetadataType_METADATA_TYPE_BOOL, NewBoolValue(false)},

		// to int8 (narrowing)
		{"int64 to int8 in range", 100, MetadataType_METADATA_TYPE_INT8, NewIntValue(100)},
		{"int64 to int8 overflow", 128, MetadataType_METADATA_TYPE_INT8, NewNullValue("128")},
		{"int64 to int8 underflow", -129, MetadataType_METADATA_TYPE_INT8, NewNullValue("-129")},
		// to int16 (narrowing)
		{"int64 to int16 in range", 30000, MetadataType_METADATA_TYPE_INT16, NewIntValue(30000)},
		{"int64 to int16 overflow", 32768, MetadataType_METADATA_TYPE_INT16, NewNullValue("32768")},
		// to int32 (narrowing)
		{"int64 to int32 in range", 100000, MetadataType_METADATA_TYPE_INT32, NewIntValue(100000)},
		{"int64 to int32 overflow", math.MaxInt32 + 1, MetadataType_METADATA_TYPE_INT32, NewNullValue("2147483648")},
		// to uint8
		{"int64 to uint8 in range", 200, MetadataType_METADATA_TYPE_UINT8, NewUintValue(200)},
		{"int64 to uint8 overflow", 256, MetadataType_METADATA_TYPE_UINT8, NewNullValue("256")},
		{"int64 to uint8 negative", -1, MetadataType_METADATA_TYPE_UINT8, NewNullValue("-1")},
		// to uint16
		{"int64 to uint16 in range", 60000, MetadataType_METADATA_TYPE_UINT16, NewUintValue(60000)},
		{"int64 to uint16 overflow", 65536, MetadataType_METADATA_TYPE_UINT16, NewNullValue("65536")},
		// to uint32
		{"int64 to uint32 in range", 3000000000, MetadataType_METADATA_TYPE_UINT32, NewUintValue(3000000000)},
		{"int64 to uint32 overflow", math.MaxUint32 + 1, MetadataType_METADATA_TYPE_UINT32, NewNullValue("4294967296")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ConvertMetadataValue(NewIntValue(tt.input), tt.target)
			assert.Equal(t, tt.expected.Type, result.Type)
		})
	}
}

func TestConvertFromUint64(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    uint64
		target   MetadataType
		expected *MetadataValue
	}{
		// to string
		{"uint64 to string", 42, MetadataType_METADATA_TYPE_STRING, NewStringValue("42")},
		{"uint64 to string zero", 0, MetadataType_METADATA_TYPE_STRING, NewStringValue("0")},
		// to int64
		{"uint64 to int64 small", 42, MetadataType_METADATA_TYPE_INT64, NewIntValue(42)},
		{"uint64 to int64 max", uint64(math.MaxInt64), MetadataType_METADATA_TYPE_INT64, NewIntValue(math.MaxInt64)},
		{"uint64 to int64 overflow", uint64(math.MaxInt64) + 1, MetadataType_METADATA_TYPE_INT64, NewNullValue("9223372036854775808")},
		// to uint64 (identity)
		{"uint64 to uint64", 42, MetadataType_METADATA_TYPE_UINT64, NewUintValue(42)},
		// to bool
		{"uint64 to bool non-zero", 1, MetadataType_METADATA_TYPE_BOOL, NewBoolValue(true)},
		{"uint64 to bool zero", 0, MetadataType_METADATA_TYPE_BOOL, NewBoolValue(false)},

		// to int8 (narrowing from uint64)
		{"uint64 to int8 in range", 100, MetadataType_METADATA_TYPE_INT8, NewIntValue(100)},
		{"uint64 to int8 overflow", 128, MetadataType_METADATA_TYPE_INT8, NewNullValue("128")},
		// to int16
		{"uint64 to int16 in range", 30000, MetadataType_METADATA_TYPE_INT16, NewIntValue(30000)},
		{"uint64 to int16 overflow", 32768, MetadataType_METADATA_TYPE_INT16, NewNullValue("32768")},
		// to int32
		{"uint64 to int32 in range", 100000, MetadataType_METADATA_TYPE_INT32, NewIntValue(100000)},
		{"uint64 to int32 overflow", math.MaxInt32 + 1, MetadataType_METADATA_TYPE_INT32, NewNullValue("2147483648")},
		// to uint8
		{"uint64 to uint8 in range", 200, MetadataType_METADATA_TYPE_UINT8, NewUintValue(200)},
		{"uint64 to uint8 overflow", 256, MetadataType_METADATA_TYPE_UINT8, NewNullValue("256")},
		// to uint16
		{"uint64 to uint16 in range", 60000, MetadataType_METADATA_TYPE_UINT16, NewUintValue(60000)},
		{"uint64 to uint16 overflow", 65536, MetadataType_METADATA_TYPE_UINT16, NewNullValue("65536")},
		// to uint32
		{"uint64 to uint32 in range", 3000000000, MetadataType_METADATA_TYPE_UINT32, NewUintValue(3000000000)},
		{"uint64 to uint32 overflow", math.MaxUint32 + 1, MetadataType_METADATA_TYPE_UINT32, NewNullValue("4294967296")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ConvertMetadataValue(NewUintValue(tt.input), tt.target)
			assert.Equal(t, tt.expected.Type, result.Type)
		})
	}
}

func TestConvertFromBool(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    bool
		target   MetadataType
		expected *MetadataValue
	}{
		// to string
		{"bool to string true", true, MetadataType_METADATA_TYPE_STRING, NewStringValue("true")},
		{"bool to string false", false, MetadataType_METADATA_TYPE_STRING, NewStringValue("false")},
		// to int64
		{"bool to int64 true", true, MetadataType_METADATA_TYPE_INT64, NewIntValue(1)},
		{"bool to int64 false", false, MetadataType_METADATA_TYPE_INT64, NewIntValue(0)},
		// to uint64
		{"bool to uint64 true", true, MetadataType_METADATA_TYPE_UINT64, NewUintValue(1)},
		{"bool to uint64 false", false, MetadataType_METADATA_TYPE_UINT64, NewUintValue(0)},
		// to bool (identity)
		{"bool to bool true", true, MetadataType_METADATA_TYPE_BOOL, NewBoolValue(true)},
		{"bool to bool false", false, MetadataType_METADATA_TYPE_BOOL, NewBoolValue(false)},

		// to sub-64-bit signed (1 and 0 always fit)
		{"bool to int8 true", true, MetadataType_METADATA_TYPE_INT8, NewIntValue(1)},
		{"bool to int8 false", false, MetadataType_METADATA_TYPE_INT8, NewIntValue(0)},
		{"bool to int16 true", true, MetadataType_METADATA_TYPE_INT16, NewIntValue(1)},
		{"bool to int32 true", true, MetadataType_METADATA_TYPE_INT32, NewIntValue(1)},
		// to sub-64-bit unsigned
		{"bool to uint8 true", true, MetadataType_METADATA_TYPE_UINT8, NewUintValue(1)},
		{"bool to uint8 false", false, MetadataType_METADATA_TYPE_UINT8, NewUintValue(0)},
		{"bool to uint16 true", true, MetadataType_METADATA_TYPE_UINT16, NewUintValue(1)},
		{"bool to uint32 true", true, MetadataType_METADATA_TYPE_UINT32, NewUintValue(1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ConvertMetadataValue(NewBoolValue(tt.input), tt.target)
			assert.Equal(t, tt.expected.Type, result.Type)
		})
	}
}

func TestConvertFromNull(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		original string
		target   MetadataType
		expected *MetadataValue
	}{
		// to string (always succeeds)
		{"null to string", "hello", MetadataType_METADATA_TYPE_STRING, NewStringValue("hello")},
		{"null to string numeric", "42", MetadataType_METADATA_TYPE_STRING, NewStringValue("42")},
		// to int64
		{"null to int64 valid", "42", MetadataType_METADATA_TYPE_INT64, NewIntValue(42)},
		{"null to int64 invalid", "x", MetadataType_METADATA_TYPE_INT64, NewNullValue("x")},
		// to uint64
		{"null to uint64 valid", "42", MetadataType_METADATA_TYPE_UINT64, NewUintValue(42)},
		{"null to uint64 invalid", "x", MetadataType_METADATA_TYPE_UINT64, NewNullValue("x")},
		// to bool
		{"null to bool true", "true", MetadataType_METADATA_TYPE_BOOL, NewBoolValue(true)},
		{"null to bool 1", "1", MetadataType_METADATA_TYPE_BOOL, NewBoolValue(true)},
		{"null to bool false", "false", MetadataType_METADATA_TYPE_BOOL, NewBoolValue(false)},
		{"null to bool 0", "0", MetadataType_METADATA_TYPE_BOOL, NewBoolValue(false)},
		{"null to bool invalid", "x", MetadataType_METADATA_TYPE_BOOL, NewNullValue("x")},

		// to sub-64-bit signed
		{"null to int8 valid", "42", MetadataType_METADATA_TYPE_INT8, NewIntValue(42)},
		{"null to int8 overflow", "128", MetadataType_METADATA_TYPE_INT8, NewNullValue("128")},
		{"null to int16 valid", "1000", MetadataType_METADATA_TYPE_INT16, NewIntValue(1000)},
		{"null to int32 valid", "100000", MetadataType_METADATA_TYPE_INT32, NewIntValue(100000)},
		// to sub-64-bit unsigned
		{"null to uint8 valid", "200", MetadataType_METADATA_TYPE_UINT8, NewUintValue(200)},
		{"null to uint8 overflow", "256", MetadataType_METADATA_TYPE_UINT8, NewNullValue("256")},
		{"null to uint16 valid", "60000", MetadataType_METADATA_TYPE_UINT16, NewUintValue(60000)},
		{"null to uint32 valid", "3000000000", MetadataType_METADATA_TYPE_UINT32, NewUintValue(3000000000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ConvertMetadataValue(NewNullValue(tt.original), tt.target)
			assert.Equal(t, tt.expected.Type, result.Type)
		})
	}
}

func TestConvertNilValue(t *testing.T) {
	t.Parallel()
	result := ConvertMetadataValue(nil, MetadataType_METADATA_TYPE_STRING)
	_, ok := result.Type.(*MetadataValue_NullValue)
	assert.True(t, ok)
}

func TestConvertIdentity(t *testing.T) {
	t.Parallel()
	// When type already matches, the same pointer should be returned.
	v := NewStringValue("test")
	result := ConvertMetadataValue(v, MetadataType_METADATA_TYPE_STRING)
	assert.Same(t, v, result)

	vi := NewIntValue(42)
	result = ConvertMetadataValue(vi, MetadataType_METADATA_TYPE_INT64)
	assert.Same(t, vi, result)

	vu := NewUintValue(42)
	result = ConvertMetadataValue(vu, MetadataType_METADATA_TYPE_UINT64)
	assert.Same(t, vu, result)

	vb := NewBoolValue(true)
	result = ConvertMetadataValue(vb, MetadataType_METADATA_TYPE_BOOL)
	assert.Same(t, vb, result)

	// Sub-64-bit types: identity when value is stored in the correct storage
	// AND fits in the target range.
	vi8 := NewIntValue(100) // fits in int8 range
	result = ConvertMetadataValue(vi8, MetadataType_METADATA_TYPE_INT8)
	assert.Same(t, vi8, result)

	vu8 := NewUintValue(200) // fits in uint8 range
	result = ConvertMetadataValue(vu8, MetadataType_METADATA_TYPE_UINT8)
	assert.Same(t, vu8, result)
}
