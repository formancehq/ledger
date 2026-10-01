package protohelpers

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestNewStringValue(t *testing.T) {
	t.Parallel()

	v := commonpb.NewStringValue("hello")
	require.NotNil(t, v)
	sv, ok := v.GetType().(*commonpb.MetadataValue_StringValue)
	require.True(t, ok)
	assert.Equal(t, "hello", sv.StringValue)
}

func TestNewIntValue(t *testing.T) {
	t.Parallel()

	v := commonpb.NewIntValue(-42)
	require.NotNil(t, v)
	iv, ok := v.GetType().(*commonpb.MetadataValue_IntValue)
	require.True(t, ok)
	assert.Equal(t, int64(-42), iv.IntValue)
}

func TestNewUintValue(t *testing.T) {
	t.Parallel()

	v := commonpb.NewUintValue(42)
	require.NotNil(t, v)
	uv, ok := v.GetType().(*commonpb.MetadataValue_UintValue)
	require.True(t, ok)
	assert.Equal(t, uint64(42), uv.UintValue)
}

func TestNewBoolValue(t *testing.T) {
	t.Parallel()

	v := commonpb.NewBoolValue(true)
	require.NotNil(t, v)
	bv, ok := v.GetType().(*commonpb.MetadataValue_BoolValue)
	require.True(t, ok)
	assert.True(t, bv.BoolValue)
}

func TestNewNullValue(t *testing.T) {
	t.Parallel()

	v := commonpb.NewNullValue("original")
	require.NotNil(t, v)
	nv, ok := v.GetType().(*commonpb.MetadataValue_NullValue)
	require.True(t, ok)
	require.NotNil(t, nv.NullValue)
	assert.Equal(t, "original", nv.NullValue.GetOriginal())
}

func TestMetadataValueToString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    *commonpb.MetadataValue
		expected string
	}{
		{"nil", nil, ""},
		{"string", commonpb.NewStringValue("hello"), "hello"},
		{"int positive", commonpb.NewIntValue(42), "42"},
		{"int negative", commonpb.NewIntValue(-42), "-42"},
		{"int zero", commonpb.NewIntValue(0), "0"},
		{"uint", commonpb.NewUintValue(42), "42"},
		{"uint zero", commonpb.NewUintValue(0), "0"},
		{"bool true", commonpb.NewBoolValue(true), "true"},
		{"bool false", commonpb.NewBoolValue(false), "false"},
		{"null", commonpb.NewNullValue("original"), "original"},
		{"null empty", commonpb.NewNullValue(""), ""},
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
		value    *commonpb.MetadataValue
		target   commonpb.MetadataType
		expected bool
	}{
		{"string matches string", commonpb.NewStringValue("x"), commonpb.MetadataType_METADATA_TYPE_STRING, true},
		{"string doesn't match int", commonpb.NewStringValue("x"), commonpb.MetadataType_METADATA_TYPE_INT64, false},
		{"int matches int", commonpb.NewIntValue(1), commonpb.MetadataType_METADATA_TYPE_INT64, true},
		{"int doesn't match string", commonpb.NewIntValue(1), commonpb.MetadataType_METADATA_TYPE_STRING, false},
		{"uint matches uint", commonpb.NewUintValue(1), commonpb.MetadataType_METADATA_TYPE_UINT64, true},
		{"uint doesn't match bool", commonpb.NewUintValue(1), commonpb.MetadataType_METADATA_TYPE_BOOL, false},
		{"bool matches bool", commonpb.NewBoolValue(true), commonpb.MetadataType_METADATA_TYPE_BOOL, true},
		{"bool doesn't match uint", commonpb.NewBoolValue(true), commonpb.MetadataType_METADATA_TYPE_UINT64, false},
		{"nil value", nil, commonpb.MetadataType_METADATA_TYPE_STRING, false},

		// Sub-64-bit signed: value stored as int_value, range-checked.
		{"int8 in range", commonpb.NewIntValue(127), commonpb.MetadataType_METADATA_TYPE_INT8, true},
		{"int8 min", commonpb.NewIntValue(-128), commonpb.MetadataType_METADATA_TYPE_INT8, true},
		{"int8 overflow", commonpb.NewIntValue(128), commonpb.MetadataType_METADATA_TYPE_INT8, false},
		{"int8 underflow", commonpb.NewIntValue(-129), commonpb.MetadataType_METADATA_TYPE_INT8, false},
		{"int8 wrong storage", commonpb.NewUintValue(1), commonpb.MetadataType_METADATA_TYPE_INT8, false},

		{"int16 in range", commonpb.NewIntValue(32767), commonpb.MetadataType_METADATA_TYPE_INT16, true},
		{"int16 min", commonpb.NewIntValue(-32768), commonpb.MetadataType_METADATA_TYPE_INT16, true},
		{"int16 overflow", commonpb.NewIntValue(32768), commonpb.MetadataType_METADATA_TYPE_INT16, false},

		{"int32 in range", commonpb.NewIntValue(math.MaxInt32), commonpb.MetadataType_METADATA_TYPE_INT32, true},
		{"int32 overflow", commonpb.NewIntValue(math.MaxInt32 + 1), commonpb.MetadataType_METADATA_TYPE_INT32, false},

		// Sub-64-bit unsigned: value stored as uint_value, range-checked.
		{"uint8 in range", commonpb.NewUintValue(255), commonpb.MetadataType_METADATA_TYPE_UINT8, true},
		{"uint8 overflow", commonpb.NewUintValue(256), commonpb.MetadataType_METADATA_TYPE_UINT8, false},
		{"uint8 wrong storage", commonpb.NewIntValue(1), commonpb.MetadataType_METADATA_TYPE_UINT8, false},

		{"uint16 in range", commonpb.NewUintValue(65535), commonpb.MetadataType_METADATA_TYPE_UINT16, true},
		{"uint16 overflow", commonpb.NewUintValue(65536), commonpb.MetadataType_METADATA_TYPE_UINT16, false},

		{"uint32 in range", commonpb.NewUintValue(math.MaxUint32), commonpb.MetadataType_METADATA_TYPE_UINT32, true},
		{"uint32 overflow", commonpb.NewUintValue(math.MaxUint32 + 1), commonpb.MetadataType_METADATA_TYPE_UINT32, false},
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
		target   commonpb.MetadataType
		expected *commonpb.MetadataValue
	}{
		// to string (identity)
		{"string to string", "hello", commonpb.MetadataType_METADATA_TYPE_STRING, commonpb.NewStringValue("hello")},
		// to int64
		{"string to int64 positive", "42", commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewIntValue(42)},
		{"string to int64 negative", "-42", commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewIntValue(-42)},
		{"string to int64 zero", "0", commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewIntValue(0)},
		{"string to int64 invalid", "hello", commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewNullValue("hello")},
		{"string to int64 float", "3.14", commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewNullValue("3.14")},
		// to uint64
		{"string to uint64 positive", "42", commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewUintValue(42)},
		{"string to uint64 zero", "0", commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewUintValue(0)},
		{"string to uint64 negative", "-1", commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewNullValue("-1")},
		{"string to uint64 invalid", "hello", commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewNullValue("hello")},
		// to bool
		{"string to bool true", "true", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(true)},
		{"string to bool TRUE", "TRUE", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(true)},
		{"string to bool 1", "1", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(true)},
		{"string to bool false", "false", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(false)},
		{"string to bool FALSE", "FALSE", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(false)},
		{"string to bool 0", "0", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(false)},
		{"string to bool invalid", "maybe", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewNullValue("maybe")},

		// to int8
		{"string to int8 valid", "42", commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewIntValue(42)},
		{"string to int8 max", "127", commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewIntValue(127)},
		{"string to int8 min", "-128", commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewIntValue(-128)},
		{"string to int8 overflow", "128", commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewNullValue("128")},
		{"string to int8 underflow", "-129", commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewNullValue("-129")},
		// to int16
		{"string to int16 valid", "1000", commonpb.MetadataType_METADATA_TYPE_INT16, commonpb.NewIntValue(1000)},
		{"string to int16 overflow", "32768", commonpb.MetadataType_METADATA_TYPE_INT16, commonpb.NewNullValue("32768")},
		// to int32
		{"string to int32 valid", "100000", commonpb.MetadataType_METADATA_TYPE_INT32, commonpb.NewIntValue(100000)},
		{"string to int32 overflow", "2147483648", commonpb.MetadataType_METADATA_TYPE_INT32, commonpb.NewNullValue("2147483648")},
		// to uint8
		{"string to uint8 valid", "200", commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewUintValue(200)},
		{"string to uint8 max", "255", commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewUintValue(255)},
		{"string to uint8 overflow", "256", commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewNullValue("256")},
		{"string to uint8 negative", "-1", commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewNullValue("-1")},
		// to uint16
		{"string to uint16 valid", "60000", commonpb.MetadataType_METADATA_TYPE_UINT16, commonpb.NewUintValue(60000)},
		{"string to uint16 overflow", "65536", commonpb.MetadataType_METADATA_TYPE_UINT16, commonpb.NewNullValue("65536")},
		// to uint32
		{"string to uint32 valid", "3000000000", commonpb.MetadataType_METADATA_TYPE_UINT32, commonpb.NewUintValue(3000000000)},
		{"string to uint32 overflow", "4294967296", commonpb.MetadataType_METADATA_TYPE_UINT32, commonpb.NewNullValue("4294967296")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ConvertMetadataValue(commonpb.NewStringValue(tt.input), tt.target)
			assert.Equal(t, tt.expected.GetType(), result.GetType())
		})
	}
}

func TestConvertFromInt64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    int64
		target   commonpb.MetadataType
		expected *commonpb.MetadataValue
	}{
		// to string
		{"int64 to string positive", 42, commonpb.MetadataType_METADATA_TYPE_STRING, commonpb.NewStringValue("42")},
		{"int64 to string negative", -42, commonpb.MetadataType_METADATA_TYPE_STRING, commonpb.NewStringValue("-42")},
		{"int64 to string zero", 0, commonpb.MetadataType_METADATA_TYPE_STRING, commonpb.NewStringValue("0")},
		// to int64 (identity)
		{"int64 to int64", 42, commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewIntValue(42)},
		// to uint64
		{"int64 to uint64 positive", 42, commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewUintValue(42)},
		{"int64 to uint64 zero", 0, commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewUintValue(0)},
		{"int64 to uint64 negative", -1, commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewNullValue("-1")},
		// to bool
		{"int64 to bool non-zero", 1, commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(true)},
		{"int64 to bool negative", -1, commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(true)},
		{"int64 to bool zero", 0, commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(false)},

		// to int8 (narrowing)
		{"int64 to int8 in range", 100, commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewIntValue(100)},
		{"int64 to int8 overflow", 128, commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewNullValue("128")},
		{"int64 to int8 underflow", -129, commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewNullValue("-129")},
		// to int16 (narrowing)
		{"int64 to int16 in range", 30000, commonpb.MetadataType_METADATA_TYPE_INT16, commonpb.NewIntValue(30000)},
		{"int64 to int16 overflow", 32768, commonpb.MetadataType_METADATA_TYPE_INT16, commonpb.NewNullValue("32768")},
		// to int32 (narrowing)
		{"int64 to int32 in range", 100000, commonpb.MetadataType_METADATA_TYPE_INT32, commonpb.NewIntValue(100000)},
		{"int64 to int32 overflow", math.MaxInt32 + 1, commonpb.MetadataType_METADATA_TYPE_INT32, commonpb.NewNullValue("2147483648")},
		// to uint8
		{"int64 to uint8 in range", 200, commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewUintValue(200)},
		{"int64 to uint8 overflow", 256, commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewNullValue("256")},
		{"int64 to uint8 negative", -1, commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewNullValue("-1")},
		// to uint16
		{"int64 to uint16 in range", 60000, commonpb.MetadataType_METADATA_TYPE_UINT16, commonpb.NewUintValue(60000)},
		{"int64 to uint16 overflow", 65536, commonpb.MetadataType_METADATA_TYPE_UINT16, commonpb.NewNullValue("65536")},
		// to uint32
		{"int64 to uint32 in range", 3000000000, commonpb.MetadataType_METADATA_TYPE_UINT32, commonpb.NewUintValue(3000000000)},
		{"int64 to uint32 overflow", math.MaxUint32 + 1, commonpb.MetadataType_METADATA_TYPE_UINT32, commonpb.NewNullValue("4294967296")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ConvertMetadataValue(commonpb.NewIntValue(tt.input), tt.target)
			assert.Equal(t, tt.expected.GetType(), result.GetType())
		})
	}
}

func TestConvertFromUint64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    uint64
		target   commonpb.MetadataType
		expected *commonpb.MetadataValue
	}{
		// to string
		{"uint64 to string", 42, commonpb.MetadataType_METADATA_TYPE_STRING, commonpb.NewStringValue("42")},
		{"uint64 to string zero", 0, commonpb.MetadataType_METADATA_TYPE_STRING, commonpb.NewStringValue("0")},
		// to int64
		{"uint64 to int64 small", 42, commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewIntValue(42)},
		{"uint64 to int64 max", uint64(math.MaxInt64), commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewIntValue(math.MaxInt64)},
		{"uint64 to int64 overflow", uint64(math.MaxInt64) + 1, commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewNullValue("9223372036854775808")},
		// to uint64 (identity)
		{"uint64 to uint64", 42, commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewUintValue(42)},
		// to bool
		{"uint64 to bool non-zero", 1, commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(true)},
		{"uint64 to bool zero", 0, commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(false)},

		// to int8 (narrowing from uint64)
		{"uint64 to int8 in range", 100, commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewIntValue(100)},
		{"uint64 to int8 overflow", 128, commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewNullValue("128")},
		// to int16
		{"uint64 to int16 in range", 30000, commonpb.MetadataType_METADATA_TYPE_INT16, commonpb.NewIntValue(30000)},
		{"uint64 to int16 overflow", 32768, commonpb.MetadataType_METADATA_TYPE_INT16, commonpb.NewNullValue("32768")},
		// to int32
		{"uint64 to int32 in range", 100000, commonpb.MetadataType_METADATA_TYPE_INT32, commonpb.NewIntValue(100000)},
		{"uint64 to int32 overflow", math.MaxInt32 + 1, commonpb.MetadataType_METADATA_TYPE_INT32, commonpb.NewNullValue("2147483648")},
		// to uint8
		{"uint64 to uint8 in range", 200, commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewUintValue(200)},
		{"uint64 to uint8 overflow", 256, commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewNullValue("256")},
		// to uint16
		{"uint64 to uint16 in range", 60000, commonpb.MetadataType_METADATA_TYPE_UINT16, commonpb.NewUintValue(60000)},
		{"uint64 to uint16 overflow", 65536, commonpb.MetadataType_METADATA_TYPE_UINT16, commonpb.NewNullValue("65536")},
		// to uint32
		{"uint64 to uint32 in range", 3000000000, commonpb.MetadataType_METADATA_TYPE_UINT32, commonpb.NewUintValue(3000000000)},
		{"uint64 to uint32 overflow", math.MaxUint32 + 1, commonpb.MetadataType_METADATA_TYPE_UINT32, commonpb.NewNullValue("4294967296")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ConvertMetadataValue(commonpb.NewUintValue(tt.input), tt.target)
			assert.Equal(t, tt.expected.GetType(), result.GetType())
		})
	}
}

func TestConvertFromBool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    bool
		target   commonpb.MetadataType
		expected *commonpb.MetadataValue
	}{
		// to string
		{"bool to string true", true, commonpb.MetadataType_METADATA_TYPE_STRING, commonpb.NewStringValue("true")},
		{"bool to string false", false, commonpb.MetadataType_METADATA_TYPE_STRING, commonpb.NewStringValue("false")},
		// to int64
		{"bool to int64 true", true, commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewIntValue(1)},
		{"bool to int64 false", false, commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewIntValue(0)},
		// to uint64
		{"bool to uint64 true", true, commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewUintValue(1)},
		{"bool to uint64 false", false, commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewUintValue(0)},
		// to bool (identity)
		{"bool to bool true", true, commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(true)},
		{"bool to bool false", false, commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(false)},

		// to sub-64-bit signed (1 and 0 always fit)
		{"bool to int8 true", true, commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewIntValue(1)},
		{"bool to int8 false", false, commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewIntValue(0)},
		{"bool to int16 true", true, commonpb.MetadataType_METADATA_TYPE_INT16, commonpb.NewIntValue(1)},
		{"bool to int32 true", true, commonpb.MetadataType_METADATA_TYPE_INT32, commonpb.NewIntValue(1)},
		// to sub-64-bit unsigned
		{"bool to uint8 true", true, commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewUintValue(1)},
		{"bool to uint8 false", false, commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewUintValue(0)},
		{"bool to uint16 true", true, commonpb.MetadataType_METADATA_TYPE_UINT16, commonpb.NewUintValue(1)},
		{"bool to uint32 true", true, commonpb.MetadataType_METADATA_TYPE_UINT32, commonpb.NewUintValue(1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ConvertMetadataValue(commonpb.NewBoolValue(tt.input), tt.target)
			assert.Equal(t, tt.expected.GetType(), result.GetType())
		})
	}
}

func TestConvertFromNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		original string
		target   commonpb.MetadataType
		expected *commonpb.MetadataValue
	}{
		// to string (always succeeds)
		{"null to string", "hello", commonpb.MetadataType_METADATA_TYPE_STRING, commonpb.NewStringValue("hello")},
		{"null to string numeric", "42", commonpb.MetadataType_METADATA_TYPE_STRING, commonpb.NewStringValue("42")},
		// to int64
		{"null to int64 valid", "42", commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewIntValue(42)},
		{"null to int64 invalid", "x", commonpb.MetadataType_METADATA_TYPE_INT64, commonpb.NewNullValue("x")},
		// to uint64
		{"null to uint64 valid", "42", commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewUintValue(42)},
		{"null to uint64 invalid", "x", commonpb.MetadataType_METADATA_TYPE_UINT64, commonpb.NewNullValue("x")},
		// to bool
		{"null to bool true", "true", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(true)},
		{"null to bool 1", "1", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(true)},
		{"null to bool false", "false", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(false)},
		{"null to bool 0", "0", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewBoolValue(false)},
		{"null to bool invalid", "x", commonpb.MetadataType_METADATA_TYPE_BOOL, commonpb.NewNullValue("x")},

		// to sub-64-bit signed
		{"null to int8 valid", "42", commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewIntValue(42)},
		{"null to int8 overflow", "128", commonpb.MetadataType_METADATA_TYPE_INT8, commonpb.NewNullValue("128")},
		{"null to int16 valid", "1000", commonpb.MetadataType_METADATA_TYPE_INT16, commonpb.NewIntValue(1000)},
		{"null to int32 valid", "100000", commonpb.MetadataType_METADATA_TYPE_INT32, commonpb.NewIntValue(100000)},
		// to sub-64-bit unsigned
		{"null to uint8 valid", "200", commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewUintValue(200)},
		{"null to uint8 overflow", "256", commonpb.MetadataType_METADATA_TYPE_UINT8, commonpb.NewNullValue("256")},
		{"null to uint16 valid", "60000", commonpb.MetadataType_METADATA_TYPE_UINT16, commonpb.NewUintValue(60000)},
		{"null to uint32 valid", "3000000000", commonpb.MetadataType_METADATA_TYPE_UINT32, commonpb.NewUintValue(3000000000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ConvertMetadataValue(commonpb.NewNullValue(tt.original), tt.target)
			assert.Equal(t, tt.expected.GetType(), result.GetType())
		})
	}
}

func TestConvertNilValue(t *testing.T) {
	t.Parallel()

	result := ConvertMetadataValue(nil, commonpb.MetadataType_METADATA_TYPE_STRING)
	_, ok := result.GetType().(*commonpb.MetadataValue_NullValue)
	assert.True(t, ok)
}

func TestConvertIdentity(t *testing.T) {
	t.Parallel()
	// When type already matches, the same pointer should be returned.
	v := commonpb.NewStringValue("test")
	result := ConvertMetadataValue(v, commonpb.MetadataType_METADATA_TYPE_STRING)
	assert.Same(t, v, result)

	vi := commonpb.NewIntValue(42)
	result = ConvertMetadataValue(vi, commonpb.MetadataType_METADATA_TYPE_INT64)
	assert.Same(t, vi, result)

	vu := commonpb.NewUintValue(42)
	result = ConvertMetadataValue(vu, commonpb.MetadataType_METADATA_TYPE_UINT64)
	assert.Same(t, vu, result)

	vb := commonpb.NewBoolValue(true)
	result = ConvertMetadataValue(vb, commonpb.MetadataType_METADATA_TYPE_BOOL)
	assert.Same(t, vb, result)

	// Sub-64-bit types: identity when value is stored in the correct storage
	// AND fits in the target range.
	vi8 := commonpb.NewIntValue(100) // fits in int8 range
	result = ConvertMetadataValue(vi8, commonpb.MetadataType_METADATA_TYPE_INT8)
	assert.Same(t, vi8, result)

	vu8 := commonpb.NewUintValue(200) // fits in uint8 range
	result = ConvertMetadataValue(vu8, commonpb.MetadataType_METADATA_TYPE_UINT8)
	assert.Same(t, vu8, result)
}

func TestSchemaFieldForTarget(t *testing.T) {
	t.Parallel()

	schema := &commonpb.MetadataSchema{
		AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"age": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
		},
		TransactionFields: map[string]*commonpb.MetadataFieldSchema{
			"category": {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		},
		LedgerFields: map[string]*commonpb.MetadataFieldSchema{
			"env": {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		},
	}

	t.Run("nil schema", func(t *testing.T) {
		t.Parallel()
		fields, fs := SchemaFieldForTarget(nil, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age")
		assert.Nil(t, fields)
		assert.Nil(t, fs)
	})

	t.Run("account hit", func(t *testing.T) {
		t.Parallel()
		fields, fs := SchemaFieldForTarget(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age")
		require.NotNil(t, fields)
		require.NotNil(t, fs)
		assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, fs.GetType())
	})

	t.Run("transaction hit", func(t *testing.T) {
		t.Parallel()
		fields, fs := SchemaFieldForTarget(schema, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category")
		require.NotNil(t, fields)
		require.NotNil(t, fs)
		assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, fs.GetType())
	})

	t.Run("ledger hit", func(t *testing.T) {
		t.Parallel()
		fields, fs := SchemaFieldForTarget(schema, commonpb.TargetType_TARGET_TYPE_LEDGER, "env")
		require.NotNil(t, fields)
		require.NotNil(t, fs)
		assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, fs.GetType())
	})

	t.Run("missing key returns field map and nil schema", func(t *testing.T) {
		t.Parallel()
		fields, fs := SchemaFieldForTarget(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "missing")
		require.NotNil(t, fields)
		assert.Nil(t, fs)
	})

	t.Run("empty field map returns nil, nil", func(t *testing.T) {
		t.Parallel()
		fields, fs := SchemaFieldForTarget(&commonpb.MetadataSchema{}, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age")
		assert.Nil(t, fields)
		assert.Nil(t, fs)
	})
}

func TestCoerceToDeclaredType(t *testing.T) {
	t.Parallel()

	schema := &commonpb.MetadataSchema{
		AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"age": {Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
		},
	}

	t.Run("nil value passes through", func(t *testing.T) {
		t.Parallel()
		got := CoerceToDeclaredType(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age", nil)
		assert.Nil(t, got)
	})

	t.Run("no schema passes through", func(t *testing.T) {
		t.Parallel()
		v := commonpb.NewStringValue("hello")
		got := CoerceToDeclaredType(nil, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age", v)
		assert.Same(t, v, got)
	})

	t.Run("no declared field passes through", func(t *testing.T) {
		t.Parallel()
		v := commonpb.NewStringValue("hello")
		got := CoerceToDeclaredType(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "unknown", v)
		assert.Same(t, v, got)
	})

	t.Run("already matching type passes through (identity)", func(t *testing.T) {
		t.Parallel()
		v := commonpb.NewUintValue(42)
		got := CoerceToDeclaredType(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age", v)
		assert.Same(t, v, got)
	})

	t.Run("string coerced to uint64 keeps numeric value", func(t *testing.T) {
		t.Parallel()
		v := commonpb.NewStringValue("030")
		got := CoerceToDeclaredType(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age", v)
		require.NotNil(t, got)
		assert.Equal(t, uint64(30), got.GetUintValue())
	})

	t.Run("uncoercible string returns Null sentinel preserving original", func(t *testing.T) {
		t.Parallel()
		v := commonpb.NewStringValue("abc")
		got := CoerceToDeclaredType(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age", v)
		require.NotNil(t, got)
		nv, ok := got.GetType().(*commonpb.MetadataValue_NullValue)
		require.True(t, ok, "expected Null sentinel, got %T", got.GetType())
		assert.Equal(t, "abc", nv.NullValue.GetOriginal())
	})

	t.Run("pure function: same input yields equal output across calls", func(t *testing.T) {
		t.Parallel()
		v := commonpb.NewStringValue("42")
		a := CoerceToDeclaredType(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age", v)
		b := CoerceToDeclaredType(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age", v)
		assert.Equal(t, a.GetUintValue(), b.GetUintValue())
	})
}

func TestConvertFromString_Datetime(t *testing.T) {
	t.Parallel()

	// 2024-01-15T10:00:00Z = 1705312800000000 micros.
	const baseMicros = int64(1705312800000000)

	// wantNull == "" means expect a datetime_value equal to wantMicros;
	// otherwise expect a null_value preserving wantNull as the original string.
	tests := []struct {
		name       string
		input      string
		wantMicros int64
		wantNull   string
	}{
		{name: "rfc3339 utc", input: "2024-01-15T10:00:00Z", wantMicros: baseMicros},
		{name: "rfc3339nano fractional millis", input: "2024-01-15T10:00:00.123Z", wantMicros: baseMicros + 123_000},
		{name: "rfc3339nano fractional micros", input: "2024-01-15T10:00:00.123456Z", wantMicros: baseMicros + 123_456},
		{name: "sub-micro truncates", input: "2024-01-15T10:00:00.123456789Z", wantMicros: baseMicros + 123_456},
		{name: "non-utc offset normalizes", input: "2024-01-15T11:00:00+01:00", wantMicros: baseMicros},
		{name: "invalid string", input: "tot", wantNull: "tot"},
		{name: "pre-epoch is valid (negative micros)", input: "1969-12-31T23:59:59Z", wantMicros: -1_000_000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ConvertMetadataValue(commonpb.NewStringValue(tc.input), commonpb.MetadataType_METADATA_TYPE_DATETIME)
			require.NotNil(t, got)
			if tc.wantNull == "" {
				dv, ok := got.GetType().(*commonpb.MetadataValue_DatetimeValue)
				require.True(t, ok, "expected datetime_value, got %T", got.GetType())
				assert.Equal(t, tc.wantMicros, dv.DatetimeValue)

				return
			}
			nv, ok := got.GetType().(*commonpb.MetadataValue_NullValue)
			require.True(t, ok, "expected null_value, got %T", got.GetType())
			assert.Equal(t, tc.wantNull, nv.NullValue.GetOriginal())
		})
	}
}

func TestConvertFromInt64_Datetime(t *testing.T) {
	t.Parallel()

	// int64 is treated as raw micros, including negative (pre-1970) values.
	pos := ConvertMetadataValue(commonpb.NewIntValue(1705312800000000), commonpb.MetadataType_METADATA_TYPE_DATETIME)
	dv, ok := pos.GetType().(*commonpb.MetadataValue_DatetimeValue)
	require.True(t, ok)
	assert.Equal(t, int64(1705312800000000), dv.DatetimeValue)

	neg := ConvertMetadataValue(commonpb.NewIntValue(-1), commonpb.MetadataType_METADATA_TYPE_DATETIME)
	dvNeg, ok := neg.GetType().(*commonpb.MetadataValue_DatetimeValue)
	require.True(t, ok, "negative int -> datetime is now valid")
	assert.Equal(t, int64(-1), dvNeg.DatetimeValue)
}

func TestConvertFromUint64_Datetime(t *testing.T) {
	t.Parallel()

	ok64 := ConvertMetadataValue(commonpb.NewUintValue(1705312800000000), commonpb.MetadataType_METADATA_TYPE_DATETIME)
	dv, ok := ok64.GetType().(*commonpb.MetadataValue_DatetimeValue)
	require.True(t, ok)
	assert.Equal(t, int64(1705312800000000), dv.DatetimeValue)

	// uint64 > MaxInt64 cannot be a signed micros value → commonpb.NullValue.
	overflow := ConvertMetadataValue(commonpb.NewUintValue(uint64(math.MaxInt64)+1), commonpb.MetadataType_METADATA_TYPE_DATETIME)
	_, ok = overflow.GetType().(*commonpb.MetadataValue_NullValue)
	require.True(t, ok, "uint64 > MaxInt64 -> datetime must be commonpb.NullValue")
}

func TestConvertFromBool_Datetime(t *testing.T) {
	t.Parallel()

	got := ConvertMetadataValue(commonpb.NewBoolValue(true), commonpb.MetadataType_METADATA_TYPE_DATETIME)
	_, ok := got.GetType().(*commonpb.MetadataValue_NullValue)
	require.True(t, ok, "bool -> datetime must be commonpb.NullValue")
}

func TestTypeMatches_Datetime(t *testing.T) {
	t.Parallel()

	assert.True(t, TypeMatches(commonpb.NewDatetimeValue(123), commonpb.MetadataType_METADATA_TYPE_DATETIME),
		"a stored datetime_value already matches datetime (no re-parse)")
	assert.False(t, TypeMatches(commonpb.NewUintValue(123), commonpb.MetadataType_METADATA_TYPE_DATETIME),
		"a uint_value is no longer a datetime")
	assert.False(t, TypeMatches(commonpb.NewStringValue("2024-01-15T10:00:00Z"), commonpb.MetadataType_METADATA_TYPE_DATETIME))
}

func TestMetadataValueToString_Datetime(t *testing.T) {
	t.Parallel()

	// 2024-01-15T10:00:00.123456Z = 1705312800123456 micros.
	v := commonpb.NewDatetimeValue(1705312800123456)
	assert.Equal(t, "2024-01-15T10:00:00.123456Z", MetadataValueToString(v))

	// Pre-epoch round-trips as an RFC3339 string.
	assert.Equal(t, "1969-12-31T23:59:59Z", MetadataValueToString(commonpb.NewDatetimeValue(-1_000_000)))
}

func TestConvertFromDatetime(t *testing.T) {
	t.Parallel()

	// 2024-01-15T10:00:00Z = 1705312800000000 micros.
	const micros = int64(1705312800000000)

	// datetime -> string formats RFC3339, not the decimal micros.
	str := ConvertMetadataValue(commonpb.NewDatetimeValue(micros), commonpb.MetadataType_METADATA_TYPE_STRING)
	sv, ok := str.GetType().(*commonpb.MetadataValue_StringValue)
	require.True(t, ok, "datetime -> string must be a string_value, got %T", str.GetType())
	assert.Equal(t, "2024-01-15T10:00:00Z", sv.StringValue)

	// datetime -> int64 keeps the raw micros.
	i64 := ConvertMetadataValue(commonpb.NewDatetimeValue(micros), commonpb.MetadataType_METADATA_TYPE_INT64)
	iv, ok := i64.GetType().(*commonpb.MetadataValue_IntValue)
	require.True(t, ok, "datetime -> int64 must be an int_value, got %T", i64.GetType())
	assert.Equal(t, micros, iv.IntValue)

	// datetime -> uint64 keeps the raw micros when non-negative.
	u64 := ConvertMetadataValue(commonpb.NewDatetimeValue(micros), commonpb.MetadataType_METADATA_TYPE_UINT64)
	uv, ok := u64.GetType().(*commonpb.MetadataValue_UintValue)
	require.True(t, ok, "datetime -> uint64 must be a uint_value, got %T", u64.GetType())
	assert.Equal(t, uint64(micros), uv.UintValue)

	// pre-epoch datetime -> uint64 cannot fit and falls back to a commonpb.NullValue
	// preserving the RFC3339 representation (not an empty original).
	negToUint := ConvertMetadataValue(commonpb.NewDatetimeValue(-1_000_000), commonpb.MetadataType_METADATA_TYPE_UINT64)
	nv, ok := negToUint.GetType().(*commonpb.MetadataValue_NullValue)
	require.True(t, ok, "negative datetime -> uint64 must be commonpb.NullValue, got %T", negToUint.GetType())
	assert.Equal(t, "1969-12-31T23:59:59Z", nv.NullValue.GetOriginal())

	// datetime -> bool has no meaningful form and yields a commonpb.NullValue.
	toBool := ConvertMetadataValue(commonpb.NewDatetimeValue(micros), commonpb.MetadataType_METADATA_TYPE_BOOL)
	bnv, ok := toBool.GetType().(*commonpb.MetadataValue_NullValue)
	require.True(t, ok, "datetime -> bool must be commonpb.NullValue, got %T", toBool.GetType())
	assert.Equal(t, "2024-01-15T10:00:00Z", bnv.NullValue.GetOriginal())

	// datetime -> int8 overflows and falls back to a commonpb.NullValue.
	toInt8 := ConvertMetadataValue(commonpb.NewDatetimeValue(micros), commonpb.MetadataType_METADATA_TYPE_INT8)
	_, ok = toInt8.GetType().(*commonpb.MetadataValue_NullValue)
	require.True(t, ok, "datetime overflowing int8 must be commonpb.NullValue, got %T", toInt8.GetType())
}
