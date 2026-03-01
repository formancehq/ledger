package commonpb

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Constructors for MetadataValue variants.

func NewStringValue(s string) *MetadataValue {
	return &MetadataValue{Type: &MetadataValue_StringValue{StringValue: s}}
}

func NewIntValue(n int64) *MetadataValue {
	return &MetadataValue{Type: &MetadataValue_IntValue{IntValue: n}}
}

func NewUintValue(n uint64) *MetadataValue {
	return &MetadataValue{Type: &MetadataValue_UintValue{UintValue: n}}
}

func NewBoolValue(b bool) *MetadataValue {
	return &MetadataValue{Type: &MetadataValue_BoolValue{BoolValue: b}}
}

func NewNullValue(original string) *MetadataValue {
	return &MetadataValue{Type: &MetadataValue_NullValue{NullValue: &NullValue{Original: original}}}
}

// signedRange returns (min, max) for a signed integer MetadataType.
// Returns (0, 0, false) for non-signed types.
func signedRange(t MetadataType) (int64, int64, bool) {
	switch t {
	case MetadataType_METADATA_TYPE_INT8:
		return math.MinInt8, math.MaxInt8, true
	case MetadataType_METADATA_TYPE_INT16:
		return math.MinInt16, math.MaxInt16, true
	case MetadataType_METADATA_TYPE_INT32:
		return math.MinInt32, math.MaxInt32, true
	case MetadataType_METADATA_TYPE_INT64:
		return math.MinInt64, math.MaxInt64, true
	default:
		return 0, 0, false
	}
}

// unsignedRange returns max for an unsigned integer MetadataType.
// Returns (0, false) for non-unsigned types.
func unsignedRange(t MetadataType) (uint64, bool) {
	switch t {
	case MetadataType_METADATA_TYPE_UINT8:
		return math.MaxUint8, true
	case MetadataType_METADATA_TYPE_UINT16:
		return math.MaxUint16, true
	case MetadataType_METADATA_TYPE_UINT32:
		return math.MaxUint32, true
	case MetadataType_METADATA_TYPE_UINT64:
		return math.MaxUint64, true
	default:
		return 0, false
	}
}

// IsSignedType returns true for INT8, INT16, INT32, INT64.
func IsSignedType(t MetadataType) bool {
	_, _, ok := signedRange(t)
	return ok
}

// IsUnsignedType returns true for UINT8, UINT16, UINT32, UINT64.
func IsUnsignedType(t MetadataType) bool {
	_, ok := unsignedRange(t)
	return ok
}

// MetadataValueToString converts any MetadataValue to its string representation.
// This always succeeds — every type has a string form.
func MetadataValueToString(v *MetadataValue) string {
	if v == nil {
		return ""
	}
	switch t := v.Type.(type) {
	case *MetadataValue_StringValue:
		return t.StringValue
	case *MetadataValue_IntValue:
		return strconv.FormatInt(t.IntValue, 10)
	case *MetadataValue_UintValue:
		return strconv.FormatUint(t.UintValue, 10)
	case *MetadataValue_BoolValue:
		return strconv.FormatBool(t.BoolValue)
	case *MetadataValue_NullValue:
		if t.NullValue != nil {
			return t.NullValue.Original
		}
		return ""
	default:
		return ""
	}
}

// TypeMatches returns true if the value already has the target type.
// For sub-64-bit integer types (INT8, INT16, INT32, UINT8, UINT16, UINT32),
// the value must be stored in int_value/uint_value AND fit in the target range.
func TypeMatches(v *MetadataValue, target MetadataType) bool {
	if v == nil {
		return false
	}
	switch target {
	case MetadataType_METADATA_TYPE_STRING:
		_, ok := v.Type.(*MetadataValue_StringValue)
		return ok
	case MetadataType_METADATA_TYPE_BOOL:
		_, ok := v.Type.(*MetadataValue_BoolValue)
		return ok
	}

	// Signed integer types: stored as int_value, range-checked.
	if lo, hi, ok := signedRange(target); ok {
		iv, isInt := v.Type.(*MetadataValue_IntValue)
		return isInt && iv.IntValue >= lo && iv.IntValue <= hi
	}

	// Unsigned integer types: stored as uint_value, range-checked.
	if hi, ok := unsignedRange(target); ok {
		uv, isUint := v.Type.(*MetadataValue_UintValue)
		return isUint && uv.UintValue <= hi
	}

	return false
}

// ConvertMetadataValue converts a MetadataValue to the target type using the
// conversion matrix defined in the RFC. If the conversion is not possible,
// returns a NullValue preserving the original string representation.
func ConvertMetadataValue(v *MetadataValue, target MetadataType) *MetadataValue {
	if v == nil {
		return NewNullValue("")
	}
	if TypeMatches(v, target) {
		return v
	}
	switch t := v.Type.(type) {
	case *MetadataValue_StringValue:
		return convertFromString(t.StringValue, target)
	case *MetadataValue_IntValue:
		return convertFromInt64(t.IntValue, target)
	case *MetadataValue_UintValue:
		return convertFromUint64(t.UintValue, target)
	case *MetadataValue_BoolValue:
		return convertFromBool(t.BoolValue, target)
	case *MetadataValue_NullValue:
		return convertFromNull(t.NullValue, target)
	default:
		return NewNullValue("")
	}
}

func convertFromString(s string, target MetadataType) *MetadataValue {
	switch {
	case target == MetadataType_METADATA_TYPE_STRING:
		return NewStringValue(s)

	case target == MetadataType_METADATA_TYPE_BOOL:
		lower := strings.ToLower(s)
		switch lower {
		case "true", "1":
			return NewBoolValue(true)
		case "false", "0":
			return NewBoolValue(false)
		default:
			return NewNullValue(s)
		}

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < lo || n > hi {
			return NewNullValue(s)
		}
		return NewIntValue(n)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil || n > hi {
			return NewNullValue(s)
		}
		return NewUintValue(n)

	default:
		return NewNullValue(s)
	}
}

func convertFromInt64(n int64, target MetadataType) *MetadataValue {
	s := strconv.FormatInt(n, 10)
	switch {
	case target == MetadataType_METADATA_TYPE_STRING:
		return NewStringValue(s)

	case target == MetadataType_METADATA_TYPE_BOOL:
		return NewBoolValue(n != 0)

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)
		if n < lo || n > hi {
			return NewNullValue(s)
		}
		return NewIntValue(n)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)
		if n < 0 || uint64(n) > hi {
			return NewNullValue(s)
		}
		return NewUintValue(uint64(n))

	default:
		return NewNullValue(s)
	}
}

func convertFromUint64(n uint64, target MetadataType) *MetadataValue {
	s := strconv.FormatUint(n, 10)
	switch {
	case target == MetadataType_METADATA_TYPE_STRING:
		return NewStringValue(s)

	case target == MetadataType_METADATA_TYPE_BOOL:
		return NewBoolValue(n != 0)

	case IsSignedType(target):
		_, hi, _ := signedRange(target)
		if n > uint64(hi) {
			return NewNullValue(s)
		}
		return NewIntValue(int64(n))

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)
		if n > hi {
			return NewNullValue(s)
		}
		return NewUintValue(n)

	default:
		return NewNullValue(s)
	}
}

func convertFromBool(b bool, target MetadataType) *MetadataValue {
	switch {
	case target == MetadataType_METADATA_TYPE_STRING:
		return NewStringValue(fmt.Sprintf("%t", b))

	case target == MetadataType_METADATA_TYPE_BOOL:
		return NewBoolValue(b)

	case IsSignedType(target):
		if b {
			return NewIntValue(1)
		}
		return NewIntValue(0)

	case IsUnsignedType(target):
		if b {
			return NewUintValue(1)
		}
		return NewUintValue(0)

	default:
		return NewNullValue(fmt.Sprintf("%t", b))
	}
}

func convertFromNull(nv *NullValue, target MetadataType) *MetadataValue {
	if nv == nil {
		return NewNullValue("")
	}
	original := nv.Original

	switch {
	case target == MetadataType_METADATA_TYPE_STRING:
		return NewStringValue(original)

	case target == MetadataType_METADATA_TYPE_BOOL:
		lower := strings.ToLower(original)
		switch lower {
		case "true", "1":
			return NewBoolValue(true)
		case "false", "0":
			return NewBoolValue(false)
		default:
			return NewNullValue(original)
		}

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)
		n, err := strconv.ParseInt(original, 10, 64)
		if err != nil || n < lo || n > hi {
			return NewNullValue(original)
		}
		return NewIntValue(n)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)
		n, err := strconv.ParseUint(original, 10, 64)
		if err != nil || n > hi {
			return NewNullValue(original)
		}
		return NewUintValue(n)

	default:
		return NewNullValue(original)
	}
}
