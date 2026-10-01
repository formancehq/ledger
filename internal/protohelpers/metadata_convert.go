package protohelpers

import (
	"math"
	"strconv"
	"strings"
	"time"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// signedRange returns (min, max) for a signed integer commonpb.MetadataType.
// Returns (0, 0, false) for non-signed types.
func signedRange(t commonpb.MetadataType) (int64, int64, bool) {
	switch t {
	case commonpb.MetadataType_METADATA_TYPE_INT8:
		return math.MinInt8, math.MaxInt8, true
	case commonpb.MetadataType_METADATA_TYPE_INT16:
		return math.MinInt16, math.MaxInt16, true
	case commonpb.MetadataType_METADATA_TYPE_INT32:
		return math.MinInt32, math.MaxInt32, true
	case commonpb.MetadataType_METADATA_TYPE_INT64:
		return math.MinInt64, math.MaxInt64, true
	default:
		return 0, 0, false
	}
}

// unsignedRange returns max for an unsigned integer commonpb.MetadataType.
// Returns (0, false) for non-unsigned types.
func unsignedRange(t commonpb.MetadataType) (uint64, bool) {
	switch t {
	case commonpb.MetadataType_METADATA_TYPE_UINT8:
		return math.MaxUint8, true
	case commonpb.MetadataType_METADATA_TYPE_UINT16:
		return math.MaxUint16, true
	case commonpb.MetadataType_METADATA_TYPE_UINT32:
		return math.MaxUint32, true
	case commonpb.MetadataType_METADATA_TYPE_UINT64:
		return math.MaxUint64, true
	default:
		return 0, false
	}
}

// IsDatetimeType reports whether t is the datetime metadata type. Datetime
// values are stored in datetime_value (signed int64 microseconds since the
// Unix epoch), reusing the order-preserving int64 index encoding so range
// queries route through the signed integer path.
func IsDatetimeType(t commonpb.MetadataType) bool {
	return t == commonpb.MetadataType_METADATA_TYPE_DATETIME
}

// IsSignedType returns true for INT8, INT16, INT32, INT64.
func IsSignedType(t commonpb.MetadataType) bool {
	_, _, ok := signedRange(t)

	return ok
}

// IsUnsignedType returns true for UINT8, UINT16, UINT32, UINT64.
func IsUnsignedType(t commonpb.MetadataType) bool {
	_, ok := unsignedRange(t)

	return ok
}

// MetadataValueToString converts any commonpb.MetadataValue to its string representation.
// This always succeeds — every type has a string form.
func MetadataValueToString(v *commonpb.MetadataValue) string {
	if v == nil {
		return ""
	}

	switch t := v.GetType().(type) {
	case *commonpb.MetadataValue_StringValue:
		return t.StringValue
	case *commonpb.MetadataValue_IntValue:
		return strconv.FormatInt(t.IntValue, 10)
	case *commonpb.MetadataValue_UintValue:
		return strconv.FormatUint(t.UintValue, 10)
	case *commonpb.MetadataValue_DatetimeValue:
		return time.UnixMicro(t.DatetimeValue).UTC().Format(time.RFC3339Nano)
	case *commonpb.MetadataValue_BoolValue:
		return strconv.FormatBool(t.BoolValue)
	case *commonpb.MetadataValue_NullValue:
		if t.NullValue != nil {
			return t.NullValue.GetOriginal()
		}

		return ""
	default:
		return ""
	}
}

// TypeMatches returns true if the value already has the target type.
// For sub-64-bit integer types (INT8, INT16, INT32, UINT8, UINT16, UINT32),
// the value must be stored in int_value/uint_value AND fit in the target range.
func TypeMatches(v *commonpb.MetadataValue, target commonpb.MetadataType) bool {
	if v == nil {
		return false
	}

	switch target {
	case commonpb.MetadataType_METADATA_TYPE_STRING:
		_, ok := v.GetType().(*commonpb.MetadataValue_StringValue)

		return ok
	case commonpb.MetadataType_METADATA_TYPE_BOOL:
		_, ok := v.GetType().(*commonpb.MetadataValue_BoolValue)

		return ok
	}

	// Signed integer types: stored as int_value, range-checked.
	if lo, hi, ok := signedRange(target); ok {
		iv, isInt := v.GetType().(*commonpb.MetadataValue_IntValue)

		return isInt && iv.IntValue >= lo && iv.IntValue <= hi
	}

	// Unsigned integer types: stored as uint_value, range-checked.
	if hi, ok := unsignedRange(target); ok {
		uv, isUint := v.GetType().(*commonpb.MetadataValue_UintValue)

		return isUint && uv.UintValue <= hi
	}

	// Datetime is stored in datetime_value (signed int64 micros since epoch);
	// any datetime_value already matches and must not be re-converted.
	if IsDatetimeType(target) {
		_, isDatetime := v.GetType().(*commonpb.MetadataValue_DatetimeValue)

		return isDatetime
	}

	return false
}

// SchemaFieldForTarget returns the field map and field schema for the given
// target type and key. Returns nil field if the schema, field map, or key does
// not exist.
func SchemaFieldForTarget(schema *commonpb.MetadataSchema, targetType commonpb.TargetType, key string) (map[string]*commonpb.MetadataFieldSchema, *commonpb.MetadataFieldSchema) {
	if schema == nil {
		return nil, nil
	}

	var fields map[string]*commonpb.MetadataFieldSchema

	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		fields = schema.GetAccountFields()
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		fields = schema.GetTransactionFields()
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		fields = schema.GetLedgerFields()
	}

	if fields == nil {
		return nil, nil
	}

	fs, ok := fields[key]
	if !ok {
		return fields, nil
	}

	return fields, fs
}

// CoerceToDeclaredType returns v coerced to the metadata field's declared type
// for (targetType, key). The indexer uses it to encode forward-index entries
// under the current declared type; reads return stored bytes verbatim, so
// API responses are NOT routed through this helper. v is returned unchanged
// when it is nil or the key has no declared type. Coercion is a pure
// function of (stored value, declared type), so it is deterministic across
// replicas and across time.
func CoerceToDeclaredType(schema *commonpb.MetadataSchema, targetType commonpb.TargetType, key string, v *commonpb.MetadataValue) *commonpb.MetadataValue {
	if v == nil {
		return v
	}

	_, fs := SchemaFieldForTarget(schema, targetType, key)
	if fs == nil || TypeMatches(v, fs.GetType()) {
		return v
	}

	return ConvertMetadataValue(v, fs.GetType())
}

// ConvertMetadataValue converts a commonpb.MetadataValue to the target type using the
// conversion matrix defined in the RFC. If the conversion is not possible,
// returns a commonpb.NullValue preserving the original string representation.
func ConvertMetadataValue(v *commonpb.MetadataValue, target commonpb.MetadataType) *commonpb.MetadataValue {
	if v == nil {
		return commonpb.NewNullValue("")
	}

	if TypeMatches(v, target) {
		return v
	}

	switch t := v.GetType().(type) {
	case *commonpb.MetadataValue_StringValue:
		return convertFromString(t.StringValue, target)
	case *commonpb.MetadataValue_IntValue:
		return convertFromInt64(t.IntValue, target)
	case *commonpb.MetadataValue_UintValue:
		return convertFromUint64(t.UintValue, target)
	case *commonpb.MetadataValue_BoolValue:
		return convertFromBool(t.BoolValue, target)
	case *commonpb.MetadataValue_DatetimeValue:
		return convertFromDatetime(t.DatetimeValue, target)
	case *commonpb.MetadataValue_NullValue:
		return convertFromNull(t.NullValue, target)
	default:
		return commonpb.NewNullValue("")
	}
}

func convertFromString(s string, target commonpb.MetadataType) *commonpb.MetadataValue {
	switch {
	case target == commonpb.MetadataType_METADATA_TYPE_STRING:
		return commonpb.NewStringValue(s)

	case target == commonpb.MetadataType_METADATA_TYPE_BOOL:
		lower := strings.ToLower(s)
		switch lower {
		case "true", "1":
			return commonpb.NewBoolValue(true)
		case "false", "0":
			return commonpb.NewBoolValue(false)
		default:
			return commonpb.NewNullValue(s)
		}

	case IsDatetimeType(target):
		micros, ok := commonpb.ParseDatetimeMicros(s)
		if !ok {
			return commonpb.NewNullValue(s)
		}

		return commonpb.NewDatetimeValue(micros)

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)

		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < lo || n > hi {
			return commonpb.NewNullValue(s)
		}

		return commonpb.NewIntValue(n)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)

		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil || n > hi {
			return commonpb.NewNullValue(s)
		}

		return commonpb.NewUintValue(n)

	default:
		return commonpb.NewNullValue(s)
	}
}

func convertFromInt64(n int64, target commonpb.MetadataType) *commonpb.MetadataValue {
	s := strconv.FormatInt(n, 10)

	switch {
	case target == commonpb.MetadataType_METADATA_TYPE_STRING:
		return commonpb.NewStringValue(s)

	case target == commonpb.MetadataType_METADATA_TYPE_BOOL:
		return commonpb.NewBoolValue(n != 0)

	case IsDatetimeType(target):
		return commonpb.NewDatetimeValue(n)

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)
		if n < lo || n > hi {
			return commonpb.NewNullValue(s)
		}

		return commonpb.NewIntValue(n)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)
		if n < 0 || uint64(n) > hi {
			return commonpb.NewNullValue(s)
		}

		return commonpb.NewUintValue(uint64(n))

	default:
		return commonpb.NewNullValue(s)
	}
}

func convertFromUint64(n uint64, target commonpb.MetadataType) *commonpb.MetadataValue {
	s := strconv.FormatUint(n, 10)

	switch {
	case target == commonpb.MetadataType_METADATA_TYPE_STRING:
		return commonpb.NewStringValue(s)

	case target == commonpb.MetadataType_METADATA_TYPE_BOOL:
		return commonpb.NewBoolValue(n != 0)

	case IsDatetimeType(target):
		if n > math.MaxInt64 {
			return commonpb.NewNullValue(s)
		}

		return commonpb.NewDatetimeValue(int64(n))

	case IsSignedType(target):
		_, hi, _ := signedRange(target)
		if n > uint64(hi) {
			return commonpb.NewNullValue(s)
		}

		return commonpb.NewIntValue(int64(n))

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)
		if n > hi {
			return commonpb.NewNullValue(s)
		}

		return commonpb.NewUintValue(n)

	default:
		return commonpb.NewNullValue(s)
	}
}

// convertFromDatetime converts a datetime value (signed int64 microseconds
// since the Unix epoch) to the target type. A datetime is physically an int64,
// so numeric targets reuse the raw micros; the string form is RFC3339 (matching
// MetadataValueToString) rather than the decimal micros. Bool (and any other
// target) has no meaningful datetime form and yields a commonpb.NullValue preserving the
// RFC3339 representation — symmetric with convertFromBool's datetime → null.
func convertFromDatetime(micros int64, target commonpb.MetadataType) *commonpb.MetadataValue {
	s := time.UnixMicro(micros).UTC().Format(time.RFC3339Nano)

	switch {
	case target == commonpb.MetadataType_METADATA_TYPE_STRING:
		return commonpb.NewStringValue(s)

	case IsDatetimeType(target):
		return commonpb.NewDatetimeValue(micros)

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)
		if micros < lo || micros > hi {
			return commonpb.NewNullValue(s)
		}

		return commonpb.NewIntValue(micros)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)
		if micros < 0 || uint64(micros) > hi {
			return commonpb.NewNullValue(s)
		}

		return commonpb.NewUintValue(uint64(micros))

	default:
		return commonpb.NewNullValue(s)
	}
}

func convertFromBool(b bool, target commonpb.MetadataType) *commonpb.MetadataValue {
	switch {
	case target == commonpb.MetadataType_METADATA_TYPE_STRING:
		return commonpb.NewStringValue(strconv.FormatBool(b))

	case target == commonpb.MetadataType_METADATA_TYPE_BOOL:
		return commonpb.NewBoolValue(b)

	case IsDatetimeType(target):
		return commonpb.NewNullValue(strconv.FormatBool(b))

	case IsSignedType(target):
		if b {
			return commonpb.NewIntValue(1)
		}

		return commonpb.NewIntValue(0)

	case IsUnsignedType(target):
		if b {
			return commonpb.NewUintValue(1)
		}

		return commonpb.NewUintValue(0)

	default:
		return commonpb.NewNullValue(strconv.FormatBool(b))
	}
}

func convertFromNull(nv *commonpb.NullValue, target commonpb.MetadataType) *commonpb.MetadataValue {
	if nv == nil {
		return commonpb.NewNullValue("")
	}

	original := nv.GetOriginal()

	switch {
	case target == commonpb.MetadataType_METADATA_TYPE_STRING:
		return commonpb.NewStringValue(original)

	case target == commonpb.MetadataType_METADATA_TYPE_BOOL:
		lower := strings.ToLower(original)
		switch lower {
		case "true", "1":
			return commonpb.NewBoolValue(true)
		case "false", "0":
			return commonpb.NewBoolValue(false)
		default:
			return commonpb.NewNullValue(original)
		}

	case IsDatetimeType(target):
		micros, ok := commonpb.ParseDatetimeMicros(original)
		if !ok {
			return commonpb.NewNullValue(original)
		}

		return commonpb.NewDatetimeValue(micros)

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)

		n, err := strconv.ParseInt(original, 10, 64)
		if err != nil || n < lo || n > hi {
			return commonpb.NewNullValue(original)
		}

		return commonpb.NewIntValue(n)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)

		n, err := strconv.ParseUint(original, 10, 64)
		if err != nil || n > hi {
			return commonpb.NewNullValue(original)
		}

		return commonpb.NewUintValue(n)

	default:
		return commonpb.NewNullValue(original)
	}
}
