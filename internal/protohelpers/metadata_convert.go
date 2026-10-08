package protohelpers

import (
	"math"
	"strconv"
	"strings"
	"time"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// signedRange returns (min, max) for a signed integer ledgerpb.MetadataType.
// Returns (0, 0, false) for non-signed types.
func signedRange(t ledgerpb.MetadataType) (int64, int64, bool) {
	switch t {
	case ledgerpb.MetadataType_METADATA_TYPE_INT8:
		return math.MinInt8, math.MaxInt8, true
	case ledgerpb.MetadataType_METADATA_TYPE_INT16:
		return math.MinInt16, math.MaxInt16, true
	case ledgerpb.MetadataType_METADATA_TYPE_INT32:
		return math.MinInt32, math.MaxInt32, true
	case ledgerpb.MetadataType_METADATA_TYPE_INT64:
		return math.MinInt64, math.MaxInt64, true
	default:
		return 0, 0, false
	}
}

// unsignedRange returns max for an unsigned integer ledgerpb.MetadataType.
// Returns (0, false) for non-unsigned types.
func unsignedRange(t ledgerpb.MetadataType) (uint64, bool) {
	switch t {
	case ledgerpb.MetadataType_METADATA_TYPE_UINT8:
		return math.MaxUint8, true
	case ledgerpb.MetadataType_METADATA_TYPE_UINT16:
		return math.MaxUint16, true
	case ledgerpb.MetadataType_METADATA_TYPE_UINT32:
		return math.MaxUint32, true
	case ledgerpb.MetadataType_METADATA_TYPE_UINT64:
		return math.MaxUint64, true
	default:
		return 0, false
	}
}

// IsDatetimeType reports whether t is the datetime metadata type. Datetime
// values are stored in datetime_value (signed int64 microseconds since the
// Unix epoch), reusing the order-preserving int64 index encoding so range
// queries route through the signed integer path.
func IsDatetimeType(t ledgerpb.MetadataType) bool {
	return t == ledgerpb.MetadataType_METADATA_TYPE_DATETIME
}

// IsSignedType returns true for INT8, INT16, INT32, INT64.
func IsSignedType(t ledgerpb.MetadataType) bool {
	_, _, ok := signedRange(t)

	return ok
}

// IsUnsignedType returns true for UINT8, UINT16, UINT32, UINT64.
func IsUnsignedType(t ledgerpb.MetadataType) bool {
	_, ok := unsignedRange(t)

	return ok
}

// MetadataValueToString converts any ledgerpb.MetadataValue to its string representation.
// This always succeeds — every type has a string form.
func MetadataValueToString(v *ledgerpb.MetadataValue) string {
	if v == nil {
		return ""
	}

	switch t := v.GetType().(type) {
	case *ledgerpb.MetadataValue_StringValue:
		return t.StringValue
	case *ledgerpb.MetadataValue_IntValue:
		return strconv.FormatInt(t.IntValue, 10)
	case *ledgerpb.MetadataValue_UintValue:
		return strconv.FormatUint(t.UintValue, 10)
	case *ledgerpb.MetadataValue_DatetimeValue:
		return time.UnixMicro(t.DatetimeValue).UTC().Format(time.RFC3339Nano)
	case *ledgerpb.MetadataValue_BoolValue:
		return strconv.FormatBool(t.BoolValue)
	case *ledgerpb.MetadataValue_NullValue:
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
func TypeMatches(v *ledgerpb.MetadataValue, target ledgerpb.MetadataType) bool {
	if v == nil {
		return false
	}

	switch target {
	case ledgerpb.MetadataType_METADATA_TYPE_STRING:
		_, ok := v.GetType().(*ledgerpb.MetadataValue_StringValue)

		return ok
	case ledgerpb.MetadataType_METADATA_TYPE_BOOL:
		_, ok := v.GetType().(*ledgerpb.MetadataValue_BoolValue)

		return ok
	}

	// Signed integer types: stored as int_value, range-checked.
	if lo, hi, ok := signedRange(target); ok {
		iv, isInt := v.GetType().(*ledgerpb.MetadataValue_IntValue)

		return isInt && iv.IntValue >= lo && iv.IntValue <= hi
	}

	// Unsigned integer types: stored as uint_value, range-checked.
	if hi, ok := unsignedRange(target); ok {
		uv, isUint := v.GetType().(*ledgerpb.MetadataValue_UintValue)

		return isUint && uv.UintValue <= hi
	}

	// Datetime is stored in datetime_value (signed int64 micros since epoch);
	// any datetime_value already matches and must not be re-converted.
	if IsDatetimeType(target) {
		_, isDatetime := v.GetType().(*ledgerpb.MetadataValue_DatetimeValue)

		return isDatetime
	}

	return false
}

// SchemaFieldForTarget returns the field map and field schema for the given
// target type and key. Returns nil field if the schema, field map, or key does
// not exist.
func SchemaFieldForTarget(schema *ledgerpb.MetadataSchema, targetType ledgerpb.TargetType, key string) (map[string]*ledgerpb.MetadataFieldSchema, *ledgerpb.MetadataFieldSchema) {
	if schema == nil {
		return nil, nil
	}

	var fields map[string]*ledgerpb.MetadataFieldSchema

	switch targetType {
	case ledgerpb.TargetType_TARGET_TYPE_ACCOUNT:
		fields = schema.GetAccountFields()
	case ledgerpb.TargetType_TARGET_TYPE_TRANSACTION:
		fields = schema.GetTransactionFields()
	case ledgerpb.TargetType_TARGET_TYPE_LEDGER:
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
func CoerceToDeclaredType(schema *ledgerpb.MetadataSchema, targetType ledgerpb.TargetType, key string, v *ledgerpb.MetadataValue) *ledgerpb.MetadataValue {
	if v == nil {
		return v
	}

	_, fs := SchemaFieldForTarget(schema, targetType, key)
	if fs == nil || TypeMatches(v, fs.GetType()) {
		return v
	}

	return ConvertMetadataValue(v, fs.GetType())
}

// ConvertMetadataValue converts a ledgerpb.MetadataValue to the target type using the
// conversion matrix defined in the RFC. If the conversion is not possible,
// returns a ledgerpb.NullValue preserving the original string representation.
func ConvertMetadataValue(v *ledgerpb.MetadataValue, target ledgerpb.MetadataType) *ledgerpb.MetadataValue {
	if v == nil {
		return ledgerpb.NewNullValue("")
	}

	if TypeMatches(v, target) {
		return v
	}

	switch t := v.GetType().(type) {
	case *ledgerpb.MetadataValue_StringValue:
		return convertFromString(t.StringValue, target)
	case *ledgerpb.MetadataValue_IntValue:
		return convertFromInt64(t.IntValue, target)
	case *ledgerpb.MetadataValue_UintValue:
		return convertFromUint64(t.UintValue, target)
	case *ledgerpb.MetadataValue_BoolValue:
		return convertFromBool(t.BoolValue, target)
	case *ledgerpb.MetadataValue_DatetimeValue:
		return convertFromDatetime(t.DatetimeValue, target)
	case *ledgerpb.MetadataValue_NullValue:
		return convertFromNull(t.NullValue, target)
	default:
		return ledgerpb.NewNullValue("")
	}
}

func convertFromString(s string, target ledgerpb.MetadataType) *ledgerpb.MetadataValue {
	switch {
	case target == ledgerpb.MetadataType_METADATA_TYPE_STRING:
		return ledgerpb.NewStringValue(s)

	case target == ledgerpb.MetadataType_METADATA_TYPE_BOOL:
		lower := strings.ToLower(s)
		switch lower {
		case "true", "1":
			return ledgerpb.NewBoolValue(true)
		case "false", "0":
			return ledgerpb.NewBoolValue(false)
		default:
			return ledgerpb.NewNullValue(s)
		}

	case IsDatetimeType(target):
		micros, ok := ledgerpb.ParseDatetimeMicros(s)
		if !ok {
			return ledgerpb.NewNullValue(s)
		}

		return ledgerpb.NewDatetimeValue(micros)

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)

		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < lo || n > hi {
			return ledgerpb.NewNullValue(s)
		}

		return ledgerpb.NewIntValue(n)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)

		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil || n > hi {
			return ledgerpb.NewNullValue(s)
		}

		return ledgerpb.NewUintValue(n)

	default:
		return ledgerpb.NewNullValue(s)
	}
}

func convertFromInt64(n int64, target ledgerpb.MetadataType) *ledgerpb.MetadataValue {
	s := strconv.FormatInt(n, 10)

	switch {
	case target == ledgerpb.MetadataType_METADATA_TYPE_STRING:
		return ledgerpb.NewStringValue(s)

	case target == ledgerpb.MetadataType_METADATA_TYPE_BOOL:
		return ledgerpb.NewBoolValue(n != 0)

	case IsDatetimeType(target):
		return ledgerpb.NewDatetimeValue(n)

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)
		if n < lo || n > hi {
			return ledgerpb.NewNullValue(s)
		}

		return ledgerpb.NewIntValue(n)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)
		if n < 0 || uint64(n) > hi {
			return ledgerpb.NewNullValue(s)
		}

		return ledgerpb.NewUintValue(uint64(n))

	default:
		return ledgerpb.NewNullValue(s)
	}
}

func convertFromUint64(n uint64, target ledgerpb.MetadataType) *ledgerpb.MetadataValue {
	s := strconv.FormatUint(n, 10)

	switch {
	case target == ledgerpb.MetadataType_METADATA_TYPE_STRING:
		return ledgerpb.NewStringValue(s)

	case target == ledgerpb.MetadataType_METADATA_TYPE_BOOL:
		return ledgerpb.NewBoolValue(n != 0)

	case IsDatetimeType(target):
		if n > math.MaxInt64 {
			return ledgerpb.NewNullValue(s)
		}

		return ledgerpb.NewDatetimeValue(int64(n))

	case IsSignedType(target):
		_, hi, _ := signedRange(target)
		if n > uint64(hi) {
			return ledgerpb.NewNullValue(s)
		}

		return ledgerpb.NewIntValue(int64(n))

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)
		if n > hi {
			return ledgerpb.NewNullValue(s)
		}

		return ledgerpb.NewUintValue(n)

	default:
		return ledgerpb.NewNullValue(s)
	}
}

// convertFromDatetime converts a datetime value (signed int64 microseconds
// since the Unix epoch) to the target type. A datetime is physically an int64,
// so numeric targets reuse the raw micros; the string form is RFC3339 (matching
// MetadataValueToString) rather than the decimal micros. Bool (and any other
// target) has no meaningful datetime form and yields a ledgerpb.NullValue preserving the
// RFC3339 representation — symmetric with convertFromBool's datetime → null.
func convertFromDatetime(micros int64, target ledgerpb.MetadataType) *ledgerpb.MetadataValue {
	s := time.UnixMicro(micros).UTC().Format(time.RFC3339Nano)

	switch {
	case target == ledgerpb.MetadataType_METADATA_TYPE_STRING:
		return ledgerpb.NewStringValue(s)

	case IsDatetimeType(target):
		return ledgerpb.NewDatetimeValue(micros)

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)
		if micros < lo || micros > hi {
			return ledgerpb.NewNullValue(s)
		}

		return ledgerpb.NewIntValue(micros)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)
		if micros < 0 || uint64(micros) > hi {
			return ledgerpb.NewNullValue(s)
		}

		return ledgerpb.NewUintValue(uint64(micros))

	default:
		return ledgerpb.NewNullValue(s)
	}
}

func convertFromBool(b bool, target ledgerpb.MetadataType) *ledgerpb.MetadataValue {
	switch {
	case target == ledgerpb.MetadataType_METADATA_TYPE_STRING:
		return ledgerpb.NewStringValue(strconv.FormatBool(b))

	case target == ledgerpb.MetadataType_METADATA_TYPE_BOOL:
		return ledgerpb.NewBoolValue(b)

	case IsDatetimeType(target):
		return ledgerpb.NewNullValue(strconv.FormatBool(b))

	case IsSignedType(target):
		if b {
			return ledgerpb.NewIntValue(1)
		}

		return ledgerpb.NewIntValue(0)

	case IsUnsignedType(target):
		if b {
			return ledgerpb.NewUintValue(1)
		}

		return ledgerpb.NewUintValue(0)

	default:
		return ledgerpb.NewNullValue(strconv.FormatBool(b))
	}
}

func convertFromNull(nv *ledgerpb.NullValue, target ledgerpb.MetadataType) *ledgerpb.MetadataValue {
	if nv == nil {
		return ledgerpb.NewNullValue("")
	}

	original := nv.GetOriginal()

	switch {
	case target == ledgerpb.MetadataType_METADATA_TYPE_STRING:
		return ledgerpb.NewStringValue(original)

	case target == ledgerpb.MetadataType_METADATA_TYPE_BOOL:
		lower := strings.ToLower(original)
		switch lower {
		case "true", "1":
			return ledgerpb.NewBoolValue(true)
		case "false", "0":
			return ledgerpb.NewBoolValue(false)
		default:
			return ledgerpb.NewNullValue(original)
		}

	case IsDatetimeType(target):
		micros, ok := ledgerpb.ParseDatetimeMicros(original)
		if !ok {
			return ledgerpb.NewNullValue(original)
		}

		return ledgerpb.NewDatetimeValue(micros)

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)

		n, err := strconv.ParseInt(original, 10, 64)
		if err != nil || n < lo || n > hi {
			return ledgerpb.NewNullValue(original)
		}

		return ledgerpb.NewIntValue(n)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)

		n, err := strconv.ParseUint(original, 10, 64)
		if err != nil || n > hi {
			return ledgerpb.NewNullValue(original)
		}

		return ledgerpb.NewUintValue(n)

	default:
		return ledgerpb.NewNullValue(original)
	}
}
