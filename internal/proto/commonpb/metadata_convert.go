package commonpb

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
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

func NewDatetimeValue(micros int64) *MetadataValue {
	return &MetadataValue{Type: &MetadataValue_DatetimeValue{DatetimeValue: micros}}
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

// IsDatetimeType reports whether t is the datetime metadata type. Datetime
// values are stored in datetime_value (signed int64 microseconds since the
// Unix epoch), reusing the order-preserving int64 index encoding so range
// queries route through the signed integer path.
func IsDatetimeType(t MetadataType) bool {
	return t == MetadataType_METADATA_TYPE_DATETIME
}

// ParseDatetimeMicros parses an RFC3339/RFC3339Nano string into signed int64
// microseconds since the Unix epoch (UTC). RFC3339Nano is a superset that also
// parses values without fractional seconds. Pre-1970 timestamps are valid and
// produce a negative result. Returns ok=false only when the string is not a
// valid datetime.
func ParseDatetimeMicros(s string) (int64, bool) {
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, false
	}

	return ts.UnixMicro(), true
}

// ErrDatetimeBeforeEpoch is returned by CoerceDatetimeMicros when a value parses
// as a valid RFC3339 timestamp but predates the Unix epoch. The builtin date
// indexes (transaction timestamp/insertedAt/revertedAt, log date) store unsigned
// microseconds, so a negative UnixMicro has no representable bound: casting it to
// uint64 would wrap to a huge value and silently corrupt the range filter. It is
// rejected deterministically rather than accepted with garbage semantics — the
// same contract the HTTP transport layer enforces (EN-1542).
var ErrDatetimeBeforeEpoch = errors.New("dates before the Unix epoch (1970-01-01) are not supported")

// CoerceDatetimeMicros turns a date-index bound written as EITHER an RFC3339
// timestamp (e.g. "2023-11-14T22:13:20Z") OR raw unsigned microseconds
// ("1700000000000000") into the uint64 microseconds the index stores. It is the
// single coercion the whole filter surface shares (EN-1544): the structured
// QueryFilter JSON DSL date/timestamp bounds, the textual filterexpr
// date/timestamp field, and the audit[timestamp] datetime field all funnel
// through it so RFC3339 acceptance and pre-epoch rejection are defined once.
//
// An RFC3339 value before the Unix epoch returns ErrDatetimeBeforeEpoch. A value
// that is neither RFC3339 nor a decimal uint returns a parse error.
func CoerceDatetimeMicros(s string) (uint64, error) {
	if micros, ok := ParseDatetimeMicros(s); ok {
		if micros < 0 {
			return 0, ErrDatetimeBeforeEpoch
		}

		return uint64(micros), nil
	}

	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("expected an RFC3339 timestamp or unsigned microseconds, got %q", s)
	}

	return n, nil
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

	switch t := v.GetType().(type) {
	case *MetadataValue_StringValue:
		return t.StringValue
	case *MetadataValue_IntValue:
		return strconv.FormatInt(t.IntValue, 10)
	case *MetadataValue_UintValue:
		return strconv.FormatUint(t.UintValue, 10)
	case *MetadataValue_DatetimeValue:
		return time.UnixMicro(t.DatetimeValue).UTC().Format(time.RFC3339Nano)
	case *MetadataValue_BoolValue:
		return strconv.FormatBool(t.BoolValue)
	case *MetadataValue_NullValue:
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
func TypeMatches(v *MetadataValue, target MetadataType) bool {
	if v == nil {
		return false
	}

	switch target {
	case MetadataType_METADATA_TYPE_STRING:
		_, ok := v.GetType().(*MetadataValue_StringValue)

		return ok
	case MetadataType_METADATA_TYPE_BOOL:
		_, ok := v.GetType().(*MetadataValue_BoolValue)

		return ok
	}

	// Signed integer types: stored as int_value, range-checked.
	if lo, hi, ok := signedRange(target); ok {
		iv, isInt := v.GetType().(*MetadataValue_IntValue)

		return isInt && iv.IntValue >= lo && iv.IntValue <= hi
	}

	// Unsigned integer types: stored as uint_value, range-checked.
	if hi, ok := unsignedRange(target); ok {
		uv, isUint := v.GetType().(*MetadataValue_UintValue)

		return isUint && uv.UintValue <= hi
	}

	// Datetime is stored in datetime_value (signed int64 micros since epoch);
	// any datetime_value already matches and must not be re-converted.
	if IsDatetimeType(target) {
		_, isDatetime := v.GetType().(*MetadataValue_DatetimeValue)

		return isDatetime
	}

	return false
}

// SchemaFieldForTarget returns the field map and field schema for the given
// target type and key. Returns nil field if the schema, field map, or key does
// not exist.
func SchemaFieldForTarget(schema *MetadataSchema, targetType TargetType, key string) (map[string]*MetadataFieldSchema, *MetadataFieldSchema) {
	if schema == nil {
		return nil, nil
	}

	var fields map[string]*MetadataFieldSchema

	switch targetType {
	case TargetType_TARGET_TYPE_ACCOUNT:
		fields = schema.GetAccountFields()
	case TargetType_TARGET_TYPE_TRANSACTION:
		fields = schema.GetTransactionFields()
	case TargetType_TARGET_TYPE_LEDGER:
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
func CoerceToDeclaredType(schema *MetadataSchema, targetType TargetType, key string, v *MetadataValue) *MetadataValue {
	if v == nil {
		return v
	}

	_, fs := SchemaFieldForTarget(schema, targetType, key)
	if fs == nil || TypeMatches(v, fs.GetType()) {
		return v
	}

	return ConvertMetadataValue(v, fs.GetType())
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

	switch t := v.GetType().(type) {
	case *MetadataValue_StringValue:
		return convertFromString(t.StringValue, target)
	case *MetadataValue_IntValue:
		return convertFromInt64(t.IntValue, target)
	case *MetadataValue_UintValue:
		return convertFromUint64(t.UintValue, target)
	case *MetadataValue_BoolValue:
		return convertFromBool(t.BoolValue, target)
	case *MetadataValue_DatetimeValue:
		return convertFromDatetime(t.DatetimeValue, target)
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

	case IsDatetimeType(target):
		micros, ok := ParseDatetimeMicros(s)
		if !ok {
			return NewNullValue(s)
		}

		return NewDatetimeValue(micros)

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

	case IsDatetimeType(target):
		return NewDatetimeValue(n)

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

	case IsDatetimeType(target):
		if n > math.MaxInt64 {
			return NewNullValue(s)
		}

		return NewDatetimeValue(int64(n))

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

// convertFromDatetime converts a datetime value (signed int64 microseconds
// since the Unix epoch) to the target type. A datetime is physically an int64,
// so numeric targets reuse the raw micros; the string form is RFC3339 (matching
// MetadataValueToString) rather than the decimal micros. Bool (and any other
// target) has no meaningful datetime form and yields a NullValue preserving the
// RFC3339 representation — symmetric with convertFromBool's datetime → null.
func convertFromDatetime(micros int64, target MetadataType) *MetadataValue {
	s := time.UnixMicro(micros).UTC().Format(time.RFC3339Nano)

	switch {
	case target == MetadataType_METADATA_TYPE_STRING:
		return NewStringValue(s)

	case IsDatetimeType(target):
		return NewDatetimeValue(micros)

	case IsSignedType(target):
		lo, hi, _ := signedRange(target)
		if micros < lo || micros > hi {
			return NewNullValue(s)
		}

		return NewIntValue(micros)

	case IsUnsignedType(target):
		hi, _ := unsignedRange(target)
		if micros < 0 || uint64(micros) > hi {
			return NewNullValue(s)
		}

		return NewUintValue(uint64(micros))

	default:
		return NewNullValue(s)
	}
}

func convertFromBool(b bool, target MetadataType) *MetadataValue {
	switch {
	case target == MetadataType_METADATA_TYPE_STRING:
		return NewStringValue(strconv.FormatBool(b))

	case target == MetadataType_METADATA_TYPE_BOOL:
		return NewBoolValue(b)

	case IsDatetimeType(target):
		return NewNullValue(strconv.FormatBool(b))

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
		return NewNullValue(strconv.FormatBool(b))
	}
}

func convertFromNull(nv *NullValue, target MetadataType) *MetadataValue {
	if nv == nil {
		return NewNullValue("")
	}

	original := nv.GetOriginal()

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

	case IsDatetimeType(target):
		micros, ok := ParseDatetimeMicros(original)
		if !ok {
			return NewNullValue(original)
		}

		return NewDatetimeValue(micros)

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
