package grpc

import (
	"errors"
	"fmt"
	"strconv"
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
// date/timestamp field, and the audit timestamp datetime field all funnel
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
