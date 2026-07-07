package commonpb

import (
	"errors"
	"fmt"
	libtime "time"

	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// Timestamp is a value-typed timestamp in microseconds since the Unix epoch.
//
// The proto schema stores timestamps as `fixed64` scalars directly on the
// parent message (no wrapper message), so a Timestamp costs zero heap
// allocation: it lives inline in the containing struct field. Immutability
// falls out of Go value semantics — pass by copy, mutation is trivially
// impossible.
//
// The zero value (0) is a sentinel meaning "unset" for nullable fields
// (RevertedAt, DeletedAt, ...). The ledger's HLC never produces micros=0 in
// practice (epoch = 1970-01-01T00:00:00.000000Z), so the sentinel is safe.
// Callers building from a `time.Time` at exactly epoch will fail in the
// constructor.
type Timestamp uint64

// TimestampUnset is the sentinel value used by nullable timestamp fields.
const TimestampUnset Timestamp = 0

// ErrTimestampBeforeEpoch is returned when a timestamp at or before the Unix
// epoch is provided. Timestamps are stored as unsigned microseconds since
// epoch, so pre-epoch values cannot be represented, and epoch itself collides
// with the "unset" sentinel.
var ErrTimestampBeforeEpoch = errors.New("timestamp at or before Unix epoch (1970-01-01T00:00:00Z) is not supported")

// NewTimestamp creates a Timestamp from a time.Time. Time values at or before
// the Unix epoch collapse to TimestampUnset (0), since 0 doubles as the
// "unset" sentinel; the HLC never emits micros=0 in practice so real data
// paths are unaffected. Callers that must distinguish "unset" from "epoch"
// on untrusted input should validate `t.UnixMicro() > 0` before constructing,
// or gate the result on IsSet.
func NewTimestamp(t time.Time) Timestamp {
	m := t.UnixMicro()
	if m <= 0 {
		return TimestampUnset
	}

	return Timestamp(m)
}

// NewTimestampFromMicros wraps a raw microseconds value. Zero maps to
// TimestampUnset; any other value is treated as a valid Timestamp. Prefer
// NewTimestamp when a time.Time is available; this constructor is for FSM
// code paths (HLC advance, replay, checker replay) that already work in
// raw microseconds.
func NewTimestampFromMicros(micros uint64) Timestamp {
	return Timestamp(micros)
}

// IsSet reports whether the timestamp carries a value (i.e. != TimestampUnset).
func (t Timestamp) IsSet() bool { return t != TimestampUnset }

// Micros returns the raw microseconds-since-epoch representation, for use in
// proto struct fields declared as `fixed64` (which decode as uint64 in Go).
func (t Timestamp) Micros() uint64 { return uint64(t) }

// AsTime converts to a time.Time. When called on TimestampUnset, returns the
// Unix epoch — callers that need to distinguish "unset" from "epoch" must
// gate on IsSet first.
func (t Timestamp) AsTime() time.Time {
	return time.New(libtime.UnixMicro(int64(t)))
}

// MarshalJSON emits the timestamp as an RFC3339Nano string. Unset timestamps
// serialize as JSON null — parent messages should use `omitempty`-equivalent
// logic if they want to skip the field entirely.
func (t Timestamp) MarshalJSON() ([]byte, error) {
	if !t.IsSet() {
		return []byte("null"), nil
	}
	v := t.AsTime().Format(time.RFC3339Nano)

	return fmt.Appendf(nil, "\"%s\"", v), nil
}

// UnmarshalJSON parses an RFC3339Nano string. `null` maps to TimestampUnset.
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*t = TimestampUnset

		return nil
	}
	var v string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	parsed, err := time.ParseTime(v)
	if err != nil {
		return err
	}
	if parsed.UnixMicro() <= 0 {
		return ErrTimestampBeforeEpoch
	}
	*t = Timestamp(parsed.UnixMicro())

	return nil
}
