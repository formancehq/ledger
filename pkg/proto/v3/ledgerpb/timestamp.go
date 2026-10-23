package ledgerpb

import (
	"errors"
	"fmt"
	"time"

	"github.com/formancehq/ledger/pkg/proto/v3/json"
)

// ErrTimestampBeforeEpoch is returned when a timestamp before the Unix epoch is provided.
// The Data field is stored as uint64 microseconds, so pre-epoch (negative) values
// cannot be represented correctly.
var ErrTimestampBeforeEpoch = errors.New("timestamp before Unix epoch (1970-01-01T00:00:00Z) is not supported")

// NewTimestamp creates a Timestamp from a value exposing UnixMicro, such as time.Time.
// Caller must ensure the time is not before the Unix epoch; pre-epoch times
// will silently overflow the uint64 Data field.
func NewTimestamp(value interface{ UnixMicro() int64 }) *Timestamp {
	return &Timestamp{
		Data: uint64(value.UnixMicro()),
	}
}

func (x *Timestamp) AsTime() time.Time {
	return time.UnixMicro(int64(x.GetData())).UTC()
}

func (x *Timestamp) MarshalJSON() ([]byte, error) {
	v := x.AsTime().Format(time.RFC3339Nano)

	return fmt.Appendf(nil, "\"%s\"", v), nil
}

func (x *Timestamp) UnmarshalJSON(data []byte) error {
	var v string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return err
	}

	if t.UnixMicro() < 0 {
		return ErrTimestampBeforeEpoch
	}

	x.Data = uint64(t.UTC().Round(time.Microsecond).UnixMicro())

	return nil
}
