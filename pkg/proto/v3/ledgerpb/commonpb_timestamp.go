package ledgerpb

import (
	"errors"
	"time"
)

// ErrTimestampBeforeEpoch is returned when a timestamp before the Unix epoch is provided.
// The Data field is stored as uint64 microseconds, so pre-epoch (negative) values
// cannot be represented correctly.
var ErrTimestampBeforeEpoch = errors.New("timestamp before Unix epoch (1970-01-01T00:00:00Z) is not supported")

// NewTimestamp creates a Timestamp from a time.Time.
// Caller must ensure the time is not before the Unix epoch; pre-epoch times
// will silently overflow the uint64 Data field.
func NewTimestamp(t interface{ UnixMicro() int64 }) *Timestamp {
	return &Timestamp{
		Data: uint64(t.UnixMicro()),
	}
}

func (x *Timestamp) AsTime() time.Time {
	return time.UnixMicro(int64(x.GetData())).UTC()
}
