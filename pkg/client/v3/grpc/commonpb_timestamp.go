package grpc

import (
	"errors"
	"fmt"
	"time"

	"github.com/formancehq/ledger/pkg/client/v3/internal/json"
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

	rounded := t.Round(time.Microsecond).UTC()
	if rounded.UnixMicro() < 0 {
		return ErrTimestampBeforeEpoch
	}

	x.Data = uint64(rounded.UnixMicro())

	return nil
}
