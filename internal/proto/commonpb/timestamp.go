package commonpb

import (
	"fmt"
	libtime "time"

	"github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
)

func NewTimestamp(time time.Time) *Timestamp {
	return &Timestamp{
		Data: uint64(time.UnixMicro()),
	}
}

func (x *Timestamp) AsTime() time.Time {
	return time.New(libtime.UnixMicro(int64(x.GetData())))
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

	t, err := time.ParseTime(v)
	if err != nil {
		return err
	}

	x.Data = uint64(t.UnixMicro())

	return nil
}
