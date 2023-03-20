package core

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/pkg/errors"
)

const (
	DatePrecision = time.Microsecond
	DateFormat    = time.RFC3339Nano
)

type Time struct {
	time.Time
}

func (t *Time) Scan(src interface{}) (err error) {
	switch src := src.(type) {
	case time.Time:
		*t = Time{
			Time: src,
		}
		return nil
	case string:
		*t = Time{}
		t.Time, err = time.ParseInLocation(DateFormat, src, time.UTC)
		return err
	case []byte:
		*t = Time{}
		t.Time, err = time.ParseInLocation(DateFormat, string(src), time.UTC)
		return err
	case nil:
		*t = Time{}
		t.Time = time.Time{}
		return nil
	default:
		return fmt.Errorf("unsupported data type: %T", src)
	}
}

func (t Time) Value() (driver.Value, error) {
	return t.Format(DateFormat), nil
}

func (t Time) Before(t2 Time) bool {
	return t.Time.Before(t2.Time)
}

func (t Time) After(t2 Time) bool {
	return t.Time.After(t2.Time)
}

func (t Time) Sub(t2 Time) time.Duration {
	return t.Time.Sub(t2.Time)
}

func (t Time) Add(d time.Duration) Time {
	return Time{
		Time: t.Time.Add(d),
	}
}

func (t Time) UTC() Time {
	return Time{
		Time: t.Time.UTC(),
	}
}

func (t Time) Round(precision time.Duration) Time {
	return Time{
		Time: t.Time.Round(precision),
	}
}

func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, t.Format(DateFormat))), nil
}

func (t *Time) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		*t = Time{}
		return nil
	}
	if data[0] != '"' || data[len(data)-1] != '"' {
		return errors.New("invalid date format")
	}

	parsed, err := ParseTime(string(data[1 : len(data)-1]))
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}

func Now() Time {
	return Time{
		Time: time.Now().UTC().Round(DatePrecision),
	}
}

func ParseTime(v string) (Time, error) {
	t, err := time.Parse(DateFormat, v)
	if err != nil {
		return Time{}, err
	}
	return Time{
		Time: t.Round(DatePrecision),
	}, nil
}
