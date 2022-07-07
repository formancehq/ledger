package core

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
)

type Time struct {
	time time.Time
}

var (
	_ driver.Valuer    = Time{}
	_ sql.Scanner      = &Time{}
	_ json.Marshaler   = Time{}
	_ json.Unmarshaler = &Time{}
)

func (t Time) Value() (driver.Value, error) {
	return t.time.UTC().Format(time.RFC3339), nil
}

func (t *Time) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	spew.Dump(value)
	fmt.Println("got value", value)
	v, err := driver.String.ConvertValue(value)
	if err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339, v.(string))
	if err != nil {
		return err
	}
	*t = Time{time: parsed.UTC()}
	return nil
}

func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(t.time.UTC().Format(time.RFC3339))), nil
}

func (t *Time) UnmarshalJSON(bytes []byte) error {
	unquoted, err := strconv.Unquote(string(bytes))
	if err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339, unquoted)
	if err != nil {
		return err
	}
	*t = Time{time: parsed.UTC()}
	return nil
}

func (t Time) UTC() Time {
	return Time{time: t.time.UTC()}
}

func (t Time) Format(layout string) string {
	return t.time.Format(layout)
}

func (t Time) Truncate(d time.Duration) Time {
	return Time{time: t.time.Truncate(d)}
}

func (t Time) Add(duration time.Duration) Time {
	return Time{time: t.time.Add(duration)}
}

func (t Time) Round(d time.Duration) Time {
	return Time{time: t.time.Round(d)}
}

func (t Time) IsZero() bool {
	return t.time.IsZero()
}

func (t Time) String() string {
	return t.time.String()
}

func ParseTime(layout string, value string) (Time, error) {
	t, err := time.Parse(layout, value)
	if err != nil {
		return Time{}, err
	}
	return Time{time: t}, nil
}

func Now() Time {
	return Time{time: time.Now()}
}
