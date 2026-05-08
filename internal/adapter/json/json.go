// Package json provides a compatibility layer for JSON encoding/decoding using sonic.
// It provides the same API surface as encoding/json/v2 but uses sonic for performance.
package json

import (
	"io"

	"github.com/bytedance/sonic"
)

// Marshal returns the JSON encoding of v using sonic.
func Marshal(v any) ([]byte, error) {
	return sonic.Marshal(v)
}

// Unmarshal parses the JSON-encoded data and stores the result in the value pointed to by v.
func Unmarshal(data []byte, v any) error {
	return sonic.Unmarshal(data, v)
}

// UnmarshalRead reads JSON from the reader and unmarshals it into v.
// This is equivalent to encoding/json/v2's UnmarshalRead.
func UnmarshalRead(r io.Reader, v any) error {
	return sonic.ConfigStd.NewDecoder(r).Decode(v)
}

// RawValue represents a raw JSON value that can be stored and used later.
// This is equivalent to encoding/json/v2's jsontext.Value.
type RawValue []byte

// MarshalJSON implements json.Marshaler for RawValue.
func (r RawValue) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}

	return r, nil
}

// UnmarshalJSON implements json.Unmarshaler for RawValue.
func (r *RawValue) UnmarshalJSON(data []byte) error {
	if r == nil {
		return nil
	}

	*r = append((*r)[:0], data...)

	return nil
}

// String returns the raw JSON value as a string.
func (r RawValue) String() string {
	return string(r)
}
