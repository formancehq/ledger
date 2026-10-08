// Package json provides the JSON helpers used by the public Ledger models.
//
// The package deliberately keeps the implementation behind a private boundary:
// callers get the standard encoding/json contract without inheriting a server
// JSON engine. On Go 1.27 and later, encoding/json uses the v2 implementation
// internally while retaining the v1-compatible API and semantics.
package json

import (
	stdjson "encoding/json"
	"io"
)

// Marshal returns the JSON encoding of v.
func Marshal(v any) ([]byte, error) {
	return stdjson.Marshal(v)
}

// Unmarshal parses the JSON-encoded data and stores the result in the value pointed to by v.
func Unmarshal(data []byte, v any) error {
	return stdjson.Unmarshal(data, v)
}

// UnmarshalRead reads JSON from the reader and unmarshals it into v.
// This is equivalent to encoding/json/v2's UnmarshalRead.
func UnmarshalRead(r io.Reader, v any) error {
	return stdjson.NewDecoder(r).Decode(v)
}

// MarshalWrite encodes v as JSON directly into the writer, avoiding
// an intermediate byte-slice allocation.
func MarshalWrite(w io.Writer, v any) error {
	return stdjson.NewEncoder(w).Encode(v)
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
