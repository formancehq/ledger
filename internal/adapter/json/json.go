// Package json uses Sonic for ordinary JSON encoding and decoding, with a
// scoped encoding/json/v2 path for option-aware HTTP monetary responses.
package json

import (
	stdjson "encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"io"

	"github.com/bytedance/sonic"
)

// MarshalEncode is the option-aware path for nested public projections. Unlike
// an opaque MarshalJSON call, it retains the request's type-specific marshalers.
// Existing custom projections retain unsorted nested maps. Escaping follows
// the outer writer: ConfigStd streaming escapes HTML/JS, buffered defaults do not.
func MarshalEncode(enc *jsontext.Encoder, value any) error {
	return jsonv2.MarshalEncode(enc, value, jsonv2.Deterministic(false))
}

// MarshalWithOptions uses the scoped v2 path with Sonic ConfigDefault's public
// compatibility settings. Ordinary Marshal calls continue to use Sonic.
func MarshalWithOptions(value any, opts ...jsonv2.Options) ([]byte, error) {
	options := append([]jsonv2.Options{stdjson.DefaultOptionsV1(), jsonv2.Deterministic(false),
		jsontext.EscapeForHTML(false), jsontext.EscapeForJS(false)}, opts...)

	return jsonv2.Marshal(value, options...)
}

// MarshalWriteWithOptions retains the streaming writer's legacy options and
// trailing newline. Nested public projections select their existing options.
func MarshalWriteWithOptions(w io.Writer, value any, opts ...jsonv2.Options) error {
	options := append([]jsonv2.Options{stdjson.DefaultOptionsV1()}, opts...)
	if err := jsonv2.MarshalWrite(w, value, options...); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\n")

	return err
}

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

// MarshalWrite encodes v as JSON directly into the writer, avoiding
// an intermediate byte-slice allocation.
func MarshalWrite(w io.Writer, v any) error {
	return sonic.ConfigStd.NewEncoder(w).Encode(v)
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
