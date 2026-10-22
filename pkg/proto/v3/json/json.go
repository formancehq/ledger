// Package json provides the JSON boundary used by the shared protobuf model.
//
// The default implementation uses the Go standard library. A server may
// install a process-local codec (for example Sonic) without making that
// implementation a dependency of the public protobuf module.
package json

import (
	"bytes"
	stdjson "encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"io"
	"sync/atomic"
)

// RawValue stores an encoded JSON value without interpreting it.
type RawValue []byte

func (r RawValue) MarshalJSON() ([]byte, error) {
	if len(r) == 0 {
		return []byte("null"), nil
	}

	return r, nil
}

func (r *RawValue) UnmarshalJSON(data []byte) error {
	if r != nil {
		*r = append((*r)[:0], data...)
	}
	return nil
}

func (r RawValue) String() string { return string(r) }

// Codec contains the operations that may be replaced by a server runtime.
// Nil fields use the standard-library implementation.
type Codec struct {
	Marshal       func(any) ([]byte, error)
	Unmarshal     func([]byte, any) error
	UnmarshalRead func(io.Reader, any) error
	MarshalWrite  func(io.Writer, any) error
}

var active atomic.Value // stores Codec

func init() { active.Store(defaultCodec()) }

// Configure installs a process-wide codec for the shared model. It is
// intended for server startup, before serving requests.
func Configure(codec Codec) {
	defaults := defaultCodec()
	if codec.Marshal == nil {
		codec.Marshal = defaults.Marshal
	}
	if codec.Unmarshal == nil {
		codec.Unmarshal = defaults.Unmarshal
	}
	if codec.UnmarshalRead == nil {
		codec.UnmarshalRead = defaults.UnmarshalRead
	}
	if codec.MarshalWrite == nil {
		codec.MarshalWrite = defaults.MarshalWrite
	}
	active.Store(codec)
}

func current() Codec { return active.Load().(Codec) }

func Marshal(v any) ([]byte, error) { return current().Marshal(v) }

func Unmarshal(data []byte, v any) error { return current().Unmarshal(data, v) }

// UnmarshalUseNumber decodes JSON while preserving numbers as json.Number.
// It is intentionally independent from the installed codec because callers
// use it when deciding whether a number is an integer or a fractional value.
func UnmarshalUseNumber(data []byte, v any) error {
	decoder := stdjson.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(v); err != nil {
		return err
	}

	var trailing any
	if err := decoder.Decode(&trailing); err == io.EOF {
		return nil
	} else if err == nil {
		return errors.New("json: trailing data")
	} else {
		return err
	}
}

func UnmarshalRead(r io.Reader, v any) error { return current().UnmarshalRead(r, v) }

func MarshalWrite(w io.Writer, v any) error { return current().MarshalWrite(w, v) }

// MarshalEncode uses the Go JSON v2 streaming API for request-scoped options.
func MarshalEncode(enc *jsontext.Encoder, value any) error {
	return jsonv2.MarshalEncode(enc, value, jsonv2.Deterministic(false))
}

func MarshalWithOptions(value any, opts ...jsonv2.Options) ([]byte, error) {
	options := append([]jsonv2.Options{stdjson.DefaultOptionsV1(), jsonv2.Deterministic(false),
		jsontext.EscapeForHTML(false), jsontext.EscapeForJS(false)}, opts...)
	return jsonv2.Marshal(value, options...)
}

func MarshalWriteWithOptions(w io.Writer, value any, opts ...jsonv2.Options) error {
	options := append([]jsonv2.Options{stdjson.DefaultOptionsV1()}, opts...)
	if err := jsonv2.MarshalWrite(w, value, options...); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\n")
	return err
}

func defaultCodec() Codec {
	return Codec{
		Marshal:   stdjson.Marshal,
		Unmarshal: stdjson.Unmarshal,
		UnmarshalRead: func(r io.Reader, v any) error {
			return jsonv2.UnmarshalRead(r, v, stdjson.DefaultOptionsV1())
		},
		MarshalWrite: func(w io.Writer, v any) error {
			if err := jsonv2.MarshalWrite(w, v, stdjson.DefaultOptionsV1()); err != nil {
				return err
			}

			_, err := io.WriteString(w, "\n")
			return err
		},
	}
}
