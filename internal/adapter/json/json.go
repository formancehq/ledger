// Package json uses Sonic for ordinary JSON encoding and decoding, with a
// scoped encoding/json/v2 path for request-scoped HTTP response options.
package json

import (
	"bytes"
	stdjson "encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"io"
	"strconv"

	"github.com/bytedance/sonic"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

// MarshalProtoJSONNumbers emits protobuf JSON while representing all signed
// and unsigned 64-bit scalar fields as JSON numbers. protojson deliberately
// quotes these fields to preserve precision in JavaScript; Ledger's public
// JSON contract instead uses decimal number tokens. The descriptor-guided
// walk avoids converting ordinary business strings that happen to contain
// digits.
func MarshalProtoJSONNumbers(msg proto.Message, opts protojson.MarshalOptions) ([]byte, error) {
	raw, err := opts.Marshal(msg)
	if err != nil {
		return nil, err
	}

	normalized, err := normalizeProtoJSONNumbers(raw, msg.ProtoReflect().Descriptor())
	if err != nil {
		return nil, err
	}
	if !opts.Multiline {
		return normalized, nil
	}

	var formatted bytes.Buffer
	if err := stdjson.Indent(&formatted, normalized, "", opts.Indent); err != nil {
		return nil, err
	}

	return formatted.Bytes(), nil
}

func normalizeProtoJSONNumbers(raw []byte, desc protoreflect.MessageDescriptor) ([]byte, error) {
	var object map[string]stdjson.RawMessage
	if err := stdjson.Unmarshal(raw, &object); err != nil {
		return nil, err
	}

	fields := desc.Fields()
	for name, value := range object {
		field := fields.ByJSONName(name)
		if field == nil {
			continue
		}
		normalized, err := normalizeProtoJSONValue(value, field)
		if err != nil {
			return nil, err
		}
		object[name] = normalized
	}

	return stdjson.Marshal(object)
}

func normalizeProtoJSONValue(raw []byte, field protoreflect.FieldDescriptor) ([]byte, error) {
	if field.IsMap() {
		var object map[string]stdjson.RawMessage
		if err := stdjson.Unmarshal(raw, &object); err != nil {
			return nil, err
		}
		valueDesc := field.MapValue()
		for key, value := range object {
			normalized, err := normalizeProtoJSONScalarOrMessage(value, valueDesc)
			if err != nil {
				return nil, err
			}
			object[key] = normalized
		}

		return stdjson.Marshal(object)
	}
	if field.IsList() {
		var values []stdjson.RawMessage
		if err := stdjson.Unmarshal(raw, &values); err != nil {
			return nil, err
		}
		for i, value := range values {
			normalized, err := normalizeProtoJSONScalarOrMessage(value, field)
			if err != nil {
				return nil, err
			}
			values[i] = normalized
		}

		return stdjson.Marshal(values)
	}

	return normalizeProtoJSONScalarOrMessage(raw, field)
}

func normalizeProtoJSONScalarOrMessage(raw []byte, field protoreflect.FieldDescriptor) ([]byte, error) {
	if nested := field.Message(); nested != nil {
		return normalizeProtoJSONNumbers(raw, nested)
	}

	switch field.Kind() {
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		var value string
		if len(raw) > 0 && raw[0] == '"' {
			if err := stdjson.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
			if _, err := strconv.ParseInt(value, 10, 64); err != nil {
				return nil, err
			}
			return []byte(value), nil
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		var value string
		if len(raw) > 0 && raw[0] == '"' {
			if err := stdjson.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
			if _, err := strconv.ParseUint(value, 10, 64); err != nil {
				return nil, err
			}
			return []byte(value), nil
		}
	}

	return raw, nil
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
