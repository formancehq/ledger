// Package json uses Sonic for ordinary JSON encoding and decoding, with a
// scoped encoding/json/v2 path for request-scoped HTTP response options.
package json

import (
	"bytes"
	stdjson "encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/bytedance/sonic"
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

// MarshalMessageJSON is the standard-library JSON runtime used by
// protoc-gen-ledger-json generated MarshalJSON methods. It intentionally
// follows the Ledger API projection: descriptor JSON names, enum names, native
// 64-bit JSON numbers, and recursive oneof/map/list handling.
func MarshalMessageJSON(msg proto.Message) ([]byte, error) {
	return MarshalMessageJSONWithOptions(msg, MarshalOptions{})
}

// MarshalOptions controls the standard-library protobuf JSON projection.
// Unlike protojson, 64-bit integer fields are always emitted as JSON numbers.
type MarshalOptions struct {
	Multiline       bool
	Indent          string
	EmitUnpopulated bool
}

// MarshalMessageJSONWithOptions is the option-aware form used by CLI output
// and persistence inspection tools.
func MarshalMessageJSONWithOptions(msg proto.Message, options MarshalOptions) ([]byte, error) {
	value, err := messageJSONValueWithOptions(msg.ProtoReflect(), options)
	if err != nil {
		return nil, err
	}
	raw, err := stdjson.Marshal(value)
	if err != nil || !options.Multiline {
		return raw, err
	}
	var formatted bytes.Buffer
	if err := stdjson.Indent(&formatted, raw, "", options.Indent); err != nil {
		return nil, err
	}
	return formatted.Bytes(), nil
}

func messageJSONValue(msg protoreflect.Message) (map[string]any, error) {
	return messageJSONValueWithOptions(msg, MarshalOptions{})
}

func messageJSONValueWithOptions(msg protoreflect.Message, options MarshalOptions) (map[string]any, error) {
	if !msg.IsValid() {
		return nil, nil
	}
	out := make(map[string]any)
	var err error
	msg.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		out[field.JSONName()], err = fieldJSONValue(field, value)
		return err == nil
	})
	if options.EmitUnpopulated {
		fields := msg.Descriptor().Fields()
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)
			if _, ok := out[field.JSONName()]; ok || field.HasPresence() {
				continue
			}
			out[field.JSONName()] = defaultJSONValue(field)
		}
	}
	return out, err
}

func defaultJSONValue(field protoreflect.FieldDescriptor) any {
	if field.IsList() {
		return []any{}
	}
	if field.IsMap() {
		return map[string]any{}
	}
	if field.Message() != nil {
		return nil
	}
	if field.Kind() == protoreflect.EnumKind {
		if enum := field.Enum().Values().ByNumber(field.DefaultEnumValue().Number()); enum != nil {
			return string(enum.Name())
		}
	}
	return field.Default().Interface()
}

func fieldJSONValue(field protoreflect.FieldDescriptor, value protoreflect.Value) (any, error) {
	if field.IsMap() {
		out := make(map[string]any)
		var mapErr error
		value.Map().Range(func(key protoreflect.MapKey, item protoreflect.Value) bool {
			converted, err := scalarJSONValue(field.MapValue(), item)
			if err != nil {
				mapErr = err
				return false
			}
			out[mapKeyString(key)] = converted
			return true
		})
		return out, mapErr
	}
	if field.IsList() {
		values := make([]any, value.List().Len())
		for i := range values {
			converted, err := scalarJSONValue(field, value.List().Get(i))
			if err != nil {
				return nil, err
			}
			values[i] = converted
		}
		return values, nil
	}
	return scalarJSONValue(field, value)
}

func scalarJSONValue(field protoreflect.FieldDescriptor, value protoreflect.Value) (any, error) {
	if field.Message() != nil {
		if marshaler, ok := value.Message().Interface().(interface{ MarshalJSON() ([]byte, error) }); ok {
			raw, err := marshaler.MarshalJSON()
			if err != nil {
				return nil, err
			}
			var nested any
			if err := stdjson.Unmarshal(raw, &nested); err != nil {
				return nil, err
			}
			return nested, nil
		}
		if field.Message().FullName() == "google.protobuf.Timestamp" {
			seconds := value.Message().Get(field.Message().Fields().ByName("seconds")).Int()
			nanos := value.Message().Get(field.Message().Fields().ByName("nanos")).Int()
			return time.Unix(seconds, nanos).UTC().Format(time.RFC3339Nano), nil
		}
		return messageJSONValue(value.Message())
	}
	switch field.Kind() {
	case protoreflect.BoolKind:
		return value.Bool(), nil
	case protoreflect.StringKind:
		if !utf8.ValidString(value.String()) {
			return nil, fmt.Errorf("invalid UTF-8 in protobuf string field %s", field.FullName())
		}
		return value.String(), nil
	case protoreflect.BytesKind:
		return value.Bytes(), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return value.Int(), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return value.Uint(), nil
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return value.Float(), nil
	case protoreflect.EnumKind:
		enum := field.Enum().Values().ByNumber(value.Enum())
		if enum == nil {
			return int32(value.Enum()), nil
		}
		return string(enum.Name()), nil
	default:
		return nil, nil
	}
}

func mapKeyString(key protoreflect.MapKey) string {
	switch value := key.Interface().(type) {
	case string:
		return value
	case bool:
		return strconv.FormatBool(value)
	case uint32:
		return strconv.FormatUint(uint64(value), 10)
	case uint64:
		return strconv.FormatUint(value, 10)
	case int32:
		return strconv.FormatInt(int64(value), 10)
	case int64:
		return strconv.FormatInt(value, 10)
	default:
		return fmt.Sprint(value)
	}
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
