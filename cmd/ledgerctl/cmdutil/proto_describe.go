package cmdutil

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// OneofDescription holds display info extracted from a protobuf oneof variant.
type OneofDescription struct {
	Type     string
	Detail   string      // compact "key=value key=value" for single-line display
	Fields   [][2]string // individual scalar field pairs for multi-line display
	MapLines [][2]string // key/value pairs from string maps
}

// PrependField inserts a field at the beginning of both Fields and Detail.
func (d *OneofDescription) PrependField(key, value string) {
	d.Fields = append([][2]string{{key, value}}, d.Fields...)

	if d.Detail != "" {
		d.Detail = key + "=" + value + " " + d.Detail
	} else {
		d.Detail = key + "=" + value
	}
}

// DescribeOneofField uses protobuf reflection to extract the active oneof
// variant name, its scalar fields, and string map entries. suffixToTrim is
// removed from the message name (e.g., "Order", "Log").
func DescribeOneofField(msg protoreflect.Message, oneofName protoreflect.Name, suffixToTrim string, verbose bool) OneofDescription {
	od := msg.Descriptor().Oneofs().ByName(oneofName)
	if od == nil {
		return OneofDescription{Type: "Unknown"}
	}

	fd := msg.WhichOneof(od)
	if fd == nil {
		return OneofDescription{Type: "Unknown"}
	}

	typeName := strings.TrimSuffix(string(fd.Message().Name()), suffixToTrim)

	desc := OneofDescription{Type: typeName}
	if fd.Kind() == protoreflect.MessageKind {
		innerMsg := msg.Get(fd).Message()
		desc.Fields = ExtractScalarFieldPairs(innerMsg, verbose)
		desc.Detail = joinFieldPairs(desc.Fields)
		desc.MapLines = ExtractStringMapEntries(innerMsg)
	}

	return desc
}

// ExtractScalarFieldPairs returns all populated scalar fields as key/value pairs,
// recursing into nested messages. google.protobuf.Timestamp fields are formatted
// as RFC3339. Lists, maps, and bytes are skipped. When verbose is false, multiline
// strings are also skipped.
func ExtractScalarFieldPairs(msg protoreflect.Message, verbose bool) [][2]string {
	var pairs [][2]string
	collectScalarFieldPairs(msg, verbose, &pairs)

	return pairs
}

// ExtractScalarFields returns a compact "key=value" representation of all
// populated scalar fields in a protobuf message.
func ExtractScalarFields(msg protoreflect.Message, verbose bool) string {
	return joinFieldPairs(ExtractScalarFieldPairs(msg, verbose))
}

// ExtractStringMapEntries recursively collects all string-to-string map entries
// from a protobuf message, returning them as key/value pairs.
func ExtractStringMapEntries(msg protoreflect.Message) [][2]string {
	var entries [][2]string
	collectStringMapEntries(msg, &entries)

	return entries
}

func joinFieldPairs(pairs [][2]string) string {
	var parts []string
	for _, kv := range pairs {
		parts = append(parts, kv[0]+"="+kv[1])
	}

	return strings.Join(parts, " ")
}

func collectScalarFieldPairs(msg protoreflect.Message, verbose bool, pairs *[][2]string) {
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.IsList() || fd.IsMap() || fd.Kind() == protoreflect.BytesKind || fd.Kind() == protoreflect.GroupKind {
			return true
		}

		if fd.Kind() == protoreflect.MessageKind {
			innerMsg := v.Message()

			switch innerMsg.Descriptor().FullName() {
			case "google.protobuf.Timestamp":
				seconds := innerMsg.Get(innerMsg.Descriptor().Fields().ByName("seconds")).Int()
				nanos := innerMsg.Get(innerMsg.Descriptor().Fields().ByName("nanos")).Int()
				t := time.Unix(seconds, nanos)
				*pairs = append(*pairs, [2]string{fd.JSONName(), t.Format(time.RFC3339)})
			case "common.Timestamp":
				micros := innerMsg.Get(innerMsg.Descriptor().Fields().ByName("data")).Uint()
				t := time.UnixMicro(int64(micros))
				*pairs = append(*pairs, [2]string{fd.JSONName(), t.UTC().Format(time.RFC3339)})
			default:
				collectScalarFieldPairs(innerMsg, verbose, pairs)
			}

			return true
		}

		s := fmt.Sprintf("%v", v.Interface())
		if fd.Kind() == protoreflect.EnumKind {
			s = string(fd.Enum().Values().ByNumber(v.Enum()).Name())
		}

		if !verbose && strings.Contains(s, "\n") {
			return true
		}

		*pairs = append(*pairs, [2]string{fd.JSONName(), s})

		return true
	})
}

func collectStringMapEntries(msg protoreflect.Message, entries *[][2]string) {
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.IsMap() && fd.MapKey().Kind() == protoreflect.StringKind && fd.MapValue().Kind() == protoreflect.StringKind {
			v.Map().Range(func(mk protoreflect.MapKey, mv protoreflect.Value) bool {
				*entries = append(*entries, [2]string{mk.String(), mv.String()})

				return true
			})

			return true
		}

		if fd.Kind() == protoreflect.MessageKind && !fd.IsList() && !fd.IsMap() {
			collectStringMapEntries(v.Message(), entries)
		}

		return true
	})
}
