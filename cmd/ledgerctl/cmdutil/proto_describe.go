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
	Detail   string      // scalar fields as "key=value" on the main line
	MapLines [][2]string // key/value pairs for indented multi-line display
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
		desc.Detail = ExtractScalarFields(innerMsg, verbose)
		desc.MapLines = ExtractStringMapEntries(innerMsg)
	}

	return desc
}

// ExtractScalarFields returns a compact "key=value" representation of all
// populated scalar fields in a protobuf message, recursing into nested messages.
// google.protobuf.Timestamp fields are formatted as RFC3339. Lists, maps, and
// bytes are skipped. When verbose is false, multiline strings are also skipped.
func ExtractScalarFields(msg protoreflect.Message, verbose bool) string {
	var parts []string
	collectScalarFields(msg, verbose, &parts)

	return strings.Join(parts, " ")
}

// ExtractStringMapEntries recursively collects all string-to-string map entries
// from a protobuf message, returning them as key/value pairs.
func ExtractStringMapEntries(msg protoreflect.Message) [][2]string {
	var entries [][2]string
	collectStringMapEntries(msg, &entries)

	return entries
}

func collectScalarFields(msg protoreflect.Message, verbose bool, parts *[]string) {
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.IsList() || fd.IsMap() || fd.Kind() == protoreflect.BytesKind || fd.Kind() == protoreflect.GroupKind {
			return true
		}

		if fd.Kind() == protoreflect.MessageKind {
			innerMsg := v.Message()
			if innerMsg.Descriptor().FullName() == "google.protobuf.Timestamp" {
				seconds := innerMsg.Get(innerMsg.Descriptor().Fields().ByName("seconds")).Int()
				nanos := innerMsg.Get(innerMsg.Descriptor().Fields().ByName("nanos")).Int()
				t := time.Unix(seconds, nanos)
				*parts = append(*parts, fmt.Sprintf("%s=%s", fd.JSONName(), t.Format(time.RFC3339)))

				return true
			}

			collectScalarFields(innerMsg, verbose, parts)

			return true
		}

		s := fmt.Sprintf("%v", v.Interface())
		if fd.Kind() == protoreflect.EnumKind {
			s = string(fd.Enum().Values().ByNumber(v.Enum()).Name())
		}

		if !verbose && strings.Contains(s, "\n") {
			return true
		}

		*parts = append(*parts, fmt.Sprintf("%s=%s", fd.JSONName(), s))

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
