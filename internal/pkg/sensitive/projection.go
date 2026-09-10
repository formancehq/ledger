// Package sensitive builds detached public views of annotated protobuf messages.
// It must never be used when persisting or signing authoritative records.
package sensitive

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

const Marker = "[redacted]"

// Clone returns a deep copy with sensitive fields masked. Unknown wire fields
// are omitted because their confidentiality cannot be established by the schema.
// The original message, including its exact byte fields, is never modified.
func Clone[T proto.Message](message T) T {
	if any(message) == nil || !message.ProtoReflect().IsValid() {
		return message
	}
	result := proto.Clone(message).(T)
	redact(result.ProtoReflect())

	return result
}

func redact(message protoreflect.Message) {
	message.SetUnknown(nil)
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if proto.GetExtension(field.Options(), commonpb.E_Sensitive).(bool) {
			if field.Kind() == protoreflect.StringKind && !field.IsList() && !field.IsMap() {
				if value.String() != "" {
					message.Set(field, protoreflect.ValueOfString(Marker))
				}
			} else {
				message.Clear(field)
			}

			return true
		}
		switch {
		case field.IsMap():
			if field.MapValue().Message() != nil {
				value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
					redact(item.Message())

					return true
				})
			}
		case field.IsList():
			if field.Message() != nil {
				list := value.List()
				for i := range list.Len() {
					redact(list.Get(i).Message())
				}
			}
		case field.Message() != nil:
			redact(value.Message())
		}

		return true
	})
}
