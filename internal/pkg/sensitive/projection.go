// Package sensitive builds detached public views of annotated protobuf messages.
// It must never be used when persisting or signing authoritative records.
package sensitive

import (
	"net/url"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

const Marker = "[redacted]"

// credentialQueryKeys are query parameter names that commonly carry credentials.
// The lookup is case-folded and dash-normalised on the key before comparison.
var credentialQueryKeys = map[string]bool{
	"password": true, "passwd": true, "secret": true, "token": true,
	"api_key": true, "apikey": true, "access_token": true, "access_key": true,
	"client_secret": true, "authorization": true,
}

// redactURL returns a copy of the URL/DSN string with credentials removed:
// the password in the userinfo component is replaced with "xxxxx", the username
// is redacted for NATS (where the token travels as the username with no password),
// and recognised credential query parameters are set to "xxxxx".
// The host, port, path and non-credential query parameters are preserved so that
// operational context remains visible. If the value cannot be parsed as a URL,
// Marker is returned.
func redactURL(raw string) string {
	if raw == "" {
		return raw
	}
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" {
		return Marker
	}
	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			parsed.User = url.UserPassword(parsed.User.Username(), "xxxxx")
		} else if parsed.Scheme == "nats" && parsed.User.Username() != "" {
			// NATS places a token in the username position without a password.
			parsed.User = url.User("xxxxx")
		}
	}
	if parsed.RawQuery != "" {
		values, qErr := url.ParseQuery(parsed.RawQuery)
		if qErr != nil {
			parsed.RawQuery = "xxxxx"
		} else {
			for key, items := range values {
				norm := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
				if credentialQueryKeys[norm] {
					for i := range items {
						items[i] = "xxxxx"
					}
					values[key] = items
				}
			}
			parsed.RawQuery = values.Encode()
		}
	}

	return parsed.String()
}

// Redact returns a deep copy with sensitive fields masked. Unknown wire fields
// are omitted because their confidentiality cannot be established by the schema.
// The original message, including its exact byte fields, is never modified.
func Redact[T proto.Message](message T) T {
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
		if proto.GetExtension(field.Options(), commonpb.E_SensitiveUrl).(bool) {
			if field.Kind() == protoreflect.StringKind && !field.IsList() && !field.IsMap() {
				if value.String() != "" {
					message.Set(field, protoreflect.ValueOfString(redactURL(value.String())))
				}
			} else {
				message.Clear(field)
			}

			return true
		}
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
					if item := list.Get(i); item.IsValid() {
						redact(item.Message())
					}
				}
			}
		case field.Message() != nil:
			redact(value.Message())
		}

		return true
	})
}
