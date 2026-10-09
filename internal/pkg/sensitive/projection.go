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
	"password": true, "sslpassword": true, "passwd": true, "secret": true, "token": true,
	"api_key": true, "apikey": true, "access_token": true, "access_key": true,
	"client_secret": true, "authorization": true,
}

// proxyQueryKeys are query parameter names whose values are themselves URLs
// that may carry credentials (e.g. ClickHouse http_proxy, https_proxy).
var proxyQueryKeys = map[string]bool{
	"http_proxy": true, "https_proxy": true,
}

// natsTokenSchemes are URL schemes where the NATS driver treats a bare
// username (no password) as an authentication token.
var natsTokenSchemes = map[string]bool{
	"nats": true, "tls": true, "ws": true, "wss": true,
}

// redactSingleURL redacts credentials from a single URL that is guaranteed
// not to be a comma-separated NATS server list.
func redactSingleURL(raw string) (string, bool) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Host == "" && parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		return "", false
	}
	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			parsed.User = url.UserPassword(parsed.User.Username(), "xxxxx")
		} else if (natsTokenSchemes[parsed.Scheme] || parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.User.Username() != "" {
			// Passwordless HTTP(S) and NATS-family userinfo can contain reusable tokens.
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
				} else if proxyQueryKeys[norm] {
					// The value is itself a URL that may contain credentials.
					// url.ParseQuery already decoded the value; do not call QueryUnescape
					// again or inner encoded delimiters will be misinterpreted.
					for i, v := range items {
						if sanitized, ok := redactSingleURL(v); ok {
							items[i] = sanitized
						} else {
							items[i] = "xxxxx"
						}
					}
					values[key] = items
				}
			}
			parsed.RawQuery = values.Encode()
		}
	}

	return parsed.String(), true
}

// redactURL returns a copy of the URL/DSN string with credentials removed.
// For NATS server lists (comma-separated URLs) each entry is redacted individually.
// If the value cannot be parsed, Marker is returned.
func redactURL(raw string) string {
	if raw == "" {
		return raw
	}
	if !strings.Contains(raw, "://") && strings.Contains(raw, "=") {
		return redactKeywordDSN(raw)
	}
	trimmed := strings.TrimSpace(raw)

	// NATS and its TLS/WebSocket variants support comma-separated server URLs.
	// url.Parse treats everything after the first comma as part of the path, so
	// we must split and redact each server independently. Other drivers (e.g.
	// ClickHouse multi-host DSNs) use commas inside the host component and must
	// be handled by url.Parse as a single URL.
	before, _, ok := strings.Cut(trimmed, "://")
	if ok && strings.Contains(trimmed, ",") {
		scheme := strings.ToLower(before)
		if natsTokenSchemes[scheme] {
			parts := strings.Split(trimmed, ",")
			redacted := make([]string, len(parts))
			for i, part := range parts {
				if s, ok := redactSingleURL(strings.TrimSpace(part)); ok {
					redacted[i] = s
				} else {
					redacted[i] = Marker
				}
			}

			return strings.Join(redacted, ",")
		}
	}

	if s, ok := redactSingleURL(trimmed); ok {
		return s
	}

	return Marker
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
