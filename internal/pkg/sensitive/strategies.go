package sensitive

import "google.golang.org/protobuf/reflect/protoreflect"

// redactConnectionField is the complete static format registry. A new
// sensitive_url declaration must opt into a format here; unknown fields are
// fully masked so adding an annotation cannot silently select the wrong parser.
func redactConnectionField(field protoreflect.FullName, raw string) string {
	if raw == "" {
		return raw
	}
	switch field {
	case "common.PostgresMirrorSourceConfig.dsn":
		return redactPostgres(raw)
	case "common.NatsSinkConfig.url":
		return redactNATS(raw)
	case "common.ClickHouseSinkConfig.dsn":
		return redactClickHouse(raw)
	case "common.HttpSinkConfig.endpoint", "common.HttpMirrorSourceConfig.base_url", "common.OAuth2ClientCredentials.token_endpoint":
		if result, ok := redactHTTPURL(raw, 0); ok {
			return result
		}
	}

	return Marker
}
