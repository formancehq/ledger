// Package readprojection builds credential-safe public response copies without
// changing the authoritative configurations or audit records held by callers.
package readprojection

import (
	"net/url"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

const (
	redactedSecret     = "[redacted]"
	redactedDiagnostic = "[redacted diagnostic]"
)

// Sink returns a deep copy of a sink with reusable credentials redacted.
func Sink(config *commonpb.SinkConfig) *commonpb.SinkConfig {
	out := config.CloneVT()
	redactSink(out)

	return out
}

// Mirror returns a deep copy of a mirror source with credentials redacted.
func Mirror(config *commonpb.MirrorSourceConfig) *commonpb.MirrorSourceConfig {
	out := config.CloneVT()
	redactMirror(out)

	return out
}

// Ledger preserves ledger metadata and mirror progress, while redacting source
// credentials and the free-form mirror diagnostic, which can echo credentials.
func Ledger(info *commonpb.LedgerInfo) *commonpb.LedgerInfo {
	out := info.CloneVT()
	if out == nil {
		return nil
	}
	redactMirror(out.GetMirrorSource())
	if progress := out.GetMirrorSyncProgress(); progress != nil && progress.GetError() != nil {
		progress.Error.Message = redactedDiagnostic
	}

	return out
}

// SinkStatus preserves operational status, timestamps and cursor, while hiding
// the free-form driver diagnostic, which can echo credentials from any config.
func SinkStatus(status *commonpb.SinkStatus) *commonpb.SinkStatus {
	out := status.CloneVT()
	if out != nil && out.GetError() != nil {
		out.Error.Message = redactedDiagnostic
	}

	return out
}

// redactSink only mutates the supplied projection. Its result says whether any
// bytes changed, so an audit projection can retain untouched serialized orders.
func redactSink(config *commonpb.SinkConfig) bool {
	if config == nil {
		return false
	}
	changed := false
	switch source := config.GetType().(type) {
	case *commonpb.SinkConfig_Nats:
		if source.Nats != nil {
			changed = replace(&source.Nats.Url, redactNATS(source.Nats.GetUrl()))
		}
	case *commonpb.SinkConfig_Clickhouse:
		if source.Clickhouse != nil {
			changed = replace(&source.Clickhouse.Dsn, redactURL(source.Clickhouse.GetDsn(), urlClickHouse))
		}
	case *commonpb.SinkConfig_Kafka:
		if source.Kafka != nil {
			changed = mask(&source.Kafka.SaslPassword)
		}
	case *commonpb.SinkConfig_Http:
		if source.Http != nil {
			changed = mask(&source.Http.Secret)
			changed = replace(&source.Http.Endpoint, redactURL(source.Http.GetEndpoint(), urlHTTP)) || changed
		}
	case *commonpb.SinkConfig_Databricks:
		if source.Databricks != nil {
			switch auth := source.Databricks.GetAuth().(type) {
			case *commonpb.DatabricksSinkConfig_Token:
				changed = mask(&auth.Token)
			case *commonpb.DatabricksSinkConfig_OauthM2M:
				if auth.OauthM2M != nil {
					changed = mask(&auth.OauthM2M.ClientSecret)
				}
			}
		}
	}

	return changed
}

func redactMirror(config *commonpb.MirrorSourceConfig) bool {
	if config == nil {
		return false
	}
	changed := false
	switch source := config.GetType().(type) {
	case *commonpb.MirrorSourceConfig_Http:
		if source.Http != nil {
			changed = replace(&source.Http.BaseUrl, redactURL(source.Http.GetBaseUrl(), urlHTTP))
			if auth := source.Http.GetOauth2ClientCredentials(); auth != nil {
				changed = mask(&auth.ClientSecret) || changed
				changed = replace(&auth.TokenEndpoint, redactURL(auth.GetTokenEndpoint(), urlHTTP)) || changed
			}
		}
	case *commonpb.MirrorSourceConfig_Postgres:
		if source.Postgres != nil {
			dsn := source.Postgres.GetDsn()
			if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
				dsn = redactURL(dsn, urlPostgres)
			} else {
				dsn = redactPostgresKeywords(dsn)
			}
			changed = replace(&source.Postgres.Dsn, dsn)
		}
	}

	return changed
}

func replace(target *string, value string) bool {
	if *target == value {
		return false
	}
	*target = value

	return true
}

func mask(target *string) bool {
	if *target == "" {
		return false
	}

	return replace(target, redactedSecret)
}

type urlKind uint8

const (
	urlHTTP urlKind = iota
	urlNATS
	urlClickHouse
	urlPostgres
	urlProxy
)

func (kind urlKind) accepts(scheme string) bool {
	switch kind {
	case urlHTTP:
		return scheme == "http" || scheme == "https"
	case urlProxy:
		return scheme == "http" || scheme == "https" || scheme == "socks5" || scheme == "socks5h"
	case urlNATS:
		return scheme == "nats" || scheme == "tls" || scheme == "ws" || scheme == "wss"
	case urlClickHouse:
		return scheme == "clickhouse" || scheme == "http" || scheme == "https"
	case urlPostgres:
		return scheme == "postgres" || scheme == "postgresql"
	default:
		return false
	}
}

// redactURL fails closed on unparseable or unsupported addresses. HTTP query
// values are opaque authentication material, regardless of parameter name.
// Database parameters retain operational settings, with known credential fields
// masked. Only changed URLs are re-encoded; untouched inputs remain byte-exact.
func redactURL(raw string, kind urlKind) string {
	if raw == "" || raw == redactedSecret {
		return raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Host == "" && kind != urlPostgres) || parsed.Opaque != "" || !kind.accepts(parsed.Scheme) {
		return redactedSecret
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return redactedSecret
	}
	changed := false
	if parsed.User != nil {
		password, hasPassword := parsed.User.Password()
		if kind == urlHTTP || kind == urlNATS || kind == urlProxy {
			// NATS also treats the username-only form as a bearer token.
			if parsed.User.Username() != "" || password != "" {
				if parsed.User.Username() != redactedSecret || hasPassword {
					parsed.User = url.User(redactedSecret)
					changed = true
				}
			}
		} else if password != "" && password != redactedSecret {
			parsed.User = url.UserPassword(parsed.User.Username(), redactedSecret)
			changed = true
		}
	}
	queryChanged := false
	for key, values := range query {
		for i, value := range values {
			replacement := value
			switch {
			case kind == urlHTTP || kind == urlNATS || kind == urlProxy || strings.EqualFold(key, "password") || strings.EqualFold(key, "sslpassword"):
				if value != "" {
					replacement = redactedSecret
				}
			case kind == urlClickHouse && key == "http_proxy":
				replacement = redactURL(value, urlProxy)
			}
			if replacement != value {
				values[i] = replacement
				queryChanged = true
			}
		}
	}
	if queryChanged {
		parsed.RawQuery = query.Encode()
		changed = true
	}
	if parsed.Fragment != "" && parsed.Fragment != redactedSecret {
		parsed.Fragment = redactedSecret
		parsed.RawFragment = ""
		changed = true
	}
	if !changed {
		return raw
	}

	return parsed.String()
}

func redactNATS(raw string) string {
	if raw == "" || raw == redactedSecret {
		return raw
	}
	servers := strings.Split(raw, ",")
	for i, server := range servers {
		address := strings.TrimSpace(server)
		if address == "" {
			return redactedSecret
		}
		hasScheme := strings.Contains(address, "://")
		normalized := address
		if !hasScheme {
			normalized = "nats://" + address
		}
		projected := redactURL(normalized, urlNATS)
		if projected == redactedSecret {
			return redactedSecret
		}
		if projected == normalized {
			continue
		}
		if !hasScheme {
			projected = strings.TrimPrefix(projected, "nats://")
		}
		servers[i] = strings.Replace(server, address, projected, 1)
	}

	return strings.Join(servers, ",")
}

// redactPostgresKeywords follows the pinned pgx keyword/value lexical rules
// (single quotes, backslash escapes and ASCII whitespace), without calling
// ParseConfig: that API reads environment, service files and TLS files. Preserve
// every non-credential substring verbatim; ambiguous/malformed input is hidden.
func redactPostgresKeywords(raw string) string {
	if raw == "" || raw == redactedSecret {
		return raw
	}
	var out strings.Builder
	written := 0
	for position := 0; position < len(raw); {
		for position < len(raw) && isKeywordSpace(raw[position]) {
			position++
		}
		if position == len(raw) {
			break
		}
		relativeEqual := strings.IndexByte(raw[position:], '=')
		if relativeEqual < 0 {
			return redactedSecret
		}
		equal := position + relativeEqual
		key := strings.Trim(raw[position:equal], " \t\n\r\v\f")
		if key == "" || strings.ContainsAny(key, " \t\n\r\v\f'\\:/") {
			return redactedSecret
		}
		start := equal + 1
		for start < len(raw) && isKeywordSpace(raw[start]) {
			start++
		}
		end, content, ok := keywordValue(raw, start)
		if !ok {
			return redactedSecret
		}
		if (strings.EqualFold(key, "password") || strings.EqualFold(key, "sslpassword")) && content != "" && content != redactedSecret {
			out.WriteString(raw[written:start])
			if raw[start] == '\'' {
				out.WriteByte('\'')
				out.WriteString(redactedSecret)
				out.WriteByte('\'')
			} else {
				out.WriteString(redactedSecret)
			}
			written = end
		}
		position = end
		if position < len(raw) && isKeywordSpace(raw[position]) {
			position++ // pgx consumes one delimiter after an unquoted value.
		}
	}
	if written == 0 {
		return raw
	}
	out.WriteString(raw[written:])

	return out.String()
}

func keywordValue(raw string, start int) (end int, content string, ok bool) {
	if start == len(raw) {
		return start, "", true
	}
	quoted := raw[start] == '\''
	position := start
	if quoted {
		position++
	}
	var value strings.Builder
	for position < len(raw) {
		character := raw[position]
		if quoted && character == '\'' {
			return position + 1, value.String(), true
		}
		if !quoted && isKeywordSpace(character) {
			return position, value.String(), true
		}
		if character == '\\' {
			position++
			if position == len(raw) {
				return 0, "", false
			}
			character = raw[position]
		}
		value.WriteByte(character)
		position++
	}

	return position, value.String(), !quoted
}

func isKeywordSpace(character byte) bool {
	return character == ' ' || character == '\t' || character == '\n' || character == '\r' || character == '\v' || character == '\f'
}
