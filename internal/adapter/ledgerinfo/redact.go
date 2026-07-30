package ledgerinfo

import (
	"net/url"
	"strings"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

const redactedDSNPlaceholder = "[redacted]"

// RedactSecrets returns a deep-cloned LedgerInfo safe for external read
// responses. The stored value must retain the credentials used by mirror
// workers, so redaction belongs at the adapter boundary and never mutates the
// caller-owned message.
func RedactSecrets(info *commonpb.LedgerInfo) *commonpb.LedgerInfo {
	if info == nil {
		return nil
	}

	redacted := info.CloneVT()
	source := redacted.GetMirrorSource()

	if source == nil {
		return redacted
	}

	switch typed := source.GetType().(type) {
	case *commonpb.MirrorSourceConfig_Http:
		if credentials := typed.Http.GetOauth2ClientCredentials(); credentials != nil {
			credentials.ClientSecret = ""
		}
	case *commonpb.MirrorSourceConfig_Postgres:
		if typed.Postgres != nil {
			typed.Postgres.Dsn = redactPostgresDSN(typed.Postgres.GetDsn())
		}
	}

	return redacted
}

// NewRedactingCursor applies RedactSecrets lazily while preserving the wrapped
// cursor's streaming and close semantics.
func NewRedactingCursor(inner cursor.Cursor[*commonpb.LedgerInfo]) cursor.Cursor[*commonpb.LedgerInfo] {
	return &redactingCursor{inner: inner}
}

type redactingCursor struct {
	inner cursor.Cursor[*commonpb.LedgerInfo]
}

func (c *redactingCursor) Next() (*commonpb.LedgerInfo, error) {
	info, err := c.inner.Next()
	if err != nil {
		return nil, err
	}

	return RedactSecrets(info), nil
}

func (c *redactingCursor) Close() error {
	return c.inner.Close()
}

// NextCursor preserves the optional routed-cursor trailer capability used by
// the gRPC pagination layer. A local cursor has no upstream token.
func (c *redactingCursor) NextCursor() string {
	if provider, ok := c.inner.(interface{ NextCursor() string }); ok {
		return provider.NextCursor()
	}

	return ""
}

// redactPostgresDSN removes password-bearing URI components while retaining
// non-secret connection metadata. Unknown/non-URI forms are replaced in full
// so a legacy value cannot bypass the response boundary through parser
// ambiguity. The returned value is a display projection, not a connectable
// DSN; url.URL normalizes query ordering while removing secrets.
func redactPostgresDSN(dsn string) string {
	if dsn == "" {
		return ""
	}

	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
		return redactedDSNPlaceholder
	}

	changed := false

	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			parsed.User = url.User(parsed.User.Username())
			changed = true
		}
	}

	query := parsed.Query()

	for key := range query {
		if strings.Contains(strings.ToLower(key), "password") {
			query.Del(key)
			changed = true
		}
	}

	if !changed {
		return dsn
	}

	parsed.RawQuery = query.Encode()

	return parsed.String()
}
