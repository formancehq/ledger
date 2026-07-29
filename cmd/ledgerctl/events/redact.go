package events

import (
	"github.com/formancehq/ledger/v3/internal/adapter/eventsink"
	"github.com/formancehq/ledger/v3/internal/pkg/redact"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Markers used when redacting secrets in structured (--json / --yaml /
// --result-file) and table output. Sentinels are intentionally distinguishable
// from any plausible secret value so downstream readers cannot confuse them
// with real data.
const (
	secretSet  = redact.SecretSet
	secretNone = redact.SecretNone
)

// redactSecret returns secretSet if s is non-empty, secretNone otherwise.
// Use for opaque secrets where preserving any byte of the original value would
// be a leak (PATs, OAuth client secrets, SASL passwords, HMAC keys).
func redactSecret(s string) string {
	return redact.Secret(s)
}

// redactSinkConfig returns a deep clone of cfg with every secret-bearing
// field replaced by a sentinel. Safe to hand to EncodeStructured / printf
// without leaking PATs, OAuth client secrets, SASL passwords, HMAC keys, or
// DSN passwords.
func redactSinkConfig(cfg *commonpb.SinkConfig) *commonpb.SinkConfig {
	return eventsink.RedactConfig(cfg)
}

// redactGetEventsSinksResponse returns a deep clone of resp with every sink
// config redacted. Sink statuses are cloned but otherwise untouched (they do
// not carry secrets).
func redactGetEventsSinksResponse(resp *servicepb.GetEventsSinksResponse) *servicepb.GetEventsSinksResponse {
	return eventsink.RedactGetResponse(resp)
}
