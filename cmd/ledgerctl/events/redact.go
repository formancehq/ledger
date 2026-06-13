package events

import (
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Markers used when redacting secrets in structured (--json / --yaml /
// --result-file) and table output. Sentinels are intentionally distinguishable
// from any plausible secret value so downstream readers cannot confuse them
// with real data.
const (
	secretSet  = "(set)"
	secretNone = "(none)"
)

// redactSecret returns secretSet if s is non-empty, secretNone otherwise.
// Use for opaque secrets where preserving any byte of the original value would
// be a leak (PATs, OAuth client secrets, SASL passwords, HMAC keys).
func redactSecret(s string) string {
	if s == "" {
		return secretNone
	}

	return secretSet
}

// redactSinkConfigInPlace mutates cfg, replacing every secret-bearing field
// with a sentinel. URL-shaped DSNs go through ObfuscateDSN so the host and
// user remain visible for operator troubleshooting.
//
// Call redactSinkConfig (which clones first) when handing the result to an
// encoder; this in-place variant is for callers that already own a deep copy.
func redactSinkConfigInPlace(cfg *commonpb.SinkConfig) {
	if cfg == nil {
		return
	}

	switch t := cfg.GetType().(type) {
	case *commonpb.SinkConfig_Kafka:
		if t.Kafka != nil {
			t.Kafka.SaslPassword = redactSecret(t.Kafka.GetSaslPassword())
		}
	case *commonpb.SinkConfig_Http:
		if t.Http != nil {
			t.Http.Secret = redactSecret(t.Http.GetSecret())
		}
	case *commonpb.SinkConfig_Clickhouse:
		if t.Clickhouse != nil {
			t.Clickhouse.Dsn = cmdutil.ObfuscateDSN(t.Clickhouse.GetDsn())
		}
	case *commonpb.SinkConfig_Databricks:
		if t.Databricks != nil {
			redactDatabricksAuthInPlace(t.Databricks)
		}
	}
}

// redactDatabricksAuthInPlace mutates d.Auth, masking the PAT or OAuth M2M
// client secret depending on which auth variant is set. Public fields
// (server hostname, HTTP path, catalog, schema, table, OAuth client_id) are
// left visible.
func redactDatabricksAuthInPlace(d *commonpb.DatabricksSinkConfig) {
	switch a := d.GetAuth().(type) {
	case *commonpb.DatabricksSinkConfig_Token:
		a.Token = redactSecret(a.Token)
	case *commonpb.DatabricksSinkConfig_OauthM2M:
		if a.OauthM2M != nil {
			a.OauthM2M.ClientSecret = redactSecret(a.OauthM2M.GetClientSecret())
		}
	}
}

// redactSinkConfig returns a deep clone of cfg with every secret-bearing
// field replaced by a sentinel. Safe to hand to EncodeStructured / printf
// without leaking PATs, OAuth client secrets, SASL passwords, HMAC keys, or
// DSN passwords.
func redactSinkConfig(cfg *commonpb.SinkConfig) *commonpb.SinkConfig {
	if cfg == nil {
		return nil
	}

	cloned, _ := proto.Clone(cfg).(*commonpb.SinkConfig)
	redactSinkConfigInPlace(cloned)

	return cloned
}

// redactGetEventsSinksResponse returns a deep clone of resp with every sink
// config redacted. Sink statuses are cloned but otherwise untouched (they do
// not carry secrets).
func redactGetEventsSinksResponse(resp *servicepb.GetEventsSinksResponse) *servicepb.GetEventsSinksResponse {
	if resp == nil {
		return nil
	}

	cloned, _ := proto.Clone(resp).(*servicepb.GetEventsSinksResponse)
	for _, s := range cloned.GetSinks() {
		redactSinkConfigInPlace(s)
	}

	return cloned
}
