// Package eventsink provides the read-side projection of event-sink
// configuration. Runtime configuration retains the original credentials;
// externally serialized read models expose only redaction markers.
package eventsink

import (
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/redact"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// RedactConfig returns a deep clone of cfg with every credential-bearing field
// replaced by a non-reusable marker. The supplied runtime configuration is not
// mutated.
func RedactConfig(cfg *commonpb.SinkConfig) *commonpb.SinkConfig {
	if cfg == nil {
		return nil
	}

	cloned, _ := proto.Clone(cfg).(*commonpb.SinkConfig)
	redactConfigInPlace(cloned)

	return cloned
}

// RedactGetResponse returns a deep clone of resp whose sink configurations are
// safe to return through read-scoped APIs. Status records are cloned unchanged.
func RedactGetResponse(resp *servicepb.GetEventsSinksResponse) *servicepb.GetEventsSinksResponse {
	if resp == nil {
		return nil
	}

	cloned, _ := proto.Clone(resp).(*servicepb.GetEventsSinksResponse)
	for _, sink := range cloned.GetSinks() {
		redactConfigInPlace(sink)
	}

	return cloned
}

func redactConfigInPlace(cfg *commonpb.SinkConfig) {
	if cfg == nil {
		return
	}

	switch sink := cfg.GetType().(type) {
	case *commonpb.SinkConfig_Clickhouse:
		if sink.Clickhouse != nil {
			sink.Clickhouse.Dsn = redact.DSN(sink.Clickhouse.GetDsn())
		}
	case *commonpb.SinkConfig_Kafka:
		if sink.Kafka != nil {
			sink.Kafka.SaslPassword = redact.Secret(sink.Kafka.GetSaslPassword())
		}
	case *commonpb.SinkConfig_Nats:
		if sink.Nats != nil {
			sink.Nats.Url = redactNATSURLs(sink.Nats.GetUrl())
		}
	case *commonpb.SinkConfig_Http:
		if sink.Http != nil {
			sink.Http.Endpoint = redact.URLUserinfo(sink.Http.GetEndpoint())
			sink.Http.Secret = redact.Secret(sink.Http.GetSecret())
		}
	case *commonpb.SinkConfig_Databricks:
		if sink.Databricks != nil {
			redactDatabricksAuthInPlace(sink.Databricks)
		}
	}
}

// redactNATSURLs removes password and token userinfo from the comma-separated
// server list accepted by the NATS client while preserving server addresses.
func redactNATSURLs(value string) string {
	servers := strings.Split(value, ",")
	for index, server := range servers {
		trimmed := strings.TrimSpace(server)
		leadingSpace := server[:len(server)-len(strings.TrimLeft(server, " \t"))]
		trailingSpace := server[len(strings.TrimRight(server, " \t")):]
		servers[index] = leadingSpace + redact.URLUserinfo(trimmed) + trailingSpace
	}

	return strings.Join(servers, ",")
}

func redactDatabricksAuthInPlace(cfg *commonpb.DatabricksSinkConfig) {
	switch auth := cfg.GetAuth().(type) {
	case *commonpb.DatabricksSinkConfig_Token:
		auth.Token = redact.Secret(auth.Token)
	case *commonpb.DatabricksSinkConfig_OauthM2M:
		if auth.OauthM2M != nil {
			auth.OauthM2M.ClientSecret = redact.Secret(auth.OauthM2M.GetClientSecret())
		}
	}
}
