package http

import (
	"cmp"
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// This file holds the HTTP response DTOs for GET /_/events-sinks. See
// dto_indexes.go for the general rationale (this package owns its wire format
// rather than deriving it from the .proto descriptor via protojson) and for the
// `xxxDTO` / `newXxxDTO` naming convention.
//
// SECURITY RULE — five secret-bearing proto fields have NO DTO field at all:
//
//	KafkaSinkConfig.sasl_password          Kafka SASL password
//	HttpSinkConfig.secret                  HMAC-SHA256 webhook signing secret
//	ClickHouseSinkConfig.dsn               embeds credentials (clickhouse://user:pass@…)
//	DatabricksSinkConfig.token             Databricks personal access token
//	DatabricksOAuthM2M.client_secret       OAuth M2M client secret
//
// Do NOT add a field for any of them — not a masked value, not a sentinel, not
// one with omitempty. A field that does not exist cannot be populated by a
// later edit, which is a stronger guarantee than redacting at conversion time.
// Until EN-1791 this route marshalled the raw SinkConfig with protojson and
// shipped all five in plaintext to any caller holding ledger:OpsRead; the only
// redaction in the tree was cmd/ledgerctl/events/redact.go, i.e. client-side
// display logic applied after the secrets had already crossed the wire.
// dto_sinks_test.go and handlers_get_events_sinks_test.go both assert absence.
//
// NOT a claim that no credential can reach this route. Two URL fields are
// returned verbatim, userinfo included:
//
//	NatsSinkConfig.url        nats://user:pass@host:4222
//	HttpSinkConfig.endpoint   https://user:pass@host/hook
//
// NatsSinkConfig has no credential field at all — nats.Connect takes the user
// and token from the URL userinfo (sink_nats.go), so that IS the NATS auth
// channel, and there is no alternative to prefer. HttpSinkConfig is weaker:
// `secret` signs the payload to the receiver rather than authenticating this
// client, so endpoint userinfo also reaches the wire, but the signed-payload
// pattern is the primary mechanism there. Nothing validates or strips userinfo
// on admission (no url.Parse in internal/application/events/) and
// cmd/ledgerctl/events/redact.go has no Nats case, so the CLI does not mask it
// either. openapi.yml states this on both fields; do not shorten it back to
// "every credential-bearing setting is omitted", which is false for NATS.
//
// The gRPC surface still returns these fields and is deliberately out of scope
// here; it is tracked separately.

type natsSinkDTO struct {
	URL   string `json:"url"`
	Topic string `json:"topic"`
}

type clickhouseSinkDTO struct {
	Table string `json:"table"`
	// dsn intentionally absent: it embeds credentials (clickhouse://user:pass@…).
}

type kafkaSinkDTO struct {
	// Allocated by the converter so it marshals as [] rather than null: the
	// proto getter returns a nil slice for a sink with no configured broker.
	// No omitempty, so [] is what the OpenAPI `required` membership promises.
	Brokers []string `json:"brokers"`
	Topic   string   `json:"topic"`
	// No omitempty: tls=false is a real setting, not an absent one.
	TLS           bool   `json:"tls"`
	SaslMechanism string `json:"saslMechanism"`
	SaslUsername  string `json:"saslUsername"`
	// saslPassword intentionally absent.
}

type httpSinkDTO struct {
	Endpoint string `json:"endpoint"`
	// secret intentionally absent: HMAC signing secret.
}

type databricksOAuthM2MDTO struct {
	ClientID string `json:"clientId"`
	// clientSecret intentionally absent.
}

type databricksSinkDTO struct {
	ServerHostname string `json:"serverHostname"`
	HTTPPath       string `json:"httpPath"`
	Catalog        string `json:"catalog"`
	Schema         string `json:"schema"`
	Table          string `json:"table"`
	Port           int32  `json:"port"`
	// AuthMethod is "token" or "oauthM2m". It has no proto counterpart: the PAT
	// variant is entirely secret, so without this discriminator omitting the
	// token would erase the fact that token auth is configured at all.
	AuthMethod string                 `json:"authMethod"`
	OauthM2M   *databricksOAuthM2MDTO `json:"oauthM2m,omitempty"`
	// token intentionally absent.
}

type sinkConfigDTO struct {
	Name string `json:"name"`
	// Exactly one of these is non-nil (the SinkConfig.type oneof).
	Nats       *natsSinkDTO       `json:"nats,omitempty"`
	Clickhouse *clickhouseSinkDTO `json:"clickhouse,omitempty"`
	Kafka      *kafkaSinkDTO      `json:"kafka,omitempty"`
	Http       *httpSinkDTO       `json:"http,omitempty"`
	Databricks *databricksSinkDTO `json:"databricks,omitempty"`
	Format     string             `json:"format"`
	BatchSize  int32              `json:"batchSize"`
	// int64 in the proto, not uint64. Emitted unquoted.
	BatchDelayMs int64 `json:"batchDelayMs"`
	// Allocated by the converter: [] means "all event types", never null.
	EventTypes []string `json:"eventTypes"`
}

type sinkErrorDTO struct {
	Message    string  `json:"message"`
	OccurredAt *string `json:"occurredAt,omitempty"`
}

type sinkStatusDTO struct {
	SinkName string `json:"sinkName"`
	// No omitempty: cursor 0 means nothing emitted yet, a real state.
	Cursor uint64        `json:"cursor"`
	Error  *sinkErrorDTO `json:"error,omitempty"` // nil = healthy
}

type eventsSinksDTO struct {
	Sinks        []sinkConfigDTO `json:"sinks"`
	SinkStatuses []sinkStatusDTO `json:"sinkStatuses"`
}

// newDatabricksSinkDTO hand-dispatches the nested DatabricksSinkConfig.auth
// oneof. Neither secret variant is carried: the PAT is dropped entirely and the
// OAuth branch keeps only clientId, so authMethod is what tells the caller which
// credential is configured.
func newDatabricksSinkDTO(cfg *commonpb.DatabricksSinkConfig) *databricksSinkDTO {
	if cfg == nil {
		return nil
	}

	dto := &databricksSinkDTO{
		ServerHostname: cfg.GetServerHostname(),
		HTTPPath:       cfg.GetHttpPath(),
		Catalog:        cfg.GetCatalog(),
		Schema:         cfg.GetSchema(),
		Table:          cfg.GetTable(),
		Port:           cfg.GetPort(),
	}

	switch a := cfg.GetAuth().(type) {
	case *commonpb.DatabricksSinkConfig_Token:
		dto.AuthMethod = "token"
	case *commonpb.DatabricksSinkConfig_OauthM2M:
		dto.AuthMethod = "oauthM2m"
		if a.OauthM2M != nil {
			dto.OauthM2M = &databricksOAuthM2MDTO{ClientID: a.OauthM2M.GetClientId()}
		}
	}

	return dto
}

// newSinkConfigDTO hand-dispatches the SinkConfig.type oneof. There is no
// tag-driven path: the generated wrapper structs carry only protobuf: tags, so
// a reflective marshal would emit {"Type":{"Kafka":{...}}}.
func newSinkConfigDTO(cfg *commonpb.SinkConfig) *sinkConfigDTO {
	if cfg == nil {
		return nil
	}

	eventTypes := make([]string, 0, len(cfg.GetEventTypes()))
	for _, et := range cfg.GetEventTypes() {
		eventTypes = append(eventTypes, et.String())
	}

	dto := &sinkConfigDTO{
		Name:         cfg.GetName(),
		Format:       cfg.GetFormat(),
		BatchSize:    cfg.GetBatchSize(),
		BatchDelayMs: cfg.GetBatchDelayMs(),
		EventTypes:   eventTypes,
	}

	switch t := cfg.GetType().(type) {
	case *commonpb.SinkConfig_Nats:
		if t.Nats != nil {
			dto.Nats = &natsSinkDTO{URL: t.Nats.GetUrl(), Topic: t.Nats.GetTopic()}
		}
	case *commonpb.SinkConfig_Clickhouse:
		if t.Clickhouse != nil {
			dto.Clickhouse = &clickhouseSinkDTO{Table: t.Clickhouse.GetTable()}
		}
	case *commonpb.SinkConfig_Kafka:
		if t.Kafka != nil {
			brokers := make([]string, 0, len(t.Kafka.GetBrokers()))
			brokers = append(brokers, t.Kafka.GetBrokers()...)

			dto.Kafka = &kafkaSinkDTO{
				Brokers:       brokers,
				Topic:         t.Kafka.GetTopic(),
				TLS:           t.Kafka.GetTls(),
				SaslMechanism: t.Kafka.GetSaslMechanism(),
				SaslUsername:  t.Kafka.GetSaslUsername(),
			}
		}
	case *commonpb.SinkConfig_Http:
		if t.Http != nil {
			dto.Http = &httpSinkDTO{Endpoint: t.Http.GetEndpoint()}
		}
	case *commonpb.SinkConfig_Databricks:
		dto.Databricks = newDatabricksSinkDTO(t.Databricks)
	}

	return dto
}

func newSinkStatusDTO(status *commonpb.SinkStatus) *sinkStatusDTO {
	if status == nil {
		return nil
	}

	dto := &sinkStatusDTO{
		SinkName: status.GetSinkName(),
		Cursor:   status.GetCursor(),
	}

	if e := status.GetError(); e != nil {
		dto.Error = &sinkErrorDTO{
			Message:    e.GetMessage(),
			OccurredAt: formatTimestamp(e.GetOccurredAt()),
		}
	}

	return dto
}

// newEventsSinksDTO converts both lists, allocating each so an empty input
// marshals as [] rather than null.
//
// Both arrays are sorted by name before they are returned, so two identical
// requests produce byte-identical bodies. The DTO owns that guarantee outright
// rather than inheriting it from the caller: query.BuildSinkStatuses collects
// its result by ranging over a map, so statuses arrive in Go map-iteration
// order and the array order changed between requests. Configs happen to arrive
// sorted today (query.ReadAllSinkConfigs scans a key prefix), but the wire
// promise is documented in openapi.yml, so it is enforced here instead of
// resting on that upstream detail.
func newEventsSinksDTO(sinks []*commonpb.SinkConfig, statuses []*commonpb.SinkStatus) eventsSinksDTO {
	out := eventsSinksDTO{
		Sinks:        make([]sinkConfigDTO, 0, len(sinks)),
		SinkStatuses: make([]sinkStatusDTO, 0, len(statuses)),
	}

	for _, s := range sinks {
		dto := newSinkConfigDTO(s)
		// A nil element would be a backend bug. Skip rather than deref: this is a
		// read-only ops surface, so degrading the list beats a 500, and nothing can
		// desync from it.
		if dto == nil {
			continue
		}

		out.Sinks = append(out.Sinks, *dto)
	}

	for _, s := range statuses {
		dto := newSinkStatusDTO(s)
		// Same reasoning as above.
		if dto == nil {
			continue
		}

		out.SinkStatuses = append(out.SinkStatuses, *dto)
	}

	slices.SortFunc(out.Sinks, func(a, b sinkConfigDTO) int {
		return cmp.Compare(a.Name, b.Name)
	})
	slices.SortFunc(out.SinkStatuses, func(a, b sinkStatusDTO) int {
		return cmp.Compare(a.SinkName, b.SinkName)
	})

	return out
}
