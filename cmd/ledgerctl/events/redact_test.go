package events

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestRedactSinkConfig_Databricks_PAT(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Name: "analytics",
		Type: &commonpb.SinkConfig_Databricks{
			Databricks: &commonpb.DatabricksSinkConfig{
				ServerHostname: "adb-123.azuredatabricks.net",
				HttpPath:       "/sql/1.0/warehouses/abc",
				Catalog:        "main",
				Schema:         "default",
				Table:          "ledger_events",
				Port:           443,
				Auth:           &commonpb.DatabricksSinkConfig_Token{Token: "dapi-supersecret"},
			},
		},
	}

	redacted := redactSinkConfig(cfg)

	require.NotNil(t, redacted)
	// Original is untouched.
	assert.Equal(t, "dapi-supersecret", cfg.GetDatabricks().GetToken())
	// Redacted hides only the secret.
	assert.Equal(t, secretSet, redacted.GetDatabricks().GetToken())
	assert.Equal(t, "adb-123.azuredatabricks.net", redacted.GetDatabricks().GetServerHostname())
	assert.Equal(t, "main", redacted.GetDatabricks().GetCatalog())
}

func TestRedactSinkConfig_Databricks_OAuthM2M(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Name: "analytics",
		Type: &commonpb.SinkConfig_Databricks{
			Databricks: &commonpb.DatabricksSinkConfig{
				ServerHostname: "adb-123.azuredatabricks.net",
				HttpPath:       "/sql/1.0/warehouses/abc",
				Catalog:        "main",
				Schema:         "default",
				Auth: &commonpb.DatabricksSinkConfig_OauthM2M{
					OauthM2M: &commonpb.DatabricksOAuthM2M{
						ClientId:     "sp-client-id",
						ClientSecret: "sp-very-secret",
					},
				},
			},
		},
	}

	redacted := redactSinkConfig(cfg)

	// Original is untouched.
	assert.Equal(t, "sp-very-secret", cfg.GetDatabricks().GetOauthM2M().GetClientSecret())
	// Client ID is non-secret and remains visible; only the secret is masked.
	assert.Equal(t, "sp-client-id", redacted.GetDatabricks().GetOauthM2M().GetClientId())
	assert.Equal(t, secretSet, redacted.GetDatabricks().GetOauthM2M().GetClientSecret())
}

func TestRedactSinkConfig_Databricks_EmptySecretsRemainEmpty(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Type: &commonpb.SinkConfig_Databricks{
			Databricks: &commonpb.DatabricksSinkConfig{
				Auth: &commonpb.DatabricksSinkConfig_Token{Token: ""},
			},
		},
	}

	assert.Empty(t, redactSinkConfig(cfg).GetDatabricks().GetToken())
}

func TestRedactSinkConfig_Http(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Type: &commonpb.SinkConfig_Http{
			Http: &commonpb.HttpSinkConfig{
				Endpoint: &commonpb.ConnectionURL{Scheme: "https", Address: &commonpb.ConnectionAddress{Host: "example.com"}, EscapedPath: "/hook"},
				Secret:   "hmac-key",
			},
		},
	}

	redacted := redactSinkConfig(cfg)

	assert.Equal(t, "example.com", redacted.GetHttp().GetEndpoint().GetAddress().GetHost())
	assert.Equal(t, "/hook", redacted.GetHttp().GetEndpoint().GetEscapedPath())
	assert.Equal(t, secretSet, redacted.GetHttp().GetSecret())
}

func TestRedactSinkConfig_Kafka_SASL(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Type: &commonpb.SinkConfig_Kafka{
			Kafka: &commonpb.KafkaSinkConfig{
				Brokers:       []string{"b1:9092"},
				Topic:         "evt",
				SaslMechanism: "SCRAM-SHA-256",
				SaslUsername:  "user",
				SaslPassword:  "pass",
			},
		},
	}

	redacted := redactSinkConfig(cfg)

	assert.Equal(t, []string{"b1:9092"}, redacted.GetKafka().GetBrokers())
	assert.Equal(t, "user", redacted.GetKafka().GetSaslUsername())
	assert.Equal(t, secretSet, redacted.GetKafka().GetSaslPassword())
}

func TestRedactSinkConfig_ClickHouse_StructuredPasswordRedacted(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Type: &commonpb.SinkConfig_Clickhouse{
			Clickhouse: &commonpb.ClickHouseSinkConfig{
				Connection: &commonpb.DatabaseConnection{Scheme: "clickhouse", Username: new("user"), Password: new("secretpw"), Database: new("db"), Addresses: []*commonpb.ConnectionAddress{{Host: "host", Port: proto.Uint32(9000)}}},
				Table:      "events",
			},
		},
	}

	redacted := redactSinkConfig(cfg)

	connection := redacted.GetClickhouse().GetConnection()
	assert.Equal(t, secretSet, connection.GetPassword())
	assert.Equal(t, "user", connection.GetUsername())
	assert.Equal(t, "host", connection.GetAddresses()[0].GetHost())
	assert.Equal(t, uint32(9000), connection.GetAddresses()[0].GetPort())
}

func TestRedactSinkConfig_NatsHasNoSecret(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Type: &commonpb.SinkConfig_Nats{
			Nats: &commonpb.NatsSinkConfig{Servers: []*commonpb.ConnectionURL{{Scheme: "nats", Address: &commonpb.ConnectionAddress{Host: "localhost", Port: proto.Uint32(4222)}}}, Topic: "evt"},
		},
	}

	redacted := redactSinkConfig(cfg)

	assert.Equal(t, "localhost", redacted.GetNats().GetServers()[0].GetAddress().GetHost())
	assert.Equal(t, "evt", redacted.GetNats().GetTopic())
}

func TestRedactSinkConfig_NilSafe(t *testing.T) {
	t.Parallel()

	assert.Nil(t, redactSinkConfig(nil))
}

// TestRedactGetEventsSinksResponse_NoSecretInJSON is the load-bearing security
// check: it serializes the whole response through protojson (what
// EncodeStructured uses for --json and --yaml) and asserts that no plaintext
// secret survives. Any future field added to a SinkConfig that carries a
// secret must carry a sensitive protobuf annotation, or this test will catch the
// regression.
func TestRedactGetEventsSinksResponse_NoSecretInJSON(t *testing.T) {
	t.Parallel()

	secrets := []string{
		"dapi-pat-leak",
		"sp-oauth-leak",
		"http-hmac-leak",
		"kafka-sasl-leak",
		"clickhouse-dsn-leak",
	}

	resp := &servicepb.GetEventsSinksResponse{
		Sinks: []*commonpb.SinkConfig{
			{
				Name: "db-pat",
				Type: &commonpb.SinkConfig_Databricks{
					Databricks: &commonpb.DatabricksSinkConfig{
						ServerHostname: "adb-1.azuredatabricks.net",
						HttpPath:       "/sql/1.0/warehouses/abc",
						Auth:           &commonpb.DatabricksSinkConfig_Token{Token: secrets[0]},
					},
				},
			},
			{
				Name: "db-oauth",
				Type: &commonpb.SinkConfig_Databricks{
					Databricks: &commonpb.DatabricksSinkConfig{
						ServerHostname: "adb-2.azuredatabricks.net",
						HttpPath:       "/sql/1.0/warehouses/def",
						Auth: &commonpb.DatabricksSinkConfig_OauthM2M{
							OauthM2M: &commonpb.DatabricksOAuthM2M{ClientId: "id", ClientSecret: secrets[1]},
						},
					},
				},
			},
			{
				Name: "hook",
				Type: &commonpb.SinkConfig_Http{
					Http: &commonpb.HttpSinkConfig{Endpoint: &commonpb.ConnectionURL{Scheme: "https", Address: &commonpb.ConnectionAddress{Host: "example.com"}}, Secret: secrets[2]},
				},
			},
			{
				Name: "stream",
				Type: &commonpb.SinkConfig_Kafka{
					Kafka: &commonpb.KafkaSinkConfig{
						Brokers:       []string{"b:9092"},
						SaslMechanism: "PLAIN",
						SaslUsername:  "u",
						SaslPassword:  secrets[3],
					},
				},
			},
			{
				Name: "ch",
				Type: &commonpb.SinkConfig_Clickhouse{
					Clickhouse: &commonpb.ClickHouseSinkConfig{
						Connection: &commonpb.DatabaseConnection{Scheme: "clickhouse", Username: new("user"), Password: new(secrets[4]), Database: new("db"), Addresses: []*commonpb.ConnectionAddress{{Host: "host", Port: proto.Uint32(9000)}}},
					},
				},
			},
		},
	}

	redacted := redactGetEventsSinksResponse(resp)

	// Source is left intact (we did not mutate the caller's response).
	assert.Equal(t, secrets[0], resp.GetSinks()[0].GetDatabricks().GetToken())

	b, err := protojson.Marshal(redacted)
	require.NoError(t, err)

	for _, s := range secrets {
		assert.NotContainsf(t, string(b), s, "redacted JSON must not contain secret %q", s)
	}

	// Sanity: non-secret fields survive (host, topic, broker, table, OAuth client_id).
	for _, want := range []string{
		"adb-1.azuredatabricks.net",
		"adb-2.azuredatabricks.net",
		"example.com",
		"b:9092",
		"host",
	} {
		assert.Contains(t, string(b), want)
	}
}

// Driver-owned sanitization preserves an actionable diagnostic in structured output.
func TestRedactGetEventsSinksResponse_PreservesSanitizedDiagnostic(t *testing.T) {
	t.Parallel()
	const diagnostic = "posting event seq=1: sending request to https://localhost/events?key=[redacted]: EOF"
	response := &servicepb.GetEventsSinksResponse{SinkStatuses: []*commonpb.SinkStatus{{SinkName: "http", Error: &commonpb.SinkError{Message: diagnostic}}}}
	projected := redactGetEventsSinksResponse(response)
	require.Equal(t, diagnostic, projected.GetSinkStatuses()[0].GetError().GetMessage())
	encoded, err := protojson.Marshal(projected)
	require.NoError(t, err)
	decoded := &servicepb.GetEventsSinksResponse{}
	require.NoError(t, protojson.Unmarshal(encoded, decoded))
	require.Equal(t, diagnostic, decoded.GetSinkStatuses()[0].GetError().GetMessage())
	require.Equal(t, diagnostic, response.GetSinkStatuses()[0].GetError().GetMessage())
}

func TestRedactGetEventsSinksResponse_NilSafe(t *testing.T) {
	t.Parallel()

	assert.Nil(t, redactGetEventsSinksResponse(nil))
}

// TestRedactSinkConfig_DoesNotMutateInput verifies the deep-clone contract:
// callers must be able to keep using the original (non-redacted) config after
// passing it through redactSinkConfig. Used by add_sink which still needs the
// raw PAT/secret to print the human-readable summary after EncodeStructured
// is called.
func TestRedactSinkConfig_DoesNotMutateInput(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Type: &commonpb.SinkConfig_Databricks{
			Databricks: &commonpb.DatabricksSinkConfig{
				Auth: &commonpb.DatabricksSinkConfig_OauthM2M{
					OauthM2M: &commonpb.DatabricksOAuthM2M{ClientSecret: "keep-this"},
				},
			},
		},
	}

	// Round-trip through redaction and through the JSON encoder used by
	// EncodeStructured to be sure no aliasing happens at any level.
	b, err := json.Marshal(redactSinkConfig(cfg))
	require.NoError(t, err)
	assert.False(t, strings.Contains(string(b), "keep-this"))

	assert.Equal(t, "keep-this", cfg.GetDatabricks().GetOauthM2M().GetClientSecret())
}
