package events

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

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

func TestRedactSinkConfig_Databricks_EmptySecretsReportedNone(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Type: &commonpb.SinkConfig_Databricks{
			Databricks: &commonpb.DatabricksSinkConfig{
				Auth: &commonpb.DatabricksSinkConfig_Token{Token: ""},
			},
		},
	}

	assert.Equal(t, secretNone, redactSinkConfig(cfg).GetDatabricks().GetToken())
}

func TestRedactSinkConfig_Http(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Type: &commonpb.SinkConfig_Http{
			Http: &commonpb.HttpSinkConfig{
				Endpoint: "https://example.com/hook",
				Secret:   "hmac-key",
			},
		},
	}

	redacted := redactSinkConfig(cfg)

	assert.Equal(t, "https://example.com/hook", redacted.GetHttp().GetEndpoint())
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

func TestRedactSinkConfig_ClickHouse_DSNObfuscated(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Type: &commonpb.SinkConfig_Clickhouse{
			Clickhouse: &commonpb.ClickHouseSinkConfig{
				Dsn:   "clickhouse://user:secretpw@host:9000/db",
				Table: "events",
			},
		},
	}

	redacted := redactSinkConfig(cfg)

	dsn := redacted.GetClickhouse().GetDsn()
	assert.NotContains(t, dsn, "secretpw")
	assert.Contains(t, dsn, "user")
	assert.Contains(t, dsn, "host:9000")
}

func TestRedactSinkConfig_NatsHasNoSecret(t *testing.T) {
	t.Parallel()

	cfg := &commonpb.SinkConfig{
		Type: &commonpb.SinkConfig_Nats{
			Nats: &commonpb.NatsSinkConfig{Url: "nats://localhost:4222", Topic: "evt"},
		},
	}

	redacted := redactSinkConfig(cfg)

	assert.Equal(t, "nats://localhost:4222", redacted.GetNats().GetUrl())
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
// secret must be added to redactSinkConfigInPlace, or this test will catch the
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
					Http: &commonpb.HttpSinkConfig{Endpoint: "https://example.com", Secret: secrets[2]},
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
						Dsn: "clickhouse://user:" + secrets[4] + "@host:9000/db",
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
		"https://example.com",
		"b:9092",
		"host:9000",
	} {
		assert.Contains(t, string(b), want)
	}
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
