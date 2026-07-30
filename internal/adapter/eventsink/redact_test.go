package eventsink

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/pkg/redact"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestRedactGetResponse_RemovesEverySinkCredential(t *testing.T) {
	t.Parallel()

	secrets := []string{
		"clickhouse-password",
		"kafka-password",
		"webhook-secret",
		"dapi-pat-plaintext",
		"oauth-secret-plaintext",
		"nats-password",
		"nats-token",
	}
	response := &servicepb.GetEventsSinksResponse{
		Sinks: []*commonpb.SinkConfig{
			{
				Name: "clickhouse",
				Type: &commonpb.SinkConfig_Clickhouse{Clickhouse: &commonpb.ClickHouseSinkConfig{
					Dsn: "clickhouse://operator:" + secrets[0] + "@example.com:9000/ledger",
				}},
			},
			{
				Name: "kafka",
				Type: &commonpb.SinkConfig_Kafka{Kafka: &commonpb.KafkaSinkConfig{
					Brokers:      []string{"broker:9092"},
					SaslUsername: "operator",
					SaslPassword: secrets[1],
				}},
			},
			{
				Name: "webhook",
				Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{
					Endpoint: "https://example.com/events",
					Secret:   secrets[2],
				}},
			},
			{
				Name: "nats",
				Type: &commonpb.SinkConfig_Nats{Nats: &commonpb.NatsSinkConfig{
					Url: "nats://operator:" + secrets[5] + "@one.example:4222, nats://" + secrets[6] + "@two.example:4222,nats://three.example:4222",
				}},
			},
			{
				Name: "databricks-pat",
				Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{
					ServerHostname: "adb.example.com",
					Auth:           &commonpb.DatabricksSinkConfig_Token{Token: secrets[3]},
				}},
			},
			{
				Name: "databricks-oauth",
				Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{
					ServerHostname: "adb.example.com",
					Auth: &commonpb.DatabricksSinkConfig_OauthM2M{OauthM2M: &commonpb.DatabricksOAuthM2M{
						ClientId:     "operator-client",
						ClientSecret: secrets[4],
					}},
				}},
			},
		},
		SinkStatuses: []*commonpb.SinkStatus{{SinkName: "kafka", Cursor: 42}},
	}

	redacted := RedactGetResponse(response)
	rendered, err := protojson.Marshal(redacted)
	require.NoError(t, err)

	for _, secret := range secrets {
		assert.NotContains(t, string(rendered), secret)
	}
	assert.Equal(t, "clickhouse://operator:****@example.com:9000/ledger", redacted.GetSinks()[0].GetClickhouse().GetDsn())
	assert.Equal(t, redact.SecretSet, redacted.GetSinks()[1].GetKafka().GetSaslPassword())
	assert.Equal(t, redact.SecretSet, redacted.GetSinks()[2].GetHttp().GetSecret())
	assert.Equal(t, "nats://operator:(set)@one.example:4222, nats://(set)@two.example:4222,nats://three.example:4222", redacted.GetSinks()[3].GetNats().GetUrl())
	assert.Equal(t, redact.SecretSet, redacted.GetSinks()[4].GetDatabricks().GetToken())
	assert.Equal(t, redact.SecretSet, redacted.GetSinks()[5].GetDatabricks().GetOauthM2M().GetClientSecret())
	assert.Equal(t, "operator-client", redacted.GetSinks()[5].GetDatabricks().GetOauthM2M().GetClientId())
	assert.Equal(t, uint64(42), redacted.GetSinkStatuses()[0].GetCursor())

	// Read-side projection must not corrupt the controller-owned runtime config.
	assert.Contains(t, response.GetSinks()[0].GetClickhouse().GetDsn(), secrets[0])
	assert.Equal(t, secrets[1], response.GetSinks()[1].GetKafka().GetSaslPassword())
	assert.Equal(t, secrets[2], response.GetSinks()[2].GetHttp().GetSecret())
	assert.Contains(t, response.GetSinks()[3].GetNats().GetUrl(), secrets[5])
	assert.Contains(t, response.GetSinks()[3].GetNats().GetUrl(), secrets[6])
	assert.Equal(t, secrets[3], response.GetSinks()[4].GetDatabricks().GetToken())
	assert.Equal(t, secrets[4], response.GetSinks()[5].GetDatabricks().GetOauthM2M().GetClientSecret())
}

func TestRedactConfig_ReportsAbsentSecretWithoutMutatingInput(t *testing.T) {
	t.Parallel()

	config := &commonpb.SinkConfig{
		Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{
			Endpoint: "https://example.com/events",
		}},
	}

	redacted := RedactConfig(config)

	assert.Equal(t, redact.SecretNone, redacted.GetHttp().GetSecret())
	assert.Empty(t, config.GetHttp().GetSecret())
	assert.Nil(t, RedactConfig(nil))
	assert.Nil(t, RedactGetResponse(nil))
}
