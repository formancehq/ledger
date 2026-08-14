package http

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Every secret-bearing field has NO field on the DTO, so it cannot reach the
// wire even by a later mistake. Before EN-1791 this route emitted all five in
// plaintext to any ledger:OpsRead caller; redaction lived only in
// cmd/ledgerctl/events/redact.go, i.e. client-side display logic.
func TestNewSinkConfigDTO_SecretsAreAbsent(t *testing.T) {
	t.Parallel()

	const secret = "SUPERSECRETVALUE"

	cases := []struct {
		name string
		cfg  *commonpb.SinkConfig
	}{
		{
			name: "kafka saslPassword",
			cfg: &commonpb.SinkConfig{Name: "k", Type: &commonpb.SinkConfig_Kafka{
				Kafka: &commonpb.KafkaSinkConfig{
					Brokers: []string{"b:9092"}, Topic: "t",
					SaslUsername: "u", SaslPassword: secret,
				},
			}},
		},
		{
			name: "http secret",
			cfg: &commonpb.SinkConfig{Name: "h", Type: &commonpb.SinkConfig_Http{
				Http: &commonpb.HttpSinkConfig{Endpoint: "https://x", Secret: secret},
			}},
		},
		{
			name: "clickhouse dsn",
			cfg: &commonpb.SinkConfig{Name: "c", Type: &commonpb.SinkConfig_Clickhouse{
				Clickhouse: &commonpb.ClickHouseSinkConfig{
					Dsn: "clickhouse://user:" + secret + "@host:9000", Table: "events",
				},
			}},
		},
		{
			name: "databricks token",
			cfg: &commonpb.SinkConfig{Name: "d", Type: &commonpb.SinkConfig_Databricks{
				Databricks: &commonpb.DatabricksSinkConfig{
					ServerHostname: "adb-1.azuredatabricks.net",
					Auth:           &commonpb.DatabricksSinkConfig_Token{Token: secret},
				},
			}},
		},
		{
			name: "databricks oauth clientSecret",
			cfg: &commonpb.SinkConfig{Name: "d", Type: &commonpb.SinkConfig_Databricks{
				Databricks: &commonpb.DatabricksSinkConfig{
					ServerHostname: "adb-1.azuredatabricks.net",
					Auth: &commonpb.DatabricksSinkConfig_OauthM2M{
						OauthM2M: &commonpb.DatabricksOAuthM2M{ClientId: "cid", ClientSecret: secret},
					},
				},
			}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw, err := json.Marshal(newSinkConfigDTO(tc.cfg))
			require.NoError(t, err)
			require.NotContains(t, string(raw), secret,
				"secret leaked into the HTTP response body: %s", string(raw))
		})
	}
}

func TestNewSinkConfigDTO_KafkaShape(t *testing.T) {
	t.Parallel()

	dto := newSinkConfigDTO(&commonpb.SinkConfig{
		Name: "k",
		Type: &commonpb.SinkConfig_Kafka{Kafka: &commonpb.KafkaSinkConfig{
			Brokers:       []string{"b1:9092", "b2:9092"},
			Topic:         "events",
			Tls:           false,
			SaslMechanism: "PLAIN",
			SaslUsername:  "u",
			SaslPassword:  "p",
		}},
		Format:       "json",
		BatchSize:    64,
		BatchDelayMs: 10,
	})
	require.NotNil(t, dto)

	// Exactly one variant is populated; the other four stay nil so the response
	// carries no empty sibling objects.
	require.NotNil(t, dto.Kafka)
	require.Nil(t, dto.Nats)
	require.Nil(t, dto.Clickhouse)
	require.Nil(t, dto.Http)
	require.Nil(t, dto.Databricks)

	require.Equal(t, []string{"b1:9092", "b2:9092"}, dto.Kafka.Brokers)
	require.Equal(t, "events", dto.Kafka.Topic)
	require.Equal(t, "PLAIN", dto.Kafka.SaslMechanism)
	require.Equal(t, "u", dto.Kafka.SaslUsername)
	require.Equal(t, "json", dto.Format)
	require.Equal(t, int32(64), dto.BatchSize)
	require.Equal(t, int64(10), dto.BatchDelayMs)
	// Allocated, so an empty set marshals as [] ("all event types") not null.
	require.NotNil(t, dto.EventTypes)
	require.Empty(t, dto.EventTypes)

	// tls=false is a real setting, not an absent one, so it must survive the
	// round trip rather than be dropped by an omitempty.
	raw, err := json.Marshal(dto)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"tls":false`)
	require.Contains(t, string(raw), `"eventTypes":[]`)
	// int64, emitted unquoted — protojson quoted its 64-bit fields.
	require.Contains(t, string(raw), `"batchDelayMs":10`)
}

func TestNewSinkConfigDTO_DatabricksAuthMethod(t *testing.T) {
	t.Parallel()

	t.Run("token", func(t *testing.T) {
		t.Parallel()

		dto := newSinkConfigDTO(&commonpb.SinkConfig{
			Name: "d",
			Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{
				ServerHostname: "adb-1.azuredatabricks.net",
				HttpPath:       "/sql/1.0/warehouses/abc123",
				Catalog:        "main",
				Schema:         "default",
				Table:          "ledger_events",
				Port:           443,
				Auth:           &commonpb.DatabricksSinkConfig_Token{Token: "pat"},
			}},
		})
		require.NotNil(t, dto)
		require.NotNil(t, dto.Databricks)

		require.Equal(t, "adb-1.azuredatabricks.net", dto.Databricks.ServerHostname)
		require.Equal(t, "/sql/1.0/warehouses/abc123", dto.Databricks.HTTPPath)
		require.Equal(t, "main", dto.Databricks.Catalog)
		require.Equal(t, "default", dto.Databricks.Schema)
		require.Equal(t, "ledger_events", dto.Databricks.Table)
		require.Equal(t, int32(443), dto.Databricks.Port)
		// The PAT itself is omitted, so authMethod is the only thing left that
		// records that token auth is configured at all.
		require.Equal(t, "token", dto.Databricks.AuthMethod)
		require.Nil(t, dto.Databricks.OauthM2M)
	})

	t.Run("oauthM2m", func(t *testing.T) {
		t.Parallel()

		dto := newSinkConfigDTO(&commonpb.SinkConfig{
			Name: "d",
			Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{
				ServerHostname: "adb-1.azuredatabricks.net",
				Auth: &commonpb.DatabricksSinkConfig_OauthM2M{
					OauthM2M: &commonpb.DatabricksOAuthM2M{ClientId: "cid", ClientSecret: "csecret"},
				},
			}},
		})
		require.NotNil(t, dto)
		require.NotNil(t, dto.Databricks)

		require.Equal(t, "oauthM2m", dto.Databricks.AuthMethod)
		require.NotNil(t, dto.Databricks.OauthM2M)
		require.Equal(t, "cid", dto.Databricks.OauthM2M.ClientID)
	})
}

func TestNewSinkStatusDTO(t *testing.T) {
	t.Parallel()

	t.Run("healthy", func(t *testing.T) {
		t.Parallel()

		dto := newSinkStatusDTO(&commonpb.SinkStatus{SinkName: "kafka", Cursor: 0})
		require.NotNil(t, dto)

		require.Equal(t, "kafka", dto.SinkName)
		require.Equal(t, uint64(0), dto.Cursor)
		require.Nil(t, dto.Error)

		// Cursor 0 means nothing emitted yet, a real state, so it is present and
		// unquoted (protojson quoted this fixed64).
		raw, err := json.Marshal(dto)
		require.NoError(t, err)
		require.Contains(t, string(raw), `"cursor":0`)
		require.NotContains(t, string(raw), `"error"`)
	})

	t.Run("with error", func(t *testing.T) {
		t.Parallel()

		dto := newSinkStatusDTO(&commonpb.SinkStatus{
			SinkName: "kafka",
			Cursor:   7,
			Error: &commonpb.SinkError{
				Message:    "broker unreachable",
				OccurredAt: &commonpb.Timestamp{Data: 1786540255458491},
			},
		})
		require.NotNil(t, dto)
		require.NotNil(t, dto.Error)

		require.Equal(t, "broker unreachable", dto.Error.Message)
		require.NotNil(t, dto.Error.OccurredAt)
		require.Equal(t, "2026-08-12T13:10:55.458491Z", *dto.Error.OccurredAt)

		// RFC3339Nano string, not protojson's {"data":<micros>} wrapper.
		raw, err := json.Marshal(dto)
		require.NoError(t, err)
		require.NotContains(t, string(raw), "data")
	})
}

func TestNewEventsSinksDTO_EmptyMarshalsAsArrays(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(newEventsSinksDTO(nil, nil))
	require.NoError(t, err)
	require.JSONEq(t, `{"sinks":[],"sinkStatuses":[]}`, string(raw))
}
