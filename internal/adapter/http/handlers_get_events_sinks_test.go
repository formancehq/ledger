package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetEventsSinks_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetEventsSinks(gomock.Any()).DoAndReturn(
		func(_ context.Context) ([]*commonpb.SinkConfig, []*commonpb.SinkStatus, error) {
			return []*commonpb.SinkConfig{{Name: "kafka"}},
				[]*commonpb.SinkStatus{{SinkName: "kafka", Cursor: 42}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/events-sinks", nil, nil)

	srv.handleGetEventsSinks(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	// The HTTP endpoint must expose the per-sink status data at parity with
	// gRPC, not just the configs (EN-1472).
	var body struct {
		Data struct {
			Sinks        []map[string]any `json:"sinks"`
			SinkStatuses []map[string]any `json:"sinkStatuses"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body.Data.Sinks, 1)
	require.Len(t, body.Data.SinkStatuses, 1)
	require.Equal(t, "kafka", body.Data.SinkStatuses[0]["sinkName"])
}

// TestHandleGetEventsSinks_NoSecretsInBody is the route-level counterpart to
// TestNewSinkConfigDTO_SecretsAreAbsent: the DTO types carry no field for any
// secret, so the full response body cannot contain one. Before EN-1791 this
// route marshalled the raw SinkConfig with protojson and shipped every
// credential in plaintext to any ledger:OpsRead caller.
func TestHandleGetEventsSinks_NoSecretsInBody(t *testing.T) {
	t.Parallel()

	const secret = "SUPERSECRETVALUE"

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetEventsSinks(gomock.Any()).DoAndReturn(
		func(_ context.Context) ([]*commonpb.SinkConfig, []*commonpb.SinkStatus, error) {
			return []*commonpb.SinkConfig{
				{Name: "kafka", Type: &commonpb.SinkConfig_Kafka{Kafka: &commonpb.KafkaSinkConfig{
					Brokers: []string{"b:9092"}, Topic: "t",
					SaslUsername: "u", SaslPassword: secret,
				}}},
				{Name: "http", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{
					Endpoint: "https://x", Secret: secret,
				}}},
				{Name: "clickhouse", Type: &commonpb.SinkConfig_Clickhouse{
					Clickhouse: &commonpb.ClickHouseSinkConfig{
						Dsn: "clickhouse://user:" + secret + "@host:9000", Table: "events",
					}},
				},
				{Name: "databricksPat", Type: &commonpb.SinkConfig_Databricks{
					Databricks: &commonpb.DatabricksSinkConfig{
						ServerHostname: "adb-1.azuredatabricks.net",
						Auth:           &commonpb.DatabricksSinkConfig_Token{Token: secret},
					}},
				},
				{Name: "databricksOauth", Type: &commonpb.SinkConfig_Databricks{
					Databricks: &commonpb.DatabricksSinkConfig{
						ServerHostname: "adb-1.azuredatabricks.net",
						Auth: &commonpb.DatabricksSinkConfig_OauthM2M{
							OauthM2M: &commonpb.DatabricksOAuthM2M{ClientId: "cid", ClientSecret: secret},
						},
					}},
				},
			}, []*commonpb.SinkStatus{{SinkName: "kafka", Cursor: 42}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/events-sinks", nil, nil)

	srv.handleGetEventsSinks(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), secret,
		"secret leaked into the HTTP response body: %s", w.Body.String())
	// The status payload still has to be there — this is not a check that the
	// route stopped returning data.
	require.Contains(t, w.Body.String(), `"sinkName"`)
}

func TestHandleGetEventsSinks_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetEventsSinks(gomock.Any()).DoAndReturn(
		func(_ context.Context) ([]*commonpb.SinkConfig, []*commonpb.SinkStatus, error) {
			return nil, nil, errors.New("boom")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/events-sinks", nil, nil)

	srv.handleGetEventsSinks(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
