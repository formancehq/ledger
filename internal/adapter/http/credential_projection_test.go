package http

import (
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain/connectionconfig"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

// Exercise the registered routes with auth enabled and only the read scope
// each route requires, rather than bypassing middleware by calling handlers.
func credentialReadHandler(backend Backend, scope internalauth.Scope) http.Handler {
	return NewHandler(logging.Testing(), backend, internalauth.AuthConfig{Enabled: true, ScopeMapping: internalauth.ScopeMapping{internalauth.ScopeMappingAnonymousKey: {scope}}}, version.Info{})
}

func credentialRead(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	return response
}

func TestCredentialProjectionLiveHTTP(t *testing.T) {
	t.Parallel()
	inputs := []*commonpb.SinkConfigInput{
		{Name: "nats", Type: &commonpb.SinkConfigInput_Nats{Nats: &commonpb.NatsSinkConfigInput{Url: "nats://natsSecret@localhost:4222,user:natsPassword@localhost:4223"}}},
		{Name: "clickhouse", Type: &commonpb.SinkConfigInput_Clickhouse{Clickhouse: &commonpb.ClickHouseSinkConfigInput{Dsn: "clickhouse://u:chSecret@localhost/db?password=chQuerySecret"}}},
		{Name: "kafka", Type: &commonpb.SinkConfigInput_Kafka{Kafka: &commonpb.KafkaSinkConfig{SaslPassword: "kafkaSecret"}}},
		{Name: "http", Type: &commonpb.SinkConfigInput_Http{Http: &commonpb.HttpSinkConfigInput{Endpoint: "https://u:httpBasicSecret@localhost/?key=httpQuerySecret", Secret: "hmacSecret"}}},
		{Name: "databricks-pat", Type: &commonpb.SinkConfigInput_Databricks{Databricks: &commonpb.DatabricksSinkConfig{Auth: &commonpb.DatabricksSinkConfig_Token{Token: "patSecret"}}}},
		{Name: "databricks-oauth", Type: &commonpb.SinkConfigInput_Databricks{Databricks: &commonpb.DatabricksSinkConfig{Auth: &commonpb.DatabricksSinkConfig_OauthM2M{OauthM2M: &commonpb.DatabricksOAuthM2M{ClientSecret: "dbOAuthSecret"}}}}},
	}
	sinks := make([]*commonpb.SinkConfig, len(inputs))
	for i, input := range inputs {
		var err error
		sinks[i], err = connectionconfig.Sink(input)
		require.NoError(t, err)
	}
	originals := make([]*commonpb.SinkConfig, len(sinks))
	for i, sink := range sinks {
		originals[i] = sink.CloneVT()
	}
	// Sink adapters sanitize diagnostics before persistence; public reads retain them.
	const diagnostic = "posting event seq=1: sending request to https://localhost/events?key=[redacted]: EOF"
	statuses := []*commonpb.SinkStatus{{SinkName: "http", Error: &commonpb.SinkError{Message: diagnostic}}}
	originalStatus := statuses[0].CloneVT()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetEventsSinks(gomock.Any()).Return(sinks, statuses, nil)
	response := credentialRead(t, credentialReadHandler(backend, internalauth.ScopeOpsRead), "/v3/_/events-sinks")
	for _, secret := range []string{"natsSecret", "natsPassword", "chSecret", "chQuerySecret", "kafkaSecret", "httpBasicSecret", "httpQuerySecret", "hmacSecret", "patSecret", "dbOAuthSecret"} {
		require.NotContains(t, response.Body.String(), secret)
	}
	var body struct{ Data stdjson.RawMessage }
	require.NoError(t, stdjson.Unmarshal(response.Body.Bytes(), &body))
	projected := &servicepb.GetEventsSinksResponse{}
	require.NoError(t, protojson.Unmarshal(body.Data, projected))
	require.Len(t, projected.GetSinks(), len(sinks))
	require.Equal(t, "http", projected.GetSinkStatuses()[0].GetSinkName())
	require.Equal(t, diagnostic, projected.GetSinkStatuses()[0].GetError().GetMessage())
	for i, sink := range sinks {
		require.True(t, proto.Equal(originals[i], sink))
	}
	require.True(t, proto.Equal(originalStatus, statuses[0]))
	for _, input := range []*commonpb.MirrorSourceConfigInput{
		{Type: &commonpb.MirrorSourceConfigInput_Http{Http: &commonpb.HttpMirrorSourceConfigInput{BaseUrl: "https://u:mirrorBasicSecret@localhost", Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentialsInput{ClientSecret: "mirrorOAuthSecret", TokenEndpoint: "https://u:tokenEndpointSecret@localhost/token"}}}},
		{Type: &commonpb.MirrorSourceConfigInput_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfigInput{Dsn: "postgres://u:postgresSecret@localhost/db?password=postgresQuerySecret"}}},
	} {
		mirror, err := connectionconfig.Mirror(input)
		require.NoError(t, err)
		info := &commonpb.LedgerInfo{Name: "mirror", MirrorSource: mirror}
		original := info.CloneVT()
		backend.EXPECT().GetLedgerByName(gomock.Any(), "mirror").Return(info, nil)
		backend.EXPECT().ListLedgers(gomock.Any()).Return(cursor.NewSliceCursor([]*commonpb.LedgerInfo{info}), nil)
		handler := credentialReadHandler(backend, internalauth.ScopeLedgersRead)
		for _, path := range []string{"/v3/mirror", "/v3/"} {
			response := credentialRead(t, handler, path)
			for _, secret := range []string{"postgresSecret", "postgresQuerySecret", "mirrorBasicSecret", "mirrorOAuthSecret", "tokenEndpointSecret"} {
				require.NotContains(t, response.Body.String(), secret)
			}
			require.Contains(t, response.Body.String(), "mirror")
		}
		require.True(t, proto.Equal(original, info))
	}
}

func TestCredentialProjectionLogHTTP(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	log := &commonpb.Log{Sequence: 9, ResponseSignature: &signaturepb.SignedLog{KeyId: "server", Payload: []byte("opaqueCredential"), Signature: []byte("proof")}, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_AddedEventsSink{AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: &commonpb.SinkConfig{Name: "hook", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Secret: "credentialSentinel"}}}}}}}
	original := log.CloneVT()
	backend.EXPECT().GetLog(gomock.Any(), uint64(9)).Return(log, nil)
	response := credentialRead(t, credentialReadHandler(backend, internalauth.ScopeOpsRead), "/v3/_/logs/9")
	require.NotContains(t, response.Body.String(), "credentialSentinel")
	require.NotContains(t, response.Body.String(), "responseSignature")
	require.NotContains(t, response.Body.String(), "opaqueCredential")
	require.Contains(t, response.Body.String(), "hook")
	require.True(t, proto.Equal(original, log))
}

// A normal ledger has no mirror projection. Encoding a typed nil as {} invents
// a progress object that cannot satisfy the public mirror status contract.
func TestCredentialProjectionNormalLedgerOmitsMirrorFields(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	info := &commonpb.LedgerInfo{Name: "normal"}
	backend.EXPECT().GetLedgerByName(gomock.Any(), "normal").Return(info, nil)
	backend.EXPECT().ListLedgers(gomock.Any()).Return(cursor.NewSliceCursor([]*commonpb.LedgerInfo{info}), nil)
	handler := credentialReadHandler(backend, internalauth.ScopeLedgersRead)
	for _, path := range []string{"/v3/normal", "/v3/"} {
		response := credentialRead(t, handler, path)
		require.Contains(t, response.Body.String(), "normal")
		require.NotContains(t, response.Body.String(), "mirrorSource")
		require.NotContains(t, response.Body.String(), "mirrorSyncProgress")
	}
}
