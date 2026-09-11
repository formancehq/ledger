package http

import (
	"encoding/base64"
	"encoding/hex"
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
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
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
	sinks := []*commonpb.SinkConfig{
		{Name: "nats", Type: &commonpb.SinkConfig_Nats{Nats: &commonpb.NatsSinkConfig{Url: "nats://natsSecret@localhost:4222,user:natsPassword@localhost:4223"}}},
		{Name: "clickhouse", Type: &commonpb.SinkConfig_Clickhouse{Clickhouse: &commonpb.ClickHouseSinkConfig{Dsn: "clickhouse://u:chSecret@localhost/db?password=chQuerySecret"}}},
		{Name: "kafka", Type: &commonpb.SinkConfig_Kafka{Kafka: &commonpb.KafkaSinkConfig{SaslPassword: "kafkaSecret"}}},
		{Name: "http", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "https://u:httpBasicSecret@localhost/?key=httpQuerySecret", Secret: "hmacSecret"}}},
		{Name: "databricks-pat", Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{Auth: &commonpb.DatabricksSinkConfig_Token{Token: "patSecret"}}}},
		{Name: "databricks-oauth", Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{Auth: &commonpb.DatabricksSinkConfig_OauthM2M{OauthM2M: &commonpb.DatabricksOAuthM2M{ClientSecret: "dbOAuthSecret"}}}}},
	}
	originals := make([]*commonpb.SinkConfig, len(sinks))
	for i, sink := range sinks {
		originals[i] = sink.CloneVT()
	}
	statuses := []*commonpb.SinkStatus{{SinkName: "http", Error: &commonpb.SinkError{Message: "dial https://u:statusSecret@localhost failed"}}}
	originalStatus := statuses[0].CloneVT()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetEventsSinks(gomock.Any()).Return(sinks, statuses, nil)
	response := credentialRead(t, credentialReadHandler(backend, internalauth.ScopeOpsRead), "/v3/_/events-sinks")
	for _, secret := range []string{"natsSecret", "natsPassword", "chSecret", "chQuerySecret", "kafkaSecret", "httpBasicSecret", "httpQuerySecret", "hmacSecret", "patSecret", "dbOAuthSecret", "statusSecret"} {
		require.NotContains(t, response.Body.String(), secret)
	}
	var body struct{ Data stdjson.RawMessage }
	require.NoError(t, stdjson.Unmarshal(response.Body.Bytes(), &body))
	projected := &servicepb.GetEventsSinksResponse{}
	require.NoError(t, protojson.Unmarshal(body.Data, projected))
	require.Len(t, projected.GetSinks(), len(sinks))
	require.Equal(t, "http", projected.GetSinkStatuses()[0].GetSinkName())
	for i, sink := range sinks {
		require.True(t, proto.Equal(originals[i], sink))
	}
	require.True(t, proto.Equal(originalStatus, statuses[0]))
	for _, mirror := range []*commonpb.MirrorSourceConfig{
		{Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "https://u:mirrorBasicSecret@localhost", Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{ClientSecret: "mirrorOAuthSecret", TokenEndpoint: "https://u:tokenEndpointSecret@localhost/token"}}}},
		{Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: "postgres://u:postgresSecret@localhost/db?password=postgresQuerySecret"}}},
	} {
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

func TestCredentialProjectionHistoryHTTP(t *testing.T) {
	t.Parallel()
	for _, config := range []struct{ name, order, batch, log string }{
		{"sink", `{"systemScoped":{"addEventsSink":{"config":{"name":"sink","http":{"secret":"credentialSentinel","endpoint":"https://u:urlSentinel@localhost"}}}}}`, `{"requests":[{"addEventsSink":{"config":{"name":"sink","http":{"secret":"credentialSentinel","endpoint":"https://u:urlSentinel@localhost"}}}}]}`, `{"payload":{"addedEventsSink":{"config":{"name":"sink","http":{"secret":"credentialSentinel","endpoint":"https://u:urlSentinel@localhost"}}}}}`},
		{"mirror", `{"ledgerScoped":{"ledger":"mirror","createLedger":{"mirrorSource":{"postgres":{"dsn":"postgres://u:credentialSentinel@localhost/db?password=urlSentinel"}}}}}`, `{"requests":[{"createLedger":{"name":"mirror","mirrorSource":{"postgres":{"dsn":"postgres://u:credentialSentinel@localhost/db?password=urlSentinel"}}}}]}`, `{"payload":{"createLedger":{"name":"mirror","mirrorSource":{"postgres":{"dsn":"postgres://u:credentialSentinel@localhost/db?password=urlSentinel"}}}}}`},
	} {
		t.Run(config.name, func(t *testing.T) {
			t.Parallel()
			order := &raftcmdpb.Order{}
			require.NoError(t, protojson.Unmarshal([]byte(config.order), order))
			orderBytes, err := proto.Marshal(order)
			require.NoError(t, err)
			batch := &servicepb.ApplyBatch{}
			require.NoError(t, protojson.Unmarshal([]byte(config.batch), batch))
			batchBytes, err := proto.Marshal(batch)
			require.NoError(t, err)
			entry := &auditpb.AuditEntry{Sequence: 7, OrderCount: 1, Items: []*auditpb.AuditItem{{SerializedOrder: orderBytes, LogSequence: 9}}, Signature: &signaturepb.SignedApplyBatch{Payload: batchBytes, Signature: []byte("signature")}}
			original := entry.CloneVT()
			listed := entry.CloneVT()
			listed.Items = nil
			originalList := listed.CloneVT()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().GetAuditEntry(gomock.Any(), uint64(7)).Return(entry, nil)
			backend.EXPECT().ListAuditEntries(gomock.Any(), uint32(100), uint64(0), gomock.Any(), false).Return(cursor.NewSliceCursor([]*auditpb.AuditEntry{listed}), nil)
			handler := credentialReadHandler(backend, internalauth.ScopeAuditRead)
			for _, path := range []string{"/v3/_/audit-entries/7", "/v3/_/audit-entries"} {
				response := credentialRead(t, handler, path)
				var body struct{ Data stdjson.RawMessage }
				require.NoError(t, stdjson.Unmarshal(response.Body.Bytes(), &body))
				var entries []struct {
					Items     []struct{ SerializedOrder string }
					Signature struct{ Payload string }
				}
				if path == "/v3/_/audit-entries/7" {
					body.Data = append(append([]byte{'['}, body.Data...), ']')
				}
				require.NoError(t, stdjson.Unmarshal(body.Data, &entries))
				require.Len(t, entries, 1)
				for _, item := range entries[0].Items {
					decoded, err := hex.DecodeString(item.SerializedOrder)
					require.NoError(t, err)
					require.NotEmpty(t, decoded)
					require.NotContains(t, string(decoded), "credentialSentinel")
					require.NotContains(t, string(decoded), "urlSentinel")
					projected := &raftcmdpb.Order{}
					require.NoError(t, proto.Unmarshal(decoded, projected))
				}
				decoded, err := base64.StdEncoding.DecodeString(entries[0].Signature.Payload)
				require.NoError(t, err)
				require.NotEmpty(t, decoded)
				require.NotContains(t, string(decoded), "credentialSentinel")
				require.NotContains(t, string(decoded), "urlSentinel")
				projected := &servicepb.ApplyBatch{}
				require.NoError(t, proto.Unmarshal(decoded, projected))
				require.Len(t, projected.GetRequests(), 1)
			}
			require.True(t, proto.Equal(original, entry))
			require.True(t, proto.Equal(originalList, listed))
			log := &commonpb.Log{}
			require.NoError(t, protojson.Unmarshal([]byte(config.log), log))
			log.Sequence = 9
			originalLog := log.CloneVT()
			backend.EXPECT().GetLog(gomock.Any(), uint64(9)).Return(log, nil)
			// ListLogs currently returns Apply only; retain this defensive boundary
			// case alongside the independent, reachable GetLog system-history case.
			backend.EXPECT().ListLogs(gomock.Any(), "mirror", uint64(0), uint32(100), gomock.Any()).Return(cursor.NewSliceCursor([]*commonpb.Log{log}), nil)
			for _, test := range []struct {
				path  string
				scope internalauth.Scope
			}{{"/v3/_/logs/9", internalauth.ScopeOpsRead}, {"/v3/mirror/logs", internalauth.ScopeLedgersRead}} {
				response := credentialRead(t, credentialReadHandler(backend, test.scope), test.path)
				require.NotContains(t, response.Body.String(), "credentialSentinel")
				require.NotContains(t, response.Body.String(), "urlSentinel")
				require.Contains(t, response.Body.String(), "9")
			}
			require.True(t, proto.Equal(originalLog, log))
		})
	}
}

func TestCredentialProjectionMalformedAuditHTTP(t *testing.T) {
	t.Parallel()
	for _, list := range []bool{false, true} {
		t.Run(map[bool]string{false: "get", true: "list"}[list], func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			entry := &auditpb.AuditEntry{Sequence: 7, Items: []*auditpb.AuditItem{{SerializedOrder: []byte("credentialSentinel")}}}
			path := "/v3/_/audit-entries/7"
			if list {
				path = "/v3/_/audit-entries"
				backend.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cursor.NewSliceCursor([]*auditpb.AuditEntry{entry}), nil)
			} else {
				backend.EXPECT().GetAuditEntry(gomock.Any(), uint64(7)).Return(entry, nil)
			}
			response := httptest.NewRecorder()
			credentialReadHandler(backend, internalauth.ScopeAuditRead).ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			require.Equal(t, http.StatusInternalServerError, response.Code)
			require.NotContains(t, response.Body.String(), "credentialSentinel")
			require.NotContains(t, response.Body.String(), "serializedOrder")
		})
	}
}
