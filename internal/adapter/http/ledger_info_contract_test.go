package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestLedgerInfoReadResponsesRedactCredentials(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	for index, source := range []*commonpb.MirrorSourceConfig{
		{LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{
			BaseUrl: "https://reader:base-canary@source.example/prefix", Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{
				ClientId: "client", ClientSecret: "oauth-canary-2780", TokenEndpoint: "https://client:token-canary@auth.example/token", Scopes: []string{"ledger:read"},
			},
		}}},
		{LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{
			Dsn: "postgres://reader:pg-canary-2780@db.example/ledger?sslmode=require",
		}}},
	} {
		for _, path := range []string{"/v3/", "/v3/mirror"} {
			t.Run(fmt.Sprintf("source-%d%s", index, path), func(t *testing.T) {
				ledger := &commonpb.LedgerInfo{Name: "mirror", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: source}
				ledger.MirrorSyncProgress = &commonpb.MirrorSyncProgress{SourceLogCount: 9007199254740993}
				ledger.Metadata = map[string]*commonpb.MetadataValue{"owner": commonpb.NewStringValue("customer")}
				ledger.MetadataSchema = &commonpb.MetadataSchema{LedgerFields: map[string]*commonpb.MetadataFieldSchema{"owner": {}}}
				original := proto.Clone(ledger)
				backend := NewMockBackend(gomock.NewController(t))
				if path == "/v3/" {
					backend.EXPECT().ListLedgers(gomock.Any()).Return(cursor.NewSliceCursor([]*commonpb.LedgerInfo{ledger}), nil)
				} else {
					backend.EXPECT().GetLedgerByName(gomock.Any(), "mirror").Return(ledger, nil)
				}
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
				require.Equal(t, http.StatusOK, response.Code)
				require.Contains(t, response.Body.String(), `"name":"mirror"`)
				require.Contains(t, response.Body.String(), `"ledgerName":"source"`)
				require.NotContains(t, response.Body.String(), "canary")
				require.NotContains(t, response.Body.String(), "pg-canary-2780")
				require.True(t, proto.Equal(original, ledger), "controller-owned configuration was mutated")
				var body struct {
					Data any `json:"data"`
				}
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
				if list, ok := body.Data.([]any); ok {
					body.Data = list[0]
				}
				require.NoError(t, doc.Components.Schemas["LedgerInfo"].Value.VisitJSON(body.Data))
			})
		}
	}
}

func TestLedgerInfoReadMarshalFailureReturnsInternalError(t *testing.T) {
	for _, path := range []string{"/v3/", "/v3/mirror"} {
		t.Run(path, func(t *testing.T) {
			backend := NewMockBackend(gomock.NewController(t))
			ledger := &commonpb.LedgerInfo{Name: "mirror", Mode: commonpb.LedgerMode(999)}
			if path == "/v3/" {
				backend.EXPECT().ListLedgers(gomock.Any()).Return(cursor.NewSliceCursor([]*commonpb.LedgerInfo{ledger}), nil)
			} else {
				backend.EXPECT().GetLedgerByName(gomock.Any(), "mirror").Return(ledger, nil)
			}
			response := httptest.NewRecorder()
			NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}).ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			require.Equal(t, http.StatusInternalServerError, response.Code)
			require.NotContains(t, response.Body.String(), "999")
			require.Contains(t, response.Body.String(), "INTERNAL_ERROR")
		})
	}
}
