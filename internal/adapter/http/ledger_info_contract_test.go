package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/adapter/v2/celrewrite"
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

// These routed fixtures are also inputs to generated SDK get/list operations.
// Comparing the emitted rules prevents schema validation's permissive unknown
// properties from hiding a valueExpr property missing from generated models.
func TestLedgerInfoReadRewriteRules(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	for _, tc := range []struct{ path, fixture string }{
		{"/v3/mirror", "testdata/ledger_info_rewrite_get.json"},
		{"/v3/", "testdata/ledger_info_rewrite_list.json"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			fixture, err := os.ReadFile(tc.fixture)
			require.NoError(t, err)
			var envelope struct {
				Data json.RawMessage `json:"data"`
			}
			require.NoError(t, json.Unmarshal(fixture, &envelope))
			if tc.path == "/v3/" {
				var rows []json.RawMessage
				require.NoError(t, json.Unmarshal(envelope.Data, &rows))
				envelope.Data = rows[0]
			}
			var input struct {
				MirrorSource json.RawMessage `json:"mirrorSource"`
			}
			require.NoError(t, json.Unmarshal(envelope.Data, &input))
			source := &commonpb.MirrorSourceConfig{}
			require.NoError(t, protojson.Unmarshal(input.MirrorSource, source))
			_, rewriteErr := celrewrite.NewRewriter(source.GetRewriteRules())
			require.NoError(t, rewriteErr, "read fixture must contain admission-valid rules")
			ledger := &commonpb.LedgerInfo{Name: "mirror", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: source}
			backend := NewMockBackend(gomock.NewController(t))
			if tc.path == "/v3/" {
				backend.EXPECT().ListLedgers(gomock.Any()).Return(cursor.NewSliceCursor([]*commonpb.LedgerInfo{ledger}), nil)
			} else {
				backend.EXPECT().GetLedgerByName(gomock.Any(), "mirror").Return(ledger, nil)
			}
			response := httptest.NewRecorder()
			NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}).ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
			require.Equal(t, http.StatusOK, response.Code)
			require.JSONEq(t, string(fixture), response.Body.String())
			var data any
			require.NoError(t, json.Unmarshal(envelope.Data, &data))
			require.NoError(t, doc.Components.Schemas["LedgerInfo"].Value.VisitJSON(data))
		})
	}
	for _, scope := range []string{"createdTransaction", "revertedTransaction", "savedMetadata"} {
		rule := doc.Components.Schemas["MirrorSourceRead"].Value.Properties["rewriteRules"].Value.Items.Value.Properties[scope].Value
		actions := rule.Properties["actions"].Value.Items.Value
		metadata := actions.Properties["setMetadata"].Value
		require.Contains(t, metadata.Properties, "valueExpr")
		require.NotContains(t, metadata.Properties, "value_expr")
	}
	account := doc.Components.Schemas["MirrorSourceRead"].Value.Properties["rewriteRules"].Value.Items.Value.Properties["createdTransaction"].Value.Properties["actions"].Value.Items.Value.Properties["setAccountMetadata"].Value
	require.Contains(t, account.Properties, "valueExpr")
	require.NotContains(t, account.Properties, "value_expr")
	// Read fixes must preserve the separate creation contract.
	creation := doc.Components.Schemas["SetMetadataAction"].Value
	require.Contains(t, creation.Properties, "value_expr")
	require.NotContains(t, creation.Properties, "valueExpr")
	require.Contains(t, doc.Components.Schemas["SetAccountMetadataFromAddressReplacement"].Value.Required, "replacement")
}
