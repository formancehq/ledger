package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

// Validate the bodies emitted by the real router, rather than only checking
// that a status appears in YAML. Authentication failures precede the handler's
// JSON response middleware and therefore carry text/plain bodies.
func TestOpenAPI_NumscriptAuthentication(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	key, keySet := testKeyPair(t)

	operations := []struct {
		method, path string
		scope        internalauth.Scope
	}{
		{http.MethodGet, "/v3/{ledgerName}/numscripts", internalauth.ScopeLedgersRead},
		{http.MethodGet, "/v3/{ledgerName}/numscripts/{name}", internalauth.ScopeLedgersRead},
		{http.MethodGet, "/v3/{ledgerName}/numscripts/{name}/versions", internalauth.ScopeLedgersRead},
		{http.MethodGet, "/v3/{ledgerName}/numscripts/{name}/usage", internalauth.ScopeLedgersRead},
		{http.MethodPut, "/v3/{ledgerName}/numscripts/{name}", internalauth.ScopeLedgersWrite},
	}
	for _, operation := range operations {
		t.Run(operation.method+" "+operation.path, func(t *testing.T) {
			t.Parallel()
			t.Run("optional bearer declaration", func(t *testing.T) {
				security := doc.Paths.Value(operation.path).GetOperation(operation.method).Security
				require.NotNil(t, security, "declare optional bearer authentication explicitly")
				require.Equal(t, openapi3.SecurityRequirements{{"BearerAuth": []string{}}, {}}, *security)
			})

			cases := []struct {
				name, token, body  string
				enabled, anonymous bool
				status             int
			}{
				{"missing credentials", "", "missing authentication\n", true, false, http.StatusUnauthorized},
				{"insufficient scope", signToken(t, key, testClaims("ledger:OpsRead")), "missing required scope\n", true, false, http.StatusForbidden},
				{"invalid credentials", "invalid", "", true, false, http.StatusUnauthorized},
				{"valid bearer", signToken(t, key, testClaims(string(operation.scope))), "", true, false, http.StatusNotFound},
				{"anonymous scope", "", "", true, true, http.StatusNotFound},
				{"authentication disabled", "", "", false, false, http.StatusNotFound},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					backend := NewMockBackend(gomock.NewController(t))
					if tc.status == http.StatusNotFound {
						// A concrete controller rejection proves the request passed
						// authorization, including the anonymous and disabled modes.
						failure := &domain.ErrLedgerNotFound{Name: "ledger1"}
						switch {
						case operation.method == http.MethodPut:
							backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, failure)
						case strings.HasSuffix(operation.path, "/versions"):
							backend.EXPECT().ListNumscriptVersions(gomock.Any(), "ledger1", "script").Return("", nil, failure)
						case strings.HasSuffix(operation.path, "/usage"):
							backend.EXPECT().GetTemplateUsage(gomock.Any(), "ledger1", "script").Return(nil, failure)
						case strings.HasSuffix(operation.path, "/{name}"):
							backend.EXPECT().GetNumscript(gomock.Any(), "ledger1", "script", "").Return(nil, failure)
						default:
							backend.EXPECT().ListNumscripts(gomock.Any(), "ledger1").Return(nil, failure)
						}
					}
					mapping := internalauth.DefaultMapping("ledger")
					if tc.anonymous {
						mapping["anonymous"] = []internalauth.Scope{operation.scope}
					}
					handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{
						Enabled: tc.enabled, KeySet: keySet, Issuer: testAuthIssuer,
						Service: "ledger", ScopeMapping: mapping,
					}, version.Info{})
					path := strings.NewReplacer("{ledgerName}", "ledger1", "{name}", "script").Replace(operation.path)
					r := httptest.NewRequest(operation.method, "http://localhost:9000"+path,
						strings.NewReader(`{"content":"send [USD 1] ( source = @world destination = @alice )","version":"1.0.0"}`))
					r.Header.Set("Content-Type", "application/json")
					if tc.token != "" {
						r.Header.Set("Authorization", "Bearer "+tc.token)
					}
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, r)
					require.Equal(t, tc.status, w.Code)
					if tc.status == http.StatusNotFound {
						require.JSONEq(t, `{"errorCode":"LEDGER_NOT_FOUND","errorMessage":"ledger does not exist: ledger1"}`, w.Body.String())

						return
					}
					require.Equal(t, "text/plain; charset=utf-8", w.Header().Get("Content-Type"))
					if tc.body != "" {
						require.Equal(t, tc.body, w.Body.String())
					} else {
						require.Contains(t, w.Body.String(), "invalid token:")
					}
					validateOpenAPIResponse(t, doc, r, w)
				})
			}
		})
	}
}

func validateOpenAPIResponse(t *testing.T, doc *openapi3.T, r *http.Request, w *httptest.ResponseRecorder) {
	t.Helper()
	// Existing reference descriptions are documentation annotations; accepting
	// them does not relax status, media-type or response-body validation.
	router, err := legacy.NewRouter(doc, openapi3.AllowExtraSiblingFields("description"))
	require.NoError(t, err)
	route, params, err := router.FindRoute(r)
	require.NoError(t, err)
	input := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: r, PathParams: params, Route: route},
		Status:                 w.Code, Header: w.Header(),
		Options: &openapi3filter.Options{IncludeResponseStatus: true},
	}
	input.SetBodyBytes(w.Body.Bytes())
	require.NoError(t, openapi3filter.ValidateResponse(context.Background(), input))
}
