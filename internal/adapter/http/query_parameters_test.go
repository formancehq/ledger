package http

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.yaml.in/yaml/v3"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

func TestQueryParameters_RejectUnknownAtRouter(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	for _, tc := range []struct {
		method, path, message string
	}{
		{http.MethodGet, "/v3/l1/accounts?prefix=users", `use filter instead`},
		{http.MethodGet, "/v3/l1/transactions?reference=ref-1", `use filter instead`},
		{http.MethodGet, "/v3/l1/accounts?address=users:1", `unknown query parameter "address"`},
		{http.MethodGet, "/v3/l1/accounts?nonsense=1", `unknown query parameter "nonsense"`},
		{http.MethodGet, "/v3/l1/accounts/bank?checkpointId=999999", `checkpoint-pinned reads are available through gRPC only`},
		{http.MethodGet, "/v3/l1/stats?checkpointId=999999", `checkpoint-pinned reads are available through gRPC only`},
		{http.MethodGet, "/v3/_/audit-entries?checkpointId=999999", `checkpoint-pinned reads are available through gRPC only`},
		{http.MethodGet, "/v3/l1/accounts?filter=address&prefix=users", `use filter instead`},
		{http.MethodGet, "/v3/l1/transactions?filter=reference&reference=ref-1", `use filter instead`},
		{http.MethodGet, "/v3/l1/accounts?filtre=x", `unknown query parameter "filtre"`},
		{http.MethodGet, "/v3/l1/accounts?filter=x%GG", `invalid query string`},
		{http.MethodPost, "/v3/l1/bulk?atomic=true&nonsense=1", `unknown query parameter "nonsense"`},
		{http.MethodDelete, "/v3/_/events-sinks/sink?controllerId=one&nonsense=1", `unknown query parameter "nonsense"`},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			resp := decodeResponse[ErrorResponse](t, w)
			require.Equal(t, "INVALID_REQUEST", resp.ErrorCode)
			require.Contains(t, resp.ErrorMessage, tc.message)
		})
	}
}

func TestKnownQuery_AllowsDeclaredParameters(t *testing.T) {
	t.Parallel()

	called := false
	handler := knownQuery(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}, "pageSize", "cursor", "reverse", "filter")
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest(http.MethodGet, "/v3/l1/accounts?pageSize=10&cursor=abc&reverse=true&filter=x", nil))
	require.Equal(t, http.StatusNoContent, w.Code)
	require.True(t, called)
}

// The OpenAPI operation declarations are the source of truth for existing
// names. Probe every documented parameter through the real router so a route
// allow-list cannot accidentally remove a supported option.
func TestQueryParameters_MatchOpenAPI(t *testing.T) {
	t.Parallel()

	specBytes, err := os.ReadFile("../../../openapi.yml")
	require.NoError(t, err)
	var spec map[string]any
	require.NoError(t, yaml.Unmarshal(specBytes, &spec))

	// The handler may reach the backend for valid parameters. A nil embedded
	// backend panics there and the router's recoverer converts that into a 500;
	// the assertion below only concerns rejection at the query boundary.
	handler := NewHandler(logging.Testing(), &struct{ Backend }{}, internalauth.AuthConfig{}, version.Info{})
	pathParam := regexp.MustCompile(`\{[^}]+\}`)
	for path, raw := range spec["paths"].(map[string]any) {
		if !strings.HasPrefix(path, "/v3/") {
			continue
		}
		pathSpec := raw.(map[string]any)
		for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
			opRaw, ok := pathSpec[strings.ToLower(method)]
			if !ok {
				continue
			}
			operation := opRaw.(map[string]any)
			params := append([]any{}, parameters(pathSpec)...)
			params = append(params, parameters(operation)...)
			probePath := pathParam.ReplaceAllString(path, "probe")
			t.Run(method+" "+path+" unknown", func(t *testing.T) {
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, httptest.NewRequest(method, probePath+"?__unknown=1", nil))
				require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				resp := decodeResponse[ErrorResponse](t, w)
				require.Equal(t, "INVALID_REQUEST", resp.ErrorCode)
				require.Contains(t, resp.ErrorMessage, `unknown query parameter "__unknown"`)
			})
			for _, rawParam := range params {
				param := rawParam.(map[string]any)
				if ref, ok := param["$ref"].(string); ok {
					var value any = spec
					for part := range strings.SplitSeq(strings.TrimPrefix(ref, "#/"), "/") {
						value = value.(map[string]any)[part]
					}
					param = value.(map[string]any)
				}
				if param["in"] != "query" {
					continue
				}
				name := param["name"].(string)
				t.Run(method+" "+path+" "+name, func(t *testing.T) {
					w := httptest.NewRecorder()
					target := probePath + "?" + url.QueryEscape(name) + "=test"
					handler.ServeHTTP(w, httptest.NewRequest(method, target, nil))
					require.NotEqual(t, http.StatusNotFound, w.Code, w.Body.String())
					require.NotContains(t, w.Body.String(), "unknown query parameter", w.Body.String())
				})
			}
		}
	}
}

func parameters(operation map[string]any) []any {
	if value, ok := operation["parameters"]; ok {
		return value.([]any)
	}

	return nil
}
