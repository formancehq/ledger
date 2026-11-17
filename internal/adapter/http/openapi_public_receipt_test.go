package http

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/pkg/httpreceipt"
)

func TestOpenAPI_PublicReceiptMutationCoverage(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	require.NoError(t, doc.Validate(context.Background(), openapi3.AllowExtraSiblingFields("description")))
	count := 0
	for path, item := range doc.Paths.Map() {
		for method, operation := range item.Operations() {
			supported := false
			for _, parameter := range operation.Parameters {
				if parameter.Value.In == "header" && parameter.Value.Name == httpreceipt.Header {
					supported = true
					require.False(t, parameter.Value.Required)
				}
			}
			if !supported {
				continue
			}
			count++
			t.Run(method+path, func(t *testing.T) {
				t.Parallel()
				for status, response := range operation.Responses.Map() {
					if strings.HasPrefix(status, "2") {
						require.Contains(t, response.Value.Headers, httpreceipt.Header)
					}
				}
				require.NotNil(t, operation.Responses.Value("503"))
				target := strings.NewReplacer("{ledgerName}", "ledger1", "{transactionId}", "1", "{address}", "users:1", "{key}", "color", "{name}", "script", "{queryName}", "query", "{canonicalId}", "index", "{sinkName}", "sink", "{typeName}", "user", "{targetType}", "account").Replace(path)
				backend := NewMockBackend(gomock.NewController(t))
				router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				req := httptest.NewRequest(method, target, strings.NewReader(`[]`))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set(httpreceipt.Header, "v1")
				response := httptest.NewRecorder()
				router.ServeHTTP(response, req)
				require.Equal(t, 503, response.Code, "receipt must be rejected before any dispatch on %s %s: %s", method, path, response.Body.String())
				require.Contains(t, response.Body.String(), "RESPONSE_SIGNING_UNAVAILABLE")
			})
		}
	}
	require.Equal(t, 25, count, "all existing Apply mutation operations must declare public receipts")
}
