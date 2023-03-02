package middlewares_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
)

func TestAdditionalGlobalMiddleware(t *testing.T) {
	internal.RunTest(t,
		routes.ProvideMiddlewares(func() []func(h http.Handler) http.Handler {
			return []func(http http.Handler) http.Handler{
				func(handler http.Handler) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(418)
					})
				},
			}
		}),
		fx.Invoke(func(api *api.API) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/_info", nil)

			api.ServeHTTP(rec, req)
			assert.Equal(t, 418, rec.Code)
		}),
	)
}
