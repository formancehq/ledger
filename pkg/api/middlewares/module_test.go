package middlewares_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
)

func TestAdditionalGlobalMiddleware(t *testing.T) {
	internal.RunTest(t,
		routes.ProvideMiddlewares(func() []gin.HandlerFunc {
			return []gin.HandlerFunc{
				func(context *gin.Context) {
					_ = context.AbortWithError(418, errors.New(""))
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
