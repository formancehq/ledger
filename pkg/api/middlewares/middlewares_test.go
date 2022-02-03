package middlewares_test

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdditionalGlobalMiddleware(t *testing.T) {
	internal.WithNewModule(t,
		routes.ProvideGlobalMiddleware(func() gin.HandlerFunc {
			return func(context *gin.Context) {
				context.AbortWithError(418, errors.New(""))
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

func TestAdditionalPerLedgerMiddleware(t *testing.T) {
	internal.WithNewModule(t,
		routes.ProvidePerLedgerMiddleware(func() gin.HandlerFunc {
			return func(context *gin.Context) {
				context.AbortWithError(418, errors.New(""))
			}
		}),
		fx.Invoke(func(api *api.API) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/XXX/transactions", nil)

			api.ServeHTTP(rec, req)
			assert.Equal(t, 418, rec.Code)
		}),
	)
}
