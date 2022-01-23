package api

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"net/http"
	"net/http/httptest"
	"testing"
)

func withNewModule(t *testing.T, options ...fx.Option) {
	module := Module(Config{
		StorageDriver: viper.GetString("sqlite"),
		LedgerLister: controllers.LedgerListerFn(func(r *http.Request) []string {
			return []string{}
		}),
		Version: "latest",
	})
	ch := make(chan struct{})
	options = append([]fx.Option{
		module,
		ledger.ResolveModule(),
		storage.DefaultModule(),
		sqlstorage.TestingModule(),
	}, options...)
	options = append(options, fx.Invoke(func() {
		close(ch)
	}))

	fx.New(options...)
	select {
	case <-ch:
	default:
		assert.Fail(t, "something went wrong")
	}
}

func TestAdditionalGlobalMiddleware(t *testing.T) {
	withNewModule(t,
		routes.ProvideGlobalMiddleware(func() gin.HandlerFunc {
			return func(context *gin.Context) {
				context.AbortWithError(418, errors.New(""))
			}
		}),
		fx.Invoke(func(api *API) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/_info", nil)

			api.ServeHTTP(rec, req)
			assert.Equal(t, 418, rec.Code)
		}),
	)
}

func TestAdditionalPerLedgerMiddleware(t *testing.T) {
	withNewModule(t,
		routes.ProvidePerLedgerMiddleware(func() gin.HandlerFunc {
			return func(context *gin.Context) {
				context.AbortWithError(418, errors.New(""))
			}
		}),
		fx.Invoke(func(api *API) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/XXX/transactions", nil)

			api.ServeHTTP(rec, req)
			assert.Equal(t, 418, rec.Code)
		}),
	)
}
