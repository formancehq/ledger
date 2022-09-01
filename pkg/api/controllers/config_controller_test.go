package controllers_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestGetInfo(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, h *api.API, driver ledger.StorageDriver) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.GetInfo(h)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				info, ok := internal.DecodeSingleResponse[controllers.ConfigInfo](t, rsp.Body)
				require.True(t, ok)

				info.Config.LedgerStorage.Ledgers = []string{}
				assert.EqualValues(t, controllers.ConfigInfo{
					Server:  "numary-ledger",
					Version: "latest",
					Config: &controllers.Config{
						LedgerStorage: &controllers.LedgerStorage{
							Driver:  driver.Name(),
							Ledgers: []string{},
						},
					},
				}, info)
				return nil
			},
		})
	}))
}
