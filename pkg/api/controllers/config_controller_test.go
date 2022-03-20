package controllers_test

import (
	"context"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/config"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"net/http"
	"testing"
)

func TestGetInfo(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, h *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.GetInfo(h)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				info := config.ConfigInfo{}
				internal.DecodeSingleResponse(t, rsp.Body, &info)
				assert.EqualValues(t, config.ConfigInfo{
					Server:  "numary-ledger",
					Version: "latest",
					Config: &config.Config{
						LedgerStorage: &config.LedgerStorage{
							Driver:  "sqlite",
							Ledgers: []string{"quickstart"},
						},
					},
				}, info)
				return nil
			},
		})
	}))
}
