package controllers_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
)

func TestGetStats(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, h *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, h, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
				}, false)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, h, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "boc",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
				}, false)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.GetStats(h)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				stats, _ := internal.DecodeSingleResponse[ledger.Stats](t, rsp.Body)

				assert.EqualValues(t, ledger.Stats{
					Transactions: 2,
					Accounts:     3,
				}, stats)
				return nil
			},
		})
	}))
}
