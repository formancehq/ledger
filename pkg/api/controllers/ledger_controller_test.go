package controllers_test

import (
	"context"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"net/http"
	"testing"
)

func TestGetStats(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, h *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, h, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      100,
							Asset:       "USD",
						},
					},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, h, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "boc",
							Amount:      100,
							Asset:       "USD",
						},
					},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.GetStats(h)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				stats := ledger.Stats{}
				internal.DecodeSingleResponse(t, rsp.Body, &stats)
				assert.EqualValues(t, ledger.Stats{
					Transactions: 2,
					Accounts:     3,
				}, stats)
				return nil
			},
		})
	}))
}
