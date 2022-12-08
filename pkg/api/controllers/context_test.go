package controllers_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/numary/ledger/pkg"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestContext(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				t.Run("GET/stats", func(t *testing.T) {
					rsp := internal.GetStats(api)
					_, err := uuid.Parse(rsp.Header().Get(string(pkg.ContextKeyID)))
					require.NoError(t, err)
				})
				t.Run("GET/log", func(t *testing.T) {
					rsp := internal.GetLogs(api, url.Values{})
					_, err := uuid.Parse(rsp.Header().Get(string(pkg.ContextKeyID)))
					require.NoError(t, err)
				})
				t.Run("GET/accounts", func(t *testing.T) {
					rsp := internal.GetAccounts(api, url.Values{})
					_, err := uuid.Parse(rsp.Header().Get(string(pkg.ContextKeyID)))
					require.NoError(t, err)
				})
				t.Run("GET/transactions", func(t *testing.T) {
					rsp := internal.GetTransactions(api, url.Values{})
					_, err := uuid.Parse(rsp.Header().Get(string(pkg.ContextKeyID)))
					require.NoError(t, err)
				})
				t.Run("POST/transactions", func(t *testing.T) {
					rsp := internal.PostTransaction(t, api, controllers.PostTransaction{}, true)
					_, err := uuid.Parse(rsp.Header().Get(string(pkg.ContextKeyID)))
					require.NoError(t, err)
				})
				t.Run("POST/transactions/batch", func(t *testing.T) {
					rsp := internal.PostTransactionBatch(t, api, core.Transactions{})
					_, err := uuid.Parse(rsp.Header().Get(string(pkg.ContextKeyID)))
					require.NoError(t, err)
				})
				t.Run("GET/balances", func(t *testing.T) {
					rsp := internal.GetBalances(api, url.Values{})
					_, err := uuid.Parse(rsp.Header().Get(string(pkg.ContextKeyID)))
					require.NoError(t, err)
				})

				return nil
			},
		})
	}))
}
