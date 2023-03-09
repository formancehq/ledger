package controllers_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/formancehq/ledger/pkg/api"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/internal"
	"github.com/formancehq/ledger/pkg/core"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestGetBalancesAggregated(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(150),
							Asset:       "USD",
						},
					},
				}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bob",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
				}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				t.Run("all", func(t *testing.T) {
					rsp = internal.GetBalancesAggregated(api, url.Values{})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)
					assert.Equal(t, ok, true)
					assert.Equal(t, core.AssetsBalances{"USD": core.NewMonetaryInt(0)}, resp)
				})

				t.Run("filter by address", func(t *testing.T) {
					rsp = internal.GetBalancesAggregated(api, url.Values{"address": []string{"world"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)
					assert.Equal(t, true, ok)
					assert.Equal(t, core.AssetsBalances{"USD": core.NewMonetaryInt(-250)}, resp)
				})

				t.Run("filter by address no result", func(t *testing.T) {
					rsp = internal.GetBalancesAggregated(api, url.Values{"address": []string{"XXX"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)
					assert.Equal(t, ok, true)
					assert.Equal(t, core.AssetsBalances{}, resp)
				})

				return nil
			},
		})
	}))
}

func TestGetBalances(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(150),
							Asset:       "USD",
						},
					},
				}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bob",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
				}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(200),
							Asset:       "CAD",
						},
					},
				}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(400),
							Asset:       "EUR",
						},
					},
				}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				to := ledgerstore.BalancesPaginationToken{}
				raw, err := json.Marshal(to)
				require.NoError(t, err)

				t.Run("valid empty "+controllers.QueryKeyCursor, func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{
						controllers.QueryKeyCursor: []string{base64.RawURLEncoding.EncodeToString(raw)},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				})

				t.Run(fmt.Sprintf("valid empty %s with any other param is forbidden", controllers.QueryKeyCursor), func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{
						controllers.QueryKeyCursor: []string{base64.RawURLEncoding.EncodeToString(raw)},
						"after":                    []string{"bob"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())
				})

				t.Run(fmt.Sprintf("invalid %s", controllers.QueryKeyCursor), func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{
						controllers.QueryKeyCursor: []string{"invalid"},
					})

					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())
					assert.Contains(t, rsp.Body.String(),
						fmt.Sprintf(`"invalid '%s' query param"`, controllers.QueryKeyCursor))
				})

				t.Run("all", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
					assert.Equal(t, []core.AccountsBalances{
						{"world": core.AssetsBalances{"USD": core.NewMonetaryInt(-250), "EUR": core.NewMonetaryInt(-400), "CAD": core.NewMonetaryInt(-200)}},
						{"bob": core.AssetsBalances{"USD": core.NewMonetaryInt(100)}},
						{"alice": core.AssetsBalances{"USD": core.NewMonetaryInt(150), "EUR": core.NewMonetaryInt(400), "CAD": core.NewMonetaryInt(200)}},
					}, resp.Data)
				})

				t.Run("after address", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{"after": []string{"bob"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
					assert.Equal(t, []core.AccountsBalances{
						{"alice": core.AssetsBalances{"USD": core.NewMonetaryInt(150), "EUR": core.NewMonetaryInt(400), "CAD": core.NewMonetaryInt(200)}},
					}, resp.Data)
				})

				t.Run("filter by address", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{"address": []string{"world"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
					assert.Equal(t, []core.AccountsBalances{
						{"world": core.AssetsBalances{"USD": core.NewMonetaryInt(-250), "EUR": core.NewMonetaryInt(-400), "CAD": core.NewMonetaryInt(-200)}},
					}, resp.Data)
				})

				t.Run("filter by address no results", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{"address": []string{"TEST"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
					assert.Equal(t, []core.AccountsBalances{}, resp.Data)
				})

				return nil
			},
		})
	}))
}
