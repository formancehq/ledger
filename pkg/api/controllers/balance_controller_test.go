package controllers_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestGetBalancesAggregated(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      150,
							Asset:       "USD",
						},
					},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bob",
							Amount:      100,
							Asset:       "USD",
						},
					},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				t.Run("all", func(t *testing.T) {
					rsp = internal.GetBalancesAggregated(api, url.Values{})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)
					assert.Equal(t, ok, true)
					assert.Equal(t, core.AssetsBalances{"USD": 0}, resp)
				})

				t.Run("filter by address", func(t *testing.T) {
					rsp = internal.GetBalancesAggregated(api, url.Values{"address": []string{"world"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)
					assert.Equal(t, true, ok)
					assert.Equal(t, core.AssetsBalances{"USD": -250}, resp)
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
				rsp := internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      150,
							Asset:       "USD",
						},
					},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bob",
							Amount:      100,
							Asset:       "USD",
						},
					},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      200,
							Asset:       "CAD",
						},
					},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      400,
							Asset:       "EUR",
						},
					},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				to := sqlstorage.BalancesPaginationToken{}
				raw, err := json.Marshal(to)
				require.NoError(t, err)
				t.Run("valid empty pagination_token", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{
						"pagination_token": []string{base64.RawURLEncoding.EncodeToString(raw)},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				})

				t.Run("valid empty pagination_token with any other param is forbidden", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{
						"pagination_token": []string{base64.RawURLEncoding.EncodeToString(raw)},
						"after":            []string{"bob"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())
				})

				t.Run("invalid pagination_token", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{
						"pagination_token": []string{"invalid"},
					})

					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())
					assert.Contains(t, rsp.Body.String(), `error_message":"invalid query value 'pagination_token'"`)
				})

				t.Run("all", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
					assert.Equal(t, []core.AccountsBalances{
						{"world": core.AssetsBalances{"USD": -250, "EUR": -400, "CAD": -200}},
						{"bob": core.AssetsBalances{"USD": 100}},
						
						{"alice": core.AssetsBalances{"USD": 150, "EUR": 400, "CAD": 200}},
					}, resp.Data)
				})

				t.Run("after address", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{"after": []string{"bob"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
					assert.Equal(t, []core.AccountsBalances{
						{"alice": core.AssetsBalances{"USD": 150, "EUR": 400, "CAD": 200}},
					}, resp.Data)
				})

				t.Run("filter by address", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{"address": []string{"world"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
					assert.Equal(t, []core.AccountsBalances{
						{"world": core.AssetsBalances{"USD": -250, "EUR": -400, "CAD": -200}},
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
