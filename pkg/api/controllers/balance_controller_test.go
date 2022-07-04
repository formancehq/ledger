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

				rsp = internal.PostAccountMetadata(t, api, "bob", core.Metadata{
					"roles":     json.RawMessage(`"admin"`),
					"accountId": json.RawMessage("3"),
					"enabled":   json.RawMessage(`"true"`),
					"a":         json.RawMessage(`{"nested": {"key": "hello"}}`),
				})
				require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

				rsp = internal.CountAccounts(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				require.Equal(t, "3", rsp.Header().Get("Count"))

				t.Run("success full", func(t *testing.T) {
					rsp = internal.GetBalancesAggregated(api, url.Values{})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)

					assert.Equal(t, ok, true)

					assert.Equal(t, resp["USD"], int64(0))
				})

				t.Run("after bob", func(t *testing.T) {
					rsp = internal.GetBalancesAggregated(api, url.Values{"after": []string{"bob"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)

					assert.Equal(t, ok, true)

					assert.Equal(t, resp["USD"], int64(0))
				})

				t.Run("after world", func(t *testing.T) {
					rsp = internal.GetBalancesAggregated(api, url.Values{"after": []string{"world"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)

					assert.Equal(t, ok, true)
					assert.Equal(t, resp["USD"], int64(0))
				})

				t.Run("success account world", func(t *testing.T) {
					rsp = internal.GetBalancesAggregated(api, url.Values{"address": []string{"world"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)

					assert.Equal(t, true, ok)

					assert.Equal(t, int64(-250), resp["USD"])
				})

				t.Run("no  result", func(t *testing.T) {
					rsp = internal.GetBalancesAggregated(api, url.Values{"address": []string{"XXX"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)

					assert.Equal(t, ok, true)
					assert.Len(t, resp, 0)
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

				rsp = internal.PostAccountMetadata(t, api, "bob", core.Metadata{
					"roles":     json.RawMessage(`"admin"`),
					"accountId": json.RawMessage("3"),
					"enabled":   json.RawMessage(`"true"`),
					"a":         json.RawMessage(`{"nested": {"key": "hello"}}`),
				})
				require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

				rsp = internal.CountAccounts(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				require.Equal(t, "3", rsp.Header().Get("Count"))

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

				t.Run("success full", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)

					assert.Len(t, resp.Data, 3)
					assert.Equal(t, resp.Data[0]["world"]["USD"], int64(-250))
					assert.Equal(t, resp.Data[0]["world"]["EUR"], int64(-400))
					assert.Equal(t, resp.Data[0]["world"]["CAD"], int64(-200))
					assert.Equal(t, resp.Data[1]["bob"]["USD"], int64(100))
					assert.Equal(t, resp.Data[2]["alice"]["USD"], int64(150))
					assert.Equal(t, resp.Data[2]["alice"]["EUR"], int64(400))
					assert.Equal(t, resp.Data[2]["alice"]["CAD"], int64(200))

				})

				t.Run("after bob", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{"after": []string{"bob"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)

					assert.Len(t, resp.Data, 1)
					assert.Equal(t, resp.Data[0]["alice"]["USD"], int64(150))
					assert.Equal(t, resp.Data[0]["alice"]["EUR"], int64(400))
					assert.Equal(t, resp.Data[0]["alice"]["CAD"], int64(200))
				})

				t.Run("account world", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{"address": []string{"world"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)

					assert.Len(t, resp.Data, 1)
					assert.Equal(t, resp.Data[0]["world"]["USD"], int64(-250))
					assert.Equal(t, resp.Data[0]["world"]["EUR"], int64(-400))
					assert.Equal(t, resp.Data[0]["world"]["CAD"], int64(-200))
				})

				t.Run("no result", func(t *testing.T) {
					rsp = internal.GetBalances(api, url.Values{"address": []string{"TEST"}})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)

					assert.Len(t, resp.Data, 0)
				})

				return nil
			},
		})
	}))
}
