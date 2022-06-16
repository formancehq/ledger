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

func TestGetAccounts(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
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

				t.Run("all", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 3 accounts: world, bob, alice
					assert.Len(t, cursor.Data, 3)
					assert.Equal(t, cursor.Data[0].Address, "world")
					assert.Equal(t, cursor.Data[1].Address, "bob")
					assert.Equal(t, cursor.Data[2].Address, "alice")
				})

				t.Run("meta roles", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"metadata[roles]": []string{"admin"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: bob
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "bob")
				})

				t.Run("meta accountId", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"metadata[accountId]": []string{"3"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: bob
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "bob")
				})

				t.Run("meta enabled", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"metadata[enabled]": []string{"true"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: bob
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "bob")
				})

				t.Run("meta nested", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"metadata[a.nested.key]": []string{"hello"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: bob
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "bob")
				})

				t.Run("meta unknown", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"metadata[unknown]": []string{"key"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, 0)
				})

				t.Run("after", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"after": []string{"bob"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: alice
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "alice")
				})

				t.Run("address", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"address": []string{"b.b"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: bob
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "bob")
				})

				to := sqlstorage.AccPaginationToken{}
				raw, err := json.Marshal(to)
				require.NoError(t, err)
				t.Run("valid empty pagination_token", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"pagination_token": []string{base64.RawURLEncoding.EncodeToString(raw)},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				})

				t.Run("valid empty pagination_token with any other param is forbidden", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"pagination_token": []string{base64.RawURLEncoding.EncodeToString(raw)},
						"after":            []string{"bob"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())
				})

				t.Run("invalid pagination_token", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"pagination_token": []string{"invalid"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())
				})

				return nil
			},
		})
	}))
}

func TestGetAccount(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      100,
							Asset:       "USD",
						},
					},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostAccountMetadata(t, api, "alice", core.Metadata{
					"foo": json.RawMessage(`"bar"`),
				})
				require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

				rsp = internal.GetAccount(api, "alice")
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp, _ := internal.DecodeSingleResponse[core.Account](t, rsp.Body)

				assert.EqualValues(t, core.Account{
					Address: "alice",
					Type:    "",
					Balances: map[string]int64{
						"USD": 100,
					},
					Volumes: core.AssetsVolumes{
						"USD": {
							Input: 100,
						},
					},
					Metadata: core.Metadata{
						"foo": json.RawMessage(`"bar"`),
					},
				}, resp)

				return nil
			},
		})
	}))
}
