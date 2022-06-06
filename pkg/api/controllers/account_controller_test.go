package controllers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

type GetAccountsCursor struct {
	PageSize int            `json:"page_size,omitempty"`
	HasMore  bool           `json:"has_more"`
	Previous string         `json:"previous,omitempty"`
	Next     string         `json:"next,omitempty"`
	Data     []core.Account `json:"data"`
}

type getAccountsResponse struct {
	Cursor *GetAccountsCursor `json:"cursor,omitempty"`
}

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

				rsp = internal.GetAccounts(api, url.Values{})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp := getAccountsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				// 3 accounts: world, bob, alice
				assert.Len(t, resp.Cursor.Data, 3)
				assert.Equal(t, resp.Cursor.Data[0].Address, "world")
				assert.Equal(t, resp.Cursor.Data[1].Address, "bob")
				assert.Equal(t, resp.Cursor.Data[2].Address, "alice")

				rsp = internal.GetAccounts(api, url.Values{
					"metadata[roles]": []string{"admin"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp = getAccountsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				// 1 accounts: bob
				assert.Len(t, resp.Cursor.Data, 1)
				assert.Equal(t, resp.Cursor.Data[0].Address, "bob")

				rsp = internal.GetAccounts(api, url.Values{
					"metadata[accountId]": []string{"3"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp = getAccountsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				// 1 accounts: bob
				assert.Len(t, resp.Cursor.Data, 1)
				assert.Equal(t, resp.Cursor.Data[0].Address, "bob")

				rsp = internal.GetAccounts(api, url.Values{
					"metadata[enabled]": []string{"true"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp = getAccountsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				// 1 accounts: bob
				assert.Len(t, resp.Cursor.Data, 1)
				assert.Equal(t, resp.Cursor.Data[0].Address, "bob")

				rsp = internal.GetAccounts(api, url.Values{
					"metadata[a.nested.key]": []string{"hello"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp = getAccountsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				// 1 accounts: bob
				assert.Len(t, resp.Cursor.Data, 1)
				assert.Equal(t, resp.Cursor.Data[0].Address, "bob")

				rsp = internal.GetAccounts(api, url.Values{
					"metadata[unknown]": []string{"key"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				assert.Len(t, cursor.Data, 0)

				rsp = internal.GetAccounts(api, url.Values{
					"after": []string{"bob"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp = getAccountsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				// 1 accounts: alice
				assert.Len(t, resp.Cursor.Data, 1)
				assert.Equal(t, resp.Cursor.Data[0].Address, "alice")

				rsp = internal.GetAccounts(api, url.Values{
					"address": []string{"b.b"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp = getAccountsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				// 1 accounts: bob
				assert.Len(t, resp.Cursor.Data, 1)
				assert.Equal(t, resp.Cursor.Data[0].Address, "bob")

				return nil
			},
		})
	}))
}

func TestGetAccount(t *testing.T) {
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
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostAccountMetadata(t, h, "alice", core.Metadata{
					"foo": json.RawMessage(`"bar"`),
				})
				require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

				rsp = internal.GetAccount(h, "alice")
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp := core.Account{}
				internal.DecodeSingleResponse(t, rsp.Body, &resp)

				assert.EqualValues(t, core.Account{
					Address: "alice",
					Type:    "",
					Balances: map[string]int64{
						"USD": 100,
					},
					Volumes: map[string]map[string]int64{
						"USD": {
							"input":  100,
							"output": 0,
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
