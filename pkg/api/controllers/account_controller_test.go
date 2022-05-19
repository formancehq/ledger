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
	"go.uber.org/fx"
)

func TestGetAccounts(t *testing.T) {
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
							Destination: "bob",
							Amount:      100,
							Asset:       "USD",
						},
					},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostAccountMetadata(t, h, "bob", core.Metadata{
					"roles":     json.RawMessage(`"admin"`),
					"accountId": json.RawMessage("3"),
					"enabled":   json.RawMessage(`"true"`),
					"a":         json.RawMessage(`{"nested": {"key": "hello"}}`),
				})
				assert.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

				rsp = internal.CountAccounts(h, url.Values{})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				assert.Equal(t, "3", rsp.Header().Get("Count"))

				rsp = internal.GetAccounts(h, url.Values{})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				cursor := internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				assert.Len(t, cursor.Data, 3)

				rsp = internal.GetAccounts(h, url.Values{
					"metadata[roles]": []string{"admin"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				cursor = internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				assert.Len(t, cursor.Data, 1)

				rsp = internal.GetAccounts(h, url.Values{
					"metadata[accountId]": []string{"3"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				cursor = internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				assert.Len(t, cursor.Data, 1)

				rsp = internal.GetAccounts(h, url.Values{
					"metadata[enabled]": []string{"true"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				cursor = internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				assert.Len(t, cursor.Data, 1)

				rsp = internal.GetAccounts(h, url.Values{
					"metadata[a.nested.key]": []string{"hello"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				cursor = internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				assert.Len(t, cursor.Data, 1)

				rsp = internal.GetAccounts(h, url.Values{
					"metadata[unknown]": []string{"key"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				cursor = internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				assert.Len(t, cursor.Data, 0)

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
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostAccountMetadata(t, h, "alice", core.Metadata{
					"foo": json.RawMessage(`"bar"`),
				})
				assert.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

				rsp = internal.GetAccount(h, "alice")
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				act := core.Account{}
				internal.DecodeSingleResponse(t, rsp.Body, &act)

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
				}, act)

				return nil
			},
		})
	}))
}
