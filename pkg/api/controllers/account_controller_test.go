package controllers_test

import (
	"context"
	"encoding/json"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"net/http"
	"net/url"
	"testing"
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
				if !assert.Equal(t, http.StatusOK, rsp.Result().StatusCode) {
					return nil
				}

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
				if !assert.Equal(t, http.StatusOK, rsp.Result().StatusCode) {
					return nil
				}

				rsp = internal.PostAccountMetadata(t, h, "bob", core.Metadata{
					"roles":     json.RawMessage(`"admin"`),
					"accountId": json.RawMessage("3"),
					"enabled":   json.RawMessage(`"true"`),
					"a":         json.RawMessage(`{"nested": {"key": "hello"}}`),
				})
				if !assert.Equal(t, http.StatusNoContent, rsp.Result().StatusCode) {
					return nil
				}

				rsp = internal.CountAccounts(h, url.Values{})
				if !assert.Equal(t, http.StatusOK, rsp.Result().StatusCode) {
					return nil
				}
				if !assert.Equal(t, "3", rsp.Header().Get("Count")) {
					return nil
				}

				rsp = internal.GetAccounts(h, url.Values{})
				if !assert.Equal(t, http.StatusOK, rsp.Result().StatusCode) {
					return nil
				}

				cursor := internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				if !assert.Len(t, cursor.Data, 3) {
					return nil
				}

				rsp = internal.GetAccounts(h, url.Values{
					"metadata[roles]": []string{"admin"},
				})
				if !assert.Equal(t, http.StatusOK, rsp.Result().StatusCode) {
					return nil
				}

				cursor = internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				if !assert.Len(t, cursor.Data, 1) {
					return nil
				}

				rsp = internal.GetAccounts(h, url.Values{
					"metadata[accountId]": []string{"3"},
				})
				if !assert.Equal(t, http.StatusOK, rsp.Result().StatusCode) {
					return nil
				}

				cursor = internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				if !assert.Len(t, cursor.Data, 1) {
					return nil
				}

				rsp = internal.GetAccounts(h, url.Values{
					"metadata[enabled]": []string{"true"},
				})
				if !assert.Equal(t, http.StatusOK, rsp.Result().StatusCode) {
					return nil
				}

				cursor = internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				if !assert.Len(t, cursor.Data, 1) {
					return nil
				}

				rsp = internal.GetAccounts(h, url.Values{
					"metadata[a.nested.key]": []string{"hello"},
				})
				if !assert.Equal(t, http.StatusOK, rsp.Result().StatusCode) {
					return nil
				}

				cursor = internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				if !assert.Len(t, cursor.Data, 1) {
					return nil
				}

				rsp = internal.GetAccounts(h, url.Values{
					"metadata[unknown]": []string{"key"},
				})
				if !assert.Equal(t, http.StatusOK, rsp.Result().StatusCode) {
					return nil
				}

				cursor = internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
				if !assert.Len(t, cursor.Data, 0) {
					return nil
				}
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
