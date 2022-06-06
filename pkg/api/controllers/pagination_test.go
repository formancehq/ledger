package controllers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

var maxTxsPages = 3
var maxAdditionalTxs = 2

func TestGetPagination(t *testing.T) {
	for txsPages := 0; txsPages <= maxTxsPages; txsPages++ {
		for additionalTxs := 0; additionalTxs <= maxAdditionalTxs; additionalTxs++ {
			t.Run(fmt.Sprintf("%d-pages-%d-additional", txsPages, additionalTxs), func(t *testing.T) {
				internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
					lc.Append(fx.Hook{
						OnStart: getPagination(t, api, txsPages, additionalTxs),
					})
				}))
			})
		}
	}
}

func getPagination(t *testing.T, api *api.API, txsPages, additionalTxs int) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		var rsp *httptest.ResponseRecorder

		numTxs := txsPages*query.DefaultLimit + additionalTxs
		for i := 0; i < numTxs; i++ {
			rsp = internal.PostTransaction(t, api, core.TransactionData{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: fmt.Sprintf("accounts:%d", i),
						Amount:      10,
						Asset:       "USD",
					},
				},
				Reference: fmt.Sprintf("ref:%d", i),
			})
			require.Equal(t, http.StatusOK, rsp.Code, rsp.Body.String())
		}

		// TRANSACTIONS

		rsp = internal.CountTransactions(api, url.Values{})
		require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
		require.Equal(t, fmt.Sprintf("%d", numTxs), rsp.Header().Get("Count"))

		paginationToken := ""
		rt := getTransactionsResponse{}
		for i := 0; i < txsPages; i++ {
			rsp = internal.GetTransactions(api, url.Values{
				"pagination_token": []string{paginationToken},
			})
			assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			rt = getTransactionsResponse{}
			assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &rt))
			assert.Len(t, rt.Cursor.Data, query.DefaultLimit)

			// First txid of the page
			assert.Equal(t,
				uint64((txsPages-i)*query.DefaultLimit+additionalTxs-1), rt.Cursor.Data[0].ID)

			// Last txid of the page
			assert.Equal(t,
				uint64((txsPages-i-1)*query.DefaultLimit+additionalTxs), rt.Cursor.Data[len(rt.Cursor.Data)-1].ID)

			paginationToken = rt.Cursor.Next
		}

		if additionalTxs > 0 {
			rsp = internal.GetTransactions(api, url.Values{
				"pagination_token": []string{paginationToken},
			})
			assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
			rt = getTransactionsResponse{}
			assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &rt))
			assert.Len(t, rt.Cursor.Data, additionalTxs)

			// First txid of the last page
			assert.Equal(t,
				uint64(additionalTxs-1), rt.Cursor.Data[0].ID)

			// Last txid of the last page
			assert.Equal(t,
				uint64(0), rt.Cursor.Data[len(rt.Cursor.Data)-1].ID)
		}

		if txsPages > 0 {
			for i := 0; i < txsPages; i++ {
				paginationToken = rt.Cursor.Previous
				rsp = internal.GetTransactions(api, url.Values{
					"pagination_token": []string{paginationToken},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				rt = getTransactionsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &rt))
				assert.Len(t, rt.Cursor.Data, query.DefaultLimit)
			}

			// First txid of the first page
			assert.Equal(t,
				uint64(txsPages*query.DefaultLimit+additionalTxs-1), rt.Cursor.Data[0].ID)

			// Last txid of the first page
			assert.Equal(t,
				uint64((txsPages-1)*query.DefaultLimit+additionalTxs), rt.Cursor.Data[len(rt.Cursor.Data)-1].ID)
		}

		// ACCOUNTS

		numAcc := 0
		if numTxs > 0 {
			numAcc = numTxs + 1 // + world account
		}
		rsp = internal.CountAccounts(api, url.Values{})
		require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
		require.Equal(t, fmt.Sprintf("%d", numAcc), rsp.Header().Get("Count"))

		rsp = internal.GetAccounts(api, url.Values{})
		assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
		ra := getAccountsResponse{}
		assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &ra))
		assert.Len(t, ra.Cursor.Data, query.DefaultLimit)

		spew.Dump(ra)

		return nil
	}
}
