package controllers_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

// This test makes sense if maxAdditionalTxs < pageSize
const (
	pageSize         = 10
	maxTxsPages      = 3
	maxAdditionalTxs = 2
)

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

		numTxs := txsPages*pageSize + additionalTxs
		for i := 0; i < numTxs; i++ {
			rsp = internal.PostTransaction(t, api, core.TransactionData{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: fmt.Sprintf("accounts:%06d", i),
						Amount:      10,
						Asset:       "USD",
					},
				},
				Reference: fmt.Sprintf("ref:%06d", i),
			})
			require.Equal(t, http.StatusOK, rsp.Code, rsp.Body.String())
		}

		t.Run("transactions", func(t *testing.T) {
			rsp = internal.CountTransactions(api, url.Values{})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			require.Equal(t, fmt.Sprintf("%d", numTxs), rsp.Header().Get("Count"))

			var paginationToken string
			cursor := &sharedapi.Cursor[core.Transaction]{}

			// MOVING FORWARD
			for i := 0; i < txsPages; i++ {

				values := url.Values{}
				if paginationToken == "" {
					values.Set("page_size", fmt.Sprintf("%d", pageSize))
				} else {
					values.Set("pagination_token", paginationToken)
				}

				rsp = internal.GetTransactions(api, values)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
				assert.Len(t, cursor.Data, pageSize)
				assert.Equal(t, cursor.Next != "", cursor.HasMore)

				// First txid of the page
				assert.Equal(t,
					uint64((txsPages-i)*pageSize+additionalTxs-1), cursor.Data[0].ID)

				// Last txid of the page
				assert.Equal(t,
					uint64((txsPages-i-1)*pageSize+additionalTxs), cursor.Data[len(cursor.Data)-1].ID)

				paginationToken = cursor.Next
			}

			if additionalTxs > 0 {
				rsp = internal.GetTransactions(api, url.Values{
					"pagination_token": []string{paginationToken},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				cursor = internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
				assert.Len(t, cursor.Data, additionalTxs)
				assert.Equal(t, cursor.Next != "", cursor.HasMore)

				// First txid of the last page
				assert.Equal(t,
					uint64(additionalTxs-1), cursor.Data[0].ID)

				// Last txid of the last page
				assert.Equal(t,
					uint64(0), cursor.Data[len(cursor.Data)-1].ID)
			}

			assert.Empty(t, cursor.Next)

			// MOVING BACKWARD
			if txsPages > 0 {
				back := 0
				for cursor.Previous != "" {
					paginationToken = cursor.Previous
					rsp = internal.GetTransactions(api, url.Values{
						"pagination_token": []string{paginationToken},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor = internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
					assert.Len(t, cursor.Data, pageSize)
					assert.Equal(t, cursor.Next != "", cursor.HasMore)
					back++
				}
				if additionalTxs > 0 {
					assert.Equal(t, txsPages, back)
				} else {
					assert.Equal(t, txsPages-1, back)
				}

				// First txid of the first page
				assert.Equal(t,
					uint64(txsPages*pageSize+additionalTxs-1), cursor.Data[0].ID)

				// Last txid of the first page
				assert.Equal(t,
					uint64((txsPages-1)*pageSize+additionalTxs), cursor.Data[len(cursor.Data)-1].ID)
			}

			assert.Empty(t, cursor.Previous)
		})

		t.Run("accounts", func(t *testing.T) {
			numAcc := 0
			if numTxs > 0 {
				numAcc = numTxs + 1 // + world account
			}
			rsp = internal.CountAccounts(api, url.Values{})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			require.Equal(t, fmt.Sprintf("%d", numAcc), rsp.Header().Get("Count"))

			accPages := numAcc / pageSize
			additionalAccs := numAcc % pageSize
			fmt.Printf("numAcc:%d accPages:%d addi:%d\n", numAcc, accPages, additionalAccs)

			var paginationToken string
			cursor := &sharedapi.Cursor[core.Account]{}

			// MOVING FORWARD
			for i := 0; i < accPages; i++ {

				values := url.Values{}
				if paginationToken == "" {
					values.Set("page_size", fmt.Sprintf("%d", pageSize))
				} else {
					values.Set("pagination_token", paginationToken)
				}

				rsp = internal.GetAccounts(api, values)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[core.Account](t, rsp.Body)
				assert.Len(t, cursor.Data, pageSize)
				assert.Equal(t, cursor.Next != "", cursor.HasMore)

				// First account of the page
				if i == 0 {
					assert.Equal(t, "world",
						cursor.Data[0].Address)
				} else {
					assert.Equal(t,
						fmt.Sprintf("accounts:%06d", (accPages-i)*pageSize+additionalAccs-1),
						cursor.Data[0].Address)
				}

				// Last account of the page
				assert.Equal(t,
					fmt.Sprintf("accounts:%06d", (accPages-i-1)*pageSize+additionalAccs),
					cursor.Data[len(cursor.Data)-1].Address)

				paginationToken = cursor.Next
			}

			if additionalAccs > 0 {
				rsp = internal.GetAccounts(api, url.Values{
					"pagination_token": []string{paginationToken},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				cursor = internal.DecodeCursorResponse[core.Account](t, rsp.Body)
				assert.Len(t, cursor.Data, additionalAccs)
				assert.Equal(t, cursor.Next != "", cursor.HasMore)

				// First account of the last page
				if accPages == 0 {
					assert.Equal(t, "world",
						cursor.Data[0].Address)
				} else {
					assert.Equal(t,
						fmt.Sprintf("accounts:%06d", additionalAccs-1),
						cursor.Data[0].Address)
				}

				// Last account of the last page
				assert.Equal(t,
					fmt.Sprintf("accounts:%06d", 0),
					cursor.Data[len(cursor.Data)-1].Address)
			}

			assert.Empty(t, cursor.Next)

			// MOVING BACKWARD
			if accPages > 0 {
				back := 0
				for cursor.Previous != "" {
					paginationToken = cursor.Previous
					rsp = internal.GetAccounts(api, url.Values{
						"pagination_token": []string{paginationToken},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
					cursor = internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, pageSize)
					assert.Equal(t, cursor.Next != "", cursor.HasMore)
					back++
				}
				if additionalAccs > 0 {
					assert.Equal(t, accPages, back)
				} else {
					assert.Equal(t, accPages-1, back)
				}

				// First account of the first page
				assert.Equal(t, "world",
					cursor.Data[0].Address)

				// Last account of the first page
				assert.Equal(t,
					fmt.Sprintf("accounts:%06d", (txsPages-1)*pageSize+additionalTxs+1),
					cursor.Data[len(cursor.Data)-1].Address)
			}

			assert.Empty(t, cursor.Previous)
		})

		return nil
	}
}
