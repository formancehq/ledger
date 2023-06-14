package controllers_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
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
						OnStart: testGetPagination(t, api, txsPages, additionalTxs),
					})
				}))
			})
		}
	}
}

func testGetPagination(t *testing.T, api *api.API, txsPages, additionalTxs int) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		numTxs := txsPages*pageSize + additionalTxs
		if numTxs > 0 {
			txsData := make([]core.TransactionData, numTxs)
			for i := 0; i < numTxs; i++ {
				txsData[i] = core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: fmt.Sprintf("accounts:%06d", i),
							Amount:      core.NewMonetaryInt(10),
							Asset:       "USD",
						},
					},
					Reference: fmt.Sprintf("ref:%06d", i),
				}
			}
			rsp := internal.PostTransactionBatch(t, api, core.Transactions{Transactions: txsData})
			require.Equal(t, http.StatusOK, rsp.Code, rsp.Body.String())
		}

		rsp := internal.CountTransactions(api, url.Values{})
		require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
		require.Equal(t, fmt.Sprintf("%d", numTxs), rsp.Header().Get("Count"))

		numAcc := 0
		if numTxs > 0 {
			numAcc = numTxs + 1 // + world account
		}
		rsp = internal.CountAccounts(api, url.Values{})
		require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
		require.Equal(t, fmt.Sprintf("%d", numAcc), rsp.Header().Get("Count"))

		accPages := numAcc / pageSize
		additionalAccs := numAcc % pageSize

		t.Run("transactions", func(t *testing.T) {
			var paginationToken string
			cursor := &sharedapi.Cursor[core.ExpandedTransaction]{}

			// MOVING FORWARD
			for i := 0; i < txsPages; i++ {

				values := url.Values{}
				if paginationToken == "" {
					values.Set(controllers.QueryKeyPageSize, fmt.Sprintf("%d", pageSize))
				} else {
					values.Set(controllers.QueryKeyCursor, paginationToken)
				}

				rsp = internal.GetTransactions(api, values)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
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
					controllers.QueryKeyCursor: []string{paginationToken},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				cursor = internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
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
						controllers.QueryKeyCursor: []string{paginationToken},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor = internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
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
			var paginationToken string
			cursor := &sharedapi.Cursor[core.Account]{}

			// MOVING FORWARD
			for i := 0; i < accPages; i++ {

				values := url.Values{}
				if paginationToken == "" {
					values.Set(controllers.QueryKeyPageSize, fmt.Sprintf("%d", pageSize))
				} else {
					values.Set(controllers.QueryKeyCursor, paginationToken)
				}

				rsp = internal.GetAccounts(api, values)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[core.Account](t, rsp.Body)
				assert.Len(t, cursor.Data, pageSize)
				assert.Equal(t, cursor.Next != "", cursor.HasMore)

				// First account of the page
				if i == 0 {
					assert.Equal(t, "world",
						string(cursor.Data[0].Address))
				} else {
					assert.Equal(t,
						fmt.Sprintf("accounts:%06d", (accPages-i)*pageSize+additionalAccs-1),
						string(cursor.Data[0].Address))
				}

				// Last account of the page
				assert.Equal(t,
					fmt.Sprintf("accounts:%06d", (accPages-i-1)*pageSize+additionalAccs),
					string(cursor.Data[len(cursor.Data)-1].Address))

				paginationToken = cursor.Next
			}

			if additionalAccs > 0 {
				rsp = internal.GetAccounts(api, url.Values{
					controllers.QueryKeyCursor: []string{paginationToken},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				cursor = internal.DecodeCursorResponse[core.Account](t, rsp.Body)
				assert.Len(t, cursor.Data, additionalAccs)
				assert.Equal(t, cursor.Next != "", cursor.HasMore)

				// First account of the last page
				if accPages == 0 {
					assert.Equal(t, "world",
						string(cursor.Data[0].Address))
				} else {
					assert.Equal(t,
						fmt.Sprintf("accounts:%06d", additionalAccs-1),
						string(cursor.Data[0].Address))
				}

				// Last account of the last page
				assert.Equal(t,
					fmt.Sprintf("accounts:%06d", 0),
					string(cursor.Data[len(cursor.Data)-1].Address))
			}

			assert.Empty(t, cursor.Next)

			// MOVING BACKWARD
			if accPages > 0 {
				back := 0
				for cursor.Previous != "" {
					paginationToken = cursor.Previous
					rsp = internal.GetAccounts(api, url.Values{
						controllers.QueryKeyCursor: []string{paginationToken},
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
					string(cursor.Data[0].Address))

				// Last account of the first page
				assert.Equal(t,
					fmt.Sprintf("accounts:%06d", (txsPages-1)*pageSize+additionalTxs+1),
					string(cursor.Data[len(cursor.Data)-1].Address))
			}

			assert.Empty(t, cursor.Previous)
		})

		t.Run("balances", func(t *testing.T) {
			var paginationToken string
			cursor := &sharedapi.Cursor[core.AccountsBalances]{}

			// MOVING FORWARD
			for i := 0; i < accPages; i++ {

				values := url.Values{}
				if paginationToken == "" {
					values.Set(controllers.QueryKeyPageSize, fmt.Sprintf("%d", pageSize))
				} else {
					values.Set(controllers.QueryKeyCursor, paginationToken)
				}

				rsp = internal.GetBalances(api, values)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
				assert.Len(t, cursor.Data, pageSize)
				assert.Equal(t, cursor.Next != "", cursor.HasMore)

				// First account balances of the page
				if i == 0 {
					_, ok := cursor.Data[0]["world"]
					assert.True(t, ok)
				} else {
					_, ok := cursor.Data[0][fmt.Sprintf(
						"accounts:%06d", (accPages-i)*pageSize+additionalAccs-1)]
					assert.True(t, ok)
				}

				// Last account balances of the page
				_, ok := cursor.Data[len(cursor.Data)-1][fmt.Sprintf(
					"accounts:%06d", (accPages-i-1)*pageSize+additionalAccs)]
				assert.True(t, ok)

				paginationToken = cursor.Next
			}

			if additionalAccs > 0 {
				rsp = internal.GetBalances(api, url.Values{
					controllers.QueryKeyCursor: []string{paginationToken},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				cursor = internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
				assert.Len(t, cursor.Data, additionalAccs)
				assert.Equal(t, cursor.Next != "", cursor.HasMore)

				// First account balances of the last page
				if accPages == 0 {
					_, ok := cursor.Data[0]["world"]
					assert.True(t, ok)
				} else {
					_, ok := cursor.Data[0][fmt.Sprintf(
						"accounts:%06d", additionalAccs-1)]
					assert.True(t, ok)
				}

				// Last account balances of the last page
				_, ok := cursor.Data[len(cursor.Data)-1][fmt.Sprintf(
					"accounts:%06d", 0)]
				assert.True(t, ok)
			}

			// MOVING BACKWARD
			if accPages > 0 {
				back := 0
				for cursor.Previous != "" {
					paginationToken = cursor.Previous
					rsp = internal.GetBalances(api, url.Values{
						controllers.QueryKeyCursor: []string{paginationToken},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
					cursor = internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
					assert.Len(t, cursor.Data, pageSize)
					assert.Equal(t, cursor.Next != "", cursor.HasMore)
					back++
				}
				if additionalAccs > 0 {
					assert.Equal(t, accPages, back)
				} else {
					assert.Equal(t, accPages-1, back)
				}

				// First account balances of the first page
				_, ok := cursor.Data[0]["world"]
				assert.True(t, ok)

				// Last account balances of the first page
				_, ok = cursor.Data[len(cursor.Data)-1][fmt.Sprintf(
					"accounts:%06d", (txsPages-1)*pageSize+additionalTxs+1)]
				assert.True(t, ok)
			}
		})

		t.Run("logs", func(t *testing.T) {
			var paginationToken string
			cursor := &sharedapi.Cursor[core.Log]{}

			// MOVING FORWARD
			for i := 0; i < txsPages; i++ {

				values := url.Values{}
				if paginationToken == "" {
					values.Set(controllers.QueryKeyPageSize, fmt.Sprintf("%d", pageSize))
				} else {
					values.Set(controllers.QueryKeyCursor, paginationToken)
				}

				rsp = internal.GetLedgerLogs(api, values)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[core.Log](t, rsp.Body)
				assert.Len(t, cursor.Data, pageSize)
				assert.Equal(t, cursor.Next != "", cursor.HasMore)

				// First ID of the page
				assert.Equal(t,
					uint64((txsPages-i)*pageSize+additionalTxs-1), cursor.Data[0].ID)

				// Last ID of the page
				assert.Equal(t,
					uint64((txsPages-i-1)*pageSize+additionalTxs), cursor.Data[len(cursor.Data)-1].ID)

				paginationToken = cursor.Next
			}

			if additionalTxs > 0 {
				rsp = internal.GetLedgerLogs(api, url.Values{
					controllers.QueryKeyCursor: []string{paginationToken},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				cursor = internal.DecodeCursorResponse[core.Log](t, rsp.Body)
				assert.Len(t, cursor.Data, additionalTxs)
				assert.Equal(t, cursor.Next != "", cursor.HasMore)

				// First ID of the last page
				assert.Equal(t,
					uint64(additionalTxs-1), cursor.Data[0].ID)

				// Last ID of the last page
				assert.Equal(t,
					uint64(0), cursor.Data[len(cursor.Data)-1].ID)
			}

			assert.Empty(t, cursor.Next)

			// MOVING BACKWARD
			if txsPages > 0 {
				back := 0
				for cursor.Previous != "" {
					paginationToken = cursor.Previous
					rsp = internal.GetLedgerLogs(api, url.Values{
						controllers.QueryKeyCursor: []string{paginationToken},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor = internal.DecodeCursorResponse[core.Log](t, rsp.Body)
					assert.Len(t, cursor.Data, pageSize)
					assert.Equal(t, cursor.Next != "", cursor.HasMore)
					back++
				}
				if additionalTxs > 0 {
					assert.Equal(t, txsPages, back)
				} else {
					assert.Equal(t, txsPages-1, back)
				}

				// First ID of the first page
				assert.Equal(t,
					uint64(txsPages*pageSize+additionalTxs-1), cursor.Data[0].ID)

				// Last ID of the first page
				assert.Equal(t,
					uint64((txsPages-1)*pageSize+additionalTxs), cursor.Data[len(cursor.Data)-1].ID)
			}

			assert.Empty(t, cursor.Previous)
		})

		return nil
	}
}

func TestCursor(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				timestamp, err := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")
				require.NoError(t, err)
				for i := 0; i < 30; i++ {
					rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
						Postings: core.Postings{
							{
								Source:      "world",
								Destination: fmt.Sprintf("accounts:%02d", i),
								Amount:      core.NewMonetaryInt(1),
								Asset:       "USD",
							},
						},
						Reference: fmt.Sprintf("ref:%02d", i),
						Metadata:  core.Metadata{"ref": "abc"},
						Timestamp: timestamp.Add(time.Duration(i) * time.Second),
					}, false)
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					rsp = internal.PostAccountMetadata(t, api, fmt.Sprintf("accounts:%02d", i),
						core.Metadata{
							"foo": json.RawMessage(`"bar"`),
						})
					require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)
				}

				t.Run("GetAccounts", func(t *testing.T) {
					httpResponse := internal.GetAccounts(api, url.Values{
						"after":                             []string{"accounts:15"},
						"address":                           []string{"accounts:.*"},
						"metadata[foo]":                     []string{"bar"},
						"balance":                           []string{"1"},
						controllers.QueryKeyBalanceOperator: []string{"gte"},
						controllers.QueryKeyPageSize:        []string{"3"},
					})
					assert.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

					cursor := internal.DecodeCursorResponse[core.Account](t, httpResponse.Body)
					res, err := base64.RawURLEncoding.DecodeString(cursor.Next)
					require.NoError(t, err)
					require.Equal(t,
						`{"pageSize":3,"offset":3,"after":"accounts:15","address":"accounts:.*","metadata":{"foo":"bar"},"balance":"1","balanceOperator":"gte"}`,
						string(res))

					httpResponse = internal.GetAccounts(api, url.Values{
						controllers.QueryKeyCursor: []string{cursor.Next},
					})
					assert.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

					cursor = internal.DecodeCursorResponse[core.Account](t, httpResponse.Body)
					res, err = base64.RawURLEncoding.DecodeString(cursor.Previous)
					require.NoError(t, err)
					require.Equal(t,
						`{"pageSize":3,"offset":0,"after":"accounts:15","address":"accounts:.*","metadata":{"foo":"bar"},"balance":"1","balanceOperator":"gte"}`,
						string(res))
					res, err = base64.RawURLEncoding.DecodeString(cursor.Next)
					require.NoError(t, err)
					require.Equal(t,
						`{"pageSize":3,"offset":6,"after":"accounts:15","address":"accounts:.*","metadata":{"foo":"bar"},"balance":"1","balanceOperator":"gte"}`,
						string(res))
				})

				t.Run("GetTransactions", func(t *testing.T) {
					httpResponse := internal.GetTransactions(api, url.Values{
						"after":                       []string{"15"},
						"account":                     []string{"acc.*"},
						"source":                      []string{"world"},
						"destination":                 []string{"acc.*"},
						controllers.QueryKeyStartTime: []string{timestamp.Add(5 * time.Second).Format(time.RFC3339)},
						controllers.QueryKeyEndTime:   []string{timestamp.Add(25 * time.Second).Format(time.RFC3339)},
						"metadata[ref]":               []string{"abc"},
						controllers.QueryKeyPageSize:  []string{"3"},
					})
					assert.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

					cursor := internal.DecodeCursorResponse[core.Transaction](t, httpResponse.Body)
					res, err := base64.RawURLEncoding.DecodeString(cursor.Next)
					require.NoError(t, err)
					require.Equal(t,
						`{"after":12,"account":"acc.*","source":"world","destination":"acc.*","startTime":"2023-01-01T00:00:05Z","endTime":"2023-01-01T00:00:25Z","metadata":{"ref":"abc"},"pageSize":3}`,
						string(res))

					httpResponse = internal.GetTransactions(api, url.Values{
						controllers.QueryKeyCursor: []string{cursor.Next},
					})
					assert.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

					cursor = internal.DecodeCursorResponse[core.Transaction](t, httpResponse.Body)
					res, err = base64.RawURLEncoding.DecodeString(cursor.Previous)
					require.NoError(t, err)
					require.Equal(t,
						`{"after":15,"account":"acc.*","source":"world","destination":"acc.*","startTime":"2023-01-01T00:00:05Z","endTime":"2023-01-01T00:00:25Z","metadata":{"ref":"abc"},"pageSize":3}`,
						string(res))
					res, err = base64.RawURLEncoding.DecodeString(cursor.Next)
					require.NoError(t, err)
					require.Equal(t,
						`{"after":9,"account":"acc.*","source":"world","destination":"acc.*","startTime":"2023-01-01T00:00:05Z","endTime":"2023-01-01T00:00:25Z","metadata":{"ref":"abc"},"pageSize":3}`,
						string(res))
				})

				t.Run("GetBalances", func(t *testing.T) {
					httpResponse := internal.GetBalances(api, url.Values{
						"after":                      []string{"accounts:15"},
						"address":                    []string{"accounts:.*"},
						controllers.QueryKeyPageSize: []string{"3"},
					})
					assert.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

					cursor := internal.DecodeCursorResponse[core.AccountsBalances](t, httpResponse.Body)
					res, err := base64.RawURLEncoding.DecodeString(cursor.Next)
					require.NoError(t, err)
					require.Equal(t,
						`{"pageSize":3,"offset":3,"after":"accounts:15","address":"accounts:.*"}`,
						string(res))

					httpResponse = internal.GetBalances(api, url.Values{
						controllers.QueryKeyCursor: []string{cursor.Next},
					})
					assert.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

					cursor = internal.DecodeCursorResponse[core.AccountsBalances](t, httpResponse.Body)
					res, err = base64.RawURLEncoding.DecodeString(cursor.Previous)
					require.NoError(t, err)
					require.Equal(t,
						`{"pageSize":3,"offset":0,"after":"accounts:15","address":"accounts:.*"}`,
						string(res))
					res, err = base64.RawURLEncoding.DecodeString(cursor.Next)
					require.NoError(t, err)
					require.Equal(t,
						`{"pageSize":3,"offset":6,"after":"accounts:15","address":"accounts:.*"}`,
						string(res))
				})

				t.Run("GetLogs", func(t *testing.T) {
					httpResponse := internal.GetLedgerLogs(api, url.Values{
						"after":                       []string{"30"},
						controllers.QueryKeyStartTime: []string{timestamp.Add(5 * time.Second).Format(time.RFC3339)},
						controllers.QueryKeyEndTime:   []string{timestamp.Add(25 * time.Second).Format(time.RFC3339)},
						controllers.QueryKeyPageSize:  []string{"2"},
					})
					assert.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

					cursor := internal.DecodeCursorResponse[core.Log](t, httpResponse.Body)
					res, err := base64.RawURLEncoding.DecodeString(cursor.Next)
					require.NoError(t, err)
					require.Equal(t,
						`{"after":26,"pageSize":2,"startTime":"2023-01-01T00:00:05Z","endTime":"2023-01-01T00:00:25Z"}`,
						string(res))

					httpResponse = internal.GetLedgerLogs(api, url.Values{
						controllers.QueryKeyCursor: []string{cursor.Next},
					})
					assert.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

					cursor = internal.DecodeCursorResponse[core.Log](t, httpResponse.Body)
					res, err = base64.RawURLEncoding.DecodeString(cursor.Previous)
					require.NoError(t, err)
					require.Equal(t,
						`{"after":28,"pageSize":2,"startTime":"2023-01-01T00:00:05Z","endTime":"2023-01-01T00:00:25Z"}`,
						string(res))
					res, err = base64.RawURLEncoding.DecodeString(cursor.Next)
					require.NoError(t, err)
					require.Equal(t,
						`{"after":22,"pageSize":2,"startTime":"2023-01-01T00:00:05Z","endTime":"2023-01-01T00:00:25Z"}`,
						string(res))
				})

				return nil
			},
		})
	}))
}
