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

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/internal"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
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
				internal.RunTest(t, func(api chi.Router, storageDriver storage.Driver) {
					testGetPagination(t, api, storageDriver, txsPages, additionalTxs)
				})
			})
		}
	}
}

func testGetPagination(t *testing.T, api chi.Router, storageDriver storage.Driver, txsPages, additionalTxs int) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		store, _, err := storageDriver.GetLedgerStore(ctx, internal.TestingLedger, true)
		require.NoError(t, err)

		numTxs := txsPages*pageSize + additionalTxs
		if numTxs > 0 {
			for i := 0; i < numTxs; i++ {
				require.NoError(t, store.InsertTransactions(context.Background(), core.ExpandTransactionFromEmptyPreCommitVolumes(
					core.NewTransaction().
						WithPostings(core.NewPosting("world", fmt.Sprintf("accounts:%06d", i), "USD", core.NewMonetaryInt(10))).
						WithReference(fmt.Sprintf("ref:%06d", i)),
				)))
			}
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
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				require.Len(t, cursor.Data, pageSize)
				require.Equal(t, cursor.Next != "", cursor.HasMore)

				// First txid of the page
				require.Equal(t,
					uint64((txsPages-i)*pageSize+additionalTxs-1), cursor.Data[0].ID)

				// Last txid of the page
				require.Equal(t,
					uint64((txsPages-i-1)*pageSize+additionalTxs), cursor.Data[len(cursor.Data)-1].ID)

				paginationToken = cursor.Next
			}

			if additionalTxs > 0 {
				rsp = internal.GetTransactions(api, url.Values{
					controllers.QueryKeyCursor: []string{paginationToken},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				cursor = internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				require.Len(t, cursor.Data, additionalTxs)
				require.Equal(t, cursor.Next != "", cursor.HasMore)

				// First txid of the last page
				require.Equal(t,
					uint64(additionalTxs-1), cursor.Data[0].ID)

				// Last txid of the last page
				require.Equal(t,
					uint64(0), cursor.Data[len(cursor.Data)-1].ID)
			}

			require.Empty(t, cursor.Next)

			// MOVING BACKWARD
			if txsPages > 0 {
				back := 0
				for cursor.Previous != "" {
					paginationToken = cursor.Previous
					rsp = internal.GetTransactions(api, url.Values{
						controllers.QueryKeyCursor: []string{paginationToken},
					})
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor = internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
					require.Len(t, cursor.Data, pageSize)
					require.Equal(t, cursor.Next != "", cursor.HasMore)
					back++
				}
				if additionalTxs > 0 {
					require.Equal(t, txsPages, back)
				} else {
					require.Equal(t, txsPages-1, back)
				}

				// First txid of the first page
				require.Equal(t,
					uint64(txsPages*pageSize+additionalTxs-1), cursor.Data[0].ID)

				// Last txid of the first page
				require.Equal(t,
					uint64((txsPages-1)*pageSize+additionalTxs), cursor.Data[len(cursor.Data)-1].ID)
			}

			require.Empty(t, cursor.Previous)
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
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[core.Account](t, rsp.Body)
				require.Len(t, cursor.Data, pageSize)
				require.Equal(t, cursor.Next != "", cursor.HasMore)

				// First account of the page
				if i == 0 {
					require.Equal(t, "world",
						string(cursor.Data[0].Address))
				} else {
					require.Equal(t,
						fmt.Sprintf("accounts:%06d", (accPages-i)*pageSize+additionalAccs-1),
						string(cursor.Data[0].Address))
				}

				// Last account of the page
				require.Equal(t,
					fmt.Sprintf("accounts:%06d", (accPages-i-1)*pageSize+additionalAccs),
					string(cursor.Data[len(cursor.Data)-1].Address))

				paginationToken = cursor.Next
			}

			if additionalAccs > 0 {
				rsp = internal.GetAccounts(api, url.Values{
					controllers.QueryKeyCursor: []string{paginationToken},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				cursor = internal.DecodeCursorResponse[core.Account](t, rsp.Body)
				require.Len(t, cursor.Data, additionalAccs)
				require.Equal(t, cursor.Next != "", cursor.HasMore)

				// First account of the last page
				if accPages == 0 {
					require.Equal(t, "world",
						string(cursor.Data[0].Address))
				} else {
					require.Equal(t,
						fmt.Sprintf("accounts:%06d", additionalAccs-1),
						string(cursor.Data[0].Address))
				}

				// Last account of the last page
				require.Equal(t,
					fmt.Sprintf("accounts:%06d", 0),
					string(cursor.Data[len(cursor.Data)-1].Address))
			}

			require.Empty(t, cursor.Next)

			// MOVING BACKWARD
			if accPages > 0 {
				back := 0
				for cursor.Previous != "" {
					paginationToken = cursor.Previous
					rsp = internal.GetAccounts(api, url.Values{
						controllers.QueryKeyCursor: []string{paginationToken},
					})
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
					cursor = internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					require.Len(t, cursor.Data, pageSize)
					require.Equal(t, cursor.Next != "", cursor.HasMore)
					back++
				}
				if additionalAccs > 0 {
					require.Equal(t, accPages, back)
				} else {
					require.Equal(t, accPages-1, back)
				}

				// First account of the first page
				require.Equal(t, "world",
					string(cursor.Data[0].Address))

				// Last account of the first page
				require.Equal(t,
					fmt.Sprintf("accounts:%06d", (txsPages-1)*pageSize+additionalTxs+1),
					string(cursor.Data[len(cursor.Data)-1].Address))
			}

			require.Empty(t, cursor.Previous)
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
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
				require.Len(t, cursor.Data, pageSize)
				require.Equal(t, cursor.Next != "", cursor.HasMore)

				// First account balances of the page
				if i == 0 {
					_, ok := cursor.Data[0]["world"]
					require.True(t, ok)
				} else {
					_, ok := cursor.Data[0][fmt.Sprintf(
						"accounts:%06d", (accPages-i)*pageSize+additionalAccs-1)]
					require.True(t, ok)
				}

				// Last account balances of the page
				_, ok := cursor.Data[len(cursor.Data)-1][fmt.Sprintf(
					"accounts:%06d", (accPages-i-1)*pageSize+additionalAccs)]
				require.True(t, ok)

				paginationToken = cursor.Next
			}

			if additionalAccs > 0 {
				rsp = internal.GetBalances(api, url.Values{
					controllers.QueryKeyCursor: []string{paginationToken},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				cursor = internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
				require.Len(t, cursor.Data, additionalAccs)
				require.Equal(t, cursor.Next != "", cursor.HasMore)

				// First account balances of the last page
				if accPages == 0 {
					_, ok := cursor.Data[0]["world"]
					require.True(t, ok)
				} else {
					_, ok := cursor.Data[0][fmt.Sprintf(
						"accounts:%06d", additionalAccs-1)]
					require.True(t, ok)
				}

				// Last account balances of the last page
				_, ok := cursor.Data[len(cursor.Data)-1][fmt.Sprintf(
					"accounts:%06d", 0)]
				require.True(t, ok)
			}

			// MOVING BACKWARD
			if accPages > 0 {
				back := 0
				for cursor.Previous != "" {
					paginationToken = cursor.Previous
					rsp = internal.GetBalances(api, url.Values{
						controllers.QueryKeyCursor: []string{paginationToken},
					})
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
					cursor = internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
					require.Len(t, cursor.Data, pageSize)
					require.Equal(t, cursor.Next != "", cursor.HasMore)
					back++
				}
				if additionalAccs > 0 {
					require.Equal(t, accPages, back)
				} else {
					require.Equal(t, accPages-1, back)
				}

				// First account balances of the first page
				_, ok := cursor.Data[0]["world"]
				require.True(t, ok)

				// Last account balances of the first page
				_, ok = cursor.Data[len(cursor.Data)-1][fmt.Sprintf(
					"accounts:%06d", (txsPages-1)*pageSize+additionalTxs+1)]
				require.True(t, ok)
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
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[core.Log](t, rsp.Body)
				require.Len(t, cursor.Data, pageSize)
				require.Equal(t, cursor.Next != "", cursor.HasMore)

				// First ID of the page
				require.Equal(t,
					uint64((txsPages-i)*pageSize+additionalTxs-1), cursor.Data[0].ID)

				// Last ID of the page
				require.Equal(t,
					uint64((txsPages-i-1)*pageSize+additionalTxs), cursor.Data[len(cursor.Data)-1].ID)

				paginationToken = cursor.Next
			}

			if additionalTxs > 0 {
				rsp = internal.GetLedgerLogs(api, url.Values{
					controllers.QueryKeyCursor: []string{paginationToken},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				cursor = internal.DecodeCursorResponse[core.Log](t, rsp.Body)
				require.Len(t, cursor.Data, additionalTxs)
				require.Equal(t, cursor.Next != "", cursor.HasMore)

				// First ID of the last page
				require.Equal(t,
					uint64(additionalTxs-1), cursor.Data[0].ID)

				// Last ID of the last page
				require.Equal(t,
					uint64(0), cursor.Data[len(cursor.Data)-1].ID)
			}

			require.Empty(t, cursor.Next)

			// MOVING BACKWARD
			if txsPages > 0 {
				back := 0
				for cursor.Previous != "" {
					paginationToken = cursor.Previous
					rsp = internal.GetLedgerLogs(api, url.Values{
						controllers.QueryKeyCursor: []string{paginationToken},
					})
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor = internal.DecodeCursorResponse[core.Log](t, rsp.Body)
					require.Len(t, cursor.Data, pageSize)
					require.Equal(t, cursor.Next != "", cursor.HasMore)
					back++
				}
				if additionalTxs > 0 {
					require.Equal(t, txsPages, back)
				} else {
					require.Equal(t, txsPages-1, back)
				}

				// First ID of the first page
				require.Equal(t,
					uint64(txsPages*pageSize+additionalTxs-1), cursor.Data[0].ID)

				// Last ID of the first page
				require.Equal(t,
					uint64((txsPages-1)*pageSize+additionalTxs), cursor.Data[len(cursor.Data)-1].ID)
			}

			require.Empty(t, cursor.Previous)
		})

		return nil
	}
}

func TestCursor(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, storageDriver storage.Driver) {
		timestamp, err := core.ParseTime("2023-01-01T00:00:00Z")
		require.NoError(t, err)

		store, _, err := storageDriver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
		require.NoError(t, err)

		_, err = store.Initialize(context.Background())
		require.NoError(t, err)

		for i := 0; i < 30; i++ {
			date := timestamp.Add(time.Duration(i) * time.Second)
			tx := core.NewTransaction().
				WithPostings(core.NewPosting("world", fmt.Sprintf("accounts:%02d", i), "USD", core.NewMonetaryInt(1))).
				WithReference(fmt.Sprintf("ref:%02d", i)).
				WithMetadata(core.Metadata{"ref": "abc"}).
				WithTimestamp(date).
				WithID(uint64(i))
			require.NoError(t, store.InsertTransactions(context.Background(), core.ExpandTransactionFromEmptyPreCommitVolumes(tx)))
			log := core.NewTransactionLog(tx, nil).WithDate(date)
			require.NoError(t, store.AppendLog(context.Background(), &log))
			require.NoError(t, store.EnsureAccountExists(context.Background(), fmt.Sprintf("accounts:%02d", i)))
			require.NoError(t, store.UpdateAccountMetadata(context.Background(), fmt.Sprintf("accounts:%02d", i), core.Metadata{
				"foo": json.RawMessage(`"bar"`),
			}))
			require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
				fmt.Sprintf("accounts:%02d", i): {
					"USD": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(1)),
				},
			}))
		}

		t.Run("GetAccounts", func(t *testing.T) {
			httpResponse := internal.GetAccounts(api, url.Values{
				"after":                             []string{"accounts:15"},
				"address":                           []string{"acc.*"},
				"metadata[foo]":                     []string{"bar"},
				"balance":                           []string{"1"},
				controllers.QueryKeyBalanceOperator: []string{"gte"},
				controllers.QueryKeyPageSize:        []string{"3"},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor := internal.DecodeCursorResponse[core.Account](t, httpResponse.Body)
			res, err := base64.RawURLEncoding.DecodeString(cursor.Next)
			require.NoError(t, err)
			require.Equal(t,
				`{"pageSize":3,"offset":3,"after":"accounts:15","address":"acc.*","metadata":{"foo":"bar"},"balance":"1","balanceOperator":"gte"}`,
				string(res))

			httpResponse = internal.GetAccounts(api, url.Values{
				controllers.QueryKeyCursor: []string{cursor.Next},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor = internal.DecodeCursorResponse[core.Account](t, httpResponse.Body)
			res, err = base64.RawURLEncoding.DecodeString(cursor.Previous)
			require.NoError(t, err)
			require.Equal(t,
				`{"pageSize":3,"offset":0,"after":"accounts:15","address":"acc.*","metadata":{"foo":"bar"},"balance":"1","balanceOperator":"gte"}`,
				string(res))
			res, err = base64.RawURLEncoding.DecodeString(cursor.Next)
			require.NoError(t, err)
			require.Equal(t,
				`{"pageSize":3,"offset":6,"after":"accounts:15","address":"acc.*","metadata":{"foo":"bar"},"balance":"1","balanceOperator":"gte"}`,
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
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor := internal.DecodeCursorResponse[core.Transaction](t, httpResponse.Body)
			res, err := base64.RawURLEncoding.DecodeString(cursor.Next)
			require.NoError(t, err)
			require.Equal(t,
				`{"after":12,"account":"acc.*","source":"world","destination":"acc.*","startTime":"2023-01-01T00:00:05Z","endTime":"2023-01-01T00:00:25Z","metadata":{"ref":"abc"},"pageSize":3}`,
				string(res))

			httpResponse = internal.GetTransactions(api, url.Values{
				controllers.QueryKeyCursor: []string{cursor.Next},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

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
				"address":                    []string{"acc.*"},
				controllers.QueryKeyPageSize: []string{"3"},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor := internal.DecodeCursorResponse[core.AccountsBalances](t, httpResponse.Body)
			res, err := base64.RawURLEncoding.DecodeString(cursor.Next)
			require.NoError(t, err)
			require.Equal(t,
				`{"pageSize":3,"offset":3,"after":"accounts:15","address":"acc.*"}`,
				string(res))

			httpResponse = internal.GetBalances(api, url.Values{
				controllers.QueryKeyCursor: []string{cursor.Next},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor = internal.DecodeCursorResponse[core.AccountsBalances](t, httpResponse.Body)
			res, err = base64.RawURLEncoding.DecodeString(cursor.Previous)
			require.NoError(t, err)
			require.Equal(t,
				`{"pageSize":3,"offset":0,"after":"accounts:15","address":"acc.*"}`,
				string(res))
			res, err = base64.RawURLEncoding.DecodeString(cursor.Next)
			require.NoError(t, err)
			require.Equal(t,
				`{"pageSize":3,"offset":6,"after":"accounts:15","address":"acc.*"}`,
				string(res))
		})

		t.Run("GetLogs", func(t *testing.T) {
			httpResponse := internal.GetLedgerLogs(api, url.Values{
				"after":                       []string{"30"},
				controllers.QueryKeyStartTime: []string{timestamp.Add(5 * time.Second).Format(time.RFC3339)},
				controllers.QueryKeyEndTime:   []string{timestamp.Add(25 * time.Second).Format(time.RFC3339)},
				controllers.QueryKeyPageSize:  []string{"2"},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor := internal.DecodeCursorResponse[core.Log](t, httpResponse.Body)
			res, err := base64.RawURLEncoding.DecodeString(cursor.Next)
			require.NoError(t, err)
			require.Equal(t,
				`{"after":23,"pageSize":2,"startTime":"2023-01-01T00:00:05Z","endTime":"2023-01-01T00:00:25Z"}`,
				string(res))

			httpResponse = internal.GetLedgerLogs(api, url.Values{
				controllers.QueryKeyCursor: []string{cursor.Next},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor = internal.DecodeCursorResponse[core.Log](t, httpResponse.Body)
			res, err = base64.RawURLEncoding.DecodeString(cursor.Previous)
			require.NoError(t, err)
			require.Equal(t,
				`{"after":25,"pageSize":2,"startTime":"2023-01-01T00:00:05Z","endTime":"2023-01-01T00:00:25Z"}`,
				string(res))

			res, err = base64.RawURLEncoding.DecodeString(cursor.Next)
			require.NoError(t, err)
			require.Equal(t,
				`{"after":21,"pageSize":2,"startTime":"2023-01-01T00:00:05Z","endTime":"2023-01-01T00:00:25Z"}`,
				string(res))
		})
	})
}
