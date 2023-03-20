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

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/internal"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/migrations"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGetLedgerInfo(t *testing.T) {
	internal.RunTest(t, func(h chi.Router, driver storage.Driver) {
		availableMigrations, err := migrations.CollectMigrationFiles(ledgerstore.MigrationsFS)
		require.NoError(t, err)

		rsp := internal.GetLedgerInfo(h)
		require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
		info, ok := internal.DecodeSingleResponse[controllers.Info](t, rsp.Body)
		require.Equal(t, true, ok)

		_, err = uuid.Parse(info.Name)
		require.NoError(t, err)

		require.Equal(t, len(availableMigrations), len(info.Storage.Migrations))

		for _, m := range info.Storage.Migrations {
			require.Equal(t, "DONE", m.State)
			require.NotEqual(t, "", m.Name)
			require.NotEqual(t, time.Time{}, m.Date)
		}
	})
}

func TestGetStats(t *testing.T) {
	internal.RunTest(t, func(h chi.Router, storageDriver storage.Driver) {
		store, _, err := storageDriver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
		require.NoError(t, err)

		_, err = store.Initialize(context.Background())
		require.NoError(t, err)

		require.NoError(t, store.InsertTransactions(context.Background(), core.ExpandTransactionFromEmptyPreCommitVolumes(
			core.NewTransaction().WithPostings(
				core.NewPosting("world", "alice", "USD", core.NewMonetaryInt(100)),
			),
		)))
		require.NoError(t, store.InsertTransactions(context.Background(), core.ExpandTransactionFromEmptyPreCommitVolumes(
			core.NewTransaction().
				WithPostings(core.NewPosting("world", "bob", "USD", core.NewMonetaryInt(100))).
				WithID(1),
		)))
		require.NoError(t, store.EnsureAccountExists(context.Background(), "world"))
		require.NoError(t, store.EnsureAccountExists(context.Background(), "alice"))
		require.NoError(t, store.EnsureAccountExists(context.Background(), "bob"))

		rsp := internal.GetLedgerStats(h)
		require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

		stats, _ := internal.DecodeSingleResponse[ledger.Stats](t, rsp.Body)

		require.EqualValues(t, ledger.Stats{
			Transactions: 2,
			Accounts:     3,
		}, stats)
	})
}

func TestGetLogs(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, driver storage.Driver) {
		now := core.Now()
		tx1 := core.ExpandedTransaction{
			Transaction: core.Transaction{
				ID: 0,
				TransactionData: core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
					Timestamp: now.Add(-3 * time.Hour),
				},
			},
		}
		tx2 := core.ExpandedTransaction{
			Transaction: core.Transaction{
				ID: 1,
				TransactionData: core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bob",
							Amount:      core.NewMonetaryInt(200),
							Asset:       "USD",
						},
					},
					Timestamp: now.Add(-2 * time.Hour),
				},
			},
		}
		store := internal.GetLedgerStore(t, driver, context.Background())
		_, err := store.Initialize(context.Background())
		require.NoError(t, err)

		require.NoError(t, store.InsertTransactions(context.Background(), tx1, tx2))

		for _, tx := range []core.ExpandedTransaction{tx1, tx2} {
			require.NoError(t, store.AppendLog(context.Background(), core.NewTransactionLog(tx.Transaction, nil)))
		}

		at := core.Now()
		require.NoError(t, store.UpdateTransactionMetadata(context.Background(),
			0, core.Metadata{"key": "value"}))

		require.NoError(t, store.AppendLog(context.Background(), core.NewSetMetadataLog(at, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeTransaction,
			TargetID:   0,
			Metadata:   core.Metadata{"key": "value"},
		})))

		at2 := core.Now()
		require.NoError(t, store.UpdateAccountMetadata(context.Background(), "alice", core.Metadata{"key": "value"}))

		require.NoError(t, store.AppendLog(context.Background(), core.NewSetMetadataLog(at2, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeAccount,
			TargetID:   "alice",
			Metadata:   core.Metadata{"key": "value"},
		})))

		var log0Timestamp, log1Timestamp core.Time
		t.Run("all", func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Log](t, rsp.Body)
			// all logs
			require.Len(t, cursor.Data, 4)
			require.Equal(t, uint64(3), cursor.Data[0].ID)
			require.Equal(t, uint64(2), cursor.Data[1].ID)
			require.Equal(t, uint64(1), cursor.Data[2].ID)
			require.Equal(t, uint64(0), cursor.Data[3].ID)

			log0Timestamp = cursor.Data[3].Date
			log1Timestamp = cursor.Data[2].Date
		})

		t.Run("after", func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{
				"after": []string{"1"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Log](t, rsp.Body)
			require.Len(t, cursor.Data, 1)
			require.Equal(t, uint64(0), cursor.Data[0].ID)
		})

		t.Run("invalid after", func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{
				"after": []string{"invalid"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid 'after' query param",
			}, err)
		})

		t.Run("time range", func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{
				controllers.QueryKeyStartTime: []string{log0Timestamp.Format(time.RFC3339)},
				controllers.QueryKeyEndTime:   []string{log1Timestamp.Format(time.RFC3339)},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Log](t, rsp.Body)
			require.Len(t, cursor.Data, 1)
			require.Equal(t, uint64(0), cursor.Data[0].ID)
		})

		t.Run("only start time", func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{
				controllers.QueryKeyStartTime: []string{core.Now().Add(time.Second).Format(time.RFC3339)},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Log](t, rsp.Body)
			require.Len(t, cursor.Data, 0)
		})

		t.Run("only end time", func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{
				controllers.QueryKeyEndTime: []string{core.Now().Add(time.Second).Format(time.RFC3339)},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Log](t, rsp.Body)
			require.Len(t, cursor.Data, 4)
		})

		t.Run("invalid start time", func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{
				controllers.QueryKeyStartTime: []string{"invalid time"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: controllers.ErrInvalidStartTime.Error(),
			}, err)
		})

		t.Run("invalid end time", func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{
				controllers.QueryKeyEndTime: []string{"invalid time"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: controllers.ErrInvalidEndTime.Error(),
			}, err)
		})

		to := ledgerstore.LogsPaginationToken{}
		raw, err := json.Marshal(to)
		require.NoError(t, err)

		t.Run(fmt.Sprintf("valid empty %s", controllers.QueryKeyCursor), func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{
				controllers.QueryKeyCursor: []string{base64.RawURLEncoding.EncodeToString(raw)},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
		})

		t.Run(fmt.Sprintf("valid empty %s with any other param is forbidden", controllers.QueryKeyCursor), func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{
				controllers.QueryKeyCursor: []string{base64.RawURLEncoding.EncodeToString(raw)},
				"after":                    []string{"1"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: fmt.Sprintf("no other query params can be set with '%s'", controllers.QueryKeyCursor),
			}, err)
		})

		t.Run(fmt.Sprintf("invalid %s", controllers.QueryKeyCursor), func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{
				controllers.QueryKeyCursor: []string{"invalid"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: fmt.Sprintf("invalid '%s' query param", controllers.QueryKeyCursor),
			}, err)
		})

		t.Run(fmt.Sprintf("invalid %s not base64", controllers.QueryKeyCursor), func(t *testing.T) {
			rsp := internal.GetLedgerLogs(api, url.Values{
				controllers.QueryKeyCursor: []string{"@!/"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: fmt.Sprintf("invalid '%s' query param", controllers.QueryKeyCursor),
			}, err)
		})
	})
}
