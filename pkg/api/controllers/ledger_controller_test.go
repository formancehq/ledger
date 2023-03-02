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

	"github.com/formancehq/ledger/pkg/api"
	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/internal"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestGetLedgerInfo(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, h *api.API, driver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				availableMigrations, err := sqlstorage.CollectMigrationFiles(sqlstorage.MigrationsFS)
				require.NoError(t, err)

				rsp := internal.GetLedgerInfo(h)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				info, ok := internal.DecodeSingleResponse[controllers.Info](t, rsp.Body)
				assert.Equal(t, true, ok)

				_, err = uuid.Parse(info.Name)
				assert.NoError(t, err)

				assert.Equal(t, len(availableMigrations), len(info.Storage.Migrations))

				for _, m := range info.Storage.Migrations {
					assert.Equal(t, "DONE", m.State)
					assert.NotEqual(t, "", m.Name)
					assert.NotEqual(t, time.Time{}, m.Date)
				}

				return nil
			},
		})
	}))
}

func TestGetStats(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, h *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, h, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
				}, false)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, h, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "boc",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
				}, false)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.GetLedgerStats(h)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				stats, _ := internal.DecodeSingleResponse[ledger.Stats](t, rsp.Body)

				assert.EqualValues(t, ledger.Stats{
					Transactions: 2,
					Accounts:     3,
				}, stats)
				return nil
			},
		})
	}))
}

func TestGetLogs(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				now := time.Now().UTC()
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
				store := internal.GetLedgerStore(t, driver, ctx)
				require.NoError(t, store.Commit(context.Background(), tx1, tx2))

				require.NoError(t, store.UpdateTransactionMetadata(context.Background(),
					0, core.Metadata{"key": "value"}, time.Now().UTC()))

				require.NoError(t, store.UpdateAccountMetadata(context.Background(),
					"alice", core.Metadata{"key": "value"}, time.Now().UTC()))

				var log0Timestamp, log1Timestamp time.Time
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
						controllers.QueryKeyStartTime: []string{time.Now().Add(time.Second).Format(time.RFC3339)},
					})
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Log](t, rsp.Body)
					require.Len(t, cursor.Data, 0)
				})

				t.Run("only end time", func(t *testing.T) {
					rsp := internal.GetLedgerLogs(api, url.Values{
						controllers.QueryKeyEndTime: []string{time.Now().Add(time.Second).Format(time.RFC3339)},
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

				to := sqlstorage.LogsPaginationToken{}
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

				return nil
			},
		})
	}))
}
