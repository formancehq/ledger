package controllers_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/numary/ledger/internal/pgtesting"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestPostTransactions(t *testing.T) {
	type testCase struct {
		name               string
		transactions       []core.TransactionData
		expectedStatusCode int
		expectedErrorCode  string
	}

	testCases := []testCase{
		{
			name:               "nominal",
			expectedStatusCode: http.StatusOK,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      1000,
							Asset:       "USB",
						},
					},
				},
			},
		},
		{
			name:               "no-postings",
			expectedStatusCode: http.StatusBadRequest,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{},
				},
			},
			expectedErrorCode: controllers.ErrValidation,
		},
		{
			name:               "negative-amount",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  controllers.ErrValidation,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      -1000,
							Asset:       "USB",
						},
					},
				},
			},
		},
		{
			name:               "wrong-asset",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  controllers.ErrValidation,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      1000,
							Asset:       "@TOK",
						},
					},
				},
			},
		},
		{
			name:               "bad-address",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  controllers.ErrValidation,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "#fake",
							Amount:      1000,
							Asset:       "TOK",
						},
					},
				},
			},
		},
		{
			name:               "missing-funds",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  controllers.ErrInsufficientFund,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "foo",
							Destination: "bar",
							Amount:      1000,
							Asset:       "TOK",
						},
					},
				},
			},
		},
		{
			name:               "reference-conflict",
			expectedStatusCode: http.StatusConflict,
			expectedErrorCode:  controllers.ErrConflict,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bar",
							Amount:      1000,
							Asset:       "TOK",
						},
					},
					Reference: "ref",
				},
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bar",
							Amount:      1000,
							Asset:       "TOK",
						},
					},
					Reference: "ref",
				},
			},
		},
	}

	for _, tc := range testCases {
		internal.RunSubTest(t, tc.name, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					for i := 0; i < len(tc.transactions)-1; i++ {
						rsp := internal.PostTransaction(t, api, tc.transactions[i])
						assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					}
					rsp := internal.PostTransaction(t, api, tc.transactions[len(tc.transactions)-1])
					assert.Equal(t, tc.expectedStatusCode, rsp.Result().StatusCode)
					return nil
				},
			})
		}))
	}
}

func TestGetTransaction(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      1000,
							Asset:       "USD",
						},
					},
					Reference: "ref",
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				txs, _ := internal.DecodeSingleResponse[[]core.Transaction](t, rsp.Body)
				tx := txs[0]
				assert.EqualValues(t, core.AccountsVolumes{
					"world": core.AssetsVolumes{
						"USD": {},
					},
					"central_bank": core.AssetsVolumes{
						"USD": {},
					},
				}, tx.PreCommitVolumes)
				assert.EqualValues(t, core.AccountsVolumes{
					"world": core.AssetsVolumes{
						"USD": {
							Output: 1000,
						},
					},
					"central_bank": core.AssetsVolumes{
						"USD": {
							Input: 1000,
						},
					},
				}, tx.PostCommitVolumes)

				rsp = internal.GetTransaction(api, tx.ID)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				ret, _ := internal.DecodeSingleResponse[core.Transaction](t, rsp.Body)

				assert.EqualValues(t, core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      1000,
						Asset:       "USD",
					},
				}, ret.Postings)
				assert.EqualValues(t, 0, ret.ID)
				assert.EqualValues(t, core.Metadata{}, ret.Metadata)
				assert.EqualValues(t, "ref", ret.Reference)
				assert.NotEmpty(t, ret.Timestamp)
				assert.EqualValues(t, core.AccountsVolumes{
					"world": core.AssetsVolumes{
						"USD": {},
					},
					"central_bank": core.AssetsVolumes{
						"USD": {},
					},
				}, ret.PreCommitVolumes)
				assert.EqualValues(t, core.AccountsVolumes{
					"world": core.AssetsVolumes{
						"USD": {
							Output: 1000,
						},
					},
					"central_bank": core.AssetsVolumes{
						"USD": {
							Input: 1000,
						},
					},
				}, ret.PostCommitVolumes)
				return nil
			},
		})
	}))
}

func TestPreviewTransaction(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransactionPreview(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      1000,
							Asset:       "USD",
						},
					},
					Reference: "ref",
				})
				assert.Equal(t, http.StatusNotModified, rsp.Result().StatusCode)
				return nil
			},
		})
	}))
}

func TestNotFoundTransaction(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.GetTransaction(api, 0)
				assert.Equal(t, http.StatusNotFound, rsp.Result().StatusCode)
				return nil
			},
		})
	}))
}

func TestGetTransactions(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				now := time.Now().UTC()
				tx1 := core.Transaction{
					TransactionData: core.TransactionData{
						Postings: core.Postings{
							{
								Source:      "world",
								Destination: "central_bank1",
								Amount:      1000,
								Asset:       "USD",
							},
						},
						Reference: "ref:001",
					},
					Timestamp: now.Add(-3 * time.Hour).Format(time.RFC3339),
				}
				tx2 := core.Transaction{
					ID: 1,
					TransactionData: core.TransactionData{
						Postings: core.Postings{
							{
								Source:      "world",
								Destination: "central_bank2",
								Amount:      1000,
								Asset:       "USD",
							},
						},
						Metadata: map[string]json.RawMessage{
							"foo": json.RawMessage(`"bar"`),
						},
						Reference: "ref:002",
					},
					Timestamp: now.Add(-2 * time.Hour).Format(time.RFC3339),
				}
				tx3 := core.Transaction{
					ID: 2,
					TransactionData: core.TransactionData{
						Postings: core.Postings{
							{
								Source:      "central_bank1",
								Destination: "alice",
								Amount:      10,
								Asset:       "USD",
							},
						},
						Reference: "ref:003",
					},
					Timestamp: now.Add(-1 * time.Hour).Format(time.RFC3339),
				}
				log1 := core.NewTransactionLog(nil, tx1)
				log2 := core.NewTransactionLog(&log1, tx2)
				log3 := core.NewTransactionLog(&log2, tx3)
				store := internal.GetStore(t, driver, ctx)
				err := store.AppendLog(context.Background(), log1, log2, log3)
				require.NoError(t, err)

				rsp := internal.CountTransactions(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				require.Equal(t, "3", rsp.Header().Get("Count"))

				var tx1Timestamp, tx2Timestamp string
				t.Run("all", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
					// all transactions
					assert.Len(t, cursor.Data, 3)
					assert.Equal(t, cursor.Data[0].ID, uint64(2))
					assert.Equal(t, cursor.Data[1].ID, uint64(1))
					assert.Equal(t, cursor.Data[2].ID, uint64(0))

					tx1Timestamp = cursor.Data[1].Timestamp
					tx2Timestamp = cursor.Data[0].Timestamp
				})

				t.Run("after", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"after": []string{"1"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
					// 1 transaction: txid 0
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].ID, uint64(0))
				})

				t.Run("reference", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"reference": []string{"ref:001"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
					// 1 transaction: txid 0
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].ID, uint64(0))
				})

				t.Run("destination", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"destination": []string{"central_bank1"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
					// 1 transaction: txid 0
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].ID, uint64(0))
				})

				t.Run("source", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"source": []string{"world"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
					// 2 transactions: txid 0 and txid 1
					assert.Len(t, cursor.Data, 2)
					assert.Equal(t, cursor.Data[0].ID, uint64(1))
					assert.Equal(t, cursor.Data[1].ID, uint64(0))
				})

				t.Run("account", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"account": []string{"world"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
					// 2 transactions: txid 0 and txid 1
					assert.Len(t, cursor.Data, 2)
					assert.Equal(t, cursor.Data[0].ID, uint64(1))
					assert.Equal(t, cursor.Data[1].ID, uint64(0))
				})

				t.Run("time range", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"start_time": []string{tx1Timestamp},
						"end_time":   []string{tx2Timestamp},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
					// 1 transaction: txid 1
					assert.Len(t, cursor.Data, 1)
				})

				t.Run("only start time", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"start_time": []string{time.Now().Add(time.Second).Format(time.RFC3339)},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
					// no transaction
					assert.Len(t, cursor.Data, 0)
				})

				t.Run("only end time", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"end_time": []string{time.Now().Add(time.Second).Format(time.RFC3339)},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Transaction](t, rsp.Body)
					// all transactions
					assert.Len(t, cursor.Data, 3)
				})

				t.Run("invalid start time", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"start_time": []string{"invalid time"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)
				})

				t.Run("invalid end time", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"end_time": []string{"invalid time"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)
				})

				to := sqlstorage.TxsPaginationToken{}
				raw, err := json.Marshal(to)
				require.NoError(t, err)
				t.Run("valid empty pagination_token", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"pagination_token": []string{base64.RawURLEncoding.EncodeToString(raw)},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				})

				t.Run("valid empty pagination_token with any other param is forbidden", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"pagination_token": []string{base64.RawURLEncoding.EncodeToString(raw)},
						"after":            []string{"1"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())
				})

				t.Run("invalid pagination_token", func(t *testing.T) {
					rsp = internal.GetTransactions(api, url.Values{
						"pagination_token": []string{"invalid"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())
				})

				return nil
			},
		})
	}))
}

type transaction struct {
	core.TransactionData
	ID                uint64          `json:"txid"`
	Timestamp         string          `json:"timestamp"`
	PreCommitVolumes  accountsVolumes `json:"preCommitVolumes,omitempty"`
	PostCommitVolumes accountsVolumes `json:"postCommitVolumes,omitempty"`
}
type accountsVolumes map[string]assetsVolumes
type assetsVolumes map[string]core.VolumesWithBalance

func TestTransactionsVolumes(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {

				var worldAlice int64 = 100

				rsp := internal.PostTransaction(t, api,
					core.TransactionData{
						Postings: core.Postings{
							{
								Source:      "world",
								Destination: "alice",
								Amount:      worldAlice,
								Asset:       "USD",
							},
						},
					})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				txs, ok := internal.DecodeSingleResponse[[]transaction](t, rsp.Body)
				require.True(t, ok)
				require.Len(t, txs, 1)

				expPreVolumes := accountsVolumes{
					"alice": assetsVolumes{
						"USD": core.VolumesWithBalance{},
					},
					"world": assetsVolumes{
						"USD": core.VolumesWithBalance{},
					},
				}

				expPostVolumes := accountsVolumes{
					"alice": assetsVolumes{
						"USD": core.VolumesWithBalance{
							Input:   worldAlice,
							Balance: worldAlice,
						},
					},
					"world": assetsVolumes{
						"USD": core.VolumesWithBalance{
							Output:  worldAlice,
							Balance: -worldAlice,
						},
					},
				}

				assert.Equal(t, expPreVolumes, txs[0].PreCommitVolumes)
				assert.Equal(t, expPostVolumes, txs[0].PostCommitVolumes)

				rsp = internal.GetTransactions(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[transaction](t, rsp.Body)
				require.Len(t, cursor.Data, 1)

				assert.Equal(t, expPreVolumes, cursor.Data[0].PreCommitVolumes)
				assert.Equal(t, expPostVolumes, cursor.Data[0].PostCommitVolumes)

				var aliceBob int64 = 93

				rsp = internal.PostTransaction(t, api,
					core.TransactionData{
						Postings: core.Postings{
							{
								Source:      "alice",
								Destination: "bob",
								Amount:      aliceBob,
								Asset:       "USD",
							},
						},
					})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				txs, ok = internal.DecodeSingleResponse[[]transaction](t, rsp.Body)
				require.True(t, ok)
				require.Len(t, txs, 1)

				prevAlice := expPostVolumes["alice"]

				expPreVolumes = accountsVolumes{
					"alice": prevAlice,
					"bob": assetsVolumes{
						"USD": core.VolumesWithBalance{},
					},
				}

				expPostVolumes = accountsVolumes{
					"alice": assetsVolumes{
						"USD": core.VolumesWithBalance{
							Input:   prevAlice["USD"].Input,
							Output:  prevAlice["USD"].Output + aliceBob,
							Balance: prevAlice["USD"].Input - prevAlice["USD"].Output - aliceBob,
						},
					},
					"bob": assetsVolumes{
						"USD": core.VolumesWithBalance{
							Input:   aliceBob,
							Balance: aliceBob,
						},
					},
				}

				assert.Equal(t, expPreVolumes, txs[0].PreCommitVolumes)
				assert.Equal(t, expPostVolumes, txs[0].PostCommitVolumes)

				rsp = internal.GetTransactions(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[transaction](t, rsp.Body)
				require.Len(t, cursor.Data, 2)

				assert.Equal(t, expPreVolumes, cursor.Data[0].PreCommitVolumes)
				assert.Equal(t, expPostVolumes, cursor.Data[0].PostCommitVolumes)

				return nil
			},
		})
	}))
}

func TestPostTransactionMetadata(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      1000,
							Asset:       "USD",
						},
					},
				})
				if !assert.Equal(t, http.StatusOK, rsp.Result().StatusCode) {
					return nil
				}

				tx, _ := internal.DecodeSingleResponse[[]core.Transaction](t, rsp.Body)

				rsp = internal.PostTransactionMetadata(t, api, tx[0].ID, core.Metadata{
					"foo": json.RawMessage(`"bar"`),
				})
				if !assert.Equal(t, http.StatusNoContent, rsp.Result().StatusCode) {
					return nil
				}

				rsp = internal.GetTransaction(api, tx[0].ID)
				if !assert.Equal(t, http.StatusOK, rsp.Result().StatusCode) {
					return nil
				}

				ret, _ := internal.DecodeSingleResponse[core.Transaction](t, rsp.Body)

				if !assert.EqualValues(t, core.Metadata{
					"foo": json.RawMessage(`"bar"`),
				}, ret.Metadata) {
					return nil
				}

				rsp = internal.PostTransactionMetadata(t, api, tx[0].ID, core.Metadata{
					"foo": json.RawMessage(`"baz"`),
				})
				if !assert.Equal(t, http.StatusNoContent, rsp.Result().StatusCode) {
					return nil
				}
				return nil
			},
		})
	}))
}

func TestTooManyClient(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				if ledgertesting.StorageDriverName() != "postgres" {
					return nil
				}
				if os.Getenv("NUMARY_STORAGE_POSTGRES_CONN_STRING") != "" { // Use of external server, ignore this test
					return nil
				}

				store, _, err := driver.GetStore(context.Background(), "quickstart", true)
				assert.NoError(t, err)

				// Grab all potential connections
				for i := 0; i < pgtesting.MaxConnections; i++ {
					tx, err := store.(*sqlstorage.Store).Schema().BeginTx(context.Background(), &sql.TxOptions{})
					assert.NoError(t, err)
					defer func(tx *sql.Tx) {
						if err := tx.Rollback(); err != nil {
							panic(err)
						}
					}(tx)
				}

				rsp := internal.GetTransactions(api, url.Values{})
				assert.Equal(t, http.StatusServiceUnavailable, rsp.Result().StatusCode)
				return nil
			},
		})
	}))
}
