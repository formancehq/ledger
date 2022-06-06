package controllers_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/numary/ledger/internal/pgtesting"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
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

				tx := make([]core.Transaction, 0)
				internal.DecodeSingleResponse(t, rsp.Body, &tx)

				rsp = internal.GetTransaction(api, tx[0].ID)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				ret := core.Transaction{}
				internal.DecodeSingleResponse(t, rsp.Body, &ret)

				assert.EqualValues(t, ret.Postings, core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      1000,
						Asset:       "USD",
					},
				})
				assert.EqualValues(t, 0, ret.ID)
				assert.EqualValues(t, core.Metadata{}, ret.Metadata)
				assert.EqualValues(t, "ref", ret.Reference)
				assert.NotEmpty(t, ret.Timestamp)
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

type GetTransactionsCursor struct {
	PageSize int                `json:"page_size,omitempty"`
	HasMore  bool               `json:"has_more"`
	Previous string             `json:"previous,omitempty"`
	Next     string             `json:"next,omitempty"`
	Data     []core.Transaction `json:"data"`
}

type getTransactionsResponse struct {
	Cursor *GetTransactionsCursor `json:"cursor,omitempty"`
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
								Destination: "central_bank",
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
								Destination: "central_bank",
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
								Source:      "central_bank",
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

				rsp = internal.GetTransactions(api, url.Values{})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp := getTransactionsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				// all transactions
				assert.Len(t, resp.Cursor.Data, 3)
				assert.Equal(t, resp.Cursor.Data[0].ID, uint64(2))
				assert.Equal(t, resp.Cursor.Data[1].ID, uint64(1))
				assert.Equal(t, resp.Cursor.Data[2].ID, uint64(0))
				assert.False(t, resp.Cursor.HasMore)

				tx1Timestamp := resp.Cursor.Data[1].Timestamp
				tx2Timestamp := resp.Cursor.Data[0].Timestamp

				rsp = internal.GetTransactions(api, url.Values{
					"after": []string{"1"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp = getTransactionsResponse{}
				// 1 transaction: txid 0
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				assert.Len(t, resp.Cursor.Data, 1)
				assert.Equal(t, resp.Cursor.Data[0].ID, uint64(0))
				assert.False(t, resp.Cursor.HasMore)

				rsp = internal.GetTransactions(api, url.Values{
					"reference": []string{"ref:001"},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp = getTransactionsResponse{}
				// 1 transaction: txid 0
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				assert.Len(t, resp.Cursor.Data, 1)
				assert.Equal(t, resp.Cursor.Data[0].ID, uint64(0))
				assert.False(t, resp.Cursor.HasMore)

				rsp = internal.GetTransactions(api, url.Values{
					"start_time": []string{tx1Timestamp},
					"end_time":   []string{tx2Timestamp},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp = getTransactionsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				// 1 transaction: txid 1
				assert.Len(t, resp.Cursor.Data, 1)

				rsp = internal.GetTransactions(api, url.Values{
					"start_time": []string{time.Now().Add(time.Second).Format(time.RFC3339)},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp = getTransactionsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				// no transaction
				assert.Len(t, resp.Cursor.Data, 0)

				rsp = internal.GetTransactions(api, url.Values{
					"end_time": []string{time.Now().Add(time.Second).Format(time.RFC3339)},
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				resp = getTransactionsResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
				// all transactions
				assert.Len(t, resp.Cursor.Data, 3)

				rsp = internal.GetTransactions(api, url.Values{
					"start_time": []string{"invalid time"},
				})
				assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

				rsp = internal.GetTransactions(api, url.Values{
					"end_time": []string{"invalid time"},
				})
				assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

				return nil
			},
		})
	}))
}

var maxTxsPages = 3
var maxAdditionalTxs = 2

func TestGetTransactionsPagination(t *testing.T) {
	for txsPages := 0; txsPages <= maxTxsPages; txsPages++ {
		for additionalTxs := 0; additionalTxs <= maxAdditionalTxs; additionalTxs++ {
			t.Run(fmt.Sprintf("%d-pages-%d-additional", txsPages, additionalTxs), func(t *testing.T) {
				internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							var rsp *httptest.ResponseRecorder
							numTxs := txsPages*query.DefaultLimit + additionalTxs
							for i := 0; i < numTxs; i++ {
								rsp = internal.PostTransaction(t, api, core.TransactionData{
									Postings: core.Postings{
										{
											Source:      "world",
											Destination: "alice",
											Amount:      10,
											Asset:       "USD",
										},
									},
									Reference: fmt.Sprintf("ref:%d", i),
								})
								require.Equal(t, http.StatusOK, rsp.Code, rsp.Body.String())
							}

							rsp = internal.CountTransactions(api, url.Values{})
							require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
							require.Equal(t, fmt.Sprintf("%d", numTxs), rsp.Header().Get("Count"))

							paginationToken := ""
							resp := getTransactionsResponse{}
							for i := 0; i < txsPages; i++ {
								rsp = internal.GetTransactions(api, url.Values{
									"pagination_token": []string{paginationToken},
								})
								assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
								resp = getTransactionsResponse{}
								assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
								assert.Len(t, resp.Cursor.Data, query.DefaultLimit)

								// First txid of the page
								assert.Equal(t,
									uint64((txsPages-i)*query.DefaultLimit+additionalTxs-1), resp.Cursor.Data[0].ID)

								// Last txid of the page
								assert.Equal(t,
									uint64((txsPages-i-1)*query.DefaultLimit+additionalTxs), resp.Cursor.Data[len(resp.Cursor.Data)-1].ID)

								paginationToken = resp.Cursor.Next
							}

							if additionalTxs > 0 {
								rsp = internal.GetTransactions(api, url.Values{
									"pagination_token": []string{paginationToken},
								})
								assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
								resp = getTransactionsResponse{}
								assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
								assert.Len(t, resp.Cursor.Data, additionalTxs)

								// First txid of the last page
								assert.Equal(t,
									uint64(additionalTxs-1), resp.Cursor.Data[0].ID)

								// Last txid of the last page
								assert.Equal(t,
									uint64(0), resp.Cursor.Data[len(resp.Cursor.Data)-1].ID)
							}

							if txsPages > 0 {
								for i := 0; i < txsPages; i++ {
									paginationToken = resp.Cursor.Previous
									rsp = internal.GetTransactions(api, url.Values{
										"pagination_token": []string{paginationToken},
									})
									assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
									resp = getTransactionsResponse{}
									assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &resp))
									assert.Len(t, resp.Cursor.Data, query.DefaultLimit)
								}

								// First txid of the first page
								assert.Equal(t,
									uint64(txsPages*query.DefaultLimit+additionalTxs-1), resp.Cursor.Data[0].ID)

								// Last txid of the first page
								assert.Equal(t,
									uint64((txsPages-1)*query.DefaultLimit+additionalTxs), resp.Cursor.Data[len(resp.Cursor.Data)-1].ID)
							}

							return nil
						},
					})
				}))
			})
		}
	}
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
				tx := make([]core.Transaction, 0)
				internal.DecodeSingleResponse(t, rsp.Body, &tx)

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

				ret := core.Transaction{}
				internal.DecodeSingleResponse(t, rsp.Body, &ret)

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
