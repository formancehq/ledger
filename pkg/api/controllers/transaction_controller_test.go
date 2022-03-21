package controllers_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/numary/ledger/internal/pgtesting"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"net/http"
	"os"
	"testing"
)

func TestCommitTransaction(t *testing.T) {

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
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.GetTransaction(api, 0)
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
				assert.NotEmpty(t, ret.Hash)
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

func TestGetTransactions(t *testing.T) {
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
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, core.TransactionData{
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
				})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.GetTransactions(api)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				cursor := internal.DecodeCursorResponse(t, rsp.Body, core.Transaction{})

				assert.Len(t, cursor.Data, 2)
				assert.False(t, cursor.HasMore)
				assert.EqualValues(t, 2, cursor.Total)
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
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				tx := make([]core.Transaction, 0)
				internal.DecodeSingleResponse(t, rsp.Body, &tx)

				rsp = internal.PostTransactionMetadata(t, api, tx[0].ID, core.Metadata{
					"foo": json.RawMessage(`"bar"`),
				})
				assert.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

				rsp = internal.GetTransaction(api, 0)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				ret := core.Transaction{}
				internal.DecodeSingleResponse(t, rsp.Body, &ret)

				assert.EqualValues(t, core.Metadata{
					"foo": json.RawMessage(`"bar"`),
				}, ret.Metadata)
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

				store, _, err := driver.NewStore(context.Background(), "quickstart")
				assert.NoError(t, err)

				// Grab all potential connections
				for i := 0; i < pgtesting.MaxConnections; i++ {
					tx, err := store.(*sqlstorage.Store).Schema().BeginTx(context.Background(), &sql.TxOptions{})
					assert.NoError(t, err)
					defer tx.Rollback()
				}

				rsp := internal.GetTransactions(api)
				assert.Equal(t, http.StatusServiceUnavailable, rsp.Result().StatusCode)
				return nil
			},
		})
	}))
}
