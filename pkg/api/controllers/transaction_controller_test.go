package controllers_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/internal"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

func TestPostTransactions(t *testing.T) {
	type testCase struct {
		name                string
		initialTransactions []core.Transaction
		payload             controllers.PostTransactionRequest
		expectedStatusCode  int
		expectedRes         sharedapi.BaseResponse[[]core.ExpandedTransaction]
		expectedErr         sharedapi.ErrorResponse
	}

	var timestamp1 = core.Now().Add(1 * time.Minute)
	var timestamp2 = core.Now().Add(2 * time.Minute)
	var timestamp3 = core.Now().Add(3 * time.Minute)

	testCases := []testCase{
		{
			name: "postings nominal",
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(1000),
						Asset:       "USB",
					},
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedRes: sharedapi.BaseResponse[[]core.ExpandedTransaction]{
				Data: &[]core.ExpandedTransaction{{
					Transaction: core.Transaction{
						TransactionData: core.TransactionData{
							Postings: core.Postings{
								{
									Source:      "world",
									Destination: "central_bank",
									Amount:      big.NewInt(1000),
									Asset:       "USB",
								},
							},
						},
					},
				}},
			},
		},
		{
			name: "postings asset with digit",
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(1000),
						Asset:       "US1234D",
					},
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedRes: sharedapi.BaseResponse[[]core.ExpandedTransaction]{
				Data: &[]core.ExpandedTransaction{{
					Transaction: core.Transaction{
						TransactionData: core.TransactionData{
							Postings: core.Postings{
								{
									Source:      "world",
									Destination: "central_bank",
									Amount:      big.NewInt(1000),
									Asset:       "US1234D",
								},
							},
						},
					},
				}},
			},
		},
		{
			name: "script nominal",
			payload: controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: `
					vars {
						account $acc
					}
					send [COIN 100] (
					  source = @world
					  destination = @centralbank
					)
					send [COIN 100] (
					  source = @centralbank
					  destination = $acc
					)`,
					Vars: map[string]json.RawMessage{
						"acc": json.RawMessage(`"users:001"`),
					},
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedRes: sharedapi.BaseResponse[[]core.ExpandedTransaction]{
				Data: &[]core.ExpandedTransaction{{
					Transaction: core.Transaction{
						TransactionData: core.TransactionData{
							Postings: core.Postings{
								{
									Source:      "world",
									Destination: "centralbank",
									Amount:      big.NewInt(100),
									Asset:       "COIN",
								},
								{
									Source:      "centralbank",
									Destination: "users:001",
									Amount:      big.NewInt(100),
									Asset:       "COIN",
								},
							},
						},
					},
				}},
			},
		},
		{
			name: "script with set_account_meta",
			payload: controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: `
					send [TOK 1000] (
					  source = @world
					  destination = @bar
					)
					set_account_meta(@bar, "foo", "bar")
					`,
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedRes: sharedapi.BaseResponse[[]core.ExpandedTransaction]{
				Data: &[]core.ExpandedTransaction{{
					Transaction: core.Transaction{
						TransactionData: core.TransactionData{
							Postings: core.Postings{
								{
									Source:      "world",
									Destination: "bar",
									Amount:      big.NewInt(1000),
									Asset:       "TOK",
								},
							},
						},
					},
				}},
			},
		},
		{
			name:               "no postings or script",
			payload:            controllers.PostTransactionRequest{},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid payload: should contain either postings or script",
			},
		},
		{
			name: "postings negative amount",
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(-1000),
						Asset:       "USB",
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid posting 0: negative amount",
			},
		},
		{
			name: "postings wrong asset with symbol",
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(1000),
						Asset:       "@TOK",
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid posting 0: invalid asset",
			},
		},
		{
			name: "postings wrong asset with digit as first char",
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(1000),
						Asset:       "1TOK",
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid posting 0: invalid asset",
			},
		},
		{
			name: "postings bad address",
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "#fake",
						Amount:      big.NewInt(1000),
						Asset:       "TOK",
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid posting 0: invalid destination address",
			},
		},
		{
			name: "postings insufficient funds",
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "foo",
						Destination: "bar",
						Amount:      big.NewInt(1000),
						Asset:       "TOK",
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrInsufficientFund,
				ErrorMessage: "[INSUFFICIENT_FUND] account had insufficient funds",
			},
		},
		{
			name: "postings reference conflict",
			initialTransactions: []core.Transaction{{
				TransactionData: core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bar",
							Amount:      big.NewInt(1000),
							Asset:       "TOK",
						},
					},
					Reference: "ref",
				},
			}},
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "bar",
						Amount:      big.NewInt(1000),
						Asset:       "TOK",
					},
				},
				Reference: "ref",
			},
			expectedStatusCode: http.StatusConflict,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrConflict,
				ErrorMessage: "conflict error on reference",
			},
		},
		{
			name: "script failure with insufficient funds",
			payload: controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: `
					send [COIN 100] (
					  source = @centralbank
					  destination = @users:001
					)`,
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrInsufficientFund,
				ErrorMessage: "[INSUFFICIENT_FUND] account had insufficient funds",
				Details:      apierrors.EncodeLink("account had insufficient funds"),
			},
		},
		{
			name: "script failure with metadata override",
			payload: controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: `
					set_tx_meta("priority", "low")

					send [USD/2 99] (
						source=@world
						destination=@user:001
					)`,
				},
				Metadata: core.Metadata{
					"priority": json.RawMessage(`"high"`),
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrScriptMetadataOverride,
				ErrorMessage: "[METADATA_OVERRIDE] cannot override metadata from script",
				Details:      apierrors.EncodeLink("cannot override metadata from script"),
			},
		},
		{
			name: "script failure with no postings",
			payload: controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: `
					set_account_meta(@bar, "foo", "bar")
					`,
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "transaction has no postings",
			},
		},
		{
			name: "script failure with invalid account variable",
			payload: controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: `
					vars {
						account $acc
					}
					send [USD/2 99] (
						source = @world
						destination = $acc
					)
					`,
					Vars: map[string]json.RawMessage{
						"acc": json.RawMessage(`"invalid-acc"`),
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrScriptCompilationFailed,
				ErrorMessage: "[COMPILATION_FAILED] value invalid-acc: accounts should respect pattern ^[a-zA-Z_]+[a-zA-Z0-9_:]*$",
				Details:      apierrors.EncodeLink("value invalid-acc: accounts should respect pattern ^[a-zA-Z_]+[a-zA-Z0-9_:]*$"),
			},
		},
		{
			name: "script failure with invalid monetary variable",
			payload: controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: `
					vars {
						monetary $mon
					}
					send $mon (
						source = @world
						destination = @alice
					)
					`,
					Vars: map[string]json.RawMessage{
						"mon": json.RawMessage(`{"asset": "COIN","amount":-1}`),
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrScriptCompilationFailed,
				ErrorMessage: "[COMPILATION_FAILED] could not set variables: invalid JSON value for variable $mon of type monetary: value [COIN -1]: negative amount",
				Details:      apierrors.EncodeLink("could not set variables: invalid JSON value for variable $mon of type monetary: value [COIN -1]: negative amount"),
			},
		},
		{
			name: "postings and script",
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "alice",
						Amount:      big.NewInt(100),
						Asset:       "COIN",
					},
				},
				Script: core.Script{
					Plain: `
					send [COIN 100] (
					  source = @world
					  destination = @bob
					)`,
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid payload: should contain either postings or script",
			},
		},
		{
			name: "postings with specified timestamp",
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "bar",
						Amount:      big.NewInt(1000),
						Asset:       "TOK",
					},
				},
				Timestamp: timestamp2,
			},
			expectedStatusCode: http.StatusOK,
			expectedRes: sharedapi.BaseResponse[[]core.ExpandedTransaction]{
				Data: &[]core.ExpandedTransaction{{
					Transaction: core.Transaction{
						TransactionData: core.TransactionData{
							Postings: core.Postings{
								{
									Source:      "world",
									Destination: "bar",
									Amount:      big.NewInt(1000),
									Asset:       "TOK",
								},
							},
						},
					},
				}},
			},
		},
		{
			name: "script with specified timestamp",
			payload: controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: `
					send [TOK 1000] (
					  source = @world
					  destination = @bar
					)
					`,
				},
				Timestamp: timestamp3,
			},
			expectedStatusCode: http.StatusOK,
			expectedRes: sharedapi.BaseResponse[[]core.ExpandedTransaction]{
				Data: &[]core.ExpandedTransaction{{
					Transaction: core.Transaction{
						TransactionData: core.TransactionData{
							Postings: core.Postings{
								{
									Source:      "world",
									Destination: "bar",
									Amount:      big.NewInt(1000),
									Asset:       "TOK",
								},
							},
						},
					},
				}},
			},
		},
		{
			name: "postings with specified timestamp prior to last tx",
			initialTransactions: []core.Transaction{{
				TransactionData: core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bar",
							Amount:      big.NewInt(1000),
							Asset:       "TOK",
						},
					},
					Timestamp: timestamp2,
				},
			}},
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "bar",
						Amount:      big.NewInt(1000),
						Asset:       "TOK",
					},
				},
				Timestamp: timestamp1,
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "cannot pass a timestamp prior to the last transaction:",
			},
		},
		{
			name: "script with specified timestamp prior to last tx",
			initialTransactions: []core.Transaction{
				core.NewTransaction().
					WithPostings(core.NewPosting("world", "bob", "COIN", big.NewInt(100))).
					WithTimestamp(timestamp2),
			},
			payload: controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: `
						send [COIN 100] (
						  source = @world
						  destination = @bob
						)`,
				},
				Timestamp: timestamp1,
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "cannot pass a timestamp prior to the last transaction:",
			},
		},
		{
			name: "short asset",
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "bank",
						Amount:      big.NewInt(1000),
						Asset:       "F/9",
					},
				},
				Timestamp: timestamp3,
			},
			expectedStatusCode: http.StatusOK,
			expectedRes: sharedapi.BaseResponse[[]core.ExpandedTransaction]{
				Data: &[]core.ExpandedTransaction{{
					Transaction: core.Transaction{
						TransactionData: core.TransactionData{
							Postings: core.Postings{
								{
									Source:      "world",
									Destination: "bank",
									Amount:      big.NewInt(1000),
									Asset:       "F/9",
								},
							},
							Timestamp: timestamp3,
						},
					},
				}},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			internal.RunTest(t, func(api chi.Router, storageDriver storage.Driver) {

				store, _, err := storageDriver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
				require.NoError(t, err)

				_, err = store.Initialize(context.Background())
				require.NoError(t, err)

				for _, transaction := range tc.initialTransactions {
					log := core.NewTransactionLog(transaction, nil).
						WithReference(transaction.Reference)
					require.NoError(t, store.AppendLog(context.Background(), &log))
				}

				rsp := internal.PostTransaction(t, api, tc.payload, false)
				require.Equal(t, tc.expectedStatusCode, rsp.Result().StatusCode, rsp.Body.String())

				if tc.expectedStatusCode != http.StatusOK {
					actualErr := sharedapi.ErrorResponse{}
					if internal.Decode(t, rsp.Body, &actualErr) {
						require.Equal(t, tc.expectedErr.ErrorCode, actualErr.ErrorCode, actualErr.ErrorMessage)
						if tc.expectedErr.Details != "" {
							require.Equal(t, tc.expectedErr.Details, actualErr.Details)
						}
					}
				} else {
					txs, ok := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
					require.True(t, ok)
					require.Equal(t, (*tc.expectedRes.Data)[0].Postings, txs.Postings)
					require.Equal(t, len((*tc.expectedRes.Data)[0].Metadata), len(txs.Metadata))

					if !tc.payload.Timestamp.IsZero() {
						require.Equal(t, tc.payload.Timestamp, txs.Timestamp)
					}
				}
			})
		})
	}
}

func TestPostTransactionsPreview(t *testing.T) {
	script := `
	send [COIN 100] (
	  source = @world
	  destination = @centralbank
	)`

	internal.RunTest(t, func(api chi.Router, driver storage.Driver) {
		store := internal.GetLedgerStore(t, driver, context.Background())
		t.Run("postings true", func(t *testing.T) {
			rsp := internal.PostTransaction(t, api, controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(1000),
						Asset:       "USD",
					},
				},
			}, true)
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			_, ok := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
			require.True(t, ok)

			cursor, err := store.GetTransactions(context.Background(), *storage.NewTransactionsQuery())
			require.NoError(t, err)
			require.Len(t, cursor.Data, 0)
		})

		t.Run("script true", func(t *testing.T) {
			rsp := internal.PostTransaction(t, api, controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: script,
				},
			}, true)
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			_, ok := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
			require.True(t, ok)

			cursor, err := store.GetTransactions(context.Background(), *storage.NewTransactionsQuery())
			require.NoError(t, err)
			require.Len(t, cursor.Data, 0)
		})
	})
}

func TestPostTransactionInvalidBody(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, storageDriver storage.Driver) {
		t.Run("no JSON", func(t *testing.T) {
			rsp := internal.NewPostOnLedger(t, api, "/transactions", "invalid")
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid transaction format",
			}, err)
		})

		t.Run("JSON without postings", func(t *testing.T) {
			rsp := internal.NewPostOnLedger(t, api, "/transactions", core.Account{Address: "addr"})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid payload: should contain either postings or script",
			}, err)
		})
	})
}

func TestPostTransactionMetadata(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, storageDriver storage.Driver) {
		store, _, err := storageDriver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
		require.NoError(t, err)

		_, err = store.Initialize(context.Background())
		require.NoError(t, err)

		require.NoError(t, store.InsertTransactions(context.Background(), core.ExpandTransactionFromEmptyPreCommitVolumes(
			core.NewTransaction().WithPostings(
				core.NewPosting("world", "central_bank", "USD", big.NewInt(1000)),
			),
		)))

		t.Run("valid", func(t *testing.T) {
			rsp := internal.PostTransactionMetadata(t, api, 0, core.Metadata{
				"foo": json.RawMessage(`"bar"`),
			})
			require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)
		})

		t.Run("different metadata on same key should replace it", func(t *testing.T) {
			rsp := internal.PostTransactionMetadata(t, api, 0, core.Metadata{
				"foo": "baz",
			})
			require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)
		})

		t.Run("transaction not found", func(t *testing.T) {
			rsp := internal.PostTransactionMetadata(t, api, 42, core.Metadata{
				"foo": "baz",
			})
			require.Equal(t, http.StatusNotFound, rsp.Result().StatusCode)

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrNotFound,
				ErrorMessage: "transaction not found",
			}, err)
		})

		t.Run("no JSON", func(t *testing.T) {
			rsp := internal.NewPostOnLedger(t, api, "/transactions/0/metadata", "invalid")
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid metadata format",
			}, err)
		})

		t.Run("invalid txid", func(t *testing.T) {
			rsp := internal.NewPostOnLedger(t, api, "/transactions/invalid/metadata", core.Metadata{
				"foo": json.RawMessage(`"bar"`),
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid transaction ID",
			}, err)
		})
	})
}

func TestGetTransaction(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, storageDriver storage.Driver) {

		store, _, err := storageDriver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
		require.NoError(t, err)

		_, err = store.Initialize(context.Background())
		require.NoError(t, err)

		require.NoError(t, store.InsertTransactions(context.Background(), core.ExpandTransactionFromEmptyPreCommitVolumes(
			core.NewTransaction().
				WithPostings(core.NewPosting("world", "central_bank", "USD", big.NewInt(1000))).
				WithReference("ref").
				WithTimestamp(core.Now()),
		)))

		t.Run("valid txid", func(t *testing.T) {
			rsp := internal.GetTransaction(api, 0)
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

			ret, _ := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
			require.EqualValues(t, core.Postings{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      big.NewInt(1000),
					Asset:       "USD",
				},
			}, ret.Postings)
			require.EqualValues(t, 0, ret.ID)
			require.EqualValues(t, core.Metadata{}, ret.Metadata)
			require.EqualValues(t, "ref", ret.Reference)
			require.NotEmpty(t, ret.Timestamp)
			require.EqualValues(t, core.AccountsAssetsVolumes{
				"world": core.AssetsVolumes{
					"USD": {
						Input:  big.NewInt(0),
						Output: big.NewInt(0),
					},
				},
				"central_bank": core.AssetsVolumes{
					"USD": {
						Input:  big.NewInt(0),
						Output: big.NewInt(0),
					},
				},
			}, ret.PreCommitVolumes)
			require.EqualValues(t, core.AccountsAssetsVolumes{
				"world": core.AssetsVolumes{
					"USD": {
						Input:  big.NewInt(0),
						Output: big.NewInt(1000),
					},
				},
				"central_bank": core.AssetsVolumes{
					"USD": {
						Input:  big.NewInt(1000),
						Output: big.NewInt(0),
					},
				},
			}, ret.PostCommitVolumes)
		})

		t.Run("unknown txid", func(t *testing.T) {
			rsp := internal.GetTransaction(api, 42)
			require.Equal(t, http.StatusNotFound, rsp.Result().StatusCode)

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrNotFound,
				ErrorMessage: "transaction not found",
			}, err)
		})

		t.Run("invalid txid", func(t *testing.T) {
			rsp := internal.NewGetOnLedger(api, "/transactions/invalid")
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid transaction ID",
			}, err)
		})
	})
}

func TestTransactions(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, driver storage.Driver) {
		now := core.Now()
		tx1 := core.ExpandedTransaction{
			Transaction: core.Transaction{
				TransactionData: core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank1",
							Amount:      big.NewInt(1000),
							Asset:       "USD",
						},
					},
					Reference: "ref:001",
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
							Destination: "central_bank2",
							Amount:      big.NewInt(1000),
							Asset:       "USD",
						},
					},
					Metadata: core.Metadata{
						"foo": "bar",
					},
					Reference: "ref:002",
					Timestamp: now.Add(-2 * time.Hour),
				},
			},
		}
		tx3 := core.ExpandedTransaction{
			Transaction: core.Transaction{
				ID: 2,
				TransactionData: core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "central_bank1",
							Destination: "alice",
							Amount:      big.NewInt(10),
							Asset:       "USD",
						},
					},
					Reference: "ref:003",
					Metadata: core.Metadata{
						"priority": "high",
					},
					Timestamp: now.Add(-1 * time.Hour),
				},
			},
		}
		store := internal.GetLedgerStore(t, driver, context.Background())
		_, err := store.Initialize(context.Background())
		require.NoError(t, err)

		err = store.InsertTransactions(context.Background(), tx1, tx2, tx3)
		require.NoError(t, err)

		var tx1Timestamp, tx2Timestamp core.Time
		t.Run("Get", func(t *testing.T) {
			t.Run("all", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				// all transactions
				require.Len(t, cursor.Data, 3)
				require.Equal(t, cursor.Data[0].ID, uint64(2))
				require.Equal(t, cursor.Data[1].ID, uint64(1))
				require.Equal(t, cursor.Data[2].ID, uint64(0))

				tx1Timestamp = cursor.Data[1].Timestamp
				tx2Timestamp = cursor.Data[0].Timestamp
			})

			t.Run("metadata", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					"metadata[priority]": []string{"high"},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)

				require.Len(t, cursor.Data, 1)
				require.Equal(t, cursor.Data[0].ID, tx3.ID)
			})

			t.Run("after", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					"after": []string{"1"},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				// 1 transaction: txid 0
				require.Len(t, cursor.Data, 1)
				require.Equal(t, cursor.Data[0].ID, uint64(0))
			})

			t.Run("invalid after", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
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

			t.Run("reference", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					"reference": []string{"ref:001"},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				// 1 transaction: txid 0
				require.Len(t, cursor.Data, 1)
				require.Equal(t, cursor.Data[0].ID, uint64(0))
			})

			t.Run("destination", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					"destination": []string{"central_bank1"},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				// 1 transaction: txid 0
				require.Len(t, cursor.Data, 1)
				require.Equal(t, cursor.Data[0].ID, uint64(0))
			})

			t.Run("source", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					"source": []string{"world"},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				// 2 transactions: txid 0 and txid 1
				require.Len(t, cursor.Data, 2)
				require.Equal(t, cursor.Data[0].ID, uint64(1))
				require.Equal(t, cursor.Data[1].ID, uint64(0))
			})

			t.Run("account", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					"account": []string{"world"},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				// 2 transactions: txid 0 and txid 1
				require.Len(t, cursor.Data, 2)
				require.Equal(t, cursor.Data[0].ID, uint64(1))
				require.Equal(t, cursor.Data[1].ID, uint64(0))
			})

			t.Run("account no result", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					"account": []string{"central"},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				require.Len(t, cursor.Data, 0)
			})

			t.Run("account regex expr", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					"account": []string{"central.*"},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				require.Len(t, cursor.Data, 3)
			})

			t.Run("time range", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					controllers.QueryKeyStartTime: []string{tx1Timestamp.Format(time.RFC3339)},
					controllers.QueryKeyEndTime:   []string{tx2Timestamp.Format(time.RFC3339)},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				// 1 transaction: txid 1
				require.Len(t, cursor.Data, 1)
			})

			t.Run("only start time", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					controllers.QueryKeyStartTime: []string{core.Now().Add(time.Second).Format(time.RFC3339)},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				// no transaction
				require.Len(t, cursor.Data, 0)
			})

			t.Run("only end time", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					controllers.QueryKeyEndTime: []string{core.Now().Add(time.Second).Format(time.RFC3339)},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				// all transactions
				require.Len(t, cursor.Data, 3)
			})

			t.Run("invalid start time", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
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
				rsp := internal.GetTransactions(api, url.Values{
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

			t.Run("invalid page size", func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					controllers.QueryKeyPageSize: []string{"invalid page size"},
				})
				require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

				err := sharedapi.ErrorResponse{}
				internal.Decode(t, rsp.Body, &err)
				require.EqualValues(t, sharedapi.ErrorResponse{
					ErrorCode:    apierrors.ErrValidation,
					ErrorMessage: controllers.ErrInvalidPageSize.Error(),
				}, err)
			})

			to := ledgerstore.TxsPaginationToken{}
			raw, err := json.Marshal(to)
			require.NoError(t, err)

			t.Run(fmt.Sprintf("valid empty %s", controllers.QueryKeyCursor), func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
					controllers.QueryKeyCursor: []string{base64.RawURLEncoding.EncodeToString(raw)},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
			})

			t.Run(fmt.Sprintf("valid empty %s with any other param is forbidden", controllers.QueryKeyCursor), func(t *testing.T) {
				rsp := internal.GetTransactions(api, url.Values{
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
				rsp := internal.GetTransactions(api, url.Values{
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
				rsp := internal.GetTransactions(api, url.Values{
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

		t.Run("Count", func(t *testing.T) {
			t.Run("all", func(t *testing.T) {
				rsp := internal.CountTransactions(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				require.Equal(t, "3", rsp.Header().Get("Count"))
			})

			t.Run("time range", func(t *testing.T) {
				rsp := internal.CountTransactions(api, url.Values{
					controllers.QueryKeyStartTime: []string{tx1Timestamp.Format(time.RFC3339)},
					controllers.QueryKeyEndTime:   []string{tx2Timestamp.Format(time.RFC3339)},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				require.Equal(t, "1", rsp.Header().Get("Count"))
			})

			t.Run("invalid start time", func(t *testing.T) {
				rsp := internal.CountTransactions(api, url.Values{
					controllers.QueryKeyStartTime: []string{"invalid"},
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
				rsp := internal.CountTransactions(api, url.Values{
					controllers.QueryKeyEndTime: []string{"invalid"},
				})
				require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

				err := sharedapi.ErrorResponse{}
				internal.Decode(t, rsp.Body, &err)
				require.EqualValues(t, sharedapi.ErrorResponse{
					ErrorCode:    apierrors.ErrValidation,
					ErrorMessage: controllers.ErrInvalidEndTime.Error(),
				}, err)
			})
		})
	})
}

func TestGetTransactionsWithPageSize(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, driver storage.Driver) {
		now := core.Now().UTC()
		store := internal.GetLedgerStore(t, driver, context.Background())

		_, err := store.Initialize(context.Background())
		require.NoError(t, err)

		//TODO(gfyrag): Refine tests, we don't need to insert 3000 tx to test a behavior
		for i := 0; i < 3*controllers.MaxPageSize; i++ {
			tx := core.ExpandedTransaction{
				Transaction: core.Transaction{
					ID: uint64(i),
					TransactionData: core.TransactionData{
						Postings: core.Postings{
							{
								Source:      "world",
								Destination: fmt.Sprintf("account:%d", i),
								Amount:      big.NewInt(1000),
								Asset:       "USD",
							},
						},
						Timestamp: now,
					},
				},
			}
			require.NoError(t, store.InsertTransactions(context.Background(), tx))
		}

		t.Run("invalid page size", func(t *testing.T) {
			rsp := internal.GetTransactions(api, url.Values{
				controllers.QueryKeyPageSize: []string{"nan"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: controllers.ErrInvalidPageSize.Error(),
			}, err)
		})
		t.Run("page size over maximum", func(t *testing.T) {
			httpResponse := internal.GetTransactions(api, url.Values{
				controllers.QueryKeyPageSize: []string{fmt.Sprintf("%d", 2*controllers.MaxPageSize)},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, httpResponse.Body)
			require.Len(t, cursor.Data, controllers.MaxPageSize)
			require.Equal(t, cursor.PageSize, controllers.MaxPageSize)
			require.NotEmpty(t, cursor.Next)
			require.True(t, cursor.HasMore)
		})
		t.Run("with page size greater than max count", func(t *testing.T) {
			httpResponse := internal.GetTransactions(api, url.Values{
				controllers.QueryKeyPageSize: []string{fmt.Sprintf("%d", controllers.MaxPageSize)},
				"after":                      []string{fmt.Sprintf("%d", controllers.MaxPageSize-100)},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, httpResponse.Body)
			require.Len(t, cursor.Data, controllers.MaxPageSize-100)
			require.Equal(t, cursor.PageSize, controllers.MaxPageSize)
			require.Empty(t, cursor.Next)
			require.False(t, cursor.HasMore)
		})
		t.Run("with page size lower than max count", func(t *testing.T) {
			httpResponse := internal.GetTransactions(api, url.Values{
				controllers.QueryKeyPageSize: []string{fmt.Sprintf("%d", controllers.MaxPageSize/10)},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, httpResponse.Body)
			require.Len(t, cursor.Data, controllers.MaxPageSize/10)
			require.Equal(t, cursor.PageSize, controllers.MaxPageSize/10)
			require.NotEmpty(t, cursor.Next)
			require.True(t, cursor.HasMore)
		})
	})
}

func TestRevertTransaction(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, driver storage.Driver) {
		store, _, err := driver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
		require.NoError(t, err)

		_, err = store.Initialize(context.Background())
		require.NoError(t, err)

		tx1 := core.NewTransaction().
			WithPostings(core.NewPosting("world", "alice", "USD", big.NewInt(100))).
			WithReference("ref:23434656").
			WithMetadata(core.Metadata{
				"foo1": "bar1",
			}).
			WithTimestamp(core.Now().Add(-3 * time.Minute))
		require.NoError(t, store.InsertTransactions(context.Background(), core.ExpandTransactionFromEmptyPreCommitVolumes(tx1)))
		log := core.NewTransactionLog(tx1, nil)
		require.NoError(t, store.AppendLog(context.Background(), &log))

		tx2 := core.NewTransaction().
			WithPostings(core.NewPosting("world", "bob", "USD", big.NewInt(100))).
			WithReference("ref:534646").
			WithMetadata(core.Metadata{
				"foo2": "bar2",
			}).
			WithID(1).
			WithTimestamp(core.Now().Add(-2 * time.Minute))
		require.NoError(t, store.InsertTransactions(context.Background(), core.ExpandTransactionFromEmptyPreCommitVolumes(tx2)))
		log2 := core.NewTransactionLog(tx2, nil)
		require.NoError(t, store.AppendLog(context.Background(), &log2))

		tx3 := core.NewTransaction().
			WithPostings(core.NewPosting("alice", "bob", "USD", big.NewInt(3))).
			WithMetadata(core.Metadata{
				"foo2": "bar2",
			}).
			WithID(2).
			WithTimestamp(core.Now().Add(-time.Minute))
		require.NoError(t, store.InsertTransactions(context.Background(), core.ExpandTransactionFromEmptyPreCommitVolumes(tx3)))
		log3 := core.NewTransactionLog(tx3, nil)
		require.NoError(t, store.AppendLog(context.Background(), &log3))

		require.NoError(t, store.EnsureAccountExists(context.Background(), "world"))
		require.NoError(t, store.EnsureAccountExists(context.Background(), "bob"))
		require.NoError(t, store.EnsureAccountExists(context.Background(), "alice"))
		require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
			"world": {
				"USD": core.NewEmptyVolumes().WithOutput(big.NewInt(200)),
			},
			"alice": {
				"USD": core.NewEmptyVolumes().WithInput(big.NewInt(100)).WithOutput(big.NewInt(3)),
			},
			"bob": {
				"USD": core.NewEmptyVolumes().WithInput(big.NewInt(103)),
			},
		}))

		t.Run("first revert should succeed", func(t *testing.T) {
			rsp := internal.RevertTransaction(api, 2)
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

			res, _ := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
			require.EqualValues(t, 3, res.ID)
			require.Equal(t, core.Metadata{
				core.RevertMetadataSpecKey(): "2",
			}, res.Metadata)
		})

		t.Run("transaction not found", func(t *testing.T) {
			rsp := internal.RevertTransaction(api, uint64(42))
			require.Equal(t, http.StatusNotFound, rsp.Result().StatusCode, rsp.Body.String())
			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrNotFound,
				ErrorMessage: "transaction 42 not found",
			}, err)
		})

		//TODO(gfyrag): tests MUST not depends on previous tests
		//use a table driven test
		t.Run("second revert should fail", func(t *testing.T) {
			require.NoError(t, store.UpdateTransactionMetadata(context.Background(), 2, core.RevertedMetadata(3)))

			rsp := internal.RevertTransaction(api, 2)
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "transaction 2 already reverted",
			}, err)
		})

		t.Run("invalid transaction ID format", func(t *testing.T) {
			rsp := internal.NewPostOnLedger(t, api, "/transactions/invalid/revert", nil)
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid transaction ID",
			}, err)
		})
	})
}

func TestPostTransactionsScriptConflict(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, driver storage.Driver) {
		store, _, err := driver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
		require.NoError(t, err)
		_, err = store.Initialize(context.Background())
		require.NoError(t, err)
		log := core.NewTransactionLog(
			core.NewTransaction().
				WithPostings(core.NewPosting("world", "centralbank", "COIN", big.NewInt(100))).
				WithReference("1234"),
			nil,
		)
		require.NoError(t, store.AppendLog(context.Background(), &log))
		rsp := internal.PostTransaction(t, api, controllers.PostTransactionRequest{
			Script: core.Script{
				Plain: `
				send [COIN 100] (
				  source = @world
				  destination = @centralbank
				)`,
			},
			Reference: "1234",
		}, false)

		require.Equal(t, http.StatusConflict, rsp.Result().StatusCode)
		actualErr := sharedapi.ErrorResponse{}
		internal.Decode(t, rsp.Body, &actualErr)
		require.Equal(t, apierrors.ErrConflict, actualErr.ErrorCode)
	})
}
