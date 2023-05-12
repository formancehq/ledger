package controllers_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestPostTransactions(t *testing.T) {
	type testCase struct {
		name               string
		payload            []controllers.PostTransaction
		expectedStatusCode int
		expectedRes        sharedapi.BaseResponse[[]core.ExpandedTransaction]
		expectedErr        sharedapi.ErrorResponse
	}

	var timestamp1 = time.Now().Add(1 * time.Minute).Truncate(time.Second)
	var timestamp2 = time.Now().Add(2 * time.Minute).Truncate(time.Second)
	var timestamp3 = time.Now().Add(3 * time.Minute).Truncate(time.Second)

	testCases := []testCase{
		{
			name: "postings nominal",
			payload: []controllers.PostTransaction{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "USB",
						},
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
									Amount:      core.NewMonetaryInt(1000),
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
			payload: []controllers.PostTransaction{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "US1234D",
						},
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
									Amount:      core.NewMonetaryInt(1000),
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
			payload: []controllers.PostTransaction{{
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
			}},
			expectedStatusCode: http.StatusOK,
			expectedRes: sharedapi.BaseResponse[[]core.ExpandedTransaction]{
				Data: &[]core.ExpandedTransaction{{
					Transaction: core.Transaction{
						TransactionData: core.TransactionData{
							Postings: core.Postings{
								{
									Source:      "world",
									Destination: "centralbank",
									Amount:      core.NewMonetaryInt(100),
									Asset:       "COIN",
								},
								{
									Source:      "centralbank",
									Destination: "users:001",
									Amount:      core.NewMonetaryInt(100),
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
			payload: []controllers.PostTransaction{{
				Script: core.Script{
					Plain: `
					send [TOK 1000] (
					  source = @world
					  destination = @bar
					)
					set_account_meta(@bar, "foo", "bar")
					`,
				},
			}},
			expectedStatusCode: http.StatusOK,
			expectedRes: sharedapi.BaseResponse[[]core.ExpandedTransaction]{
				Data: &[]core.ExpandedTransaction{{
					Transaction: core.Transaction{
						TransactionData: core.TransactionData{
							Postings: core.Postings{
								{
									Source:      "world",
									Destination: "bar",
									Amount:      core.NewMonetaryInt(1000),
									Asset:       "TOK",
								},
							},
						},
					},
				}},
			},
		},
		{
			name: "no postings or script",
			payload: []controllers.PostTransaction{
				{},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid payload: should contain either postings or script",
			},
		},
		{
			name: "postings negative amount",
			payload: []controllers.PostTransaction{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      core.NewMonetaryInt(-1000),
							Asset:       "USB",
						},
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
			payload: []controllers.PostTransaction{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "@TOK",
						},
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
			payload: []controllers.PostTransaction{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "1TOK",
						},
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
			payload: []controllers.PostTransaction{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "#fake",
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "TOK",
						},
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
			payload: []controllers.PostTransaction{
				{
					Postings: core.Postings{
						{
							Source:      "foo",
							Destination: "bar",
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "TOK",
						},
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
			payload: []controllers.PostTransaction{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bar",
							Amount:      core.NewMonetaryInt(1000),
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
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "TOK",
						},
					},
					Reference: "ref",
				},
			},
			expectedStatusCode: http.StatusConflict,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrConflict,
				ErrorMessage: "conflict error on reference",
			},
		},
		{
			name: "script failure with insufficient funds",
			payload: []controllers.PostTransaction{{
				Script: core.Script{
					Plain: `
					send [COIN 100] (
					  source = @centralbank
					  destination = @users:001
					)`,
				},
			}},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrInsufficientFund,
				ErrorMessage: "[INSUFFICIENT_FUND] account had insufficient funds",
				Details:      apierrors.EncodeLink("account had insufficient funds"),
			},
		},
		{
			name: "script failure with metadata override",
			payload: []controllers.PostTransaction{{
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
			}},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrScriptMetadataOverride,
				ErrorMessage: "[METADATA_OVERRIDE] cannot override metadata from script",
				Details:      apierrors.EncodeLink("cannot override metadata from script"),
			},
		},
		{
			name: "script failure with no postings",
			payload: []controllers.PostTransaction{{
				Script: core.Script{
					Plain: `
					set_account_meta(@bar, "foo", "bar")
					`,
				},
			}},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "transaction has no postings",
			},
		},
		{
			name: "script failure with invalid account variable",
			payload: []controllers.PostTransaction{{
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
			}},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrScriptCompilationFailed,
				ErrorMessage: "[COMPILATION_FAILED] could not set variables: invalid JSON value for variable $acc of type account: value invalid-acc: accounts should respect pattern ^[a-zA-Z_]+[a-zA-Z0-9_:]*$",
				Details:      apierrors.EncodeLink("could not set variables: invalid JSON value for variable $acc of type account: value invalid-acc: accounts should respect pattern ^[a-zA-Z_]+[a-zA-Z0-9_:]*$"),
			},
		},
		{
			name: "script failure with invalid monetary variable",
			payload: []controllers.PostTransaction{{
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
			}},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrScriptCompilationFailed,
				ErrorMessage: "[COMPILATION_FAILED] could not set variables: invalid JSON value for variable $mon of type monetary: value [COIN -1]: negative amount",
				Details:      apierrors.EncodeLink("could not set variables: invalid JSON value for variable $mon of type monetary: value [COIN -1]: negative amount"),
			},
		},
		{
			name: "postings and script",
			payload: []controllers.PostTransaction{{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "alice",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "COIN",
					},
				},
				Script: core.Script{
					Plain: `
					send [COIN 100] (
					  source = @world
					  destination = @bob
					)`,
				}},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid payload: should contain either postings or script",
			},
		},
		{
			name: "postings with specified timestamp",
			payload: []controllers.PostTransaction{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bar",
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "TOK",
						},
					},
					Timestamp: timestamp2,
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
									Amount:      core.NewMonetaryInt(1000),
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
			payload: []controllers.PostTransaction{{
				Script: core.Script{
					Plain: `
					send [TOK 1000] (
					  source = @world
					  destination = @bar
					)
					`,
				},
				Timestamp: timestamp3,
			}},
			expectedStatusCode: http.StatusOK,
			expectedRes: sharedapi.BaseResponse[[]core.ExpandedTransaction]{
				Data: &[]core.ExpandedTransaction{{
					Transaction: core.Transaction{
						TransactionData: core.TransactionData{
							Postings: core.Postings{
								{
									Source:      "world",
									Destination: "bar",
									Amount:      core.NewMonetaryInt(1000),
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
			payload: []controllers.PostTransaction{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bar",
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "TOK",
						},
					},
					Timestamp: timestamp1,
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "cannot pass a timestamp prior to the last transaction:",
			},
		},
		{
			name: "script with specified timestamp prior to last tx",
			payload: []controllers.PostTransaction{
				{
					Script: core.Script{
						Plain: `
						send [COIN 100] (
						  source = @world
						  destination = @bob
						)`,
					},
					Timestamp: timestamp1,
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr: sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "cannot pass a timestamp prior to the last transaction:",
			},
		},
		{
			name: "short asset",
			payload: []controllers.PostTransaction{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bank",
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "F/9",
						},
					},
					Timestamp: timestamp3,
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
									Destination: "bank",
									Amount:      core.NewMonetaryInt(1000),
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

	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				for _, tc := range testCases {
					t.Run(tc.name, func(t *testing.T) {
						for i := 0; i < len(tc.payload)-1; i++ {
							rsp := internal.PostTransaction(t, api, tc.payload[i], false)
							require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
							tx, ok := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
							require.True(t, ok)
							if !tc.payload[i].Timestamp.IsZero() {
								require.Equal(t, tc.payload[i].Timestamp.UTC(), tx.Timestamp)
							}
						}
						tcIndex := 0
						if len(tc.payload) > 0 {
							tcIndex = len(tc.payload) - 1
						}
						rsp := internal.PostTransaction(t, api, tc.payload[tcIndex], false)
						require.Equal(t, tc.expectedStatusCode, rsp.Result().StatusCode, rsp.Body.String())

						if tc.expectedStatusCode != http.StatusOK {
							actualErr := sharedapi.ErrorResponse{}
							if internal.Decode(t, rsp.Body, &actualErr) {
								require.Equal(t, tc.expectedErr.ErrorCode, actualErr.ErrorCode, actualErr.ErrorMessage)
								require.Contains(t, actualErr.ErrorMessage, tc.expectedErr.ErrorMessage)
								if tc.expectedErr.Details != "" {
									require.Equal(t, tc.expectedErr.Details, actualErr.Details)
								}
							}
						} else {
							txs, ok := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
							require.True(t, ok)
							require.Equal(t, (*tc.expectedRes.Data)[0].Postings, txs.Postings)
							require.Equal(t, len((*tc.expectedRes.Data)[0].Metadata), len(txs.Metadata))
							if !tc.payload[tcIndex].Timestamp.IsZero() {
								require.Equal(t, tc.payload[tcIndex].Timestamp.UTC(), txs.Timestamp)
							}
						}
					})
				}

				return nil
			},
		})
	}))
}

func TestPostTransactionsPreview(t *testing.T) {
	script := `
	send [COIN 100] (
	  source = @world
	  destination = @centralbank
	)`

	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				store := internal.GetLedgerStore(t, driver, ctx)

				t.Run("postings true", func(t *testing.T) {
					rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
						Postings: core.Postings{
							{
								Source:      "world",
								Destination: "central_bank",
								Amount:      core.NewMonetaryInt(1000),
								Asset:       "USD",
							},
						},
					}, true)
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					_, ok := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
					require.True(t, ok)

					cursor, err := store.GetTransactions(ctx, *ledger.NewTransactionsQuery())
					require.NoError(t, err)
					require.Len(t, cursor.Data, 0)
				})

				t.Run("script true", func(t *testing.T) {
					rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
						Script: core.Script{
							Plain: script,
						},
					}, true)
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					_, ok := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
					require.True(t, ok)

					cursor, err := store.GetTransactions(ctx, *ledger.NewTransactionsQuery())
					require.NoError(t, err)
					require.Len(t, cursor.Data, 0)
				})

				t.Run("postings false", func(t *testing.T) {
					rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
						Postings: core.Postings{
							{
								Source:      "world",
								Destination: "central_bank",
								Amount:      core.NewMonetaryInt(1000),
								Asset:       "USD",
							},
						},
						Reference: "refPostings",
					}, false)
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					_, ok := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
					require.True(t, ok)

					cursor, err := store.GetTransactions(ctx, *ledger.NewTransactionsQuery())
					require.NoError(t, err)
					require.Len(t, cursor.Data, 1)
					require.Equal(t, "refPostings", cursor.Data[0].Reference)
				})

				t.Run("script false", func(t *testing.T) {
					rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
						Script: core.Script{
							Plain: script,
						},
						Reference: "refScript",
					}, false)
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					_, ok := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
					require.True(t, ok)

					cursor, err := store.GetTransactions(ctx, *ledger.NewTransactionsQuery())
					require.NoError(t, err)
					require.Len(t, cursor.Data, 2)
					require.Equal(t, "refScript", cursor.Data[0].Reference)
				})

				return nil
			},
		})
	}))
}

func TestPostTransactionsOverdraft(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				t.Run("simple", func(t *testing.T) {
					rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
						Script: core.Script{
							Plain: `
							send [USD/2 100] (
							  source = @users:42 allowing unbounded overdraft
							  destination = @users:43
							)
							`,
						},
					}, false)
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					_, ok := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
					require.True(t, ok)
				})

				t.Run("complex", func(t *testing.T) {
					rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
						Script: core.Script{
							Plain: `
							send [USD/2 100] (
							  source = @world
							  destination = @users:42:main
							)

							send [USD/2 500] (
							  source = {
								@users:42:main
								@users:42:overdraft allowing overdraft up to [USD/2 200]
								@users:42:credit allowing overdraft up to [USD/2 1000]
							  }
							  destination = @users:100
							)
							`,
						},
					}, false)
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					_, ok := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
					require.True(t, ok)
				})

				return nil
			},
		})
	}))
}

func TestPostTransactionInvalidBody(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
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

				return nil
			},
		})
	}))
}

func TestPostTransactionMetadata(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "USD",
						},
					},
				}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				t.Run("valid", func(t *testing.T) {
					rsp = internal.PostTransactionMetadata(t, api, 0, core.Metadata{
						"foo": json.RawMessage(`"bar"`),
					})
					require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

					rsp = internal.GetTransaction(api, 0)
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					ret, _ := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
					require.EqualValues(t, core.Metadata{
						"foo": "bar",
					}, ret.Metadata)
				})

				t.Run("different metadata on same key should replace it", func(t *testing.T) {
					rsp = internal.PostTransactionMetadata(t, api, 0, core.Metadata{
						"foo": "baz",
					})
					require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

					rsp = internal.GetTransaction(api, 0)
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					ret, _ := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
					require.EqualValues(t, core.Metadata{
						"foo": "baz",
					}, ret.Metadata)
				})

				t.Run("transaction not found", func(t *testing.T) {
					rsp = internal.PostTransactionMetadata(t, api, 42, core.Metadata{
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
					rsp = internal.NewPostOnLedger(t, api, "/transactions/0/metadata", "invalid")
					require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					require.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    apierrors.ErrValidation,
						ErrorMessage: "invalid metadata format",
					}, err)
				})

				t.Run("invalid txid", func(t *testing.T) {
					rsp = internal.NewPostOnLedger(t, api, "/transactions/invalid/metadata", core.Metadata{
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

				return nil
			},
		})
	}))
}

func TestGetTransaction(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      core.NewMonetaryInt(1000),
							Asset:       "USD",
						},
					},
					Reference: "ref",
				}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				t.Run("valid txid", func(t *testing.T) {
					rsp = internal.GetTransaction(api, 0)
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					ret, _ := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
					require.EqualValues(t, core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      core.NewMonetaryInt(1000),
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
								Input:  core.NewMonetaryInt(0),
								Output: core.NewMonetaryInt(0),
							},
						},
						"central_bank": core.AssetsVolumes{
							"USD": {
								Input:  core.NewMonetaryInt(0),
								Output: core.NewMonetaryInt(0),
							},
						},
					}, ret.PreCommitVolumes)
					require.EqualValues(t, core.AccountsAssetsVolumes{
						"world": core.AssetsVolumes{
							"USD": {
								Input:  core.NewMonetaryInt(0),
								Output: core.NewMonetaryInt(1000),
							},
						},
						"central_bank": core.AssetsVolumes{
							"USD": {
								Input:  core.NewMonetaryInt(1000),
								Output: core.NewMonetaryInt(0),
							},
						},
					}, ret.PostCommitVolumes)
				})

				t.Run("unknown txid", func(t *testing.T) {
					rsp = internal.GetTransaction(api, 42)
					require.Equal(t, http.StatusNotFound, rsp.Result().StatusCode)

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					require.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    apierrors.ErrNotFound,
						ErrorMessage: "transaction not found",
					}, err)
				})

				t.Run("invalid txid", func(t *testing.T) {
					rsp = internal.NewGetOnLedger(api, "/transactions/invalid")
					require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					require.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    apierrors.ErrValidation,
						ErrorMessage: "invalid transaction ID",
					}, err)
				})

				return nil
			},
		})
	}))
}

func TestTransactions(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				now := time.Now().UTC()
				tx1 := core.ExpandedTransaction{
					Transaction: core.Transaction{
						TransactionData: core.TransactionData{
							Postings: core.Postings{
								{
									Source:      "world",
									Destination: "central_bank1",
									Amount:      core.NewMonetaryInt(1000),
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
									Amount:      core.NewMonetaryInt(1000),
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
									Amount:      core.NewMonetaryInt(10),
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
				store := internal.GetLedgerStore(t, driver, ctx)
				err := store.Commit(context.Background(), tx1, tx2, tx3)
				require.NoError(t, err)

				var tx1Timestamp, tx2Timestamp time.Time
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
							controllers.QueryKeyStartTime: []string{time.Now().Add(time.Second).Format(time.RFC3339)},
						})
						require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
						cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
						// no transaction
						require.Len(t, cursor.Data, 0)
					})

					t.Run("only end time", func(t *testing.T) {
						rsp := internal.GetTransactions(api, url.Values{
							controllers.QueryKeyEndTime: []string{time.Now().Add(time.Second).Format(time.RFC3339)},
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

					to := sqlstorage.TxsPaginationToken{}
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

				return nil
			},
		})
	}))
}

func TestGetTransactionsWithPageSize(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				now := time.Now().UTC()
				store := internal.GetLedgerStore(t, driver, context.Background())

				for i := 0; i < 3*controllers.MaxPageSize; i++ {
					tx := core.ExpandedTransaction{
						Transaction: core.Transaction{
							ID: uint64(i),
							TransactionData: core.TransactionData{
								Postings: core.Postings{
									{
										Source:      "world",
										Destination: fmt.Sprintf("account:%d", i),
										Amount:      core.NewMonetaryInt(1000),
										Asset:       "USD",
									},
								},
								Timestamp: now,
							},
						},
					}
					require.NoError(t, store.Commit(ctx, tx))
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

				return nil
			},
		})
	}))
}

type transaction struct {
	core.ExpandedTransaction
	PreCommitVolumes  accountsVolumes `json:"preCommitVolumes,omitempty"`
	PostCommitVolumes accountsVolumes `json:"postCommitVolumes,omitempty"`
}
type accountsVolumes map[string]assetsVolumes
type assetsVolumes map[string]core.VolumesWithBalance

func TestTransactionsVolumes(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {

				// Single posting - single asset
				worldAliceUSD := core.NewMonetaryInt(100)

				rsp := internal.PostTransaction(t, api,
					controllers.PostTransaction{
						Postings: core.Postings{
							{
								Source:      "world",
								Destination: "alice",
								Amount:      worldAliceUSD,
								Asset:       "USD",
							},
						},
					}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				tx, ok := internal.DecodeSingleResponse[transaction](t, rsp.Body)
				require.True(t, ok)

				expPreVolumes := accountsVolumes{
					"alice": assetsVolumes{
						"USD": core.VolumesWithBalance{
							Input:   core.NewMonetaryInt(0),
							Output:  core.NewMonetaryInt(0),
							Balance: core.NewMonetaryInt(0),
						},
					},
					"world": assetsVolumes{
						"USD": core.VolumesWithBalance{
							Input:   core.NewMonetaryInt(0),
							Output:  core.NewMonetaryInt(0),
							Balance: core.NewMonetaryInt(0),
						},
					},
				}

				expPostVolumes := accountsVolumes{
					"alice": assetsVolumes{
						"USD": core.VolumesWithBalance{
							Input:   worldAliceUSD,
							Output:  core.NewMonetaryInt(0),
							Balance: worldAliceUSD,
						},
					},
					"world": assetsVolumes{
						"USD": core.VolumesWithBalance{
							Input:   core.NewMonetaryInt(0),
							Output:  worldAliceUSD,
							Balance: worldAliceUSD.Neg(),
						},
					},
				}

				require.Equal(t, expPreVolumes, tx.PreCommitVolumes)
				require.Equal(t, expPostVolumes, tx.PostCommitVolumes)

				rsp = internal.GetTransactions(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[transaction](t, rsp.Body)
				require.Len(t, cursor.Data, 1)

				require.Equal(t, expPreVolumes, cursor.Data[0].PreCommitVolumes)
				require.Equal(t, expPostVolumes, cursor.Data[0].PostCommitVolumes)

				prevVolAliceUSD := expPostVolumes["alice"]["USD"]

				// Single posting - single asset

				aliceBobUSD := core.NewMonetaryInt(93)

				rsp = internal.PostTransaction(t, api,
					controllers.PostTransaction{
						Postings: core.Postings{
							{
								Source:      "alice",
								Destination: "bob",
								Amount:      aliceBobUSD,
								Asset:       "USD",
							},
						},
					}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				tx, ok = internal.DecodeSingleResponse[transaction](t, rsp.Body)
				require.True(t, ok)

				expPreVolumes = accountsVolumes{
					"alice": assetsVolumes{
						"USD": prevVolAliceUSD,
					},
					"bob": assetsVolumes{
						"USD": core.VolumesWithBalance{
							Input:   core.NewMonetaryInt(0),
							Output:  core.NewMonetaryInt(0),
							Balance: core.NewMonetaryInt(0),
						},
					},
				}

				expPostVolumes = accountsVolumes{
					"alice": assetsVolumes{
						"USD": core.VolumesWithBalance{
							Input:   prevVolAliceUSD.Input,
							Output:  prevVolAliceUSD.Output.Add(aliceBobUSD),
							Balance: prevVolAliceUSD.Input.Sub(prevVolAliceUSD.Output).Sub(aliceBobUSD),
						},
					},
					"bob": assetsVolumes{
						"USD": core.VolumesWithBalance{
							Input:   aliceBobUSD,
							Output:  core.NewMonetaryInt(0),
							Balance: aliceBobUSD,
						},
					},
				}

				require.Equal(t, expPreVolumes, tx.PreCommitVolumes)
				require.Equal(t, expPostVolumes, tx.PostCommitVolumes)

				rsp = internal.GetTransactions(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[transaction](t, rsp.Body)
				require.Len(t, cursor.Data, 2)

				require.Equal(t, expPreVolumes, cursor.Data[0].PreCommitVolumes)
				require.Equal(t, expPostVolumes, cursor.Data[0].PostCommitVolumes)

				prevVolAliceUSD = expPostVolumes["alice"]["USD"]
				prevVolBobUSD := expPostVolumes["bob"]["USD"]

				// Multi posting - single asset

				worldBobEUR := core.NewMonetaryInt(156)
				bobAliceEUR := core.NewMonetaryInt(3)

				rsp = internal.PostTransaction(t, api,
					controllers.PostTransaction{
						Postings: core.Postings{
							{
								Source:      "world",
								Destination: "bob",
								Amount:      worldBobEUR,
								Asset:       "EUR",
							},
							{
								Source:      "bob",
								Destination: "alice",
								Amount:      bobAliceEUR,
								Asset:       "EUR",
							},
						},
					}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				tx, ok = internal.DecodeSingleResponse[transaction](t, rsp.Body)
				require.True(t, ok)

				expPreVolumes = accountsVolumes{
					"alice": assetsVolumes{
						"EUR": core.VolumesWithBalance{
							Input:   core.NewMonetaryInt(0),
							Output:  core.NewMonetaryInt(0),
							Balance: core.NewMonetaryInt(0),
						},
					},
					"bob": assetsVolumes{
						"EUR": core.VolumesWithBalance{
							Input:   core.NewMonetaryInt(0),
							Output:  core.NewMonetaryInt(0),
							Balance: core.NewMonetaryInt(0),
						},
					},
					"world": assetsVolumes{
						"EUR": core.VolumesWithBalance{
							Input:   core.NewMonetaryInt(0),
							Output:  core.NewMonetaryInt(0),
							Balance: core.NewMonetaryInt(0),
						},
					},
				}

				expPostVolumes = accountsVolumes{
					"alice": assetsVolumes{
						"EUR": core.VolumesWithBalance{
							Input:   bobAliceEUR,
							Output:  core.NewMonetaryInt(0),
							Balance: bobAliceEUR,
						},
					},
					"bob": assetsVolumes{
						"EUR": core.VolumesWithBalance{
							Input:   worldBobEUR,
							Output:  bobAliceEUR,
							Balance: worldBobEUR.Sub(bobAliceEUR),
						},
					},
					"world": assetsVolumes{
						"EUR": core.VolumesWithBalance{
							Input:   core.NewMonetaryInt(0),
							Output:  worldBobEUR,
							Balance: worldBobEUR.Neg(),
						},
					},
				}

				require.Equal(t, expPreVolumes, tx.PreCommitVolumes)
				require.Equal(t, expPostVolumes, tx.PostCommitVolumes)

				rsp = internal.GetTransactions(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[transaction](t, rsp.Body)
				require.Len(t, cursor.Data, 3)

				require.Equal(t, expPreVolumes, cursor.Data[0].PreCommitVolumes)
				require.Equal(t, expPostVolumes, cursor.Data[0].PostCommitVolumes)

				prevVolAliceEUR := expPostVolumes["alice"]["EUR"]
				prevVolBobEUR := expPostVolumes["bob"]["EUR"]

				// Multi postings - multi assets

				bobAliceUSD := core.NewMonetaryInt(1)
				aliceBobEUR := core.NewMonetaryInt(2)

				rsp = internal.PostTransaction(t, api,
					controllers.PostTransaction{
						Postings: core.Postings{
							{
								Source:      "bob",
								Destination: "alice",
								Amount:      bobAliceUSD,
								Asset:       "USD",
							},
							{
								Source:      "alice",
								Destination: "bob",
								Amount:      aliceBobEUR,
								Asset:       "EUR",
							},
						},
					}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				tx, ok = internal.DecodeSingleResponse[transaction](t, rsp.Body)
				require.True(t, ok)

				expPreVolumes = accountsVolumes{
					"alice": assetsVolumes{
						"EUR": prevVolAliceEUR,
						"USD": prevVolAliceUSD,
					},
					"bob": assetsVolumes{
						"EUR": prevVolBobEUR,
						"USD": prevVolBobUSD,
					},
				}

				expPostVolumes = accountsVolumes{
					"alice": assetsVolumes{
						"EUR": core.VolumesWithBalance{
							Input:   prevVolAliceEUR.Input,
							Output:  prevVolAliceEUR.Output.Add(aliceBobEUR),
							Balance: prevVolAliceEUR.Balance.Sub(aliceBobEUR),
						},
						"USD": core.VolumesWithBalance{
							Input:   prevVolAliceUSD.Input.Add(bobAliceUSD),
							Output:  prevVolAliceUSD.Output,
							Balance: prevVolAliceUSD.Balance.Add(bobAliceUSD),
						},
					},
					"bob": assetsVolumes{
						"EUR": core.VolumesWithBalance{
							Input:   prevVolBobEUR.Input.Add(aliceBobEUR),
							Output:  prevVolBobEUR.Output,
							Balance: prevVolBobEUR.Balance.Add(aliceBobEUR),
						},
						"USD": core.VolumesWithBalance{
							Input:   prevVolBobUSD.Input,
							Output:  prevVolBobUSD.Output.Add(bobAliceUSD),
							Balance: prevVolBobUSD.Balance.Sub(bobAliceUSD),
						},
					},
				}

				require.Equal(t, expPreVolumes, tx.PreCommitVolumes)
				require.Equal(t, expPostVolumes, tx.PostCommitVolumes)

				rsp = internal.GetTransactions(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor = internal.DecodeCursorResponse[transaction](t, rsp.Body)
				require.Len(t, cursor.Data, 4)

				require.Equal(t, expPreVolumes, cursor.Data[0].PreCommitVolumes)
				require.Equal(t, expPostVolumes, cursor.Data[0].PostCommitVolumes)

				return nil
			},
		})
	}))
}

func TestRevertTransaction(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
					Reference: "ref:23434656",
					Metadata: core.Metadata{
						"foo1": "bar1",
					},
				}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bob",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
					Reference: "ref:534646",
					Metadata: core.Metadata{
						"foo2": "bar2",
					},
				}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, controllers.PostTransaction{
					Postings: core.Postings{
						{
							Source:      "alice",
							Destination: "bob",
							Amount:      core.NewMonetaryInt(3),
							Asset:       "USD",
						},
					},
					Reference: "ref:578632",
					Metadata: core.Metadata{
						"foo3": "bar3",
					},
				}, false)
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.GetTransactions(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
				require.Len(t, cursor.Data, 3)
				require.Equal(t, uint64(2), cursor.Data[0].ID)

				revertedTxID := cursor.Data[0].ID

				t.Run("first revert should succeed", func(t *testing.T) {
					rsp := internal.RevertTransaction(api, revertedTxID)
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					res, _ := internal.DecodeSingleResponse[core.ExpandedTransaction](t, rsp.Body)
					require.Equal(t, revertedTxID+1, res.ID)
					require.Equal(t, core.Metadata{
						core.RevertMetadataSpecKey(): fmt.Sprintf("%d", revertedTxID),
					}, res.Metadata)

					revertedByTxID := res.ID

					rsp = internal.GetTransactions(api, url.Values{})
					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.ExpandedTransaction](t, rsp.Body)
					require.Len(t, cursor.Data, 4)
					require.Equal(t, revertedByTxID, cursor.Data[0].ID)
					require.Equal(t, revertedTxID, cursor.Data[1].ID)

					require.Equal(t, core.Metadata{
						"foo3": "bar3",
						core.RevertedMetadataSpecKey(): map[string]any{
							"by": strconv.FormatUint(revertedByTxID, 10),
						},
					}, cursor.Data[1].Metadata)
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

				t.Run("second revert should fail", func(t *testing.T) {
					rsp := internal.RevertTransaction(api, revertedTxID)
					require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					require.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    apierrors.ErrValidation,
						ErrorMessage: fmt.Sprintf("transaction %d already reverted", revertedTxID),
					}, err)
				})

				t.Run("invalid transaction ID format", func(t *testing.T) {
					rsp = internal.NewPostOnLedger(t, api, "/transactions/invalid/revert", nil)
					require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					require.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    apierrors.ErrValidation,
						ErrorMessage: "invalid transaction ID",
					}, err)
				})

				return nil
			},
		})
	}))
}

func TestPostTransactionsScriptConflict(t *testing.T) {
	script := `
 	send [COIN 100] (
 	  source = @world
 	  destination = @centralbank
 	)`

	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				t.Run("first should succeed", func(t *testing.T) {
					rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
						Script: core.Script{
							Plain: script,
						},
						Reference: "1234",
					}, false)

					require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					_, ok := internal.DecodeSingleResponse[transaction](t, rsp.Body)
					require.True(t, ok)
				})

				t.Run("second should fail", func(t *testing.T) {
					rsp := internal.PostTransaction(t, api, controllers.PostTransaction{
						Script: core.Script{
							Plain: script,
						},
						Reference: "1234",
					}, false)

					assert.Equal(t, http.StatusConflict, rsp.Result().StatusCode)
					actualErr := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &actualErr)
					assert.Equal(t, apierrors.ErrConflict, actualErr.ErrorCode)
					assert.Equal(t, "conflict error on reference", actualErr.ErrorMessage)
				})

				return nil
			},
		})
	}))
}
