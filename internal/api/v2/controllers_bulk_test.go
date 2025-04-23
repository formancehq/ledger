package v2

import (
	"bytes"
	"fmt"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/uptrace/bun"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/go-libs/v3/collectionutils"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v3/time"

	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBulk(t *testing.T) {
	t.Parallel()

	now := time.Now()

	type bulkTestCase struct {
		name          string
		queryParams   url.Values
		body          string
		expectations  func(mockLedger *LedgerController)
		expectError   bool
		expectResults []bulking.APIResult
		headers       http.Header
	}

	testCases := []bulkTestCase{
		{
			name: "create transaction",
			body: fmt.Sprintf(`[{
				"action": "CREATE_TRANSACTION",
				"data": {
					"postings": [{
						"source": "world",
						"destination": "bank",
						"amount": 100,
						"asset": "USD/2"
					}],
					"timestamp": "%s"
				}
			}]`, now.Format(time.RFC3339Nano)),
			expectations: func(mockLedger *LedgerController) {
				postings := []ledger.Posting{{
					Source:      "world",
					Destination: "bank",
					Amount:      big.NewInt(100),
					Asset:       "USD/2",
				}}
				mockLedger.EXPECT().
					CreateTransaction(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
						Input: ledgercontroller.CreateTransaction{
							RunScript: ledgercontroller.TxToScriptData(ledger.TransactionData{
								Postings:  postings,
								Timestamp: now,
							}, false),
						},
					}).
					Return(&ledger.Log{
						ID: pointer.For(0),
					}, &ledger.CreatedTransaction{
						Transaction: ledger.Transaction{
							ID: pointer.For(0),
							TransactionData: ledger.TransactionData{
								Postings:  postings,
								Metadata:  metadata.Metadata{},
								Timestamp: now,
							},
						},
					}, nil)
			},
			expectResults: []bulking.APIResult{{
				Data: map[string]any{
					"postings": []any{
						map[string]any{
							"source":      "world",
							"destination": "bank",
							"amount":      float64(100),
							"asset":       "USD/2",
						},
					},
					"timestamp": now.Format(time.RFC3339Nano),
					"metadata":  map[string]any{},
					"reverted":  false,
					"id":        float64(0),
				},
				ResponseType: bulking.ActionCreateTransaction,
			}},
		},
		{
			name: "add metadata on transaction",
			body: `[{
				"action": "ADD_METADATA",
				"data": {
					"targetId": 1,
					"targetType": "TRANSACTION",
					"metadata": {
						"foo": "bar"
					}			
				}
			}]`,
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					SaveTransactionMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveTransactionMetadata]{
						Input: ledgercontroller.SaveTransactionMetadata{
							TransactionID: 1,
							Metadata: metadata.Metadata{
								"foo": "bar",
							},
						},
					}).
					Return(&ledger.Log{
						ID: pointer.For(0),
					}, nil)
			},
			expectResults: []bulking.APIResult{{
				ResponseType: bulking.ActionAddMetadata,
			}},
		},
		{
			name: "add metadata on account",
			body: `[{
				"action": "ADD_METADATA",
				"data": {
					"targetId": "world",
					"targetType": "ACCOUNT",
					"metadata": {
						"foo": "bar"
					}			
				}
			}]`,
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo": "bar",
							},
						},
					}).
					Return(&ledger.Log{
						ID: pointer.For(0),
					}, nil)
			},
			expectResults: []bulking.APIResult{{
				ResponseType: bulking.ActionAddMetadata,
			}},
		},
		{
			name: "revert transaction",
			body: `[{
				"action": "REVERT_TRANSACTION",
				"data": {
					"id": 1	
				}
			}]`,
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					RevertTransaction(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.RevertTransaction]{
						Input: ledgercontroller.RevertTransaction{
							TransactionID: 1,
						},
					}).
					Return(&ledger.Log{
						ID: pointer.For(0),
					}, &ledger.RevertedTransaction{
						RevertedTransaction: ledger.Transaction{
							ID: pointer.For(0),
						},
						RevertTransaction: ledger.Transaction{
							ID: pointer.For(0),
						},
					}, nil)
			},
			expectResults: []bulking.APIResult{{
				Data: map[string]any{
					"id":        float64(0),
					"metadata":  nil,
					"postings":  nil,
					"reverted":  false,
					"timestamp": "0001-01-01T00:00:00Z",
				},
				ResponseType: bulking.ActionRevertTransaction,
			}},
		},
		{
			name: "delete metadata",
			body: `[{
				"action": "DELETE_METADATA",
				"data": {
					"targetType": "TRANSACTION",
					"targetId": 1,
					"key": "foo"
				}
			}]`,
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					DeleteTransactionMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.DeleteTransactionMetadata]{
						Input: ledgercontroller.DeleteTransactionMetadata{
							TransactionID: 1,
							Key:           "foo",
						},
					}).
					Return(&ledger.Log{
						ID: pointer.For(0),
					}, nil)
			},
			expectResults: []bulking.APIResult{{
				ResponseType: bulking.ActionDeleteMetadata,
			}},
		},
		{
			name: "error in the middle",
			body: `[
				{
					"action": "ADD_METADATA",
					"data": {
						"targetId": "world",
						"targetType": "ACCOUNT",
						"metadata": {
							"foo": "bar"
						}			
					}
				},
				{
					"action": "ADD_METADATA",
					"data": {
						"targetId": "world",
						"targetType": "ACCOUNT",
						"metadata": {
							"foo2": "bar2"
						}			
					}
				},
				{
					"action": "ADD_METADATA",
					"data": {
						"targetId": "world",
						"targetType": "ACCOUNT",
						"metadata": {
							"foo3": "bar3"
						}			
					}
				}
			]`,
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo": "bar",
							},
						},
					}).
					Return(&ledger.Log{
						ID: pointer.For(0),
					}, nil)
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo2": "bar2",
							},
						},
					}).
					Return(nil, errors.New("unexpected error"))
			},
			expectResults: []bulking.APIResult{{
				ResponseType: bulking.ActionAddMetadata,
			}, {
				ErrorCode:        api.ErrorInternal,
				ErrorDescription: "unexpected error",
				ResponseType:     "ERROR",
			}, {
				ErrorCode:        api.ErrorInternal,
				ErrorDescription: "context canceled",
				ResponseType:     "ERROR",
			}},
			expectError: true,
		},
		{
			name: "error in the middle with continue on failure",
			body: `[
				{
					"action": "ADD_METADATA",
					"data": {
						"targetId": "world",
						"targetType": "ACCOUNT",
						"metadata": {
							"foo": "bar"
						}			
					}
				},
				{
					"action": "ADD_METADATA",
					"data": {
						"targetId": "world",
						"targetType": "ACCOUNT",
						"metadata": {
							"foo2": "bar2"
						}			
					}
				},
				{
					"action": "ADD_METADATA",
					"data": {
						"targetId": "world",
						"targetType": "ACCOUNT",
						"metadata": {
							"foo3": "bar3"
						}			
					}
				}
			]`,
			queryParams: map[string][]string{
				"continueOnFailure": {"true"},
			},
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo": "bar",
							},
						},
					}).
					Return(&ledger.Log{
						ID: pointer.For(0),
					}, nil)
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo2": "bar2",
							},
						},
					}).
					Return(nil, errors.New("unexpected error"))
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo3": "bar3",
							},
						},
					}).
					Return(&ledger.Log{
						ID: pointer.For(0),
					}, nil)
			},
			expectResults: []bulking.APIResult{{
				ResponseType: bulking.ActionAddMetadata,
			}, {
				ResponseType:     "ERROR",
				ErrorCode:        api.ErrorInternal,
				ErrorDescription: "unexpected error",
			}, {
				ResponseType: bulking.ActionAddMetadata,
			}},
			expectError: true,
		},
		{
			name: "with atomic",
			body: `[
				{
					"action": "ADD_METADATA",
					"data": {
						"targetId": "world",
						"targetType": "ACCOUNT",
						"metadata": {
							"foo": "bar"
						}			
					}
				},
				{
					"action": "ADD_METADATA",
					"data": {
						"targetId": "world",
						"targetType": "ACCOUNT",
						"metadata": {
							"foo2": "bar2"
						}			
					}
				}
			]`,
			queryParams: map[string][]string{
				"atomic": {"true"},
			},
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					BeginTX(gomock.Any(), nil).
					Return(mockLedger, &bun.Tx{}, nil)

				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo": "bar",
							},
						},
					}).
					Return(&ledger.Log{
						ID: pointer.For(0),
					}, nil)

				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo2": "bar2",
							},
						},
					}).
					Return(&ledger.Log{
						ID: pointer.For(0),
					}, nil)

				mockLedger.EXPECT().
					Commit(gomock.Any()).
					Return(nil)
			},
			expectResults: []bulking.APIResult{{
				ResponseType: bulking.ActionAddMetadata,
			}, {
				ResponseType: bulking.ActionAddMetadata,
			}},
		},
		{
			name: "with custom content type",
			headers: map[string][]string{
				"Content-Type": {"application/json; charset=utf-8"},
			},
			body: fmt.Sprintf(`[{
				"action": "CREATE_TRANSACTION",
				"data": {
					"postings": [{
						"source": "world",
						"destination": "bank",
						"amount": 100,
						"asset": "USD/2"
					}],
					"timestamp": "%s"
				}
			}]`, now.Format(time.RFC3339Nano)),
			expectations: func(mockLedger *LedgerController) {
				postings := []ledger.Posting{{
					Source:      "world",
					Destination: "bank",
					Amount:      big.NewInt(100),
					Asset:       "USD/2",
				}}
				mockLedger.EXPECT().
					CreateTransaction(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
						Input: ledgercontroller.CreateTransaction{
							RunScript: ledgercontroller.TxToScriptData(ledger.TransactionData{
								Postings:  postings,
								Timestamp: now,
							}, false),
						},
					}).
					Return(&ledger.Log{ID: pointer.For(0)}, &ledger.CreatedTransaction{
						Transaction: ledger.Transaction{
							ID: pointer.For(0),
							TransactionData: ledger.TransactionData{
								Postings:  postings,
								Metadata:  metadata.Metadata{},
								Timestamp: now,
							},
						},
					}, nil)
			},
			expectResults: []bulking.APIResult{{
				Data: map[string]any{
					"postings": []any{
						map[string]any{
							"source":      "world",
							"destination": "bank",
							"amount":      float64(100),
							"asset":       "USD/2",
						},
					},
					"timestamp": now.Format(time.RFC3339Nano),
					"metadata":  map[string]any{},
					"reverted":  false,
					"id":        float64(0),
				},
				ResponseType: bulking.ActionCreateTransaction,
			}},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			systemController, ledgerController := newTestingSystemController(t, true)
			testCase.expectations(ledgerController)

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodPost, "/xxx/_bulk", bytes.NewBufferString(testCase.body))
			req.Header = testCase.headers

			rec := httptest.NewRecorder()
			if testCase.queryParams != nil {
				req.URL.RawQuery = testCase.queryParams.Encode()
			}

			router.ServeHTTP(rec, req)

			if testCase.expectError {
				require.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				require.Equal(t, http.StatusOK, rec.Code)
			}

			ret, _ := api.DecodeSingleResponse[[]bulking.APIResult](t, rec.Body)
			ret = collectionutils.Map(ret, func(from bulking.APIResult) bulking.APIResult {
				switch data := from.Data.(type) {
				case map[string]any:
					delete(data, "insertedAt")
				}
				return from
			})
			require.Equal(t, testCase.expectResults, ret)
		})
	}
}
