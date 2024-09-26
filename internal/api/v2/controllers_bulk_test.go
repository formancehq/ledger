package v2

import (
	"bytes"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/go-libs/collectionutils"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/pkg/errors"
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
		expectations  func(mockLedger *ledgercontroller.MockController)
		expectError   bool
		expectResults []Result
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
			expectations: func(mockLedger *ledgercontroller.MockController) {
				postings := []ledger.Posting{{
					Source:      "world",
					Destination: "bank",
					Amount:      big.NewInt(100),
					Asset:       "USD/2",
				}}
				mockLedger.EXPECT().
					CreateTransaction(gomock.Any(), ledgercontroller.Parameters{}, ledger.TxToScriptData(ledger.TransactionData{
						Postings:  postings,
						Timestamp: now,
					}, false)).
					Return(&ledger.Transaction{
						TransactionData: ledger.TransactionData{
							Postings:  postings,
							Metadata:  metadata.Metadata{},
							Timestamp: now,
						},
					}, nil)
			},
			expectResults: []Result{{
				Data: map[string]any{
					"postings": []any{
						map[string]any{
							"source":      "world",
							"destination": "bank",
							"amount":      float64(100),
							"asset":       "USD/2",
						},
					},
					"timestamp":  now.Format(time.RFC3339Nano),
					"metadata":   map[string]any{},
					"reverted":   false,
					"revertedAt": nil,
					"id":         float64(0),
				},
				ResponseType: ActionCreateTransaction,
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
			expectations: func(mockLedger *ledgercontroller.MockController) {
				mockLedger.EXPECT().
					SaveTransactionMetadata(gomock.Any(), ledgercontroller.Parameters{}, 1, metadata.Metadata{
						"foo": "bar",
					}).
					Return(nil)
			},
			expectResults: []Result{{
				ResponseType: ActionAddMetadata,
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
			expectations: func(mockLedger *ledgercontroller.MockController) {
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters{}, "world", metadata.Metadata{
						"foo": "bar",
					}).
					Return(nil)
			},
			expectResults: []Result{{
				ResponseType: ActionAddMetadata,
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
			expectations: func(mockLedger *ledgercontroller.MockController) {
				mockLedger.EXPECT().
					RevertTransaction(gomock.Any(), ledgercontroller.Parameters{}, 1, false, false).
					Return(&ledger.Transaction{}, nil)
			},
			expectResults: []Result{{
				Data: map[string]any{
					"id":         float64(0),
					"metadata":   nil,
					"postings":   nil,
					"reverted":   false,
					"revertedAt": nil,
					"timestamp":  "0001-01-01T00:00:00Z",
				},
				ResponseType: ActionRevertTransaction,
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
			expectations: func(mockLedger *ledgercontroller.MockController) {
				mockLedger.EXPECT().
					DeleteTransactionMetadata(gomock.Any(), ledgercontroller.Parameters{}, 1, "foo").
					Return(nil)
			},
			expectResults: []Result{{
				ResponseType: ActionDeleteMetadata,
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
			expectations: func(mockLedger *ledgercontroller.MockController) {
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters{}, "world", metadata.Metadata{
						"foo": "bar",
					}).
					Return(nil)
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters{}, "world", metadata.Metadata{
						"foo2": "bar2",
					}).
					Return(errors.New("unexpected error"))
			},
			expectResults: []Result{{
				ResponseType: ActionAddMetadata,
			}, {
				ErrorCode:        api.ErrorInternal,
				ErrorDescription: "unexpected error",
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
			expectations: func(mockLedger *ledgercontroller.MockController) {
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters{}, "world", metadata.Metadata{
						"foo": "bar",
					}).
					Return(nil)
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters{}, "world", metadata.Metadata{
						"foo2": "bar2",
					}).
					Return(errors.New("unexpected error"))
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters{}, "world", metadata.Metadata{
						"foo3": "bar3",
					}).
					Return(nil)
			},
			expectResults: []Result{{
				ResponseType: ActionAddMetadata,
			}, {
				ResponseType:     "ERROR",
				ErrorCode:        api.ErrorInternal,
				ErrorDescription: "unexpected error",
			}, {
				ResponseType: ActionAddMetadata,
			}},
			expectError: true,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			systemController, ledgerController := newTestingSystemController(t, true)
			testCase.expectations(ledgerController)

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

			req := httptest.NewRequest(http.MethodPost, "/xxx/_bulk", bytes.NewBufferString(testCase.body))
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

			ret, _ := api.DecodeSingleResponse[[]Result](t, rec.Body)
			ret = collectionutils.Map(ret, func(from Result) Result {
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
