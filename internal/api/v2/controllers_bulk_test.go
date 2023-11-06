package v2_test

import (
	"bytes"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/formancehq/ledger/internal/api/bulk"
	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBulk(t *testing.T) {
	t.Parallel()

	now := ledger.Now()

	type bulkTestCase struct {
		name          string
		queryParams   url.Values
		body          string
		expectations  func(mockLedger *backend.MockLedger)
		expectError   bool
		expectResults []bulk.Result
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
			expectations: func(mockLedger *backend.MockLedger) {
				postings := []ledger.Posting{{
					Source:      "world",
					Destination: "bank",
					Amount:      big.NewInt(100),
					Asset:       "USD/2",
				}}
				mockLedger.EXPECT().
					CreateTransaction(gomock.Any(), command.Parameters{}, ledger.TxToScriptData(ledger.TransactionData{
						Postings:  postings,
						Timestamp: now,
					})).
					Return(&ledger.Transaction{
						TransactionData: ledger.TransactionData{
							Postings:  postings,
							Metadata:  metadata.Metadata{},
							Timestamp: now,
						},
						ID: big.NewInt(0),
					}, nil)
			},
			expectResults: []bulk.Result{{
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
				ResponseType: bulk.ActionCreateTransaction,
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
			expectations: func(mockLedger *backend.MockLedger) {
				mockLedger.EXPECT().
					SaveMeta(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeTransaction, big.NewInt(1), metadata.Metadata{
						"foo": "bar",
					}).
					Return(nil)
			},
			expectResults: []bulk.Result{{
				ResponseType: bulk.ActionAddMetadata,
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
			expectations: func(mockLedger *backend.MockLedger) {
				mockLedger.EXPECT().
					SaveMeta(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeAccount, "world", metadata.Metadata{
						"foo": "bar",
					}).
					Return(nil)
			},
			expectResults: []bulk.Result{{
				ResponseType: bulk.ActionAddMetadata,
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
			expectations: func(mockLedger *backend.MockLedger) {
				mockLedger.EXPECT().
					RevertTransaction(gomock.Any(), command.Parameters{}, big.NewInt(1)).
					Return(&ledger.Transaction{}, nil)
			},
			expectResults: []bulk.Result{{
				Data: map[string]any{
					"id":        nil,
					"metadata":  nil,
					"postings":  nil,
					"reverted":  false,
					"timestamp": "0001-01-01T00:00:00Z",
				},
				ResponseType: bulk.ActionRevertTransaction,
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
			expectations: func(mockLedger *backend.MockLedger) {
				mockLedger.EXPECT().
					DeleteMetadata(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeTransaction, big.NewInt(1), "foo").
					Return(nil)
			},
			expectResults: []bulk.Result{{
				ResponseType: bulk.ActionDeleteMetadata,
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
			expectations: func(mockLedger *backend.MockLedger) {
				mockLedger.EXPECT().
					SaveMeta(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeAccount, "world", metadata.Metadata{
						"foo": "bar",
					}).
					Return(nil)
				mockLedger.EXPECT().
					SaveMeta(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeAccount, "world", metadata.Metadata{
						"foo2": "bar2",
					}).
					Return(errors.New("unexpected error"))
			},
			expectResults: []bulk.Result{{
				ResponseType: bulk.ActionAddMetadata,
			}, {
				ErrorCode:        "INTERNAL",
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
			expectations: func(mockLedger *backend.MockLedger) {
				mockLedger.EXPECT().
					SaveMeta(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeAccount, "world", metadata.Metadata{
						"foo": "bar",
					}).
					Return(nil)
				mockLedger.EXPECT().
					SaveMeta(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeAccount, "world", metadata.Metadata{
						"foo2": "bar2",
					}).
					Return(errors.New("unexpected error"))
				mockLedger.EXPECT().
					SaveMeta(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeAccount, "world", metadata.Metadata{
						"foo3": "bar3",
					}).
					Return(nil)
			},
			expectResults: []bulk.Result{{
				ResponseType: bulk.ActionAddMetadata,
			}, {
				ResponseType:     "ERROR",
				ErrorCode:        "INTERNAL",
				ErrorDescription: "unexpected error",
			}, {
				ResponseType: bulk.ActionAddMetadata,
			}},
			expectError: true,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			backend, mock := newTestingBackend(t, true)
			testCase.expectations(mock)

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

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

			ret, _ := sharedapi.DecodeSingleResponse[[]bulk.Result](t, rec.Body)
			require.Equal(t, testCase.expectResults, ret)
		})
	}
}
