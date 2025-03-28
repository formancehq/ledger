package v1

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/internal/api/common"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsCreate(t *testing.T) {
	type testCase struct {
		name               string
		expectedPreview    bool
		expectedRunScript  ledgercontroller.RunScript
		payload            any
		expectedStatusCode int
		expectedErrorCode  string
		queryParams        url.Values
	}

	testCases := []testCase{
		{
			name: "using plain numscript",
			payload: CreateTransactionRequest{
				Script: Script{
					Script: ledgercontroller.Script{
						Plain: `XXX`,
					},
				},
			},
			expectedRunScript: ledgercontroller.RunScript{
				Script: ledgercontroller.Script{
					Plain: `XXX`,
					Vars:  map[string]string{},
				},
			},
		},
		{
			name: "using plain numscript with variables",
			payload: CreateTransactionRequest{
				Script: Script{
					Script: ledgercontroller.Script{
						Plain: `vars {
						monetary $val
					}

					send $val (
						source = @world
						destination = @bank
					)`,
					},
					Vars: map[string]json.RawMessage{
						"val": json.RawMessage(`"USD/2 100"`),
					},
				},
			},
			expectedRunScript: ledgercontroller.RunScript{
				Script: ledgercontroller.Script{
					Plain: `vars {
						monetary $val
					}

					send $val (
						source = @world
						destination = @bank
					)`,
					Vars: map[string]string{
						"val": "USD/2 100",
					},
				},
			},
		},
		{
			name: "using plain numscript with variables (legacy format)",
			payload: CreateTransactionRequest{
				Script: Script{
					Script: ledgercontroller.Script{
						Plain: `vars {
						monetary $val
					}

					send $val (
						source = @world
						destination = @bank
					)`,
					},
					Vars: map[string]json.RawMessage{
						"val": json.RawMessage(`{
							"asset":  "USD/2",
							"amount": 100
						}`),
					},
				},
			},
			expectedRunScript: ledgercontroller.RunScript{
				Script: ledgercontroller.Script{
					Plain: `vars {
						monetary $val
					}

					send $val (
						source = @world
						destination = @bank
					)`,
					Vars: map[string]string{
						"val": "USD/2 100",
					},
				},
			},
		},
		{
			name: "using plain numscript and dry run",
			payload: CreateTransactionRequest{
				Script: Script{
					Script: ledgercontroller.Script{
						Plain: `send (
						source = @world
						destination = @bank
					)`,
					},
				},
			},
			expectedRunScript: ledgercontroller.RunScript{
				Script: ledgercontroller.Script{
					Plain: `send (
						source = @world
						destination = @bank
					)`,
					Vars: map[string]string{},
				},
			},
			expectedPreview: true,
			queryParams: url.Values{
				"preview": []string{"true"},
			},
		},
		{
			name: "using JSON postings",
			payload: CreateTransactionRequest{
				Postings: []ledger.Posting{
					ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
				},
			},
			expectedRunScript: ledgercontroller.TxToScriptData(ledger.NewTransactionData().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			), false),
		},
		{
			name: "using JSON postings and dry run",
			queryParams: url.Values{
				"preview": []string{"true"},
			},
			payload: CreateTransactionRequest{
				Postings: []ledger.Posting{
					ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
				},
			},
			expectedPreview: true,
			expectedRunScript: ledgercontroller.TxToScriptData(ledger.NewTransactionData().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			), false),
		},
		{
			name:               "no postings or script",
			payload:            CreateTransactionRequest{},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
		{
			name: "postings and script",
			payload: CreateTransactionRequest{
				Postings: ledger.Postings{
					{
						Source:      "world",
						Destination: "alice",
						Amount:      big.NewInt(100),
						Asset:       "COIN",
					},
				},
				Script: Script{
					Script: ledgercontroller.Script{
						Plain: `
						send [COIN 100] (
						  source = @world
						  destination = @bob
						)`,
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
		{
			name:               "using invalid body",
			payload:            "not a valid payload",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectedStatusCode == 0 {
				tc.expectedStatusCode = http.StatusOK
			}

			expectedTx := ledger.NewTransaction().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			)
			expectedTx.ID = pointer.For(0)

			systemController, ledgerController := newTestingSystemController(t, true)
			if tc.expectedStatusCode < 300 && tc.expectedStatusCode >= 200 {
				tc.expectedRunScript.Timestamp = time.Time{}
				ledgerController.EXPECT().
					CreateTransaction(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
						DryRun: tc.expectedPreview,
						Input: ledgercontroller.CreateTransaction{
							RunScript: tc.expectedRunScript,
						},
					}).
					Return(&ledger.Log{
						ID: pointer.For(0),
					}, &ledger.CreatedTransaction{
						Transaction: expectedTx,
					}, nil)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodPost, "/xxx/transactions", api.Buffer(t, tc.payload))
			rec := httptest.NewRecorder()
			req.URL.RawQuery = tc.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectedStatusCode, rec.Code)
			if tc.expectedStatusCode < 300 && tc.expectedStatusCode >= 200 {
				tx, ok := api.DecodeSingleResponse[[]ledger.Transaction](t, rec.Body)
				require.True(t, ok)
				require.Equal(t, expectedTx, tx[0])
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
