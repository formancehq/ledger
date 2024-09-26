package v1

import (
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/pointer"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsCreate(t *testing.T) {
	type testCase struct {
		name               string
		expectedPreview    bool
		expectedRunScript  ledger.RunScript
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
					Script: ledger.Script{
						Plain: `XXX`,
					},
				},
			},
			expectedRunScript: ledger.RunScript{
				Script: ledger.Script{
					Plain: `XXX`,
					Vars:  map[string]string{},
				},
			},
		},
		{
			name: "using plain numscript with variables",
			payload: CreateTransactionRequest{
				Script: Script{
					Script: ledger.Script{
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
			expectedRunScript: ledger.RunScript{
				Script: ledger.Script{
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
					Script: ledger.Script{
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
			expectedRunScript: ledger.RunScript{
				Script: ledger.Script{
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
					Script: ledger.Script{
						Plain: `send (
						source = @world
						destination = @bank
					)`,
					},
				},
			},
			expectedRunScript: ledger.RunScript{
				Script: ledger.Script{
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
			expectedRunScript: ledger.TxToScriptData(ledger.NewTransactionData().WithPostings(
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
			expectedRunScript: ledger.TxToScriptData(ledger.NewTransactionData().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			), false),
		},
		{
			name:               "no postings or script",
			payload:            CreateTransactionRequest{},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  ErrValidation,
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
					Script: ledger.Script{
						Plain: `
						send [COIN 100] (
						  source = @world
						  destination = @bob
						)`,
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  ErrValidation,
		},
		{
			name:               "using invalid body",
			payload:            "not a valid payload",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  ErrValidation,
		},
	}

	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {
			if testCase.expectedStatusCode == 0 {
				testCase.expectedStatusCode = http.StatusOK
			}

			expectedTx := ledger.NewTransaction().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			)

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				testCase.expectedRunScript.Timestamp = time.Time{}
				ledgerController.EXPECT().
					CreateTransaction(gomock.Any(), ledgercontroller.Parameters{
						DryRun: tc.expectedPreview,
					}, testCase.expectedRunScript).
					Return(pointer.For(expectedTx), nil)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

			req := httptest.NewRequest(http.MethodPost, "/xxx/transactions", api.Buffer(t, testCase.payload))
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				tx, ok := api.DecodeSingleResponse[[]ledger.Transaction](t, rec.Body)
				require.True(t, ok)
				require.Equal(t, expectedTx, tx[0])
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
