package v1

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/transport/api"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"
	"github.com/formancehq/go-libs/v5/pkg/types/time"
	"github.com/formancehq/numscript"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
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
		returnErr          error
	}

	postingsPayload := CreateTransactionRequest{
		Postings: []ledger.Posting{
			ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
		},
	}
	postingsRunScript := ledgercontroller.TxToScriptData(ledger.NewTransactionData().WithPostings(
		ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
	), false)
	scriptPayload := CreateTransactionRequest{
		Script: Script{
			Script: ledgercontroller.Script{
				Plain: `XXX`,
			},
		},
	}
	scriptRunScript := ledgercontroller.RunScript{
		Script: ledgercontroller.Script{
			Plain: `XXX`,
			Vars:  map[string]string{},
		},
	}
	interpreterParseErr := numscriptInterpreterError(t, `not a valid numscript`)
	interpreterRuntimeErr := numscriptInterpreterError(t, `vars {
		monetary $amount
	}
	send $amount (
		source = @world
		destination = @bank
	)`)
	interpreterMissingFundsErr := numscriptInterpreterError(t, `send [USD 100] (
		source = @alice
		destination = @bank
	)`)

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
			name: "using plain numscript with invalid variable",
			payload: CreateTransactionRequest{
				Script: Script{
					Script: ledgercontroller.Script{Plain: `send $val`},
					Vars: map[string]json.RawMessage{
						"val": json.RawMessage(`42`),
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
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
		{
			name:               "using JSON postings with interpreter parse error",
			payload:            postingsPayload,
			expectedRunScript:  postingsRunScript,
			returnErr:          interpreterParseErr,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrCompilationFailed,
		},
		{
			name:               "using JSON postings with interpreter runtime error",
			payload:            postingsPayload,
			expectedRunScript:  postingsRunScript,
			returnErr:          interpreterRuntimeErr,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
		{
			name:               "using JSON postings with interpreter missing funds",
			payload:            postingsPayload,
			expectedRunScript:  postingsRunScript,
			returnErr:          interpreterMissingFundsErr,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrInsufficientFund,
		},
		{
			name:               "using plain numscript with interpreter parse error",
			payload:            scriptPayload,
			expectedRunScript:  scriptRunScript,
			returnErr:          interpreterParseErr,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
		{
			name:               "using plain numscript with interpreter runtime error",
			payload:            scriptPayload,
			expectedRunScript:  scriptRunScript,
			returnErr:          interpreterRuntimeErr,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
		{
			name:               "using plain numscript with interpreter missing funds",
			payload:            scriptPayload,
			expectedRunScript:  scriptRunScript,
			returnErr:          interpreterMissingFundsErr,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrInsufficientFund,
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
			expectedTx.ID = pointer.For(uint64(0))

			systemController, ledgerController := newTestingSystemController(t, true)
			if tc.expectedStatusCode < 300 && tc.expectedStatusCode >= 200 || tc.returnErr != nil {
				tc.expectedRunScript.Timestamp = time.Time{}
				expect := ledgerController.EXPECT().
					CreateTransaction(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
						DryRun: tc.expectedPreview,
						Input: ledgercontroller.CreateTransaction{
							RunScript: tc.expectedRunScript,
						},
					})
				if tc.returnErr != nil {
					expect.Return(nil, nil, false, tc.returnErr)
				} else {
					expect.Return(&ledger.Log{
						ID: pointer.For(uint64(0)),
					}, &ledger.CreatedTransaction{
						Transaction: expectedTx,
					}, false, nil)
				}
			}

			router := NewRouter(systemController, jwt.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

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

// numscriptInterpreterError returns the error the ledger controller produces
// when the experimental Numscript interpreter fails to parse or run script.
func numscriptInterpreterError(t *testing.T, script string) error {
	t.Helper()

	parsed := numscript.Parse(script)
	if errs := parsed.GetParsingErrors(); len(errs) > 0 {
		return ledgercontroller.ErrParsing{Source: script, Errors: errs}
	}

	_, err := parsed.Run(context.Background(), numscript.VariablesMap{}, numscript.StaticStore{})
	require.NotNil(t, err)
	return ledgercontroller.ErrRuntime{Source: script, InterpreterError: err}
}
