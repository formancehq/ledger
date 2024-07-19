package v2_test

import (
	"bytes"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/time"

	"github.com/formancehq/stack/libs/go-libs/auth"
	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/pkg/errors"

	"github.com/formancehq/ledger/internal/engine"

	"github.com/formancehq/ledger/internal/machine"

	ledger "github.com/formancehq/ledger/internal"
	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestPostTransactions(t *testing.T) {
	type testCase struct {
		name                 string
		expectedDryRun       bool
		expectedRunScript    ledger.RunScript
		returnError          error
		payload              any
		expectedStatusCode   int
		expectedErrorCode    string
		expectedErrorDetails string
		queryParams          url.Values
		expectEngineCall     bool
	}

	testCases := []testCase{
		{
			name: "using plain numscript",
			payload: ledger.TransactionRequest{
				Script: ledger.ScriptV1{
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
			expectEngineCall: true,
		},
		{
			name: "using plain numscript with variables",
			payload: ledger.TransactionRequest{
				Script: ledger.ScriptV1{
					Script: ledger.Script{
						Plain: `vars {
						monetary $val
					}

					send $val (
						source = @world
						destination = @bank
					)`,
					},
					Vars: map[string]any{
						"val": "USD/2 100",
					},
				},
			},
			expectEngineCall: true,
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
			name:             "using plain numscript with variables (legacy format)",
			expectEngineCall: true,
			payload: ledger.TransactionRequest{
				Script: ledger.ScriptV1{
					Script: ledger.Script{
						Plain: `vars {
						monetary $val
					}

					send $val (
						source = @world
						destination = @bank
					)`,
					},
					Vars: map[string]any{
						"val": map[string]any{
							"asset":  "USD/2",
							"amount": 100,
						},
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
			name:             "using plain numscript and dry run",
			expectEngineCall: true,
			payload: ledger.TransactionRequest{
				Script: ledger.ScriptV1{
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
			expectedDryRun: true,
			queryParams: url.Values{
				"dryRun": []string{"true"},
			},
		},
		{
			name:             "using JSON postings",
			expectEngineCall: true,
			payload: ledger.TransactionRequest{
				Postings: []ledger.Posting{
					ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
				},
			},
			expectedRunScript: ledger.TxToScriptData(ledger.NewTransactionData().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			), false),
		},
		{
			name:             "using JSON postings and dry run",
			expectEngineCall: true,
			queryParams: url.Values{
				"dryRun": []string{"true"},
			},
			payload: ledger.TransactionRequest{
				Postings: []ledger.Posting{
					ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
				},
			},
			expectedDryRun: true,
			expectedRunScript: ledger.TxToScriptData(ledger.NewTransactionData().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			), false),
		},
		{
			name:             "no postings or script",
			expectEngineCall: true,
			payload: ledger.TransactionRequest{
				Script: ledger.ScriptV1{
					Script: ledger.TxToScriptData(ledger.NewTransactionData(), false).Script,
				},
				Metadata: map[string]string{},
			},
			expectedRunScript:  ledger.TxToScriptData(ledger.NewTransactionData(), false),
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrNoPostings,
			returnError:        engine.NewCommandError(command.NewErrNoPostings()),
		},
		{
			name: "postings and script",
			payload: ledger.TransactionRequest{
				Postings: ledger.Postings{
					{
						Source:      "world",
						Destination: "alice",
						Amount:      big.NewInt(100),
						Asset:       "COIN",
					},
				},
				Script: ledger.ScriptV1{
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
			expectedErrorCode:  v2.ErrValidation,
		},
		{
			name:               "using invalid body",
			payload:            "not a valid payload",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrValidation,
		},
		{
			name:             "with insufficient funds",
			expectEngineCall: true,
			payload: ledger.TransactionRequest{
				Script: ledger.ScriptV1{
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
			returnError:        engine.NewCommandError(command.NewErrMachine(&machine.ErrInsufficientFund{})),
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrInsufficientFund,
		},
		{
			name: "using JSON postings and negative amount",
			payload: ledger.TransactionRequest{
				Postings: []ledger.Posting{
					ledger.NewPosting("world", "bank", "USD", big.NewInt(-100)),
				},
			},
			expectEngineCall:   true,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrCompilationFailed,
			expectedRunScript: ledger.TxToScriptData(ledger.NewTransactionData().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(-100)),
			), false),
			expectedErrorDetails: backend.EncodeLink(`compilation failed`),
			returnError: engine.NewCommandError(
				command.NewErrInvalidTransaction(command.ErrInvalidTransactionCodeCompilationFailed, errors.New("compilation failed")),
			),
		},
		{
			expectEngineCall: true,
			name:             "numscript and negative amount",
			payload: ledger.TransactionRequest{
				Script: ledger.ScriptV1{
					Script: ledger.Script{
						Plain: `send [COIN -100] (
						source = @world
						destination = @bob
					)`,
					},
				},
			},
			expectedStatusCode:   http.StatusBadRequest,
			expectedErrorCode:    v2.ErrCompilationFailed,
			expectedErrorDetails: backend.EncodeLink("compilation failed"),
			expectedRunScript: ledger.RunScript{
				Script: ledger.Script{
					Plain: `send [COIN -100] (
						source = @world
						destination = @bob
					)`,
					Vars: map[string]string{},
				},
			},
			returnError: engine.NewCommandError(
				command.NewErrInvalidTransaction(command.ErrInvalidTransactionCodeCompilationFailed, errors.New("compilation failed")),
			),
		},
		{
			name:             "numscript and compilation failed",
			expectEngineCall: true,
			payload: ledger.TransactionRequest{
				Script: ledger.ScriptV1{
					Script: ledger.Script{
						Plain: `send [COIN XXX] (
						source = @world
						destination = @bob
					)`,
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrCompilationFailed,
			expectedRunScript: ledger.RunScript{
				Script: ledger.Script{
					Plain: `send [COIN XXX] (
						source = @world
						destination = @bob
					)`,
					Vars: map[string]string{},
				},
			},
			expectedErrorDetails: backend.EncodeLink("compilation failed"),
			returnError: engine.NewCommandError(
				command.NewErrCompilationFailed(fmt.Errorf("compilation failed")),
			),
		},
		{
			name:             "numscript and no postings",
			expectEngineCall: true,
			payload: ledger.TransactionRequest{
				Script: ledger.ScriptV1{
					Script: ledger.Script{
						Plain: `vars {}`,
					},
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrNoPostings,
			expectedRunScript: ledger.RunScript{
				Script: ledger.Script{
					Plain: `vars {}`,
					Vars:  map[string]string{},
				},
			},
			returnError: engine.NewCommandError(
				command.NewErrNoPostings(),
			),
		},
		{
			name:             "numscript and conflict",
			expectEngineCall: true,
			payload: ledger.TransactionRequest{
				Script: ledger.ScriptV1{
					Script: ledger.Script{
						Plain: `vars {}`,
					},
				},
				Reference: "xxx",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrConflict,
			expectedRunScript: ledger.RunScript{
				Script: ledger.Script{
					Plain: `vars {}`,
					Vars:  map[string]string{},
				},
				Reference: "xxx",
			},
			returnError: engine.NewCommandError(
				command.NewErrConflict(),
			),
		},
		{
			name:             "numscript and metadata override",
			expectEngineCall: true,
			payload: ledger.TransactionRequest{
				Script: ledger.ScriptV1{
					Script: ledger.Script{
						Plain: `send [COIN 100] (
						source = @world
						destination = @bob
					)
					set_tx_meta("foo", "bar")`,
					},
				},
				Reference: "xxx",
				Metadata: map[string]string{
					"foo": "baz",
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrMetadataOverride,
			expectedRunScript: ledger.RunScript{
				Script: ledger.Script{
					Plain: `send [COIN 100] (
						source = @world
						destination = @bob
					)
					set_tx_meta("foo", "bar")`,
					Vars: map[string]string{},
				},
				Reference: "xxx",
				Metadata: map[string]string{
					"foo": "baz",
				},
			},
			returnError: engine.NewCommandError(
				command.NewErrMachine(&machine.ErrMetadataOverride{}),
			),
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

			backend, mockLedger := newTestingBackend(t, true)
			if testCase.expectEngineCall {
				expect := mockLedger.EXPECT().
					CreateTransaction(gomock.Any(), command.Parameters{
						DryRun: tc.expectedDryRun,
					}, testCase.expectedRunScript)

				if tc.returnError == nil {
					expect.Return(expectedTx, nil)
				} else {
					expect.Return(nil, tc.returnError)
				}
			}

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth())

			req := httptest.NewRequest(http.MethodPost, "/xxx/transactions", sharedapi.Buffer(t, testCase.payload))
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				tx, ok := sharedapi.DecodeSingleResponse[ledger.Transaction](t, rec.Body)
				require.True(t, ok)
				require.Equal(t, *expectedTx, tx)
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
				require.EqualValues(t, testCase.expectedErrorDetails, err.Details)

			}
		})
	}
}

func TestPostTransactionMetadata(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectStatusCode  int
		expectedErrorCode string
		body              any
	}

	testCases := []testCase{
		{
			name: "nominal",
			body: metadata.Metadata{
				"foo": "bar",
			},
		},
		{
			name:              "invalid body",
			body:              "invalid - not an object",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: v2.ErrValidation,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusNoContent
			}

			backend, mock := newTestingBackend(t, true)
			if testCase.expectStatusCode == http.StatusNoContent {
				mock.EXPECT().
					SaveMeta(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeTransaction, big.NewInt(0), testCase.body).
					Return(nil)
			}

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth())

			req := httptest.NewRequest(http.MethodPost, "/xxx/transactions/0/metadata", sharedapi.Buffer(t, testCase.body))
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode >= 300 || testCase.expectStatusCode < 200 {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}

func TestGetTransaction(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tx := ledger.ExpandTransaction(
		ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
		),
		nil,
	)

	query := ledgerstore.NewGetTransactionQuery(big.NewInt(0))
	query.PIT = &now

	backend, mock := newTestingBackend(t, true)
	mock.EXPECT().
		GetTransactionWithVolumes(gomock.Any(), query).
		Return(&tx, nil)

	router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth())

	req := httptest.NewRequest(http.MethodGet, "/xxx/transactions/0?pit="+now.Format(time.RFC3339Nano), nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, _ := sharedapi.DecodeSingleResponse[ledger.ExpandedTransaction](t, rec.Body)
	require.Equal(t, tx, response)
}

func TestGetTransactions(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       ledgerstore.GetTransactionsQuery
		expectStatusCode  int
		expectedErrorCode string
	}
	now := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			})),
		},
		{
			name: "using metadata",
			body: `{"$match": {"metadata[roles]": "admin"}}`,
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin"))),
		},
		{
			name: "using startTime",
			body: fmt.Sprintf(`{"$gte": {"start_time": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Gte("start_time", now.Format(time.DateFormat)))),
		},
		{
			name: "using endTime",
			body: fmt.Sprintf(`{"$lte": {"end_time": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Lte("end_time", now.Format(time.DateFormat)))),
		},
		{
			name: "using account",
			body: `{"$match": {"account": "xxx"}}`,
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Match("account", "xxx"))),
		},
		{
			name: "using reference",
			body: `{"$match": {"reference": "xxx"}}`,
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Match("reference", "xxx"))),
		},
		{
			name: "using destination",
			body: `{"$match": {"destination": "xxx"}}`,
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Match("destination", "xxx"))),
		},
		{
			name: "using source",
			body: `{"$match": {"source": "xxx"}}`,
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Match("source", "xxx"))),
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{bunpaginate.EncodeCursor(ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})))},
			},
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{},
			})),
		},
		{
			name: "using invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"XXX"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: v2.ErrValidation,
		},
		{
			name: "invalid page size",
			queryParams: url.Values{
				"pageSize": []string{"nan"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: v2.ErrValidation,
		},
		{
			name: "page size over maximum",
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			}).
				WithPageSize(v2.MaxPageSize)),
		},
		{
			name: "using cursor",
			queryParams: url.Values{
				"cursor": []string{"eyJwYWdlU2l6ZSI6MTUsImJvdHRvbSI6bnVsbCwiY29sdW1uIjoiaWQiLCJwYWdpbmF0aW9uSUQiOm51bGwsIm9yZGVyIjoxLCJmaWx0ZXJzIjp7InFiIjp7fSwicGFnZVNpemUiOjE1LCJvcHRpb25zIjp7InBpdCI6bnVsbCwidm9sdW1lcyI6ZmFsc2UsImVmZmVjdGl2ZVZvbHVtZXMiOmZhbHNlfX0sInJldmVyc2UiOmZhbHNlfQ"},
			},
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})),
		},
		{
			name: "using $exists metadata filter",
			body: `{"$exists": {"metadata": "foo"}}`,
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Exists("metadata", "foo"))),
		},
		{
			name:        "paginate using effective order",
			queryParams: map[string][]string{"order": {"effective"}},
			expectQuery: ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			})).
				WithColumn("timestamp"),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := bunpaginate.Cursor[ledger.ExpandedTransaction]{
				Data: []ledger.ExpandedTransaction{
					ledger.ExpandTransaction(
						ledger.NewTransaction().WithPostings(
							ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
						),
						nil,
					),
				},
			}

			backend, mockLedger := newTestingBackend(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					GetTransactions(gomock.Any(), testCase.expectQuery).
					Return(&expectedCursor, nil)
			}

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth())

			req := httptest.NewRequest(http.MethodGet, "/xxx/transactions", bytes.NewBufferString(testCase.body))
			rec := httptest.NewRecorder()
			params := url.Values{}
			if testCase.queryParams != nil {
				params = testCase.queryParams
			}
			params.Set("pit", now.Format(time.RFC3339Nano))
			req.URL.RawQuery = params.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := sharedapi.DecodeCursorResponse[ledger.ExpandedTransaction](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)

			}
		})
	}
}

func TestCountTransactions(t *testing.T) {
	t.Parallel()

	before := time.Now()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       ledgerstore.PaginatedQueryOptions[ledgerstore.PITFilterWithVolumes]
		expectStatusCode  int
		expectedErrorCode string
	}
	now := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
				},
			}),
		},
		{
			name: "using metadata",
			body: `{"$match": {"metadata[roles]": "admin"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")),
		},
		{
			name: "using startTime",
			body: fmt.Sprintf(`{"$gte": {"date": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Gte("date", now.Format(time.DateFormat))),
		},
		{
			name: "using endTime",
			body: fmt.Sprintf(`{"$gte": {"date": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Gte("date", now.Format(time.DateFormat))),
		},
		{
			name: "using account",
			body: `{"$match": {"account": "xxx"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("account", "xxx")),
		},
		{
			name: "using reference",
			body: `{"$match": {"reference": "xxx"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("reference", "xxx")),
		},
		{
			name: "using destination",
			body: `{"$match": {"destination": "xxx"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("destination", "xxx")),
		},
		{
			name: "using source",
			body: `{"$match": {"source": "xxx"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("source", "xxx")),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusNoContent
			}

			backend, mockLedger := newTestingBackend(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					CountTransactions(gomock.Any(), ledgerstore.NewGetTransactionsQuery(testCase.expectQuery)).
					Return(10, nil)
			}

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth())

			req := httptest.NewRequest(http.MethodHead, "/xxx/transactions?pit="+before.Format(time.RFC3339Nano), bytes.NewBufferString(testCase.body))
			rec := httptest.NewRecorder()
			if testCase.queryParams != nil {
				req.URL.RawQuery = testCase.queryParams.Encode()
			}

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				require.Equal(t, "10", rec.Header().Get("Count"))
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)

			}
		})
	}
}

func TestRevert(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name             string
		queryParams      url.Values
		returnTx         *ledger.Transaction
		returnErr        error
		expectForce      bool
		expectStatusCode int
		expectErrorCode  string
	}

	testCases := []testCase{
		{
			name: "nominal",
			returnTx: ledger.NewTransaction().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			),
		},
		{
			name: "force revert",
			returnTx: ledger.NewTransaction().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			),
			expectForce: true,
			queryParams: map[string][]string{"force": {"true"}},
		},
		{
			name: "with insufficient fund",
			returnErr: engine.NewCommandError(
				command.NewErrMachine(&machine.ErrInsufficientFund{}),
			),
			expectStatusCode: http.StatusBadRequest,
			expectErrorCode:  v2.ErrInsufficientFund,
		},
		{
			name: "with revert already occurring",
			returnErr: engine.NewCommandError(
				command.NewErrRevertTransactionOccurring(),
			),
			expectStatusCode: http.StatusBadRequest,
			expectErrorCode:  v2.ErrRevertOccurring,
		},
		{
			name: "with already revert",
			returnErr: engine.NewCommandError(
				command.NewErrRevertTransactionAlreadyReverted(),
			),
			expectStatusCode: http.StatusBadRequest,
			expectErrorCode:  v2.ErrAlreadyRevert,
		},
		{
			name: "with transaction not found",
			returnErr: engine.NewCommandError(
				command.NewErrRevertTransactionNotFound(),
			),
			expectStatusCode: http.StatusNotFound,
			expectErrorCode:  sharedapi.ErrorCodeNotFound,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend, mockLedger := newTestingBackend(t, true)
			mockLedger.
				EXPECT().
				RevertTransaction(gomock.Any(), command.Parameters{}, big.NewInt(0), tc.expectForce, false).
				Return(tc.returnTx, tc.returnErr)

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth())

			req := httptest.NewRequest(http.MethodPost, "/xxx/transactions/0/revert", nil)
			if tc.queryParams != nil {
				req.URL.RawQuery = tc.queryParams.Encode()
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if tc.expectStatusCode == 0 {
				require.Equal(t, http.StatusCreated, rec.Code)
				tx, ok := sharedapi.DecodeSingleResponse[ledger.Transaction](t, rec.Body)
				require.True(t, ok)
				require.Equal(t, *tc.returnTx, tx)
			} else {
				require.Equal(t, tc.expectStatusCode, rec.Code)
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectErrorCode, err.ErrorCode)

			}
		})
	}
}
