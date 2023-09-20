package v1_test

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	v1 "github.com/formancehq/ledger/internal/api/v1"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/paginate"
	"github.com/formancehq/ledger/internal/storage/query"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestPostTransactions(t *testing.T) {
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
			payload: v1.PostTransactionRequest{
				Script: v1.Script{
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
			payload: v1.PostTransactionRequest{
				Script: v1.Script{
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
			payload: v1.PostTransactionRequest{
				Script: v1.Script{
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
			name: "using plain numscript and dry run",
			payload: v1.PostTransactionRequest{
				Script: v1.Script{
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
			payload: v1.PostTransactionRequest{
				Postings: []ledger.Posting{
					ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
				},
			},
			expectedRunScript: ledger.TxToScriptData(ledger.NewTransactionData().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			)),
		},
		{
			name: "using JSON postings and dry run",
			queryParams: url.Values{
				"preview": []string{"true"},
			},
			payload: v1.PostTransactionRequest{
				Postings: []ledger.Posting{
					ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
				},
			},
			expectedPreview: true,
			expectedRunScript: ledger.TxToScriptData(ledger.NewTransactionData().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			)),
		},
		{
			name:               "no postings or script",
			payload:            v1.PostTransactionRequest{},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrValidation,
		},
		{
			name: "postings and script",
			payload: v1.PostTransactionRequest{
				Postings: ledger.Postings{
					{
						Source:      "world",
						Destination: "alice",
						Amount:      big.NewInt(100),
						Asset:       "COIN",
					},
				},
				Script: v1.Script{
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
			expectedErrorCode:  v1.ErrValidation,
		},
		{
			name:               "using invalid body",
			payload:            "not a valid payload",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrValidation,
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

			backend, mockLedger := newTestingBackend(t)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				mockLedger.EXPECT().
					CreateTransaction(gomock.Any(), command.Parameters{
						DryRun: tc.expectedPreview,
					}, testCase.expectedRunScript).
					Return(expectedTx, nil)
			}

			router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodPost, "/xxx/transactions", sharedapi.Buffer(t, testCase.payload))
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				tx, ok := sharedapi.DecodeSingleResponse[[]ledger.Transaction](t, rec.Body)
				require.True(t, ok)
				require.Equal(t, *expectedTx, tx[0])
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
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
			expectedErrorCode: v1.ErrValidation,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusNoContent
			}

			backend, mock := newTestingBackend(t)
			if testCase.expectStatusCode == http.StatusNoContent {
				mock.EXPECT().
					SaveMeta(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeTransaction, big.NewInt(0), testCase.body).
					Return(nil)
			}

			router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry())

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

	tx := ledger.ExpandTransaction(
		ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
		),
		nil,
	)

	backend, mock := newTestingBackend(t)
	mock.EXPECT().
		GetTransactionWithVolumes(gomock.Any(), ledgerstore.NewGetTransactionQuery(big.NewInt(0))).
		Return(&tx, nil)

	router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry())

	req := httptest.NewRequest(http.MethodGet, "/xxx/transactions/0", nil)
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
		expectQuery       ledgerstore.PaginatedQueryOptions[ledgerstore.PITFilterWithVolumes]
		expectStatusCode  int
		expectedErrorCode string
	}
	now := ledger.Now()

	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}),
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")),
		},
		{
			name: "using startTime",
			queryParams: url.Values{
				"start_time": []string{now.Format(ledger.DateFormat)},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Gte("date", now.Format(ledger.DateFormat))),
		},
		{
			name: "using endTime",
			queryParams: url.Values{
				"end_time": []string{now.Format(ledger.DateFormat)},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Lt("date", now.Format(ledger.DateFormat))),
		},
		{
			name: "using account",
			queryParams: url.Values{
				"account": []string{"xxx"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "xxx")),
		},
		{
			name: "using reference",
			queryParams: url.Values{
				"reference": []string{"xxx"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("reference", "xxx")),
		},
		{
			name: "using destination",
			queryParams: url.Values{
				"destination": []string{"xxx"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("destination", "xxx")),
		},
		{
			name: "using source",
			queryParams: url.Values{
				"source": []string{"xxx"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("source", "xxx")),
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{paginate.EncodeCursor(ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})))},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}),
		},
		{
			name: "using invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"XXX"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: v1.ErrValidation,
		},
		{
			name: "invalid page size",
			queryParams: url.Values{
				"pageSize": []string{"nan"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: v1.ErrValidation,
		},
		{
			name: "page size over maximum",
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithPageSize(v1.MaxPageSize),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := sharedapi.Cursor[ledger.ExpandedTransaction]{
				Data: []ledger.ExpandedTransaction{
					ledger.ExpandTransaction(
						ledger.NewTransaction().WithPostings(
							ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
						),
						nil,
					),
				},
			}

			backend, mockLedger := newTestingBackend(t)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					GetTransactions(gomock.Any(), ledgerstore.NewGetTransactionsQuery(testCase.expectQuery)).
					Return(&expectedCursor, nil)
			}

			router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodGet, "/xxx/transactions", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

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

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       ledgerstore.PaginatedQueryOptions[ledgerstore.PITFilterWithVolumes]
		expectStatusCode  int
		expectedErrorCode string
	}
	now := ledger.Now()

	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}),
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")),
		},
		{
			name: "using startTime",
			queryParams: url.Values{
				"start_time": []string{now.Format(ledger.DateFormat)},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Gte("date", now.Format(ledger.DateFormat))),
		},
		{
			name: "using endTime",
			queryParams: url.Values{
				"end_time": []string{now.Format(ledger.DateFormat)},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Lt("date", now.Format(ledger.DateFormat))),
		},
		{
			name: "using account",
			queryParams: url.Values{
				"account": []string{"xxx"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "xxx")),
		},
		{
			name: "using reference",
			queryParams: url.Values{
				"reference": []string{"xxx"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("reference", "xxx")),
		},
		{
			name: "using destination",
			queryParams: url.Values{
				"destination": []string{"xxx"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("destination", "xxx")),
		},
		{
			name: "using source",
			queryParams: url.Values{
				"source": []string{"xxx"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("source", "xxx")),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusNoContent
			}

			backend, mockLedger := newTestingBackend(t)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					CountTransactions(gomock.Any(), ledgerstore.NewGetTransactionsQuery(testCase.expectQuery)).
					Return(uint64(10), nil)
			}

			router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodHead, "/xxx/transactions", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

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

func TestRevertTransaction(t *testing.T) {

	expectedTx := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
	)

	backend, mockLedger := newTestingBackend(t)
	mockLedger.
		EXPECT().
		RevertTransaction(gomock.Any(), command.Parameters{}, big.NewInt(0)).
		Return(expectedTx, nil)

	router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry())

	req := httptest.NewRequest(http.MethodPost, "/xxx/transactions/0/revert", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	tx, ok := sharedapi.DecodeSingleResponse[ledger.Transaction](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, *expectedTx, tx)
}
