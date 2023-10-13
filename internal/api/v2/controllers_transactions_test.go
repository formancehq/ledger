package v2_test

import (
	"bytes"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/ledger/internal/api/shared"

	ledger "github.com/formancehq/ledger/internal"
	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/paginate"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestPostTransactions(t *testing.T) {
	type testCase struct {
		name               string
		expectedDryRun     bool
		expectedRunScript  ledger.RunScript
		payload            any
		expectedStatusCode int
		expectedErrorCode  string
		queryParams        url.Values
	}

	testCases := []testCase{
		{
			name: "using plain numscript",
			payload: v2.PostTransactionRequest{
				Script: v2.Script{
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
			payload: v2.PostTransactionRequest{
				Script: v2.Script{
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
			payload: v2.PostTransactionRequest{
				Script: v2.Script{
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
			payload: v2.PostTransactionRequest{
				Script: v2.Script{
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
			name: "using JSON postings",
			payload: v2.PostTransactionRequest{
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
				"dryRun": []string{"true"},
			},
			payload: v2.PostTransactionRequest{
				Postings: []ledger.Posting{
					ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
				},
			},
			expectedDryRun: true,
			expectedRunScript: ledger.TxToScriptData(ledger.NewTransactionData().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			)),
		},
		{
			name:               "no postings or script",
			payload:            v2.PostTransactionRequest{},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  shared.ErrValidation,
		},
		{
			name: "postings and script",
			payload: v2.PostTransactionRequest{
				Postings: ledger.Postings{
					{
						Source:      "world",
						Destination: "alice",
						Amount:      big.NewInt(100),
						Asset:       "COIN",
					},
				},
				Script: v2.Script{
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
			expectedErrorCode:  shared.ErrValidation,
		},
		{
			name:               "using invalid body",
			payload:            "not a valid payload",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  shared.ErrValidation,
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
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				mockLedger.EXPECT().
					CreateTransaction(gomock.Any(), command.Parameters{
						DryRun: tc.expectedDryRun,
					}, testCase.expectedRunScript).
					Return(expectedTx, nil)
			}

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

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
			expectedErrorCode: shared.ErrValidation,
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

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

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

	backend, mock := newTestingBackend(t, true)
	mock.EXPECT().
		GetTransactionWithVolumes(gomock.Any(), ledgerstore.NewGetTransactionQuery(big.NewInt(0))).
		Return(&tx, nil)

	router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

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
		body              string
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
			body: `{"$match": {"metadata[roles]": "admin"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")),
		},
		{
			name: "using startTime",
			body: fmt.Sprintf(`{"$gte": {"start_time": "%s"}}`, now.Format(ledger.DateFormat)),
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Gte("start_time", now.Format(ledger.DateFormat))),
		},
		{
			name: "using endTime",
			body: fmt.Sprintf(`{"$lte": {"end_time": "%s"}}`, now.Format(ledger.DateFormat)),
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Lte("end_time", now.Format(ledger.DateFormat))),
		},
		{
			name: "using account",
			body: `{"$match": {"account": "xxx"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "xxx")),
		},
		{
			name: "using reference",
			body: `{"$match": {"reference": "xxx"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("reference", "xxx")),
		},
		{
			name: "using destination",
			body: `{"$match": {"destination": "xxx"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("destination", "xxx")),
		},
		{
			name: "using source",
			body: `{"$match": {"source": "xxx"}}`,
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
			expectedErrorCode: shared.ErrValidation,
		},
		{
			name: "invalid page size",
			queryParams: url.Values{
				"pageSize": []string{"nan"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: shared.ErrValidation,
		},
		{
			name: "page size over maximum",
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithPageSize(v2.MaxPageSize),
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

			backend, mockLedger := newTestingBackend(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					GetTransactions(gomock.Any(), ledgerstore.NewGetTransactionsQuery(testCase.expectQuery)).
					Return(&expectedCursor, nil)
			}

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodGet, "/xxx/transactions", bytes.NewBufferString(testCase.body))
			rec := httptest.NewRecorder()
			if testCase.queryParams != nil {
				req.URL.RawQuery = testCase.queryParams.Encode()
			}

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
		body              string
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
			body: `{"$match": {"metadata[roles]": "admin"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")),
		},
		{
			name: "using startTime",
			body: fmt.Sprintf(`{"$gte": {"date": "%s"}}`, now.Format(ledger.DateFormat)),
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Gte("date", now.Format(ledger.DateFormat))),
		},
		{
			name: "using endTime",
			body: fmt.Sprintf(`{"$gte": {"date": "%s"}}`, now.Format(ledger.DateFormat)),
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Gte("date", now.Format(ledger.DateFormat))),
		},
		{
			name: "using account",
			body: `{"$match": {"account": "xxx"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "xxx")),
		},
		{
			name: "using reference",
			body: `{"$match": {"reference": "xxx"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("reference", "xxx")),
		},
		{
			name: "using destination",
			body: `{"$match": {"destination": "xxx"}}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("destination", "xxx")),
		},
		{
			name: "using source",
			body: `{"$match": {"source": "xxx"}}`,
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

			backend, mockLedger := newTestingBackend(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					CountTransactions(gomock.Any(), ledgerstore.NewGetTransactionsQuery(testCase.expectQuery)).
					Return(10, nil)
			}

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodHead, "/xxx/transactions", bytes.NewBufferString(testCase.body))
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

func TestRevertTransaction(t *testing.T) {

	expectedTx := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
	)

	backend, mockLedger := newTestingBackend(t, true)
	mockLedger.
		EXPECT().
		RevertTransaction(gomock.Any(), command.Parameters{}, big.NewInt(0)).
		Return(expectedTx, nil)

	router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

	req := httptest.NewRequest(http.MethodPost, "/xxx/transactions/0/revert", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	tx, ok := sharedapi.DecodeSingleResponse[ledger.Transaction](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, *expectedTx, tx)
}
