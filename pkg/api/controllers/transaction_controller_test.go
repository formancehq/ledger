package controllers_test

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestPostTransactions(t *testing.T) {
	type testCase struct {
		name               string
		expectedDryRun     bool
		expectedRunScript  core.RunScript
		payload            any
		expectedStatusCode int
		expectedErrorCode  string
		queryParams        url.Values
	}

	testCases := []testCase{
		{
			name: "using plain numscript",
			payload: controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: `send (
						source = @world
						destination = @bank
					)`,
				},
			},
			expectedRunScript: core.RunScript{
				Script: core.Script{
					Plain: `send (
						source = @world
						destination = @bank
					)`,
				},
			},
		},
		{
			name: "using plain numscript and dry run",
			payload: controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: `send (
						source = @world
						destination = @bank
					)`,
				},
			},
			expectedRunScript: core.RunScript{
				Script: core.Script{
					Plain: `send (
						source = @world
						destination = @bank
					)`,
				},
			},
			expectedDryRun: true,
			queryParams: url.Values{
				"preview": []string{"true"},
			},
		},
		{
			name: "using JSON postings",
			payload: controllers.PostTransactionRequest{
				Postings: []core.Posting{
					core.NewPosting("world", "bank", "USD", big.NewInt(100)),
				},
			},
			expectedRunScript: core.TxToScriptData(core.NewTransactionData().WithPostings(
				core.NewPosting("world", "bank", "USD", big.NewInt(100)),
			)),
		},
		{
			name: "using JSON postings and dry run",
			queryParams: url.Values{
				// TODO(gfyrag): Rename to dry run
				"preview": []string{"true"},
			},
			payload: controllers.PostTransactionRequest{
				Postings: []core.Posting{
					core.NewPosting("world", "bank", "USD", big.NewInt(100)),
				},
			},
			expectedDryRun: true,
			expectedRunScript: core.TxToScriptData(core.NewTransactionData().WithPostings(
				core.NewPosting("world", "bank", "USD", big.NewInt(100)),
			)),
		},
		{
			name:               "no postings or script",
			payload:            controllers.PostTransactionRequest{},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  apierrors.ErrValidation,
		},
		{
			name: "postings and script",
			payload: controllers.PostTransactionRequest{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "alice",
						Amount:      big.NewInt(100),
						Asset:       "COIN",
					},
				},
				Script: core.Script{
					Plain: `
					send [COIN 100] (
					  source = @world
					  destination = @bob
					)`,
				},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  apierrors.ErrValidation,
		},
		{
			name:               "using invalid body",
			payload:            "not a valid payload",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  apierrors.ErrValidation,
		},
	}

	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {
			if testCase.expectedStatusCode == 0 {
				testCase.expectedStatusCode = http.StatusOK
			}

			expectedTx := core.ExpandTransaction(
				core.NewTransaction().WithPostings(
					core.NewPosting("world", "bank", "USD", big.NewInt(100)),
				),
				nil,
			)

			backend, mockLedger := newTestingBackend(t)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				mockLedger.EXPECT().
					CreateTransaction(gomock.Any(), testCase.expectedDryRun, testCase.expectedRunScript).
					Return(&expectedTx, nil)
			}

			router := routes.NewRouter(backend, nil, nil)

			req := httptest.NewRequest(http.MethodPost, "/xxx/transactions", Buffer(t, testCase.payload))
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				tx, ok := DecodeSingleResponse[core.ExpandedTransaction](t, rec.Body)
				require.True(t, ok)
				require.Equal(t, expectedTx, tx)
			} else {
				err := sharedapi.ErrorResponse{}
				Decode(t, rec.Body, &err)
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
			expectedErrorCode: apierrors.ErrValidation,
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
					SaveMeta(gomock.Any(), core.MetaTargetTypeTransaction, uint64(0), testCase.body).
					Return(nil)
			}

			router := routes.NewRouter(backend, nil, nil)

			req := httptest.NewRequest(http.MethodPost, "/xxx/transactions/0/metadata", Buffer(t, testCase.body))
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode >= 300 || testCase.expectStatusCode < 200 {
				err := sharedapi.ErrorResponse{}
				Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}

func TestGetTransaction(t *testing.T) {
	t.Parallel()

	tx := core.ExpandTransaction(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "USD", big.NewInt(100)),
		),
		nil,
	)

	backend, mock := newTestingBackend(t)
	mock.EXPECT().
		GetTransaction(gomock.Any(), uint64(0)).
		Return(&tx, nil)

	router := routes.NewRouter(backend, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/xxx/transactions/0", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, _ := DecodeSingleResponse[core.ExpandedTransaction](t, rec.Body)
	require.Equal(t, tx, response)
}

func TestGetTransactions(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       storage.TransactionsQuery
		expectStatusCode  int
		expectedErrorCode string
	}
	now := core.Now()

	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: *storage.NewTransactionsQuery(),
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithMetadataFilter(map[string]string{
					"roles": "admin",
				}),
		},
		{
			name: "using nested metadata",
			queryParams: url.Values{
				"metadata[a.nested.key]": []string{"hello"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithMetadataFilter(map[string]string{
					"a.nested.key": "hello",
				}),
		},
		{
			name: "using after",
			queryParams: url.Values{
				"after": []string{"10"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithAfterTxID(10),
		},
		{
			name: "using startTime",
			queryParams: url.Values{
				"startTime": []string{now.Format(core.DateFormat)},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithStartTimeFilter(now),
		},
		{
			name: "using invalid startTime",
			queryParams: url.Values{
				"startTime": []string{"xxx"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "using endTime",
			queryParams: url.Values{
				"endTime": []string{now.Format(core.DateFormat)},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithEndTimeFilter(now),
		},
		{
			name: "using invalid endTime",
			queryParams: url.Values{
				"endTime": []string{"xxx"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "using account",
			queryParams: url.Values{
				"account": []string{"xxx"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithAccountFilter("xxx"),
		},
		{
			name: "using reference",
			queryParams: url.Values{
				"reference": []string{"xxx"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithReferenceFilter("xxx"),
		},
		{
			name: "using destination",
			queryParams: url.Values{
				"destination": []string{"xxx"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithDestinationFilter("xxx"),
		},
		{
			name: "using source",
			queryParams: url.Values{
				"source": []string{"xxx"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithSourceFilter("xxx"),
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{ledgerstore.TransactionsPaginationToken{}.Encode()},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithMetadataFilter(nil),
		},
		{
			name: "using cursor with other param",
			queryParams: url.Values{
				"cursor": []string{ledgerstore.TransactionsPaginationToken{}.Encode()},
				"after":  []string{"foo"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "using invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"XXX"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "invalid page size",
			queryParams: url.Values{
				"pageSize": []string{"nan"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "invalid after",
			queryParams: url.Values{
				"after": []string{"nan"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "page size over maximum",
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithPageSize(controllers.MaxPageSize).
				WithMetadataFilter(map[string]string{}),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := sharedapi.Cursor[core.ExpandedTransaction]{
				Data: []core.ExpandedTransaction{
					core.ExpandTransaction(
						core.NewTransaction().WithPostings(
							core.NewPosting("world", "bank", "USD", big.NewInt(100)),
						),
						nil,
					),
				},
			}

			backend, mockLedger := newTestingBackend(t)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					GetTransactions(gomock.Any(), testCase.expectQuery).
					Return(expectedCursor, nil)
			}

			router := routes.NewRouter(backend, nil, nil)

			req := httptest.NewRequest(http.MethodGet, "/xxx/transactions", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := DecodeCursorResponse[core.ExpandedTransaction](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := sharedapi.ErrorResponse{}
				Decode(t, rec.Body, &err)
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
		expectQuery       storage.TransactionsQuery
		expectStatusCode  int
		expectedErrorCode string
	}
	now := core.Now()

	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: *storage.NewTransactionsQuery(),
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithMetadataFilter(map[string]string{
					"roles": "admin",
				}),
		},
		{
			name: "using nested metadata",
			queryParams: url.Values{
				"metadata[a.nested.key]": []string{"hello"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithMetadataFilter(map[string]string{
					"a.nested.key": "hello",
				}),
		},
		{
			name: "using startTime",
			queryParams: url.Values{
				"startTime": []string{now.Format(core.DateFormat)},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithStartTimeFilter(now),
		},
		{
			name: "using invalid startTime",
			queryParams: url.Values{
				"startTime": []string{"xxx"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "using endTime",
			queryParams: url.Values{
				"endTime": []string{now.Format(core.DateFormat)},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithEndTimeFilter(now),
		},
		{
			name: "using invalid endTime",
			queryParams: url.Values{
				"endTime": []string{"xxx"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "using account",
			queryParams: url.Values{
				"account": []string{"xxx"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithAccountFilter("xxx"),
		},
		{
			name: "using reference",
			queryParams: url.Values{
				"reference": []string{"xxx"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithReferenceFilter("xxx"),
		},
		{
			name: "using destination",
			queryParams: url.Values{
				"destination": []string{"xxx"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithDestinationFilter("xxx"),
		},
		{
			name: "using source",
			queryParams: url.Values{
				"source": []string{"xxx"},
			},
			expectQuery: *storage.NewTransactionsQuery().
				WithSourceFilter("xxx"),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				// TODO(gfyrag): Change status code to 204
				testCase.expectStatusCode = http.StatusOK
			}

			backend, mockLedger := newTestingBackend(t)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					CountTransactions(gomock.Any(), testCase.expectQuery).
					Return(uint64(10), nil)
			}

			router := routes.NewRouter(backend, nil, nil)

			req := httptest.NewRequest(http.MethodHead, "/xxx/transactions", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				require.Equal(t, "10", rec.Header().Get("Count"))
			} else {
				err := sharedapi.ErrorResponse{}
				Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}

func TestRevertTransaction(t *testing.T) {

	expectedTx := core.ExpandTransaction(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "USD", big.NewInt(100)),
		),
		nil,
	)

	backend, mockLedger := newTestingBackend(t)
	mockLedger.
		EXPECT().
		RevertTransaction(gomock.Any(), uint64(0)).
		Return(&expectedTx, nil)

	router := routes.NewRouter(backend, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/xxx/transactions/0/revert", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	// TODO(gfyrag): Change to 201
	require.Equal(t, http.StatusOK, rec.Code)
	tx, ok := DecodeSingleResponse[core.ExpandedTransaction](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, expectedTx, tx)
}
