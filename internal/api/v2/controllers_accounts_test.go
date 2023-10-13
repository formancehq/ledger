package v2_test

import (
	"bytes"
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

func TestGetAccounts(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       ledgerstore.PaginatedQueryOptions[ledgerstore.PITFilterWithVolumes]
		expectStatusCode  int
		expectedErrorCode string
	}

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithPageSize(v2.DefaultPageSize),
		},
		{
			name: "using metadata",
			body: `{"$match": { "metadata[roles]": "admin" }}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")).
				WithPageSize(v2.DefaultPageSize),
		},
		{
			name: "using address",
			body: `{"$match": { "address": "foo" }}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("address", "foo")).
				WithPageSize(v2.DefaultPageSize),
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{paginate.EncodeCursor(ledgerstore.NewGetAccountsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})))},
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
		{
			name: "using balance filter",
			body: `{"$lt": { "balance[USD/2]": 100 }}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Lt("balance[USD/2]", float64(100))).
				WithPageSize(v2.DefaultPageSize),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := sharedapi.Cursor[ledger.ExpandedAccount]{
				Data: []ledger.ExpandedAccount{
					{
						Account: ledger.Account{
							Address:  "world",
							Metadata: metadata.Metadata{},
						},
					},
				},
			}

			backend, mockLedger := newTestingBackend(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					GetAccountsWithVolumes(gomock.Any(), ledgerstore.NewGetAccountsQuery(testCase.expectQuery)).
					Return(&expectedCursor, nil)
			}

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodGet, "/xxx/accounts", bytes.NewBufferString(testCase.body))
			rec := httptest.NewRecorder()
			if testCase.queryParams != nil {
				req.URL.RawQuery = testCase.queryParams.Encode()
			}

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := sharedapi.DecodeCursorResponse[ledger.ExpandedAccount](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}

func TestGetAccount(t *testing.T) {
	t.Parallel()

	account := ledger.ExpandedAccount{
		Account: ledger.Account{
			Address:  "foo",
			Metadata: metadata.Metadata{},
		},
	}

	backend, mock := newTestingBackend(t, true)
	mock.EXPECT().
		GetAccountWithVolumes(gomock.Any(), ledgerstore.NewGetAccountQuery("foo")).
		Return(&account, nil)

	router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

	req := httptest.NewRequest(http.MethodGet, "/xxx/accounts/foo", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, _ := sharedapi.DecodeSingleResponse[ledger.ExpandedAccount](t, rec.Body)
	require.Equal(t, account, response)
}

func TestPostAccountMetadata(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectStatusCode  int
		expectedErrorCode string
		account           string
		body              any
	}

	testCases := []testCase{
		{
			name:    "nominal",
			account: "world",
			body: metadata.Metadata{
				"foo": "bar",
			},
		},
		{
			name:              "invalid account address format",
			account:           "invalid-acc",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: shared.ErrValidation,
		},
		{
			name:              "invalid body",
			account:           "world",
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
					SaveMeta(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeAccount, testCase.account, testCase.body).
					Return(nil)
			}

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodPost, "/xxx/accounts/"+testCase.account+"/metadata", sharedapi.Buffer(t, testCase.body))
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
