package v1_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/ledger/internal/storage/bunpaginate"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	sharedapi "github.com/formancehq/ledger/internal/api/sharedapi"
	v1 "github.com/formancehq/ledger/internal/api/v1"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetAccounts(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       ledgerstore.PaginatedQueryOptions[ledgerstore.PITFilterWithVolumes]
		expectStatusCode  int
		expectedErrorCode string
	}

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithPageSize(v1.DefaultPageSize),
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.And(query.Match("metadata[roles]", "admin"))).
				WithPageSize(v1.DefaultPageSize),
		},
		{
			name: "using address",
			queryParams: url.Values{
				"address": []string{"foo"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.And(query.Match("address", "foo"))).
				WithPageSize(v1.DefaultPageSize),
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{bunpaginate.EncodeCursor(ledgerstore.NewGetAccountsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})))},
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
		{
			name: "using balance filter",
			queryParams: url.Values{
				"balance":         []string{"100"},
				"balanceOperator": []string{"e"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.And(query.Match("balance", "100"))).
				WithPageSize(v1.DefaultPageSize),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := bunpaginate.Cursor[ledger.ExpandedAccount]{
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

			router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth(), testing.Verbose())

			req := httptest.NewRequest(http.MethodGet, "/xxx/accounts", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

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
		GetAccountWithVolumes(gomock.Any(), ledgerstore.NewGetAccountQuery("foo").WithExpandVolumes()).
		Return(&account, nil)

	router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth(), testing.Verbose())

	req := httptest.NewRequest(http.MethodGet, "/xxx/accounts/foo", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, _ := sharedapi.DecodeSingleResponse[ledger.ExpandedAccount](t, rec.Body)
	require.Equal(t, account, response)
}

func TestGetAccountWithEncoded(t *testing.T) {
	t.Parallel()

	account := ledger.ExpandedAccount{
		Account: ledger.Account{
			Address:  "foo:bar",
			Metadata: metadata.Metadata{},
		},
	}

	backend, mock := newTestingBackend(t, true)
	mock.EXPECT().
		GetAccountWithVolumes(gomock.Any(), ledgerstore.NewGetAccountQuery("foo:bar").WithExpandVolumes()).
		Return(&account, nil)

	router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth(), testing.Verbose())

	req := httptest.NewRequest(http.MethodGet, "/xxx/accounts/foo%3Abar", nil)
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
			name:    "nominal dash 1",
			account: "-test",
			body: metadata.Metadata{
				"foo": "bar",
			},
		},
		{
			name:    "nominal dash 2",
			account: "-",
			body: metadata.Metadata{
				"foo": "bar",
			},
		},
		{
			name:    "nominal dash 2",
			account: "-tes--t--t--t-----",
			body: metadata.Metadata{
				"foo": "bar",
			},
		},
		{
			name:              "invalid body",
			account:           "world",
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

			backend, mock := newTestingBackend(t, true)
			if testCase.expectStatusCode == http.StatusNoContent {
				mock.EXPECT().
					SaveMeta(gomock.Any(), command.Parameters{}, ledger.MetaTargetTypeAccount, testCase.account, testCase.body).
					Return(nil)
			}

			router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth(), testing.Verbose())

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
