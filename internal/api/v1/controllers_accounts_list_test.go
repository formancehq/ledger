package v1

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestAccountsList(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes]
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnErr         error
	}

	testCases := []testCase{
		{
			name:              "nominal",
			expectBackendCall: true,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithPageSize(DefaultPageSize),
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectBackendCall: true,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")).
				WithPageSize(DefaultPageSize),
		},
		{
			name: "using address",
			queryParams: url.Values{
				"address": []string{"foo"},
			},
			expectBackendCall: true,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("address", "foo")).
				WithPageSize(DefaultPageSize),
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{bunpaginate.EncodeCursor(ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{})))},
			},
			expectBackendCall: true,
			expectQuery:       ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}),
		},
		{
			name: "using invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"XXX"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
		},
		{
			name: "invalid page size",
			queryParams: url.Values{
				"pageSize": []string{"nan"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
		},
		{
			name: "page size over maximum",
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectBackendCall: true,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithPageSize(MaxPageSize),
		},
		{
			name: "using balance filter",
			queryParams: url.Values{
				"balance":         []string{"100"},
				"balanceOperator": []string{"e"},
			},
			expectBackendCall: true,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("balance", int64(100))).
				WithPageSize(DefaultPageSize),
		},
		{
			name:              "with missing feature",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
			returnErr:         ledgercontroller.ErrMissingFeature{},
			expectBackendCall: true,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithPageSize(DefaultPageSize),
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

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectBackendCall {
				ledgerController.EXPECT().
					ListAccounts(gomock.Any(), ledgercontroller.NewListAccountsQuery(testCase.expectQuery)).
					Return(&expectedCursor, testCase.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

			req := httptest.NewRequest(http.MethodGet, "/xxx/accounts", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := api.DecodeCursorResponse[ledger.ExpandedAccount](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
