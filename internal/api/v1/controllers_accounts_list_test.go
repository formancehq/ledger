package v1

import (
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/query"
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
		expectQuery       ledgercontroller.OffsetPaginatedQuery[any]
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnErr         error
	}

	testCases := []testCase{
		{
			name:              "nominal",
			expectBackendCall: true,
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: DefaultPageSize,
			},
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectBackendCall: true,
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("metadata[roles]", "admin"),
				},
			},
		},
		{
			name: "using address",
			queryParams: url.Values{
				"address": []string{"foo"},
			},
			expectBackendCall: true,
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("address", "foo"),
				},
			},
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{bunpaginate.EncodeCursor(ledgercontroller.OffsetPaginatedQuery[any]{})},
			},
			expectBackendCall: true,
			expectQuery:       ledgercontroller.OffsetPaginatedQuery[any]{},
		},
		{
			name: "using invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"XXX"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name: "invalid page size",
			queryParams: url.Values{
				"pageSize": []string{"nan"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name: "page size over maximum",
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectBackendCall: true,
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: MaxPageSize,
			},
		},
		{
			name: "using balance filter",
			queryParams: url.Values{
				"balance":         []string{"100"},
				"balanceOperator": []string{"e"},
			},
			expectBackendCall: true,
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("balance", int64(100)),
				},
			},
		},
		{
			name:              "with missing feature",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
			returnErr:         ledgercontroller.ErrMissingFeature{},
			expectBackendCall: true,
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: DefaultPageSize,
			},
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := bunpaginate.Cursor[ledger.Account]{
				Data: []ledger.Account{
					{
						Address:  "world",
						Metadata: metadata.Metadata{},
					},
				},
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectBackendCall {
				ledgerController.EXPECT().
					ListAccounts(gomock.Any(), testCase.expectQuery).
					Return(&expectedCursor, testCase.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodGet, "/xxx/accounts", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := api.DecodeCursorResponse[ledger.Account](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
