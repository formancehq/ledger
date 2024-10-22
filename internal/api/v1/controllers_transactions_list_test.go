package v1

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsList(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes]
		expectStatusCode  int
		expectedErrorCode string
	}
	now := time.Now()

	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}),
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")),
		},
		{
			name: "using startTime",
			queryParams: url.Values{
				"start_time": []string{now.Format(time.DateFormat)},
			},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Gte("date", now.Format(time.DateFormat))),
		},
		{
			name: "using endTime",
			queryParams: url.Values{
				"end_time": []string{now.Format(time.DateFormat)},
			},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Lt("date", now.Format(time.DateFormat))),
		},
		{
			name: "using account",
			queryParams: url.Values{
				"account": []string{"xxx"},
			},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "xxx")),
		},
		{
			name: "using reference",
			queryParams: url.Values{
				"reference": []string{"xxx"},
			},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("reference", "xxx")),
		},
		{
			name: "using destination",
			queryParams: url.Values{
				"destination": []string{"xxx"},
			},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("destination", "xxx")),
		},
		{
			name: "using source",
			queryParams: url.Values{
				"source": []string{"xxx"},
			},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("source", "xxx")),
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{bunpaginate.EncodeCursor(ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{})))},
			},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}),
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
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithPageSize(MaxPageSize),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := bunpaginate.Cursor[ledger.Transaction]{
				Data: []ledger.Transaction{
					ledger.NewTransaction().WithPostings(
						ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
					),
				},
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				ledgerController.EXPECT().
					ListTransactions(gomock.Any(), ledgercontroller.NewListTransactionsQuery(testCase.expectQuery)).
					Return(&expectedCursor, nil)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodGet, "/xxx/transactions", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := api.DecodeCursorResponse[ledger.Transaction](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
