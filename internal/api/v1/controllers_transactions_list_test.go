package v1

import (
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsList(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       storagecommon.PaginatedQuery[any]
		expectStatusCode  int
		expectedErrorCode string
	}
	now := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Expand: []string{"volumes"},
				},
			},
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Match("metadata[roles]", "admin"),
					Expand:  []string{"volumes"},
				},
			},
		},
		{
			name: "using startTime",
			queryParams: url.Values{
				"startTime": []string{now.Format(time.DateFormat)},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Gte("timestamp", now.Format(time.DateFormat)),
					Expand:  []string{"volumes"},
				},
			},
		},
		{
			name: "using endTime",
			queryParams: url.Values{
				"endTime": []string{now.Format(time.DateFormat)},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Lt("timestamp", now.Format(time.DateFormat)),
					Expand:  []string{"volumes"},
				},
			},
		},
		{
			name: "using deprecated start_time",
			queryParams: url.Values{
				"start_time": []string{now.Format(time.DateFormat)},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Gte("timestamp", now.Format(time.DateFormat)),
					Expand:  []string{"volumes"},
				},
			},
		},
		{
			name: "using deprecated end_time",
			queryParams: url.Values{
				"end_time": []string{now.Format(time.DateFormat)},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Lt("timestamp", now.Format(time.DateFormat)),
					Expand:  []string{"volumes"},
				},
			},
		},
		{
			name: "startTime takes precedence over start_time",
			queryParams: url.Values{
				"startTime":  []string{now.Format(time.DateFormat)},
				"start_time": []string{now.Add(-time.Hour).Format(time.DateFormat)},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Gte("timestamp", now.Format(time.DateFormat)),
					Expand:  []string{"volumes"},
				},
			},
		},
		{
			name: "endTime takes precedence over end_time",
			queryParams: url.Values{
				"endTime":  []string{now.Format(time.DateFormat)},
				"end_time": []string{now.Add(-time.Hour).Format(time.DateFormat)},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Lt("timestamp", now.Format(time.DateFormat)),
					Expand:  []string{"volumes"},
				},
			},
		},
		{
			name: "using account",
			queryParams: url.Values{
				"account": []string{"xxx"},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Match("account", "xxx"),
					Expand:  []string{"volumes"},
				},
			},
		},
		{
			name: "using reference",
			queryParams: url.Values{
				"reference": []string{"xxx"},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Match("reference", "xxx"),
					Expand:  []string{"volumes"},
				},
			},
		},
		{
			name: "using destination",
			queryParams: url.Values{
				"destination": []string{"xxx"},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Match("destination", "xxx"),
					Expand:  []string{"volumes"},
				},
			},
		},
		{
			name: "using source",
			queryParams: url.Values{
				"source": []string{"xxx"},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Match("source", "xxx"),
					Expand:  []string{"volumes"},
				},
			},
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{bunpaginate.EncodeCursor(storagecommon.ColumnPaginatedQuery[any]{
					InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
						Options: storagecommon.ResourceQuery[any]{
							Expand: []string{"volumes"},
						},
						PageSize: DefaultPageSize,
					},
				})},
			},
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
					Options: storagecommon.ResourceQuery[any]{
						Expand: []string{"volumes"},
					},
					PageSize: DefaultPageSize,
				},
			},
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
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: MaxPageSize,
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Column:   "id",
				Options: storagecommon.ResourceQuery[any]{
					Expand: []string{"volumes"},
				},
			},
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
					ListTransactions(gomock.Any(), testCase.expectQuery).
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
