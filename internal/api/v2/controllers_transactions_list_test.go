package v2

import (
	"bytes"
	"fmt"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
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
		body              string
		expectQuery       storagecommon.ColumnPaginatedQuery[any]
		expectStatusCode  int
		expectedErrorCode string
	}
	now := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT:    &now,
					Expand: make([]string, 0),
				},
			},
		},
		{
			name: "using metadata",
			body: `{"$match": {"metadata[roles]": "admin"}}`,
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &now,
					Builder: query.Match("metadata[roles]", "admin"),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name: "using startTime",
			body: fmt.Sprintf(`{"$gte": {"start_time": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &now,
					Builder: query.Gte("start_time", now.Format(time.DateFormat)),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name: "using endTime",
			body: fmt.Sprintf(`{"$lte": {"end_time": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &now,
					Builder: query.Lte("end_time", now.Format(time.DateFormat)),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name: "using account",
			body: `{"$match": {"account": "xxx"}}`,
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &now,
					Builder: query.Match("account", "xxx"),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name: "using reference",
			body: `{"$match": {"reference": "xxx"}}`,
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &now,
					Builder: query.Match("reference", "xxx"),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name: "using destination",
			body: `{"$match": {"destination": "xxx"}}`,
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &now,
					Expand:  make([]string, 0),
					Builder: query.Match("destination", "xxx"),
				},
			},
		},
		{
			name: "using source",
			body: `{"$match": {"source": "xxx"}}`,
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &now,
					Builder: query.Match("source", "xxx"),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{bunpaginate.EncodeCursor(storagecommon.ColumnPaginatedQuery[any]{})},
			},
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
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
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.MaxPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT:    &now,
					Expand: make([]string, 0),
				},
			},
		},
		{
			name: "using cursor",
			queryParams: url.Values{
				"cursor": []string{func() string {
					return bunpaginate.EncodeCursor(storagecommon.ColumnPaginatedQuery[any]{
						PageSize: bunpaginate.QueryDefaultPageSize,
						Column:   "id",
						Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
						Options: storagecommon.ResourceQuery[any]{
							PIT: &now,
						},
					})
				}()},
			},
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT: &now,
				},
			},
		},
		{
			name: "using $exists metadata filter",
			body: `{"$exists": {"metadata": "foo"}}`,
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &now,
					Builder: query.Exists("metadata", "foo"),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name:        "paginate using effective order",
			queryParams: map[string][]string{"order": {"effective"}},
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "timestamp",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					PIT:    &now,
					Expand: make([]string, 0),
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

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

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
