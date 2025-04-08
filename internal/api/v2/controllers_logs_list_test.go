package v2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetLogs(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       storagecommon.ColumnPaginatedQuery[any]
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnErr         error
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
					Expand: make([]string, 0),
				},
			},
			expectBackendCall: true,
		},
		{
			name: "using start time",
			body: fmt.Sprintf(`{"$gte": {"date": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Gte("date", now.Format(time.DateFormat)),
					Expand:  make([]string, 0),
				},
			},
			expectBackendCall: true,
		},
		{
			name: "using end time",
			body: fmt.Sprintf(`{"$lt": {"date": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Lt("date", now.Format(time.DateFormat)),
					Expand:  make([]string, 0),
				},
			},
			expectBackendCall: true,
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{bunpaginate.EncodeCursor(storagecommon.ColumnPaginatedQuery[any]{
					PageSize: bunpaginate.QueryDefaultPageSize,
					Column:   "id",
					Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				})},
			},
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
			},
			expectBackendCall: true,
		},
		{
			name: "using invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"xxx"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name: "using invalid page size",
			queryParams: url.Values{
				"pageSize": []string{"-1"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name:              "using malformed query",
			body:              `[]`,
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name:             "with invalid query",
			expectStatusCode: http.StatusBadRequest,
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Expand: make([]string, 0),
				},
			},
			expectedErrorCode: common.ErrValidation,
			expectBackendCall: true,
			returnErr:         storagecommon.ErrInvalidQuery{},
		},
		{
			name:             "with unexpected error",
			expectStatusCode: http.StatusInternalServerError,
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Expand: make([]string, 0),
				},
			},
			expectedErrorCode: api.ErrorInternal,
			expectBackendCall: true,
			returnErr:         errors.New("unexpected error"),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := bunpaginate.Cursor[ledger.Log]{
				Data: []ledger.Log{
					ledger.NewLog(ledger.CreatedTransaction{
						Transaction:     ledger.NewTransaction(),
						AccountMetadata: ledger.AccountMetadata{},
					}).
						ChainLog(nil),
				},
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectBackendCall {
				ledgerController.EXPECT().
					ListLogs(gomock.Any(), testCase.expectQuery).
					Return(&expectedCursor, testCase.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodGet, "/xxx/logs", bytes.NewBufferString(testCase.body))
			rec := httptest.NewRecorder()
			if testCase.queryParams != nil {
				req.URL.RawQuery = testCase.queryParams.Encode()
			}

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := api.DecodeCursorResponse[ledger.Log](t, rec.Body)

				cursorData, err := json.Marshal(cursor)
				require.NoError(t, err)

				cursorAsMap := make(map[string]any)
				require.NoError(t, json.Unmarshal(cursorData, &cursorAsMap))

				expectedCursorData, err := json.Marshal(expectedCursor)
				require.NoError(t, err)

				expectedCursorAsMap := make(map[string]any)
				require.NoError(t, json.Unmarshal(expectedCursorData, &expectedCursorAsMap))

				require.Equal(t, expectedCursorAsMap, cursorAsMap)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
