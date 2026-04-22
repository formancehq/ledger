package v2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/transport/api"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

func TestLogsList(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       storagecommon.PaginatedQuery[any]
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnErr         error
	}

	now := time.Now()
	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(paginate.Order(paginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Expand: make([]string, 0),
				},
			},
			expectBackendCall: true,
		},
		{
			name: "using start time",
			body: fmt.Sprintf(`{"$gte": {"date": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(paginate.Order(paginate.OrderDesc)),
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
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(paginate.Order(paginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Lt("date", now.Format(time.DateFormat)),
					Expand:  make([]string, 0),
				},
			},
			expectBackendCall: true,
		},
		{
			name: "using type filter",
			body: `{"$match": {"type": "NEW_TRANSACTION"}}`,
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(paginate.Order(paginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Match("type", "NEW_TRANSACTION"),
					Expand:  make([]string, 0),
				},
			},
			expectBackendCall: true,
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{paginate.EncodeCursor(storagecommon.ColumnPaginatedQuery[any]{
					InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
						PageSize: paginate.QueryDefaultPageSize,
						Column:   "id",
						Order:    pointer.For(paginate.Order(paginate.OrderDesc)),
					},
				})},
			},
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
					PageSize: paginate.QueryDefaultPageSize,
					Column:   "id",
					Order:    pointer.For(paginate.Order(paginate.OrderDesc)),
				},
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
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(paginate.Order(paginate.OrderDesc)),
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
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Column:   "id",
				Order:    pointer.For(paginate.Order(paginate.OrderDesc)),
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
			t.Parallel()

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := paginate.Cursor[ledger.Log]{
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

			router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

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
