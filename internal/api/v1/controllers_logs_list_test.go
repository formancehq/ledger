package v1

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
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

func TestGetLogs(t *testing.T) {
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
				Column:   "id",
				Order:    pointer.For(paginate.Order(paginate.OrderDesc)),
			},
		},
		{
			name: "using start time",
			queryParams: url.Values{
				"start_time": []string{now.Format(time.DateFormat)},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Column:   "id",
				Order:    pointer.For(paginate.Order(paginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Gte("date", now.Format(time.DateFormat)),
				},
			},
		},
		{
			name: "using end time",
			queryParams: url.Values{
				"end_time": []string{now.Format(time.DateFormat)},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: DefaultPageSize,
				Column:   "id",
				Order:    pointer.For(paginate.Order(paginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Builder: query.Lt("date", now.Format(time.DateFormat)),
				},
			},
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{paginate.EncodeCursor(storagecommon.ColumnPaginatedQuery[any]{
					InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
						PageSize: DefaultPageSize,
					},
				})},
			},
			expectQuery: storagecommon.ColumnPaginatedQuery[any]{
				InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
					PageSize: DefaultPageSize,
				},
			},
		},
		{
			name: "using invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"xxx"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := paginate.Cursor[ledger.Log]{
				Data: []ledger.Log{
					ledger.NewLog(ledger.CreatedTransaction{
						Transaction:     ledger.NewTransaction(),
						AccountMetadata: ledger.AccountMetadata{},
					}).ChainLog(nil),
				},
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				ledgerController.EXPECT().
					ListLogs(gomock.Any(), testCase.expectQuery).
					Return(&expectedCursor, nil)
			}

			router := NewRouter(systemController, jwt.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodGet, "/xxx/logs", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

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
