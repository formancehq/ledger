package v2

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/time"

	ledger "github.com/formancehq/ledger/internal"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

func TestListSchemas(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       storagecommon.PaginatedQuery[any]
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnCursor      *bunpaginate.Cursor[ledger.Schema]
		returnErr         error
	}

	now := time.Now().UTC()
	testSchemas := []ledger.Schema{
		{
			Version:    "v1.0.0",
			CreatedAt:  now,
			SchemaData: ledger.SchemaData{},
		},
		{
			Version:    "v2.0.0",
			CreatedAt:  now.Add(time.Hour),
			SchemaData: ledger.SchemaData{},
		},
	}

	testCursor := &bunpaginate.Cursor[ledger.Schema]{
		Data:     testSchemas,
		HasMore:  false,
		PageSize: 15,
	}

	testCases := []testCase{
		{
			name:        "nominal",
			queryParams: url.Values{},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "created_at",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Expand: make([]string, 0),
				},
			},
			expectStatusCode:  http.StatusOK,
			expectBackendCall: true,
			returnCursor:      testCursor,
		},
		{
			name: "with pagination parameters",
			queryParams: url.Values{
				"pageSize": []string{"10"},
				"cursor":   []string{"eyJvZmZzZXQiOjB9"},
			},
			expectQuery: storagecommon.OffsetPaginatedQuery[any]{
				InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
					PageSize: 10,
					Options:  storagecommon.ResourceQuery[any]{},
				},
				Offset: 0,
			},
			expectStatusCode:  http.StatusOK,
			expectBackendCall: true,
			returnCursor:      testCursor,
		},
		{
			name: "with sort parameters",
			queryParams: url.Values{
				"sort":  []string{"created_at"},
				"order": []string{"desc"},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "created_at",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Expand: make([]string, 0),
				},
			},
			expectStatusCode:  http.StatusOK,
			expectBackendCall: true,
			returnCursor:      testCursor,
		},
		{
			name:        "backend error",
			queryParams: url.Values{},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Column:   "created_at",
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
				Options: storagecommon.ResourceQuery[any]{
					Expand: make([]string, 0),
				},
			},
			expectStatusCode:  http.StatusInternalServerError,
			expectedErrorCode: "INTERNAL",
			expectBackendCall: true,
			returnErr:         errors.New("database error"),
		},
		{
			name: "invalid pagination parameters",
			queryParams: url.Values{
				"pageSize": []string{"invalid"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: "VALIDATION",
			expectBackendCall: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			systemController, ledgerController := newTestingSystemController(t, true)
			if tc.expectBackendCall {
				ledgerController.EXPECT().
					ListSchemas(gomock.Any(), tc.expectQuery).
					Return(tc.returnCursor, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodGet, "/default/schemas?"+tc.queryParams.Encode(), nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectStatusCode, rec.Code)
			if tc.expectedErrorCode != "" {
				var errorResponse api.ErrorResponse
				err := json.Unmarshal(rec.Body.Bytes(), &errorResponse)
				require.NoError(t, err)
				require.Equal(t, tc.expectedErrorCode, errorResponse.ErrorCode)
			} else if tc.returnCursor != nil {
				cursor := api.DecodeCursorResponse[ledger.Schema](t, rec.Body)
				require.Len(t, cursor.Data, len(tc.returnCursor.Data))
				require.Equal(t, tc.returnCursor.HasMore, cursor.HasMore)
				require.Equal(t, tc.returnCursor.PageSize, cursor.PageSize)
			}
		})
	}
}
