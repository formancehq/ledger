package v2

import (
	"bytes"
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/go-libs/v2/time"
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
		body              string
		expectQuery       ledgercontroller.OffsetPaginatedQuery[any]
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnErr         error
	}
	before := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
			},
			expectBackendCall: true,
		},
		{
			name:              "using metadata",
			body:              `{"$match": { "metadata[roles]": "admin" }}`,
			expectBackendCall: true,
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					PIT:     &before,
					Builder: query.Match("metadata[roles]", "admin"),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name:              "using address",
			body:              `{"$match": { "address": "foo" }}`,
			expectBackendCall: true,
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					PIT:     &before,
					Builder: query.Match("address", "foo"),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name:              "using empty cursor",
			expectBackendCall: true,
			queryParams: url.Values{
				"cursor": []string{bunpaginate.EncodeCursor(ledgercontroller.OffsetPaginatedQuery[any]{
					PageSize: bunpaginate.QueryDefaultPageSize,
					Options:  ledgercontroller.ResourceQuery[any]{},
				})},
			},
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options:  ledgercontroller.ResourceQuery[any]{},
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
			name:              "page size over maximum",
			expectBackendCall: true,
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: bunpaginate.MaxPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
			},
		},
		{
			name:              "using balance filter",
			expectBackendCall: true,
			body:              `{"$lt": { "balance[USD/2]": 100 }}`,
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					PIT:     &before,
					Builder: query.Lt("balance[USD/2]", float64(100)),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name:              "using exists filter",
			expectBackendCall: true,
			body:              `{"$exists": { "metadata": "foo" }}`,
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					PIT:     &before,
					Builder: query.Exists("metadata", "foo"),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name:              "using invalid query payload",
			body:              `[]`,
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name:              "with invalid query from core point of view",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
			expectBackendCall: true,
			returnErr:         ledgercontroller.ErrInvalidQuery{},
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
			},
		},
		{
			name:              "with missing feature",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
			expectBackendCall: true,
			returnErr:         ledgercontroller.ErrMissingFeature{},
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
			},
		},
		{
			name:              "with unexpected error",
			expectStatusCode:  http.StatusInternalServerError,
			expectedErrorCode: api.ErrorInternal,
			expectBackendCall: true,
			returnErr:         errors.New("undefined error"),
			expectQuery: ledgercontroller.OffsetPaginatedQuery[any]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: ledgercontroller.ResourceQuery[any]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
			},
		},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {

			if tc.expectStatusCode == 0 {
				tc.expectStatusCode = http.StatusOK
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
			if tc.expectBackendCall {
				ledgerController.EXPECT().
					ListAccounts(gomock.Any(), tc.expectQuery).
					Return(&expectedCursor, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodGet, "/xxx/accounts?pit="+before.Format(time.RFC3339Nano), bytes.NewBufferString(tc.body))
			rec := httptest.NewRecorder()
			params := url.Values{}
			if tc.queryParams != nil {
				params = tc.queryParams
			}
			params.Set("pit", before.Format(time.RFC3339Nano))
			req.URL.RawQuery = params.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectStatusCode, rec.Code)
			if tc.expectStatusCode < 300 && tc.expectStatusCode >= 200 {
				cursor := api.DecodeCursorResponse[ledger.Account](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
