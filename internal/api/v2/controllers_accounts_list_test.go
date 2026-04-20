package v2

import (
	"bytes"
	"errors"
	"math/big"
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
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

func TestAccountsList(t *testing.T) {
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

	before := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[any]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
				Column: "address",
				Order:  pointer.For(paginate.Order(paginate.OrderAsc)),
			},
			expectBackendCall: true,
		},
		{
			name:              "using metadata",
			body:              `{"$match": { "metadata[roles]": "admin" }}`,
			expectBackendCall: true,
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &before,
					Builder: query.Match("metadata[roles]", "admin"),
					Expand:  make([]string, 0),
				},
				Column: "address",
				Order:  pointer.For(paginate.Order(paginate.OrderAsc)),
			},
		},
		{
			name:              "using address",
			body:              `{"$match": { "address": "foo" }}`,
			expectBackendCall: true,
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &before,
					Builder: query.Match("address", "foo"),
					Expand:  make([]string, 0),
				},
				Column: "address",
				Order:  pointer.For(paginate.Order(paginate.OrderAsc)),
			},
		},
		{
			name:              "using empty cursor",
			expectBackendCall: true,
			queryParams: url.Values{
				"cursor": []string{paginate.EncodeCursor(storagecommon.OffsetPaginatedQuery[any]{
					InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
						PageSize: paginate.QueryDefaultPageSize,
						Options:  storagecommon.ResourceQuery[any]{},
						Column:   "address",
						Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
					},
				})},
			},
			expectQuery: storagecommon.OffsetPaginatedQuery[any]{
				InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
					PageSize: paginate.QueryDefaultPageSize,
					Options:  storagecommon.ResourceQuery[any]{},
					Column:   "address",
					Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
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
			name:              "page size over maximum",
			expectBackendCall: true,
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.MaxPageSize,
				Options: storagecommon.ResourceQuery[any]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
				Column: "address",
				Order:  pointer.For(paginate.Order(paginate.OrderAsc)),
			},
		},
		{
			name:              "using balance filter",
			expectBackendCall: true,
			body:              `{"$lt": { "balance[USD/2]": 100 }}`,
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &before,
					Builder: query.Lt("balance[USD/2]", big.NewInt(100)),
					Expand:  make([]string, 0),
				},
				Column: "address",
				Order:  pointer.For(paginate.Order(paginate.OrderAsc)),
			},
		},
		{
			name:              "using exists filter",
			expectBackendCall: true,
			body:              `{"$exists": { "metadata": "foo" }}`,
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[any]{
					PIT:     &before,
					Builder: query.Exists("metadata", "foo"),
					Expand:  make([]string, 0),
				},
				Column: "address",
				Order:  pointer.For(paginate.Order(paginate.OrderAsc)),
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
			returnErr:         storagecommon.ErrInvalidQuery{},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[any]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
				Column: "address",
				Order:  pointer.For(paginate.Order(paginate.OrderAsc)),
			},
		},
		{
			name:              "with missing feature",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
			expectBackendCall: true,
			returnErr:         ledgerstore.ErrMissingFeature{},
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[any]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
				Column: "address",
				Order:  pointer.For(paginate.Order(paginate.OrderAsc)),
			},
		},
		{
			name:              "with unexpected error",
			expectStatusCode:  http.StatusInternalServerError,
			expectedErrorCode: api.ErrorInternal,
			expectBackendCall: true,
			returnErr:         errors.New("undefined error"),
			expectQuery: storagecommon.InitialPaginatedQuery[any]{
				PageSize: paginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[any]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
				Column: "address",
				Order:  pointer.For(paginate.Order(paginate.OrderAsc)),
			},
		},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {

			if tc.expectStatusCode == 0 {
				tc.expectStatusCode = http.StatusOK
			}

			expectedCursor := paginate.Cursor[ledger.Account]{
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

			router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

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
