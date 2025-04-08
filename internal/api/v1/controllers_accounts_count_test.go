package v1

import (
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/go-libs/v3/time"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestAccountsCount(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       storagecommon.ResourceQuery[any]
		expectStatusCode  int
		expectedErrorCode string
		returnErr         error
		expectBackendCall bool
	}
	before := time.Now()

	testCases := []testCase{
		{
			name:              "nominal",
			expectQuery:       storagecommon.ResourceQuery[any]{},
			expectBackendCall: true,
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectBackendCall: true,
			expectQuery: storagecommon.ResourceQuery[any]{
				Builder: query.Match("metadata[roles]", "admin"),
			},
		},
		{
			name:              "using address",
			queryParams:       url.Values{"address": []string{"foo"}},
			expectBackendCall: true,
			expectQuery: storagecommon.ResourceQuery[any]{
				Builder: query.Match("address", "foo"),
			},
		},
		{
			name:              "page size over maximum",
			expectBackendCall: true,
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectQuery: storagecommon.ResourceQuery[any]{},
		},
		{
			name: "using balance filter",
			queryParams: url.Values{
				"balanceOperator": []string{"lt"},
				"balance":         []string{"100"},
			},
			expectBackendCall: true,
			expectQuery: storagecommon.ResourceQuery[any]{
				Builder: query.Lt("balance", int64(100)),
			},
		},
		{
			name:              "with invalid query from core point of view",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
			expectBackendCall: true,
			returnErr:         storagecommon.ErrInvalidQuery{},
			expectQuery:       storagecommon.ResourceQuery[any]{},
		},
		{
			name:              "with missing feature",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
			expectBackendCall: true,
			returnErr:         ledgercontroller.ErrMissingFeature{},
			expectQuery:       storagecommon.ResourceQuery[any]{},
		},
		{
			name:              "with unexpected error",
			expectStatusCode:  http.StatusInternalServerError,
			expectedErrorCode: api.ErrorInternal,
			expectBackendCall: true,
			returnErr:         errors.New("undefined error"),
			expectQuery:       storagecommon.ResourceQuery[any]{},
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusNoContent
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectBackendCall {
				ledgerController.EXPECT().
					CountAccounts(gomock.Any(), testCase.expectQuery).
					Return(10, testCase.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodHead, "/xxx/accounts?pit="+before.Format(time.RFC3339Nano), nil)
			rec := httptest.NewRecorder()
			params := url.Values{}
			if testCase.queryParams != nil {
				params = testCase.queryParams
			}
			params.Set("pit", before.Format(time.RFC3339Nano))
			req.URL.RawQuery = params.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				require.Equal(t, "10", rec.Header().Get("Count"))
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
