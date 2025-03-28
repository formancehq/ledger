package v1

import (
	"bytes"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/storage/resources"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestAccountsRead(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       resources.ResourceQuery[any]
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnErr         error
		account           string
	}

	testCases := []testCase{
		{
			name:    "nominal",
			account: "foo",
			expectQuery: resources.ResourceQuery[any]{
				Builder: query.Match("address", "foo"),
				Expand:  []string{"volumes"},
			},
			expectBackendCall: true,
		},
		{
			name:    "with expand volumes",
			account: "foo",
			expectQuery: resources.ResourceQuery[any]{
				Builder: query.Match("address", "foo"),
				Expand:  []string{"volumes"},
			},
			expectBackendCall: true,
			queryParams: url.Values{
				"expand": {"volumes"},
			},
		},
		{
			name:              "invalid account address",
			account:           "%8X%2F",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name:    "with not existing account",
			account: "foo",
			expectQuery: resources.ResourceQuery[any]{
				Builder: query.Match("address", "foo"),
				Expand:  []string{"volumes"},
			},
			expectBackendCall: true,
			returnErr:         postgres.ErrNotFound,
		},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {

			if tc.expectStatusCode == 0 {
				tc.expectStatusCode = http.StatusOK
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if tc.expectBackendCall {
				ledgerController.EXPECT().
					GetAccount(gomock.Any(), tc.expectQuery).
					Return(&ledger.Account{}, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(tc.body))
			req.URL.Path = "/xxx/accounts/" + tc.account
			rec := httptest.NewRecorder()
			params := url.Values{}
			if tc.queryParams != nil {
				params = tc.queryParams
			}
			req.URL.RawQuery = params.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectStatusCode, rec.Code)
			if tc.expectStatusCode < 300 && tc.expectStatusCode >= 200 {
				_, ok := api.DecodeSingleResponse[ledger.Account](t, rec.Body)
				require.True(t, ok)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
