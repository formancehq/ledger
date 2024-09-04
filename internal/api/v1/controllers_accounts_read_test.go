package v1

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestAccountsRead(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       ledgercontroller.GetAccountQuery
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnErr         error
		account           string
	}

	testCases := []testCase{
		{
			name:              "nominal",
			account:           "foo",
			expectQuery:       ledgercontroller.NewGetAccountQuery("foo").WithExpandVolumes(),
			expectBackendCall: true,
		},
		{
			name:              "with expand volumes",
			account:           "foo",
			expectQuery:       ledgercontroller.NewGetAccountQuery("foo").WithExpandVolumes(),
			expectBackendCall: true,
			queryParams: url.Values{
				"expand": {"volumes"},
			},
		},
		{
			name:              "invalid account address",
			account:           "%8X%2F",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
		},
		{
			name:              "with not existing account",
			account:           "foo",
			expectQuery:       ledgercontroller.NewGetAccountQuery("foo").WithExpandVolumes(),
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
					Return(&ledger.ExpandedAccount{}, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

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
				_, ok := api.DecodeSingleResponse[ledger.ExpandedAccount](t, rec.Body)
				require.True(t, ok)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
