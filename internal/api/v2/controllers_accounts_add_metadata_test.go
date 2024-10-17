package v2

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestAccountsAddMetadata(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectStatusCode  int
		expectedErrorCode string
		account           string
		body              any
	}

	testCases := []testCase{
		{
			name:    "nominal",
			account: "world",
			body: metadata.Metadata{
				"foo": "bar",
			},
		},
		{
			name:              "invalid body",
			account:           "world",
			body:              "invalid - not an object",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
		},
		{
			name:              "invalid account address",
			account:           "%8X%2F",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusNoContent
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectStatusCode == http.StatusNoContent {
				ledgerController.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address:  testCase.account,
							Metadata: testCase.body.(metadata.Metadata),
						},
					}).
					Return(nil)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), testing.Verbose())

			req := httptest.NewRequest(http.MethodPost, "/", api.Buffer(t, testCase.body))
			// httptest.NewRequest check for invalid urls while we want to test invalid urls
			req.URL.Path = "/xxx/accounts/" + testCase.account + "/metadata"
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode >= 300 || testCase.expectStatusCode < 200 {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
