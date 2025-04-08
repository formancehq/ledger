package v1

import (
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/metadata"
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
			name:    "nominal dash 1",
			account: "-test",
			body: metadata.Metadata{
				"foo": "bar",
			},
		},
		{
			name:    "nominal dash 2",
			account: "-",
			body: metadata.Metadata{
				"foo": "bar",
			},
		},
		{
			name:    "nominal dash 2",
			account: "-tes--t--t--t-----",
			body: metadata.Metadata{
				"foo": "bar",
			},
		},
		{
			name:              "invalid body",
			account:           "world",
			body:              "invalid - not an object",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name:              "malformed account address",
			account:           "%8X%2F",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name:              "invalid account address",
			account:           "1\abc",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
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
					Return(&ledger.Log{}, nil)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodPost, "/", api.Buffer(t, testCase.body))
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
