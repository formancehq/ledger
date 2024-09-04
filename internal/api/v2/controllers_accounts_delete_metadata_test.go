package v2

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/logging"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestAccountsDeleteMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name               string
		queryParams        url.Values
		returnErr          error
		expectedStatusCode int
		expectedErrorCode  string
		expectBackendCall  bool
		account            string
	}

	for _, tc := range []testCase{
		{
			name:              "nominal",
			expectBackendCall: true,
			account:           "account0",
		},
		{
			name:               "unexpected backend error",
			expectBackendCall:  true,
			returnErr:          errors.New("undefined error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorCode:  api.ErrorInternal,
			account:            "account0",
		},
		{
			name:               "invalid account address",
			account:            "%8X%2F",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  ErrValidation,
			expectBackendCall:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			systemController, ledgerController := newTestingSystemController(t, true)

			if tc.expectBackendCall {
				ledgerController.EXPECT().
					DeleteAccountMetadata(gomock.Any(), ledgercontroller.Parameters{}, tc.account, "foo").
					Return(tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

			req := httptest.NewRequest(http.MethodDelete, "/", nil)
			req.URL.Path = "/ledger0/accounts/" + tc.account + "/metadata/foo"
			req = req.WithContext(ctx)
			req.URL.RawQuery = tc.queryParams.Encode()

			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if tc.expectedStatusCode == 0 || tc.expectedStatusCode == http.StatusOK {
				require.Equal(t, http.StatusNoContent, rec.Code)
			} else {
				require.Equal(t, tc.expectedStatusCode, rec.Code)
				errorResponse := api.ErrorResponse{}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errorResponse))
				require.Equal(t, tc.expectedErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}
