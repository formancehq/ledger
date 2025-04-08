package v1

import (
	"encoding/json"
	ledger "github.com/formancehq/ledger/internal"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/logging"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsDeleteMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name               string
		queryParams        url.Values
		returnErr          error
		expectedStatusCode int
		expectedErrorCode  string
		expectBackendCall  bool
	}

	for _, tc := range []testCase{
		{
			name:              "nominal",
			expectBackendCall: true,
		},
		{
			name:               "unexpected backend error",
			expectBackendCall:  true,
			returnErr:          errors.New("undefined error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorCode:  api.ErrorInternal,
		},
		{
			name:               "not found",
			expectBackendCall:  true,
			returnErr:          ledgercontroller.ErrNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedErrorCode:  api.ErrorCodeNotFound,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			systemController, ledgerController := newTestingSystemController(t, true)

			if tc.expectBackendCall {
				ledgerController.EXPECT().
					DeleteTransactionMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.DeleteTransactionMetadata]{
						Input: ledgercontroller.DeleteTransactionMetadata{
							TransactionID: 1,
							Key:           "foo",
						},
					}).
					Return(&ledger.Log{}, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodDelete, "/ledger0/transactions/1/metadata/foo", nil)
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
