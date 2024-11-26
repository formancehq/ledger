package v2

import (
	"fmt"
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"errors"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsAddMetadata(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectStatusCode  int
		expectedErrorCode string
		body              any
		id                any
		expectBackendCall bool
		returnErr         error
	}

	testCases := []testCase{
		{
			name: "nominal",
			body: metadata.Metadata{
				"foo": "bar",
			},
			expectBackendCall: true,
		},
		{
			name:              "invalid body",
			body:              "invalid - not an object",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name:              "invalid id",
			id:                "abc",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name: "not found",
			body: metadata.Metadata{
				"foo": "bar",
			},
			expectBackendCall: true,
			returnErr:         ledgercontroller.ErrNotFound,
			expectStatusCode:  http.StatusNotFound,
			expectedErrorCode: api.ErrorCodeNotFound,
		},
		{
			name: "unexpected error",
			body: metadata.Metadata{
				"foo": "bar",
			},
			expectBackendCall: true,
			returnErr:         errors.New("unexpected error"),
			expectStatusCode:  http.StatusInternalServerError,
			expectedErrorCode: api.ErrorInternal,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusNoContent
			}

			if testCase.id == nil {
				testCase.id = 1
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectBackendCall {
				ledgerController.EXPECT().
					SaveTransactionMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveTransactionMetadata]{
						Input: ledgercontroller.SaveTransactionMetadata{
							TransactionID: 1,
							Metadata:      testCase.body.(metadata.Metadata),
						},
					}).
					Return(nil, testCase.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/xxx/transactions/%v/metadata", testCase.id), api.Buffer(t, testCase.body))
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
