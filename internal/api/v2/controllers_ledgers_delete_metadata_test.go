package v2

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"errors"
	"github.com/formancehq/go-libs/api"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/logging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestLedgersDeleteMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name               string
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			name := uuid.NewString()
			systemController, _ := newTestingSystemController(t, false)
			if tc.expectBackendCall {
				systemController.EXPECT().
					DeleteLedgerMetadata(gomock.Any(), name, "foo").
					Return(tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), testing.Verbose())

			req := httptest.NewRequest(http.MethodDelete, "/"+name+"/metadata/foo", nil)
			req = req.WithContext(ctx)
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
