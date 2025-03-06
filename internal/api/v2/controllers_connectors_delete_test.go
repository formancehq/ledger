package v2

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/logging"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	sharedapi "github.com/formancehq/go-libs/v3/api"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDeleteConnector(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name                  string
		returnError           error
		expectErrorStatusCode int
		expectErrorCode       string
	}
	for _, testCase := range []testCase{
		{
			name: "nominal",
		},
		{
			name:                  "not found",
			returnError:           systemcontroller.NewErrConnectorNotFound(""),
			expectErrorStatusCode: http.StatusNotFound,
			expectErrorCode:       "NOT_FOUND",
		},
		{
			name:                  "connector used",
			returnError:           systemcontroller.NewErrConnectorUsed(""),
			expectErrorStatusCode: http.StatusBadRequest,
			expectErrorCode:       "VALIDATION",
		},
		{
			name:                  "unknown error",
			expectErrorCode:       "INTERNAL",
			expectErrorStatusCode: http.StatusInternalServerError,
			returnError:           errors.New("any error"),
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			connectorID := uuid.NewString()
			systemController, _ := newTestingSystemController(t, false)
			systemController.EXPECT().
				DeleteConnector(gomock.Any(), connectorID).
				Return(testCase.returnError)

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithConnectors(true))

			req := httptest.NewRequest(http.MethodDelete, "/_system/connectors/"+connectorID, nil)
			req = req.WithContext(ctx)
			rsp := httptest.NewRecorder()

			router.ServeHTTP(rsp, req)

			if testCase.expectErrorCode != "" {
				require.Equal(t, testCase.expectErrorStatusCode, rsp.Code)
				errorResponse := sharedapi.ErrorResponse{}
				require.NoError(t, json.NewDecoder(rsp.Body).Decode(&errorResponse))
				require.Equal(t, testCase.expectErrorCode, errorResponse.ErrorCode)
			} else {
				require.Equal(t, http.StatusNoContent, rsp.Code)
			}
		})
	}
}
