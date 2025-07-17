package v2

import (
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger/internal"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"net/http"
	"net/http/httptest"
	"testing"

	sharedapi "github.com/formancehq/go-libs/v3/testing/api"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestReadConnector(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name             string
		returnError      error
		expectSuccess    bool
		expectErrorCode  string
		expectStatusCode int
	}

	for _, testCase := range []testCase{
		{
			name:          "nominal",
			expectSuccess: true,
		},
		{
			name:          "nominal",
			expectSuccess: true,
		},
		{
			name:             "not found",
			returnError:      systemcontroller.NewErrConnectorNotFound(""),
			expectStatusCode: http.StatusNotFound,
			expectErrorCode:  "NOT_FOUND",
		},
		{
			name:             "unknown error",
			expectErrorCode:  "INTERNAL",
			expectStatusCode: http.StatusInternalServerError,
			returnError:      errors.New("any error"),
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			connectorID := uuid.NewString()
			systemController, _ := newTestingSystemController(t, false)
			systemController.EXPECT().
				GetConnector(gomock.Any(), connectorID).
				Return(&ledger.Connector{}, testCase.returnError)

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithConnectors(true))

			req := httptest.NewRequest(http.MethodGet, "/_system/connectors/"+connectorID, nil)
			req = req.WithContext(logging.TestingContext())
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if testCase.expectSuccess {
				require.Equal(t, http.StatusOK, rec.Code)
			} else {
				require.Equal(t, testCase.expectStatusCode, rec.Code)
				errorResponse := sharedapi.ReadErrorResponse(t, rec.Body)
				require.Equal(t, testCase.expectErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}
