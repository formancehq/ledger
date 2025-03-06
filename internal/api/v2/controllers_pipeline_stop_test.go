package v2

import (
	"github.com/formancehq/go-libs/v3/auth"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"net/http"
	"net/http/httptest"
	"testing"

	sharedapi "github.com/formancehq/go-libs/v3/testing/api"
	"github.com/google/uuid"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestStopPipeline(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	type testCase struct {
		name            string
		returnError     error
		expectSuccess   bool
		expectErrorCode string
		expectCode      int
	}

	for _, testCase := range []testCase{
		{
			name:          "nominal",
			expectSuccess: true,
		},
		{
			name:            "pipeline not exists",
			expectErrorCode: "NOT_FOUND",
			expectCode:      http.StatusNotFound,
			returnError:     ledger.ErrPipelineNotFound(""),
		},
		{
			name:            "unknown error",
			expectErrorCode: "INTERNAL",
			expectCode:      http.StatusInternalServerError,
			returnError:     errors.New("internal error"),
		},
		{
			name:            "pipeline actually used",
			returnError:     ledgercontroller.NewErrInUsePipeline(""),
			expectCode:      http.StatusBadRequest,
			expectErrorCode: "VALIDATION",
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			systemController, _ := newTestingSystemController(t, true)
			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithConnectors(true))

			connectorID := uuid.NewString()
			req := httptest.NewRequest(http.MethodPost, "/xxx/pipelines/"+connectorID+"/stop", nil)
			req = req.WithContext(ctx)
			rec := httptest.NewRecorder()

			systemController.EXPECT().
				StopPipeline(gomock.Any(), connectorID).
				Return(testCase.returnError)

			router.ServeHTTP(rec, req)

			if testCase.expectSuccess {
				require.Equal(t, http.StatusAccepted, rec.Code)
			} else {
				require.Equal(t, testCase.expectCode, rec.Code)
				errorResponse := sharedapi.ReadErrorResponse(t, rec.Body)
				require.Equal(t, testCase.expectErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}
