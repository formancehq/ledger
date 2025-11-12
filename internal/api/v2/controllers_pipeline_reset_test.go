package v2

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/auth"
	sharedapi "github.com/formancehq/go-libs/v3/testing/api"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func TestResetPipeline(t *testing.T) {
	t.Parallel()

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
			name:            "undefined error",
			expectErrorCode: "INTERNAL",
			expectCode:      http.StatusInternalServerError,
			returnError:     errors.New("unknown error"),
		},
		{
			name:            "pipeline not found",
			expectErrorCode: "NOT_FOUND",
			expectCode:      http.StatusNotFound,
			returnError:     ledger.ErrPipelineNotFound(""),
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
			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithExporters(true))

			exporterID := uuid.NewString()
			req := httptest.NewRequest(http.MethodPost, "/xxx/pipelines/"+exporterID+"/reset", nil)
			rec := httptest.NewRecorder()

			systemController.EXPECT().
				ResetPipeline(gomock.Any(), exporterID).
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
