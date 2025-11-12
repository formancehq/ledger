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
)

func TestReadPipeline(t *testing.T) {
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
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			systemController, _ := newTestingSystemController(t, true)
			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithExporters(true))

			exporterID := uuid.NewString()
			req := httptest.NewRequest(http.MethodGet, "/xxx/pipelines/"+exporterID, nil)
			rec := httptest.NewRecorder()

			systemController.EXPECT().
				GetPipeline(gomock.Any(), exporterID).
				Return(&ledger.Pipeline{}, testCase.returnError)

			router.ServeHTTP(rec, req)

			if testCase.expectSuccess {
				require.Equal(t, http.StatusOK, rec.Code)
			} else {
				require.Equal(t, testCase.expectCode, rec.Code)
				errorResponse := sharedapi.ReadErrorResponse(t, rec.Body)
				require.Equal(t, testCase.expectErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}
