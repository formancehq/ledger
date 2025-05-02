package v2

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v3/auth"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	sharedapi "github.com/formancehq/go-libs/v3/api"
	"github.com/pkg/errors"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDeletePipeline(t *testing.T) {
	t.Parallel()

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
			name:                  "with pipeline not existing",
			returnError:           ledger.ErrPipelineNotFound(""),
			expectErrorStatusCode: http.StatusNotFound,
			expectErrorCode:       "NOT_FOUND",
		},
		{
			name:                  "with unknown error",
			returnError:           errors.New("unknown error"),
			expectErrorStatusCode: http.StatusInternalServerError,
			expectErrorCode:       "INTERNAL",
		},
		{
			name:                  "pipeline actually used",
			returnError:           ledgercontroller.NewErrInUsePipeline(""),
			expectErrorStatusCode: http.StatusBadRequest,
			expectErrorCode:       "VALIDATION",
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			systemController, _ := newTestingSystemController(t, true)
			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithConnectors(true))

			connectorID := uuid.NewString()
			req := httptest.NewRequest(http.MethodDelete, "/xxx/pipelines/"+connectorID, nil)
			rec := httptest.NewRecorder()

			systemController.EXPECT().
				DeletePipeline(gomock.Any(), connectorID).
				Return(testCase.returnError)

			router.ServeHTTP(rec, req)

			if testCase.expectErrorCode != "" {
				require.Equal(t, testCase.expectErrorStatusCode, rec.Code)
				errorResponse := sharedapi.ErrorResponse{}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&errorResponse))
				require.Equal(t, testCase.expectErrorCode, errorResponse.ErrorCode)
			} else {
				require.Equal(t, http.StatusNoContent, rec.Code)
			}
		})
	}
}
