package v2

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v2/auth"
	ingester "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"

	sharedapi "github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/replication/controller"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestCreatePipeline(t *testing.T) {
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
			name:                  "pipeline already exists",
			returnError:           &ledgercontroller.ErrPipelineAlreadyExists{},
			expectErrorStatusCode: http.StatusBadRequest,
			expectErrorCode:       "VALIDATION",
		},
		{
			name:                  "connector not available",
			returnError:           controller.NewErrConnectorNotFound("connector1"),
			expectErrorStatusCode: http.StatusBadRequest,
			expectErrorCode:       "VALIDATION",
		},
		{
			name:                  "pipeline actually used",
			returnError:           controller.NewErrInUsePipeline(""),
			expectErrorStatusCode: http.StatusBadRequest,
			expectErrorCode:       "VALIDATION",
		},
		{
			name:                  "unknown error",
			returnError:           errors.New("unknown error"),
			expectErrorStatusCode: http.StatusInternalServerError,
			expectErrorCode:       "INTERNAL",
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			systemController, ledgerController := newTestingSystemController(t, true)
			router := NewRouter(systemController, auth.NewNoAuth(), os.Getenv("DEBUG") == "true")


			pipelineConfiguration := ingester.PipelineConfiguration{
				Ledger:      "module1",
				ConnectorID: uuid.NewString(),
			}
			req := httptest.NewRequest(http.MethodPost, "/xxx/pipelines", sharedapi.Buffer(t, pipelineConfiguration))
			req = req.WithContext(ctx)
			rec := httptest.NewRecorder()

			ledgerController.EXPECT().
				CreatePipeline(gomock.Any(), pipelineConfiguration).
				Return(nil, testCase.returnError)

			router.ServeHTTP(rec, req)

			if testCase.expectErrorCode != "" {
				require.Equal(t, testCase.expectErrorStatusCode, rec.Code)
				errorResponse := sharedapi.ErrorResponse{}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&errorResponse))
				require.Equal(t, testCase.expectErrorCode, errorResponse.ErrorCode)
			} else {
				require.Equal(t, http.StatusCreated, rec.Code)
			}
		})
	}
}
