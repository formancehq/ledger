package v2

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v3/auth"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	sharedapi "github.com/formancehq/go-libs/v3/api"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/logging"
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
			returnError:           &ledger.ErrPipelineAlreadyExists{},
			expectErrorStatusCode: http.StatusBadRequest,
			expectErrorCode:       "VALIDATION",
		},
		{
			name:                  "connector not available",
			returnError:           systemcontroller.NewErrConnectorNotFound("connector1"),
			expectErrorStatusCode: http.StatusBadRequest,
			expectErrorCode:       "VALIDATION",
		},
		{
			name:                  "pipeline actually used",
			returnError:           ledgercontroller.NewErrInUsePipeline(""),
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
			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithConnectors(true))

			pipelineConfiguration := ledger.PipelineConfiguration{
				Ledger:      "module1",
				ConnectorID: uuid.NewString(),
			}
			req := httptest.NewRequest(http.MethodPost, "/"+pipelineConfiguration.Ledger+"/pipelines", sharedapi.Buffer(t, pipelineConfiguration))
			req = req.WithContext(ctx)
			rec := httptest.NewRecorder()

			systemController.EXPECT().
				CreatePipeline(gomock.Any(), pipelineConfiguration).
				Return(nil, testCase.returnError)

			ledgerController.EXPECT().
				Info().
				Return(ledger.Ledger{
					Name: pipelineConfiguration.Ledger,
				})

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
