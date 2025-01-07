package api

import (
	"encoding/json"
	"github.com/davecgh/go-spew/spew"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	sharedapi "github.com/formancehq/go-libs/v2/api"
	ingester "github.com/formancehq/ledger/internal/replication"
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
			returnError:           &ErrPipelineAlreadyExists{},
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

			ctrl := gomock.NewController(t)
			backend := NewMockBackend(ctrl)

			api := newAPI(t, backend)
			srv := httptest.NewServer(api.Router())
			t.Cleanup(srv.Close)

			pipelineConfiguration := ingester.PipelineConfiguration{
				Ledger:      "module1",
				ConnectorID: uuid.NewString(),
			}
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/pipelines", sharedapi.Buffer(t, pipelineConfiguration))
			require.NoError(t, err)
			req = req.WithContext(ctx)

			backend.EXPECT().
				CreatePipeline(gomock.Any(), pipelineConfiguration).
				Return(nil, testCase.returnError)

			rsp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, "application/json", rsp.Header.Get("Content-Type"))
			if testCase.expectErrorCode != "" {
				spew.Dump(rsp.StatusCode)
				require.Equal(t, testCase.expectErrorStatusCode, rsp.StatusCode)
				errorResponse := sharedapi.ErrorResponse{}
				require.NoError(t, json.NewDecoder(rsp.Body).Decode(&errorResponse))
				require.Equal(t, testCase.expectErrorCode, errorResponse.ErrorCode)
			} else {
				require.Equal(t, http.StatusCreated, rsp.StatusCode)
			}
		})
	}
}
