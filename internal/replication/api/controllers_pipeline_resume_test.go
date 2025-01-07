package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	sharedapi "github.com/formancehq/go-libs/v2/testing/api"
	"github.com/google/uuid"

	"github.com/formancehq/ledger/internal/replication/controller"
	"github.com/pkg/errors"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestResumePipeline(t *testing.T) {
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
			name:             "pipeline not in paused state",
			expectErrorCode:  "VALIDATION",
			expectStatusCode: http.StatusBadRequest,
			returnError:      &ErrInvalidStateSwitch{},
		},
		{
			name:             "undefined error",
			expectErrorCode:  "INTERNAL",
			expectStatusCode: http.StatusInternalServerError,
			returnError:      errors.New("unknown error"),
		},
		{
			name:             "pipeline not found",
			expectErrorCode:  "NOT_FOUND",
			expectStatusCode: http.StatusNotFound,
			returnError:      ErrPipelineNotFound(""),
		},
		{
			name:             "pipeline actually used",
			returnError:      controller.NewErrInUsePipeline(""),
			expectStatusCode: http.StatusBadRequest,
			expectErrorCode:  "VALIDATION",
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

			connectorID := uuid.NewString()
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/pipelines/"+connectorID+"/resume", nil)
			require.NoError(t, err)

			backend.EXPECT().
				ResumePipeline(gomock.Any(), connectorID).
				Return(testCase.returnError)

			rsp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, "application/json", rsp.Header.Get("Content-Type"))

			if testCase.expectSuccess {
				require.Equal(t, http.StatusAccepted, rsp.StatusCode)
			} else {
				require.Equal(t, testCase.expectStatusCode, rsp.StatusCode)
				errorResponse := sharedapi.ReadErrorResponse(t, rsp.Body)
				require.Equal(t, testCase.expectErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}
