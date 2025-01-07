package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	sharedapi "github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/replication/controller"
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
			returnError:           ErrPipelineNotFound(""),
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
			returnError:           controller.NewErrInUsePipeline(""),
			expectErrorStatusCode: http.StatusBadRequest,
			expectErrorCode:       "VALIDATION",
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
			req, err := http.NewRequest(http.MethodDelete, srv.URL+"/pipelines/"+connectorID, nil)
			require.NoError(t, err)

			backend.EXPECT().
				DeletePipeline(gomock.Any(), connectorID).
				Return(testCase.returnError)

			rsp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, "application/json", rsp.Header.Get("Content-Type"))

			if testCase.expectErrorCode != "" {
				require.Equal(t, testCase.expectErrorStatusCode, rsp.StatusCode)
				errorResponse := sharedapi.ErrorResponse{}
				require.NoError(t, json.NewDecoder(rsp.Body).Decode(&errorResponse))
				require.Equal(t, testCase.expectErrorCode, errorResponse.ErrorCode)
			} else {
				require.Equal(t, http.StatusNoContent, rsp.StatusCode)
			}
		})
	}
}
