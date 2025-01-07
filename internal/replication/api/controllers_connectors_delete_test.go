package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/ledger/internal/replication/controller"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	sharedapi "github.com/formancehq/go-libs/v2/api"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDeleteConnector(t *testing.T) {
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
			name:                  "not found",
			returnError:           controller.NewErrConnectorNotFound(""),
			expectErrorStatusCode: http.StatusNotFound,
			expectErrorCode:       "NOT_FOUND",
		},
		{
			name:                  "connector used",
			returnError:           controller.NewErrConnectorUsed(""),
			expectErrorStatusCode: http.StatusBadRequest,
			expectErrorCode:       "VALIDATION",
		},
		{
			name:                  "unknown error",
			expectErrorCode:       "INTERNAL",
			expectErrorStatusCode: http.StatusInternalServerError,
			returnError:           errors.New("any error"),
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
			req, err := http.NewRequest(http.MethodDelete, srv.URL+"/connectors/"+connectorID, nil)
			require.NoError(t, err)

			backend.EXPECT().
				DeleteConnector(gomock.Any(), connectorID).
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
