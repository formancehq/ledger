package v2

import (
	"bytes"
	"encoding/json"
	sharedapi "github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	ledger "github.com/formancehq/ledger/internal"
	"go.uber.org/mock/gomock"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
)

func TestCreateConnector(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name                   string
		returnError            error
		expectErrorStatusCode  int
		expectErrorCode        string
		connectorConfiguration ledger.ConnectorConfiguration
	}
	for _, testCase := range []testCase{
		{
			name: "nominal",
			connectorConfiguration: ledger.ConnectorConfiguration{
				Driver: "connector1",
				Config: json.RawMessage("{}"),
			},
		},
		{
			name: "invalid connector configuration",
			connectorConfiguration: ledger.ConnectorConfiguration{
				Driver: "connector1",
				Config: json.RawMessage(`{"batching":{"flushInterval":"-1"}}`),
			},
		},
		{
			name: "unknown error",
			connectorConfiguration: ledger.ConnectorConfiguration{
				Driver: "connector1",
				Config: json.RawMessage("{}"),
			},
			expectErrorCode:       "INTERNAL",
			expectErrorStatusCode: http.StatusInternalServerError,
			returnError:           errors.New("any error"),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			systemController, _ := newTestingSystemController(t, false)
			systemController.EXPECT().
				CreateConnector(gomock.Any(), testCase.connectorConfiguration).
				Return(nil, testCase.returnError)

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithConnectors(true))

			data, err := json.Marshal(testCase.connectorConfiguration)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/_system/connectors", bytes.NewBuffer(data))
			req = req.WithContext(ctx)
			rsp := httptest.NewRecorder()

			router.ServeHTTP(rsp, req)

			require.Equal(t, "application/json", rsp.Header().Get("Content-Type"))
			if testCase.expectErrorCode != "" {
				require.Equal(t, testCase.expectErrorStatusCode, rsp.Code)
				errorResponse := sharedapi.ErrorResponse{}
				require.NoError(t, json.NewDecoder(rsp.Body).Decode(&errorResponse))
				require.Equal(t, testCase.expectErrorCode, errorResponse.ErrorCode)
			} else {
				require.Equal(t, http.StatusCreated, rsp.Code)
			}
		})
	}
}
