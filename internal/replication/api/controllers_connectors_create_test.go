package api

import (
	"encoding/json"
	ingester "github.com/formancehq/ledger/internal"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pkg/errors"

	sharedapi "github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestCreateConnector(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name                   string
		returnError            error
		expectErrorStatusCode  int
		expectErrorCode        string
		connectorConfiguration ingester.ConnectorConfiguration
	}
	for _, testCase := range []testCase{
		{
			name: "nominal",
			connectorConfiguration: ingester.ConnectorConfiguration{
				Driver: "connector1",
				Config: json.RawMessage("{}"),
			},
		},
		{
			name: "invalid connector configuration",
			connectorConfiguration: ingester.ConnectorConfiguration{
				Driver: "connector1",
				Config: json.RawMessage(`{"batching":{"flushInterval":"-1"}}`),
			},
		},
		{
			name: "unknown error",
			connectorConfiguration: ingester.ConnectorConfiguration{
				Driver: "connector1",
				Config: json.RawMessage("{}"),
			},
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

			req, err := http.NewRequest(http.MethodPost, srv.URL+"/connectors", sharedapi.Buffer(t, testCase.connectorConfiguration))
			require.NoError(t, err)
			req = req.WithContext(ctx)

			backend.EXPECT().
				CreateConnector(gomock.Any(), testCase.connectorConfiguration).
				Return(nil, testCase.returnError)

			rsp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, "application/json", rsp.Header.Get("Content-Type"))
			if testCase.expectErrorCode != "" {
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
