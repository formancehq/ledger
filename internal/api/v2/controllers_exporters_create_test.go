package v2

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	sharedapi "github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/logging"

	ledger "github.com/formancehq/ledger/internal"
)

func TestCreateExporter(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name                  string
		returnError           error
		expectErrorStatusCode int
		expectErrorCode       string
		exporterConfiguration ledger.ExporterConfiguration
	}
	for _, testCase := range []testCase{
		{
			name: "nominal",
			exporterConfiguration: ledger.ExporterConfiguration{
				Driver: "exporter1",
				Config: json.RawMessage("{}"),
			},
		},
		{
			name: "invalid exporter configuration",
			exporterConfiguration: ledger.ExporterConfiguration{
				Driver: "exporter1",
				Config: json.RawMessage(`{"batching":{"flushInterval":"-1"}}`),
			},
		},
		{
			name: "unknown error",
			exporterConfiguration: ledger.ExporterConfiguration{
				Driver: "exporter1",
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
				CreateExporter(gomock.Any(), testCase.exporterConfiguration).
				Return(nil, testCase.returnError)

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithExporters(true))

			data, err := json.Marshal(testCase.exporterConfiguration)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/_/exporters", bytes.NewBuffer(data))
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
