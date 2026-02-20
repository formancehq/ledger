package v2

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	sharedapi "github.com/formancehq/go-libs/v4/api"
	"github.com/formancehq/go-libs/v4/auth"
	"github.com/formancehq/go-libs/v4/logging"

	ledger "github.com/formancehq/ledger/internal"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

func TestUpdateExporter(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name                  string
		exporterID            string
		exporterConfiguration ledger.ExporterConfiguration
		returnError           error
		expectErrorStatusCode int
		expectErrorCode       string
	}
	for _, testCase := range []testCase{
		{
			name:       "nominal",
			exporterID: uuid.NewString(),
			exporterConfiguration: ledger.ExporterConfiguration{
				Driver: "http",
				Config: json.RawMessage(`{"url":"http://example.com"}`),
			},
		},
		{
			name:       "invalid driver configuration",
			exporterID: uuid.NewString(),
			exporterConfiguration: ledger.ExporterConfiguration{
				Driver: "http",
				Config: json.RawMessage(`{"url":"invalid"}`),
			},
			returnError:           systemcontroller.NewErrInvalidDriverConfiguration("http", errors.New("invalid config")),
			expectErrorStatusCode: http.StatusBadRequest,
			expectErrorCode:       "VALIDATION",
		},
		{
			name:       "exporter not found",
			exporterID: uuid.NewString(),
			exporterConfiguration: ledger.ExporterConfiguration{
				Driver: "http",
				Config: json.RawMessage(`{"url":"http://example.com"}`),
			},
			returnError:           systemcontroller.NewErrExporterNotFound(""),
			expectErrorStatusCode: http.StatusNotFound,
			expectErrorCode:       "NOT_FOUND",
		},
		{
			name:       "unknown error",
			exporterID: uuid.NewString(),
			exporterConfiguration: ledger.ExporterConfiguration{
				Driver: "http",
				Config: json.RawMessage(`{"url":"http://example.com"}`),
			},
			expectErrorCode:       "INTERNAL",
			expectErrorStatusCode: http.StatusInternalServerError,
			returnError:           errors.New("any error"),
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			systemController, _ := newTestingSystemController(t, false)
			systemController.EXPECT().
				UpdateExporter(gomock.Any(), testCase.exporterID, testCase.exporterConfiguration).
				Return(testCase.returnError)

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithExporters(true))

			data, err := json.Marshal(testCase.exporterConfiguration)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPut, "/_/exporters/"+testCase.exporterID, bytes.NewBuffer(data))
			req = req.WithContext(ctx)
			rsp := httptest.NewRecorder()

			router.ServeHTTP(rsp, req)

			if testCase.expectErrorCode != "" {
				require.Equal(t, testCase.expectErrorStatusCode, rsp.Code)
				errorResponse := sharedapi.ErrorResponse{}
				require.NoError(t, json.NewDecoder(rsp.Body).Decode(&errorResponse))
				require.Equal(t, testCase.expectErrorCode, errorResponse.ErrorCode)
			} else {
				require.Equal(t, http.StatusNoContent, rsp.Code)
			}
		})
	}
}
