package v2

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInsertSchema(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		version           string
		requestBody       interface{}
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnErr         error
	}

	testCases := []testCase{
		{
			name:    "nominal",
			version: "v1.0.0",
			requestBody: map[string]interface{}{
				"rules": []map[string]interface{}{
					{
						"field":    "postings",
						"required": true,
						"message":  "Postings are required",
					},
				},
			},
			expectStatusCode:  http.StatusNoContent,
			expectBackendCall: true,
		},
		{
			name:              "empty schema data",
			version:           "v1.0.0",
			requestBody:       map[string]interface{}{},
			expectStatusCode:  http.StatusNoContent,
			expectBackendCall: true,
		},
		{
			name:              "invalid JSON",
			version:           "v1.0.0",
			requestBody:       "invalid json string",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: "VALIDATION",
			expectBackendCall: false,
		},
		{
			name:    "backend error",
			version: "v1.0.0",
			requestBody: map[string]interface{}{
				"rules": []map[string]interface{}{},
			},
			expectStatusCode:  http.StatusInternalServerError,
			expectedErrorCode: "INTERNAL",
			expectBackendCall: true,
			returnErr:         errors.New("database error"),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			systemController, ledgerController := newTestingSystemController(t, true)
			if tc.expectBackendCall {
				ledgerController.EXPECT().
					UpdateSchema(gomock.Any(), gomock.Any()).
					Return(nil, nil, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			body, _ := json.Marshal(tc.requestBody)
			req := httptest.NewRequest(http.MethodPost, "/default/schema/"+tc.version, bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectStatusCode, rec.Code)
			if tc.expectedErrorCode != "" {
				var errorResponse api.ErrorResponse
				err := json.Unmarshal(rec.Body.Bytes(), &errorResponse)
				require.NoError(t, err)
				require.Equal(t, tc.expectedErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}
