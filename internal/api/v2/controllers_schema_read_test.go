package v2

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v4/api"
	"github.com/formancehq/go-libs/v4/auth"
	"github.com/formancehq/go-libs/v4/platform/postgres"
	"github.com/formancehq/go-libs/v4/time"

	ledger "github.com/formancehq/ledger/internal"
)

func TestGetSchema(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		version           string
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnSchema      *ledger.Schema
		returnErr         error
	}

	now := time.Now()
	testSchema := &ledger.Schema{
		Version:    "v1.0.0",
		CreatedAt:  now,
		SchemaData: ledger.SchemaData{},
	}

	testCases := []testCase{
		{
			name:              "nominal",
			version:           "v1.0.0",
			expectStatusCode:  http.StatusOK,
			expectBackendCall: true,
			returnSchema:      testSchema,
		},
		{
			name:              "schema not found",
			version:           "non-existent",
			expectStatusCode:  http.StatusNotFound,
			expectedErrorCode: "NOT_FOUND",
			expectBackendCall: true,
			returnErr:         postgres.ErrNotFound,
		},
		{
			name:              "backend error",
			version:           "v1.0.0",
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
					GetSchema(gomock.Any(), tc.version).
					Return(tc.returnSchema, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodGet, "/default/schemas/"+tc.version, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectStatusCode, rec.Code)
			if tc.expectedErrorCode != "" {
				var errorResponse api.ErrorResponse
				err := json.Unmarshal(rec.Body.Bytes(), &errorResponse)
				require.NoError(t, err)
				require.Equal(t, tc.expectedErrorCode, errorResponse.ErrorCode)
			} else if tc.returnSchema != nil {
				var response struct {
					Data ledger.Schema `json:"data"`
				}
				api.Decode(t, rec.Body, &response)
				require.Equal(t, tc.returnSchema.Version, response.Data.Version)
				require.Equal(t, tc.returnSchema.CreatedAt, response.Data.CreatedAt)
			}
		})
	}
}
