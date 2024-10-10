package v2

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/ledger/internal/controller/system"

	"errors"
	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/logging"
	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/go-libs/auth"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestLedgersCreate(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		configuration       string
		name                string
		expectedBackendCall bool
		returnErr           error
		expectStatusCode    int
		expectErrorCode     string
	}

	testCases := []testCase{
		{
			name:                "nominal",
			expectedBackendCall: true,
		},
		{
			name:                "with alternative bucket",
			configuration:       `{"bucket": "bucket0"}`,
			expectedBackendCall: true,
		},
		{
			name:                "with metadata",
			configuration:       `{"metadata": {"foo": "bar"}}`,
			expectedBackendCall: true,
		},
		{
			name:                "ledger already exists",
			expectedBackendCall: true,
			returnErr:           system.ErrLedgerAlreadyExists,
			expectStatusCode:    http.StatusBadRequest,
			expectErrorCode:     ErrValidation,
		},
		{
			name:                "invalid ledger name",
			expectedBackendCall: true,
			returnErr:           ledger.ErrInvalidLedgerName{},
			expectStatusCode:    http.StatusBadRequest,
			expectErrorCode:     ErrValidation,
		},
		{
			name:                "invalid bucket name",
			expectedBackendCall: true,
			returnErr:           ledger.ErrInvalidBucketName{},
			expectStatusCode:    http.StatusBadRequest,
			expectErrorCode:     ErrValidation,
		},
		{
			name:                "unexpected error",
			expectedBackendCall: true,
			returnErr:           errors.New("unexpected error"),
			expectStatusCode:    http.StatusInternalServerError,
			expectErrorCode:     api.ErrorInternal,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			systemController, _ := newTestingSystemController(t, false)
			router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

			name := uuid.NewString()

			if tc.expectedBackendCall {
				configuration := ledger.Configuration{}
				if tc.configuration != "" {
					require.NoError(t, json.Unmarshal([]byte(tc.configuration), &configuration))
				}
				systemController.
					EXPECT().
					CreateLedger(gomock.Any(), name, configuration).
					Return(tc.returnErr)
			}

			buf := bytes.NewBuffer(nil)
			if tc.configuration != "" {
				buf.Write([]byte(tc.configuration))
			}

			req := httptest.NewRequest(http.MethodPost, "/"+name, buf)
			req = req.WithContext(ctx)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if tc.expectStatusCode == 0 || tc.expectStatusCode == http.StatusNoContent {
				require.Equal(t, http.StatusNoContent, rec.Code)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectErrorCode, err.ErrorCode)
			}
		})
	}
}
