package v2

import (
	"bytes"
	"encoding/json"
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/ledger/internal/controller/system"

	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/go-libs/v3/auth"
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
			expectErrorCode:     common.ErrLedgerAlreadyExists,
		},
		{
			name:                "invalid ledger name",
			expectedBackendCall: true,
			returnErr:           ledger.ErrInvalidLedgerName{},
			expectStatusCode:    http.StatusBadRequest,
			expectErrorCode:     common.ErrValidation,
		},
		{
			name:                "invalid bucket name",
			expectedBackendCall: true,
			returnErr:           ledger.ErrInvalidBucketName{},
			expectStatusCode:    http.StatusBadRequest,
			expectErrorCode:     common.ErrValidation,
		},
		{
			name:                "invalid ledger configuration",
			expectedBackendCall: true,
			returnErr:           system.ErrInvalidLedgerConfiguration{},
			expectStatusCode:    http.StatusBadRequest,
			expectErrorCode:     common.ErrValidation,
		},
		{
			name:                "bucket actually outdated",
			expectedBackendCall: true,
			returnErr:           system.ErrBucketOutdated,
			expectStatusCode:    http.StatusBadRequest,
			expectErrorCode:     common.ErrOutdatedSchema,
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
			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

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
