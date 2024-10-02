package common

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"errors"
	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/logging"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestResolverMiddleware(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name                   string
		getLedgerControllerErr error
		isDatabaseUpToDateErr  error
		isDatabaseUpToDate     bool
		expectStatusCode       int
		expectErrorCode        string
		ledger                 string
	}

	for _, tc := range []testCase{
		{
			name:               "nominal",
			isDatabaseUpToDate: true,
			ledger:             "foo",
		},
		{
			name:             "empty name",
			ledger:           "",
			expectStatusCode: http.StatusNotFound,
			expectErrorCode:  api.ErrorCodeNotFound,
		},
		{
			name:                   "not found",
			ledger:                 "foo",
			getLedgerControllerErr: ledgercontroller.ErrNotFound,
			expectStatusCode:       http.StatusNotFound,
			expectErrorCode:        "LEDGER_NOT_FOUND",
		},
		{
			name:                   "error on retrieving ledger controller",
			ledger:                 "foo",
			getLedgerControllerErr: errors.New("unexpected error"),
			expectStatusCode:       http.StatusInternalServerError,
			expectErrorCode:        api.ErrorInternal,
		},
		{
			name:                  "error on checking database schema status",
			ledger:                "foo",
			isDatabaseUpToDateErr: errors.New("unexpected error"),
			expectStatusCode:      http.StatusInternalServerError,
			expectErrorCode:       api.ErrorInternal,
		},
		{
			name:             "database not up to date",
			ledger:           "foo",
			expectStatusCode: http.StatusBadRequest,
			expectErrorCode:  ErrOutdatedSchema,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			ctx := logging.TestingContext()
			systemController := NewSystemController(ctrl)
			ledgerController := NewLedgerController(ctrl)

			ledger := tc.ledger

			systemController.EXPECT().
				GetLedgerController(gomock.Any(), ledger).
				AnyTimes().
				Return(ledgerController, tc.getLedgerControllerErr)

			ledgerController.EXPECT().
				IsDatabaseUpToDate(gomock.Any()).
				AnyTimes().
				Return(tc.isDatabaseUpToDate, tc.isDatabaseUpToDateErr)

			m := LedgerMiddleware(systemController, func(*http.Request) string {
				return ledger
			})
			h := m(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))

			req := httptest.NewRequest(http.MethodGet, "/"+ledger+"/_info", nil)
			req = req.WithContext(ctx)
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			if tc.expectStatusCode == 0 {
				require.Equal(t, http.StatusNoContent, rec.Code)
			} else {
				require.Equal(t, tc.expectStatusCode, rec.Code)
				errorResponse := api.ErrorResponse{}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errorResponse))
				require.Equal(t, tc.expectErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}
