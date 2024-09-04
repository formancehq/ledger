package v2

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestLogsExport(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		expectStatusCode  int
		expectedErrorCode string
		returnErr         error
	}

	testCases := []testCase{
		{
			name: "nominal",
		},
		{
			name:              "undefined error",
			returnErr:         errors.New("unexpected error"),
			expectStatusCode:  http.StatusInternalServerError,
			expectedErrorCode: api.ErrorInternal,
		},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {

			if tc.expectStatusCode == 0 {
				tc.expectStatusCode = http.StatusOK
			}

			log := ledger.NewTransactionLog(ledger.NewTransaction(), map[string]metadata.Metadata{})

			systemController, ledgerController := newTestingSystemController(t, true)
			ledgerController.EXPECT().
				Export(gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, exporter ledgercontroller.ExportWriter) error {
					if tc.returnErr != nil {
						return tc.returnErr
					}
					require.NoError(t, exporter.Write(ctx, log))
					return nil
				})

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

			req := httptest.NewRequest(http.MethodPost, "/xxx/logs/export", nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectStatusCode, rec.Code)
			if tc.expectStatusCode < 300 && tc.expectStatusCode >= 200 {
				logFromExport := ledger.Log{}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&logFromExport))
				require.Equal(t, log, logFromExport)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
