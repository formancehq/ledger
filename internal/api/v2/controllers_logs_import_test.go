package v2

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestLogsImport(t *testing.T) {
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
				tc.expectStatusCode = http.StatusNoContent
			}

			log := ledger.NewTransactionLog(ledger.CreatedTransaction{
				Transaction:     ledger.NewTransaction(),
				AccountMetadata: ledger.AccountMetadata{},
			})

			systemController, ledgerController := newTestingSystemController(t, true)
			ledgerController.EXPECT().
				Import(gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, stream chan ledger.Log) error {
					if tc.returnErr != nil {
						return tc.returnErr
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case logFromStream := <-stream:
						require.Equal(t, log, logFromStream)
						select {
						case <-time.After(time.Second):
							require.Fail(t, "stream should have been closed")
						case <-stream:
						}
						return nil
					}
				})

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

			buf := bytes.NewBuffer(nil)
			require.NoError(t, json.NewEncoder(buf).Encode(log))

			req := httptest.NewRequest(http.MethodPost, "/xxx/logs/import", buf)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectStatusCode, rec.Code)
			if tc.expectStatusCode > 300 {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
