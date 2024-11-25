package v2

import (
	"encoding/json"
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/logging"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestListLedgers(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name               string
		expectQuery        ledgercontroller.ListLedgersQuery
		queryParams        url.Values
		returnData         []ledger.Ledger
		returnErr          error
		expectedStatusCode int
		expectedErrorCode  string
		expectBackendCall  bool
	}

	for _, tc := range []testCase{
		{
			name:        "nominal",
			expectQuery: ledgercontroller.NewListLedgersQuery(15),
			returnData: []ledger.Ledger{
				ledger.MustNewWithDefault(uuid.NewString()),
				ledger.MustNewWithDefault(uuid.NewString()),
			},
			expectBackendCall: true,
		},
		{
			name:        "invalid page size",
			expectQuery: ledgercontroller.NewListLedgersQuery(15),
			queryParams: url.Values{
				"pageSize": {"-1"},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
			expectBackendCall:  false,
		},
		{
			name:               "error from backend",
			expectQuery:        ledgercontroller.NewListLedgersQuery(15),
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorCode:  api.ErrorInternal,
			expectBackendCall:  true,
			returnErr:          errors.New("undefined error"),
		},
		{
			name:               "with invalid query from core point of view",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
			expectBackendCall:  true,
			returnErr:          ledgercontroller.ErrInvalidQuery{},
			expectQuery:        ledgercontroller.NewListLedgersQuery(DefaultPageSize),
		},
		{
			name:               "with missing feature",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
			expectBackendCall:  true,
			returnErr:          ledgercontroller.ErrMissingFeature{},
			expectQuery:        ledgercontroller.NewListLedgersQuery(DefaultPageSize),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			systemController, _ := newTestingSystemController(t, false)

			if tc.expectBackendCall {
				systemController.EXPECT().
					ListLedgers(gomock.Any(), ledgercontroller.NewListLedgersQuery(15)).
					Return(&bunpaginate.Cursor[ledger.Ledger]{
						Data: tc.returnData,
					}, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req = req.WithContext(ctx)
			req.URL.RawQuery = tc.queryParams.Encode()

			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if tc.expectedStatusCode == 0 || tc.expectedStatusCode == http.StatusOK {
				require.Equal(t, http.StatusOK, rec.Code)
				cursor := api.DecodeCursorResponse[ledger.Ledger](t, rec.Body)

				require.Equal(t, tc.returnData, cursor.Data)
			} else {
				require.Equal(t, tc.expectedStatusCode, rec.Code)
				errorResponse := api.ErrorResponse{}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errorResponse))
				require.Equal(t, tc.expectedErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}
