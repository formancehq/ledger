package v2

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestLedgersList(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name               string
		queryParams        url.Values
		returnData         []ledger.Ledger
		returnErr          error
		expectedStatusCode int
		expectedErrorCode  string
		expectBackendCall  bool
	}

	for _, tc := range []testCase{
		{
			name: "nominal",
			returnData: []ledger.Ledger{
				ledger.MustNewWithDefault(uuid.NewString()),
				ledger.MustNewWithDefault(uuid.NewString()),
			},
			expectBackendCall: true,
		},
		{
			name: "invalid page size",
			queryParams: url.Values{
				"pageSize": {"-1"},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
			expectBackendCall:  false,
		},
		{
			name:               "error from backend",
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
			returnErr:          storagecommon.ErrInvalidQuery{},
		},
		{
			name:               "with missing feature",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
			expectBackendCall:  true,
			returnErr:          ledgerstore.ErrMissingFeature{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			systemController, _ := newTestingSystemController(t, false)

			if tc.expectBackendCall {
				systemController.EXPECT().
					ListLedgers(gomock.Any(), storagecommon.InitialPaginatedQuery[systemstore.ListLedgersQueryPayload]{
						PageSize: 15,
						Column:   "id",
						Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
						Options: storagecommon.ResourceQuery[systemstore.ListLedgersQueryPayload]{
							Expand: []string{},
						},
					}).
					Return(&bunpaginate.Cursor[ledger.Ledger]{
						Data: tc.returnData,
					}, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req = req.WithContext(ctx)
			req.URL.RawQuery = tc.queryParams.Encode()

			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if tc.expectedStatusCode == 0 || tc.expectedStatusCode == http.StatusOK {
				require.Equal(t, http.StatusOK, rec.Code)
				cursor := api.DecodeCursorResponse[ledger.Ledger](t, rec.Body)
				for i, data := range cursor.Data {
					data.State = ledger.StateInitializing
					cursor.Data[i] = data
				}

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
