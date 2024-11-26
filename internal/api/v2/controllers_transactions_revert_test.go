package v2

import (
	"github.com/formancehq/ledger/internal/api/common"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsRevert(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name             string
		queryParams      url.Values
		returnTx         ledger.Transaction
		returnErr        error
		expectForce      bool
		expectStatusCode int
		expectErrorCode  string
	}

	testCases := []testCase{
		{
			name: "nominal",
			returnTx: ledger.NewTransaction().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			),
		},
		{
			name: "force revert",
			returnTx: ledger.NewTransaction().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			),
			expectForce: true,
			queryParams: map[string][]string{"force": {"true"}},
		},
		{
			name:             "with insufficient fund",
			returnErr:        &ledgercontroller.ErrInsufficientFunds{},
			expectStatusCode: http.StatusBadRequest,
			expectErrorCode:  common.ErrInsufficientFund,
		},
		{
			name:             "with already revert",
			returnErr:        &ledgercontroller.ErrAlreadyReverted{},
			expectStatusCode: http.StatusBadRequest,
			expectErrorCode:  common.ErrAlreadyRevert,
		},
		{
			name:             "with transaction not found",
			returnErr:        ledgercontroller.ErrNotFound,
			expectStatusCode: http.StatusNotFound,
			expectErrorCode:  api.ErrorCodeNotFound,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			systemController, ledgerController := newTestingSystemController(t, true)
			ledgerController.
				EXPECT().
				RevertTransaction(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.RevertTransaction]{
					Input: ledgercontroller.RevertTransaction{
						Force: tc.expectForce,
					},
				}).
				Return(&ledger.Log{}, &ledger.RevertedTransaction{
					RevertTransaction: tc.returnTx,
				}, tc.returnErr)

			router := NewRouter(systemController, auth.NewNoAuth(), os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodPost, "/xxx/transactions/0/revert", nil)
			if tc.queryParams != nil {
				req.URL.RawQuery = tc.queryParams.Encode()
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if tc.expectStatusCode == 0 {
				require.Equal(t, http.StatusCreated, rec.Code)
				tx, ok := api.DecodeSingleResponse[ledger.Transaction](t, rec.Body)
				require.True(t, ok)
				require.Equal(t, tc.returnTx, tx)
			} else {
				require.Equal(t, tc.expectStatusCode, rec.Code)
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectErrorCode, err.ErrorCode)
			}
		})
	}
}
