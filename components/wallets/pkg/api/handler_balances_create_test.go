package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	sdk "github.com/formancehq/formance-sdk-go"
	"github.com/formancehq/go-libs/metadata"
	wallet "github.com/formancehq/wallets/pkg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type balanceCreateTestCase struct {
	name               string
	request            wallet.CreateBalance
	expectedStatusCode int
	expectedErrorCode  string
}

var balanceCreateTestCases = []balanceCreateTestCase{
	{
		name: "nominal",
		request: wallet.CreateBalance{
			Name: uuid.NewString(),
		},
	},
	{
		name: "with invalid name",
		request: wallet.CreateBalance{
			Name: "!!!!!!!",
		},
		expectedStatusCode: http.StatusBadRequest,
		expectedErrorCode:  ErrorCodeValidation,
	},
	{
		name: "with reserved name",
		request: wallet.CreateBalance{
			Name: wallet.MainBalance,
		},
		expectedStatusCode: http.StatusBadRequest,
		expectedErrorCode:  ErrorCodeValidation,
	},
}

func TestBalancesCreate(t *testing.T) {
	t.Parallel()

	for _, testCase := range balanceCreateTestCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			walletID := uuid.NewString()
			req := newRequest(t, http.MethodPost, "/wallets/"+walletID+"/balances", testCase.request)
			rec := httptest.NewRecorder()

			var (
				targetedLedger  string
				targetedAccount string
				appliedMetadata metadata.Metadata
			)
			testEnv := newTestEnv(
				WithAddMetadataToAccount(func(ctx context.Context, ledger, account string, metadata metadata.Metadata) error {
					targetedLedger = ledger
					targetedAccount = account
					appliedMetadata = metadata
					return nil
				}),
				WithGetAccount(func(ctx context.Context, ledger, account string) (*sdk.AccountWithVolumesAndBalances, error) {
					return &sdk.AccountWithVolumesAndBalances{}, nil
				}),
			)
			testEnv.Router().ServeHTTP(rec, req)

			expectedStatusCode := testCase.expectedStatusCode
			if expectedStatusCode == 0 {
				expectedStatusCode = http.StatusCreated
			}
			require.Equal(t, expectedStatusCode, rec.Result().StatusCode)

			if expectedStatusCode == http.StatusCreated {
				balance := &wallet.Balance{}
				readResponse(t, rec, balance)
				require.Equal(t, testEnv.LedgerName(), targetedLedger)
				require.Equal(t, targetedAccount, testEnv.Chart().GetBalanceAccount(walletID, balance.Name))
				require.Equal(t, balance.LedgerMetadata(walletID), appliedMetadata)
				require.Equal(t, balance.Name, testCase.request.Name)
			} else {
				errorResponse := readErrorResponse(t, rec)
				require.Equal(t, testCase.expectedErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}
