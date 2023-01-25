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

type testCase struct {
	name               string
	request            wallet.DebitRequest
	scriptResult       sdk.ScriptResponse
	expectedScript     func(testEnv *testEnv, walletID string, h *wallet.DebitHold) sdk.Script
	expectedStatusCode int
	expectedErrorCode  string
}

var walletDebitTestCases = []testCase{
	{
		name: "nominal",
		request: wallet.DebitRequest{
			Amount: wallet.NewMonetary(wallet.NewMonetaryInt(100), "USD"),
		},
		expectedScript: func(testEnv *testEnv, walletID string, h *wallet.DebitHold) sdk.Script {
			return sdk.Script{
				Plain: wallet.BuildDebitWalletScript(testEnv.Chart().GetMainBalanceAccount(walletID)),
				Vars: map[string]interface{}{
					"destination": wallet.DefaultDebitDest.Identifier,
					"amount": map[string]any{
						"amount": uint64(100),
						"asset":  "USD",
					},
				},
				Metadata: wallet.TransactionMetadata(nil),
			}
		},
	},
	{
		name: "with custom destination as ledger account",
		request: wallet.DebitRequest{
			Amount:      wallet.NewMonetary(wallet.NewMonetaryInt(100), "USD"),
			Destination: wallet.Ptr(wallet.NewLedgerAccountSubject("account1")),
		},
		expectedScript: func(testEnv *testEnv, walletID string, h *wallet.DebitHold) sdk.Script {
			return sdk.Script{
				Plain: wallet.BuildDebitWalletScript(testEnv.Chart().GetMainBalanceAccount(walletID)),
				Vars: map[string]interface{}{
					"destination": "account1",
					"amount": map[string]any{
						"amount": uint64(100),
						"asset":  "USD",
					},
				},
				Metadata: wallet.TransactionMetadata(nil),
			}
		},
	},
	{
		name: "with custom destination as wallet",
		request: wallet.DebitRequest{
			Amount:      wallet.NewMonetary(wallet.NewMonetaryInt(100), "USD"),
			Destination: wallet.Ptr(wallet.NewWalletSubject("wallet1", "")),
		},
		expectedScript: func(testEnv *testEnv, walletID string, h *wallet.DebitHold) sdk.Script {
			return sdk.Script{
				Plain: wallet.BuildDebitWalletScript(testEnv.Chart().GetMainBalanceAccount(walletID)),
				Vars: map[string]interface{}{
					"destination": testEnv.Chart().GetMainBalanceAccount("wallet1"),
					"amount": map[string]any{
						"amount": uint64(100),
						"asset":  "USD",
					},
				},
				Metadata: wallet.TransactionMetadata(nil),
			}
		},
	},
	{
		name: "with insufficient funds",
		request: wallet.DebitRequest{
			Amount: wallet.NewMonetary(wallet.NewMonetaryInt(100), "USD"),
		},
		scriptResult: sdk.ScriptResponse{
			ErrorCode: func() *sdk.ErrorsEnum {
				ret := sdk.INSUFFICIENT_FUND
				return &ret
			}(),
		},
		expectedStatusCode: http.StatusBadRequest,
		expectedErrorCode:  string(sdk.INSUFFICIENT_FUND),
	},
	{
		name: "with debit hold",
		request: wallet.DebitRequest{
			Amount:  wallet.NewMonetary(wallet.NewMonetaryInt(100), "USD"),
			Pending: true,
			Metadata: map[string]any{
				"foo": "bar",
			},
			Description: "a first tx",
		},
		expectedScript: func(testEnv *testEnv, walletID string, h *wallet.DebitHold) sdk.Script {
			return sdk.Script{
				Plain: wallet.BuildDebitWalletScript(testEnv.Chart().GetMainBalanceAccount(walletID)),
				Vars: map[string]interface{}{
					"destination": testEnv.Chart().GetHoldAccount(h.ID),
					"amount": map[string]any{
						"amount": uint64(100),
						"asset":  "USD",
					},
				},
				Metadata: wallet.TransactionMetadata(metadata.Metadata{
					"foo": "bar",
				}),
			}
		},
		expectedStatusCode: http.StatusCreated,
	},
	{
		name: "with custom balance as source",
		request: wallet.DebitRequest{
			Amount:   wallet.NewMonetary(wallet.NewMonetaryInt(100), "USD"),
			Balances: []string{"secondary"},
		},
		expectedScript: func(testEnv *testEnv, walletID string, h *wallet.DebitHold) sdk.Script {
			return sdk.Script{
				Plain: wallet.BuildDebitWalletScript(testEnv.Chart().GetBalanceAccount(walletID, "secondary")),
				Vars: map[string]interface{}{
					"destination": "world",
					"amount": map[string]any{
						"amount": uint64(100),
						"asset":  "USD",
					},
				},
				Metadata: wallet.TransactionMetadata(nil),
			}
		},
	},
	{
		name: "with wildcard balance as source",
		request: wallet.DebitRequest{
			Amount:   wallet.NewMonetary(wallet.NewMonetaryInt(100), "USD"),
			Balances: []string{"*"},
		},
		expectedScript: func(testEnv *testEnv, walletID string, h *wallet.DebitHold) sdk.Script {
			return sdk.Script{
				Plain: wallet.BuildDebitWalletScript(testEnv.Chart().GetBalanceAccount(walletID, "secondary")),
				Vars: map[string]interface{}{
					"destination": "world",
					"amount": map[string]any{
						"amount": uint64(100),
						"asset":  "USD",
					},
				},
				Metadata: wallet.TransactionMetadata(nil),
			}
		},
	},
	{
		name: "with wildcard plus another source",
		request: wallet.DebitRequest{
			Amount:   wallet.NewMonetary(wallet.NewMonetaryInt(100), "USD"),
			Balances: []string{"*", "secondary"},
		},
		expectedScript: func(testEnv *testEnv, walletID string, h *wallet.DebitHold) sdk.Script {
			return sdk.Script{
				Plain: wallet.BuildDebitWalletScript(testEnv.Chart().GetBalanceAccount(walletID, "secondary")),
				Vars: map[string]interface{}{
					"destination": "world",
					"amount": map[string]any{
						"amount": uint64(100),
						"asset":  "USD",
					},
				},
				Metadata: wallet.TransactionMetadata(nil),
			}
		},
		expectedStatusCode: http.StatusBadRequest,
		expectedErrorCode:  string(sdk.VALIDATION),
	},
	{
		name: "with custom balance as destination",
		request: wallet.DebitRequest{
			Amount:      wallet.NewMonetary(wallet.NewMonetaryInt(100), "USD"),
			Destination: wallet.Ptr(wallet.NewWalletSubject("wallet1", "secondary")),
		},
		expectedScript: func(testEnv *testEnv, walletID string, h *wallet.DebitHold) sdk.Script {
			return sdk.Script{
				Plain: wallet.BuildDebitWalletScript(testEnv.Chart().GetMainBalanceAccount(walletID)),
				Vars: map[string]interface{}{
					"destination": testEnv.Chart().GetBalanceAccount("wallet1", "secondary"),
					"amount": map[string]any{
						"amount": uint64(100),
						"asset":  "USD",
					},
				},
				Metadata: wallet.TransactionMetadata(nil),
			}
		},
	},
}

func TestWalletsDebit(t *testing.T) {
	t.Parallel()
	for _, testCase := range walletDebitTestCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			walletID := uuid.NewString()

			req := newRequest(t, http.MethodPost, "/wallets/"+walletID+"/debit", testCase.request)
			rec := httptest.NewRecorder()

			var (
				testEnv             *testEnv
				executedScript      sdk.Script
				holdAccount         string
				holdAccountMetadata metadata.Metadata
			)
			testEnv = newTestEnv(
				WithAddMetadataToAccount(func(ctx context.Context, ledger, account string, m metadata.Metadata) error {
					require.Equal(t, testEnv.LedgerName(), ledger)
					holdAccount = account
					holdAccountMetadata = m
					return nil
				}),
				WithListAccounts(func(ctx context.Context, ledger string, query wallet.ListAccountsQuery) (*sdk.AccountsCursorResponseCursor, error) {
					require.Equal(t, testEnv.LedgerName(), ledger)
					require.Equal(t, query.Metadata, wallet.BalancesMetadataFilter(walletID))
					return &sdk.AccountsCursorResponseCursor{
						Data: []sdk.Account{{
							Address: testEnv.Chart().GetBalanceAccount(walletID, "secondary"),
							Type:    nil,
							Metadata: wallet.Balance{
								Name: "secondary",
							}.LedgerMetadata(walletID),
						}},
					}, nil
				}),
				WithRunScript(func(ctx context.Context, ledger string, script sdk.Script) (*sdk.ScriptResponse, error) {
					require.Equal(t, testEnv.LedgerName(), ledger)
					executedScript = script
					return &testCase.scriptResult, nil
				}),
			)
			testEnv.Router().ServeHTTP(rec, req)

			expectedStatusCode := testCase.expectedStatusCode
			if expectedStatusCode == 0 {
				expectedStatusCode = http.StatusNoContent
			}
			require.Equal(t, expectedStatusCode, rec.Result().StatusCode)

			hold := &wallet.DebitHold{}
			switch expectedStatusCode {
			case http.StatusCreated:
				readResponse(t, rec, hold)
			case http.StatusNoContent:
			default:
				errorResponse := readErrorResponse(t, rec)
				require.Equal(t, testCase.expectedErrorCode, errorResponse.ErrorCode)
				return
			}

			if testCase.expectedScript != nil {
				expectedScript := testCase.expectedScript(testEnv, walletID, hold)
				require.Equal(t, expectedScript, executedScript)
			}

			if testCase.request.Pending {
				require.Equal(t, testEnv.Chart().GetHoldAccount(hold.ID), holdAccount)
				require.Equal(t, walletID, hold.WalletID)
				require.Equal(t, testCase.request.Amount.Asset, hold.Asset)
				require.Equal(t, hold.LedgerMetadata(testEnv.Chart()), holdAccountMetadata)
			}
		})
	}
}
