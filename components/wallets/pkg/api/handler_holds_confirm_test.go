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

func TestHoldsConfirm(t *testing.T) {
	t.Parallel()

	walletID := uuid.NewString()
	hold := wallet.NewDebitHold(walletID, wallet.NewLedgerAccountSubject("bank"), "USD", "", metadata.Metadata{})

	req := newRequest(t, http.MethodPost, "/holds/"+hold.ID+"/confirm", nil)
	rec := httptest.NewRecorder()

	var testEnv *testEnv
	testEnv = newTestEnv(
		WithGetAccount(func(ctx context.Context, ledger, account string) (*sdk.AccountWithVolumesAndBalances, error) {
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.Equal(t, testEnv.Chart().GetHoldAccount(hold.ID), account)
			balances := map[string]int64{
				"USD": 100,
			}
			return &sdk.AccountWithVolumesAndBalances{
				Address:  testEnv.Chart().GetHoldAccount(hold.ID),
				Metadata: hold.LedgerMetadata(testEnv.Chart()),
				Balances: &balances,
			}, nil
		}),
		WithRunScript(func(ctx context.Context, name string, script sdk.Script) (*sdk.ScriptResponse, error) {
			require.EqualValues(t, sdk.Script{
				Plain: wallet.BuildConfirmHoldScript(false, "USD"),
				Vars: map[string]interface{}{
					"hold": testEnv.Chart().GetHoldAccount(hold.ID),
					"amount": map[string]any{
						"amount": uint64(100),
						"asset":  "USD",
					},
				},
				Metadata: wallet.TransactionMetadata(nil),
			}, script)
			return &sdk.ScriptResponse{}, nil
		}),
	)
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Result().StatusCode)
}

func TestHoldsPartialConfirm(t *testing.T) {
	t.Parallel()

	walletID := uuid.NewString()
	hold := wallet.NewDebitHold(walletID, wallet.NewLedgerAccountSubject("bank"), "USD", "", metadata.Metadata{})

	req := newRequest(t, http.MethodPost, "/holds/"+hold.ID+"/confirm", ConfirmHoldRequest{
		Amount: 50,
	})
	rec := httptest.NewRecorder()

	var testEnv *testEnv
	testEnv = newTestEnv(
		WithGetAccount(func(ctx context.Context, ledger, account string) (*sdk.AccountWithVolumesAndBalances, error) {
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.Equal(t, testEnv.Chart().GetHoldAccount(hold.ID), account)
			balances := map[string]int64{
				"USD": 100,
			}
			volumes := map[string]map[string]int64{
				"USD": {
					"input": 100,
				},
			}
			return &sdk.AccountWithVolumesAndBalances{
				Address:  testEnv.Chart().GetHoldAccount(hold.ID),
				Metadata: hold.LedgerMetadata(testEnv.Chart()),
				Balances: &balances,
				Volumes:  &volumes,
			}, nil
		}),
		WithRunScript(func(ctx context.Context, name string, script sdk.Script) (*sdk.ScriptResponse, error) {
			require.EqualValues(t, sdk.Script{
				Plain: wallet.BuildConfirmHoldScript(false, "USD"),
				Vars: map[string]interface{}{
					"hold": testEnv.Chart().GetHoldAccount(hold.ID),
					"amount": map[string]any{
						"amount": uint64(50),
						"asset":  "USD",
					},
				},
				Metadata: wallet.TransactionMetadata(nil),
			}, script)
			return &sdk.ScriptResponse{}, nil
		}),
	)
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Result().StatusCode)
}

func TestHoldsConfirmWithTooHighAmount(t *testing.T) {
	t.Parallel()

	walletID := uuid.NewString()
	hold := wallet.NewDebitHold(walletID, wallet.NewLedgerAccountSubject("bank"), "USD", "", metadata.Metadata{})

	req := newRequest(t, http.MethodPost, "/holds/"+hold.ID+"/confirm", ConfirmHoldRequest{
		Amount: 500,
	})
	rec := httptest.NewRecorder()

	var testEnv *testEnv
	testEnv = newTestEnv(
		WithGetAccount(func(ctx context.Context, ledger, account string) (*sdk.AccountWithVolumesAndBalances, error) {
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.Equal(t, testEnv.Chart().GetHoldAccount(hold.ID), account)
			balances := map[string]int64{
				"USD": 100,
			}
			volumes := map[string]map[string]int64{
				"USD": {
					"input": 100,
				},
			}
			return &sdk.AccountWithVolumesAndBalances{
				Address:  testEnv.Chart().GetHoldAccount(hold.ID),
				Metadata: hold.LedgerMetadata(testEnv.Chart()),
				Balances: &balances,
				Volumes:  &volumes,
			}, nil
		}),
	)
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Result().StatusCode)
	errorResponse := readErrorResponse(t, rec)
	require.Equal(t, ErrorCodeInsufficientFund, errorResponse.ErrorCode)
}

func TestHoldsConfirmWithClosedHold(t *testing.T) {
	t.Parallel()

	walletID := uuid.NewString()
	hold := wallet.NewDebitHold(walletID, wallet.NewLedgerAccountSubject("bank"), "USD", "", metadata.Metadata{})

	req := newRequest(t, http.MethodPost, "/holds/"+hold.ID+"/confirm", ConfirmHoldRequest{})
	rec := httptest.NewRecorder()

	var testEnv *testEnv
	testEnv = newTestEnv(
		WithGetAccount(func(ctx context.Context, ledger, account string) (*sdk.AccountWithVolumesAndBalances, error) {
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.Equal(t, testEnv.Chart().GetHoldAccount(hold.ID), account)
			balances := map[string]int64{
				"USD": 0,
			}
			volumes := map[string]map[string]int64{
				"USD": {
					"input": 100,
				},
			}
			return &sdk.AccountWithVolumesAndBalances{
				Address:  testEnv.Chart().GetHoldAccount(hold.ID),
				Metadata: hold.LedgerMetadata(testEnv.Chart()),
				Balances: &balances,
				Volumes:  &volumes,
			}, nil
		}),
	)
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Result().StatusCode)
	errorResponse := readErrorResponse(t, rec)
	require.Equal(t, ErrorCodeClosedHold, errorResponse.ErrorCode)
}

func TestHoldsPartialConfirmWithFinal(t *testing.T) {
	t.Parallel()

	walletID := uuid.NewString()
	hold := wallet.NewDebitHold(walletID, wallet.NewLedgerAccountSubject("bank"),
		"USD", "", metadata.Metadata{})

	req := newRequest(t, http.MethodPost, "/holds/"+hold.ID+"/confirm", ConfirmHoldRequest{
		Amount: 50,
		Final:  true,
	})
	rec := httptest.NewRecorder()

	var testEnv *testEnv
	testEnv = newTestEnv(
		WithGetAccount(func(ctx context.Context, ledger, account string) (*sdk.AccountWithVolumesAndBalances, error) {
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.Equal(t, testEnv.Chart().GetHoldAccount(hold.ID), account)
			balances := map[string]int64{
				"USD": 100,
			}
			volumes := map[string]map[string]int64{
				"USD": {
					"input": 100,
				},
			}
			return &sdk.AccountWithVolumesAndBalances{
				Address:  testEnv.Chart().GetHoldAccount(hold.ID),
				Metadata: hold.LedgerMetadata(testEnv.Chart()),
				Balances: &balances,
				Volumes:  &volumes,
			}, nil
		}),
		WithRunScript(func(ctx context.Context, name string, script sdk.Script) (*sdk.ScriptResponse, error) {
			require.EqualValues(t, sdk.Script{
				Plain: wallet.BuildConfirmHoldScript(true, "USD"),
				Vars: map[string]interface{}{
					"hold": testEnv.Chart().GetHoldAccount(hold.ID),
					"amount": map[string]any{
						"amount": uint64(50),
						"asset":  "USD",
					},
				},
				Metadata: wallet.TransactionMetadata(nil),
			}, script)
			return &sdk.ScriptResponse{}, nil
		}),
	)
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Result().StatusCode)
}
