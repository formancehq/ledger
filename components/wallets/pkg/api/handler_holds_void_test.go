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

func TestHoldsVoid(t *testing.T) {
	t.Parallel()

	walletID := uuid.NewString()
	hold := wallet.NewDebitHold(walletID, wallet.NewLedgerAccountSubject("bank"), "USD", "", metadata.Metadata{})

	req := newRequest(t, http.MethodPost, "/holds/"+hold.ID+"/void", nil)
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
			require.Equal(t, sdk.Script{
				Plain: wallet.BuildCancelHoldScript("USD"),
				Vars: map[string]interface{}{
					"hold": testEnv.Chart().GetHoldAccount(hold.ID),
				},
				Metadata: wallet.TransactionMetadata(nil),
			}, script)
			return &sdk.ScriptResponse{}, nil
		}),
	)
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Result().StatusCode)
}
