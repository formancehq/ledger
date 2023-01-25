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

func TestWalletsGet(t *testing.T) {
	t.Parallel()

	w := wallet.NewWallet(uuid.NewString(), "default", metadata.Metadata{})
	balances := map[string]int64{
		"USD": 100,
	}

	req := newRequest(t, http.MethodGet, "/wallets/"+w.ID, nil)
	rec := httptest.NewRecorder()

	var testEnv *testEnv
	testEnv = newTestEnv(
		WithGetAccount(func(ctx context.Context, ledger, account string) (*sdk.AccountWithVolumesAndBalances, error) {
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.Equal(t, testEnv.Chart().GetMainBalanceAccount(w.ID), account)
			return &sdk.AccountWithVolumesAndBalances{
				Address:  account,
				Metadata: w.LedgerMetadata(),
				Balances: &balances,
			}, nil
		}),
	)
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Result().StatusCode)
	walletWithBalances := wallet.WithBalances{}
	readResponse(t, rec, &walletWithBalances)
	require.Equal(t, wallet.WithBalances{
		Wallet:   w,
		Balances: balances,
	}, walletWithBalances)
}
