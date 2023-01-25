package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/metadata"
	wallet "github.com/formancehq/wallets/pkg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestWalletsCreate(t *testing.T) {
	t.Parallel()

	createWalletRequest := wallet.CreateRequest{
		PatchRequest: wallet.PatchRequest{
			Metadata: map[string]interface{}{
				"foo": "bar",
			},
		},
		Name: uuid.NewString(),
	}

	req := newRequest(t, http.MethodPost, "/wallets", createWalletRequest)
	rec := httptest.NewRecorder()

	var (
		ledger  string
		account string
		md      metadata.Metadata
	)
	testEnv := newTestEnv(
		WithAddMetadataToAccount(func(ctx context.Context, l, a string, m metadata.Metadata) error {
			ledger = l
			account = a
			md = m
			return nil
		}),
	)
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Result().StatusCode)
	wallet := &wallet.Wallet{}
	readResponse(t, rec, wallet)
	require.Equal(t, testEnv.LedgerName(), ledger)
	require.Equal(t, account, testEnv.Chart().GetMainBalanceAccount(wallet.ID))
	require.Equal(t, wallet.LedgerMetadata(), md)
	require.Equal(t, wallet.Metadata, createWalletRequest.Metadata)
	require.Equal(t, wallet.Name, createWalletRequest.Name)
}
