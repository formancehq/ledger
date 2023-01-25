package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdk "github.com/formancehq/formance-sdk-go"
	"github.com/formancehq/go-libs/metadata"
	wallet "github.com/formancehq/wallets/pkg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestWalletsPatch(t *testing.T) {
	t.Parallel()

	patchWalletRequest := wallet.PatchRequest{
		Metadata: map[string]interface{}{
			"role": "admin",
			"foo":  "baz",
		},
	}
	w := wallet.NewWallet(uuid.NewString(), "default", metadata.Metadata{
		"foo": "bar",
	})

	req := newRequest(t, http.MethodPatch, "/wallets/"+w.ID, patchWalletRequest)
	rec := httptest.NewRecorder()

	var testEnv *testEnv
	testEnv = newTestEnv(
		WithGetAccount(func(ctx context.Context, ledger, account string) (*sdk.AccountWithVolumesAndBalances, error) {
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.Equal(t, testEnv.Chart().GetMainBalanceAccount(w.ID), account)
			return &sdk.AccountWithVolumesAndBalances{
				Address:  account,
				Metadata: w.LedgerMetadata(),
			}, nil
		}),
		WithAddMetadataToAccount(func(ctx context.Context, ledger, account string, md metadata.Metadata) error {
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.Equal(t, testEnv.Chart().GetMainBalanceAccount(w.ID), account)
			require.EqualValues(t, metadata.Metadata{
				wallet.MetadataKeyWalletID:       w.ID,
				wallet.MetadataKeyWalletName:     w.Name,
				wallet.MetadataKeyWalletSpecType: wallet.PrimaryWallet,
				wallet.MetadataKeyWalletCustomData: metadata.Metadata{
					"role": "admin",
					"foo":  "baz",
				},
				wallet.MetadataKeyBalanceName:   wallet.MainBalance,
				wallet.MetadataKeyWalletBalance: wallet.TrueValue,
				wallet.MetadataKeyCreatedAt:     w.CreatedAt.UTC().Format(time.RFC3339Nano),
			}, md)
			return nil
		}),
	)
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Result().StatusCode)
}
