package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	sdk "github.com/formancehq/formance-sdk-go"
	sharedapi "github.com/formancehq/go-libs/api"
	wallet "github.com/formancehq/wallets/pkg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBalancesList(t *testing.T) {
	t.Parallel()

	walletID := uuid.NewString()
	var balances []wallet.Balance
	for i := 0; i < 10; i++ {
		balances = append(balances, wallet.NewBalance(uuid.NewString()))
	}
	const pageSize = 2
	numberOfPages := int64(len(balances) / pageSize)

	var testEnv *testEnv
	testEnv = newTestEnv(
		WithListAccounts(func(ctx context.Context, ledger string, query wallet.ListAccountsQuery) (*sdk.AccountsCursorResponseCursor, error) {
			if query.Cursor != "" {
				page, err := strconv.ParseInt(query.Cursor, 10, 64)
				if err != nil {
					panic(err)
				}

				if page >= numberOfPages-1 {
					return &sdk.AccountsCursorResponseCursor{}, nil
				}
				hasMore := page < numberOfPages-1
				previous := fmt.Sprint(page - 1)
				next := fmt.Sprint(page + 1)
				accounts := make([]sdk.Account, 0)
				for _, balance := range balances[page*pageSize : (page+1)*pageSize] {
					accounts = append(accounts, sdk.Account{
						Address:  testEnv.Chart().GetBalanceAccount(walletID, balance.Name),
						Metadata: balance.LedgerMetadata(walletID),
					})
				}
				return &sdk.AccountsCursorResponseCursor{
					PageSize: pageSize,
					HasMore:  hasMore,
					Previous: &previous,
					Next:     &next,
					Data:     accounts,
				}, nil
			}

			require.Equal(t, pageSize, query.Limit)
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.Equal(t, map[string]any{
				wallet.MetadataKeyWalletBalance: wallet.TrueValue,
				wallet.MetadataKeyWalletID:      walletID,
			}, query.Metadata)

			hasMore := true
			next := "1"
			accounts := make([]sdk.Account, 0)
			for _, balance := range balances[:pageSize] {
				accounts = append(accounts, sdk.Account{
					Address:  testEnv.Chart().GetBalanceAccount(walletID, balance.Name),
					Metadata: balance.LedgerMetadata(walletID),
				})
			}
			return &sdk.AccountsCursorResponseCursor{
				PageSize: pageSize,
				HasMore:  hasMore,
				Next:     &next,
				Data:     accounts,
			}, nil
		}),
	)

	req := newRequest(t, http.MethodGet, fmt.Sprintf("/wallets/%s/balances?pageSize=%d", walletID, pageSize), nil)
	rec := httptest.NewRecorder()
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Result().StatusCode)
	cursor := &sharedapi.Cursor[wallet.Balance]{}
	readCursor(t, rec, cursor)
	require.Len(t, cursor.Data, pageSize)
	require.EqualValues(t, cursor.Data, balances[:pageSize])

	req = newRequest(t, http.MethodGet, fmt.Sprintf("/wallets/%s/balances?cursor=%s", walletID, cursor.Next), nil)
	rec = httptest.NewRecorder()
	testEnv.Router().ServeHTTP(rec, req)
	cursor = &sharedapi.Cursor[wallet.Balance]{}
	readCursor(t, rec, cursor)
	require.Len(t, cursor.Data, pageSize)
	require.EqualValues(t, cursor.Data, balances[pageSize:pageSize*2])
}
