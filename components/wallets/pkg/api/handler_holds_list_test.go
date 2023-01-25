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
	"github.com/formancehq/go-libs/metadata"
	wallet "github.com/formancehq/wallets/pkg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestHoldsList(t *testing.T) {
	t.Parallel()

	walletID := uuid.NewString()

	holds := make([]wallet.DebitHold, 0)
	for i := 0; i < 10; i++ {
		holds = append(holds, wallet.NewDebitHold(walletID, wallet.NewLedgerAccountSubject("bank"),
			"USD", "", metadata.Metadata{}))
	}

	var testEnv *testEnv
	testEnv = newTestEnv(
		WithListAccounts(func(ctx context.Context, ledger string, query wallet.ListAccountsQuery) (*sdk.AccountsCursorResponseCursor, error) {
			require.Equal(t, defaultLimit, query.Limit)
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.EqualValues(t, metadata.Metadata{
				wallet.MetadataKeyWalletSpecType: wallet.HoldWallet,
			}, query.Metadata)

			hasMore := false
			accounts := make([]sdk.Account, 0)
			for _, wallet := range holds {
				accounts = append(accounts, sdk.Account{
					Address:  testEnv.Chart().GetMainBalanceAccount(wallet.ID),
					Metadata: wallet.LedgerMetadata(testEnv.Chart()),
				})
			}
			return &sdk.AccountsCursorResponseCursor{
				PageSize: defaultLimit,
				HasMore:  hasMore,
				Data:     accounts,
			}, nil
		}),
	)
	req := newRequest(t, http.MethodGet, "/holds", nil)
	rec := httptest.NewRecorder()
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Result().StatusCode)
	cursor := &sharedapi.Cursor[wallet.DebitHold]{}
	readCursor(t, rec, cursor)
}

func TestHoldsListWithPagination(t *testing.T) {
	t.Parallel()

	walletID := uuid.NewString()

	holds := make([]wallet.DebitHold, 0)
	for i := 0; i < 10; i++ {
		holds = append(holds, wallet.NewDebitHold(walletID, wallet.NewLedgerAccountSubject("bank"),
			"USD", "", metadata.Metadata{}))
	}
	const pageSize = 2
	numberOfPages := int64(len(holds) / pageSize)

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
				for _, hold := range holds[page*pageSize : (page+1)*pageSize] {
					accounts = append(accounts, sdk.Account{
						Address:  testEnv.Chart().GetMainBalanceAccount(hold.ID),
						Metadata: hold.LedgerMetadata(testEnv.Chart()),
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
			require.EqualValues(t, metadata.Metadata{
				wallet.MetadataKeyWalletSpecType: wallet.HoldWallet,
				wallet.MetadataKeyHoldWalletID:   walletID,
			}, query.Metadata)

			hasMore := true
			next := "1"
			accounts := make([]sdk.Account, 0)
			for _, wallet := range holds[:pageSize] {
				accounts = append(accounts, sdk.Account{
					Address:  testEnv.Chart().GetMainBalanceAccount(wallet.ID),
					Metadata: wallet.LedgerMetadata(testEnv.Chart()),
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
	req := newRequest(t, http.MethodGet, fmt.Sprintf("/holds?walletID=%s&pageSize=%d", walletID, pageSize), nil)
	rec := httptest.NewRecorder()
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Result().StatusCode)
	cursor := &sharedapi.Cursor[wallet.DebitHold]{}
	readCursor(t, rec, cursor)
	require.Len(t, cursor.Data, pageSize)
	require.EqualValues(t, holds[:pageSize], cursor.Data)

	req = newRequest(t, http.MethodGet, fmt.Sprintf("/holds?walletID=%s&cursor=%s", walletID, cursor.Next), nil)
	rec = httptest.NewRecorder()
	testEnv.Router().ServeHTTP(rec, req)

	cursor = &sharedapi.Cursor[wallet.DebitHold]{}
	readCursor(t, rec, cursor)
	require.Len(t, cursor.Data, pageSize)
	require.EqualValues(t, holds[pageSize:pageSize*2], cursor.Data)
}
