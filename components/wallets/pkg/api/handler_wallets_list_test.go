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

func TestWalletsList(t *testing.T) {
	t.Parallel()

	var wallets []wallet.Wallet
	for i := 0; i < 10; i++ {
		wallets = append(wallets, wallet.NewWallet(uuid.NewString(), "default", metadata.Metadata{}))
	}
	const pageSize = 2
	numberOfPages := int64(len(wallets) / pageSize)

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
				for _, wallet := range wallets[page*pageSize : (page+1)*pageSize] {
					accounts = append(accounts, sdk.Account{
						Address:  testEnv.Chart().GetMainBalanceAccount(wallet.ID),
						Metadata: wallet.LedgerMetadata(),
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
				wallet.MetadataKeyWalletSpecType: wallet.PrimaryWallet,
			}, query.Metadata)

			hasMore := true
			next := "1"
			accounts := make([]sdk.Account, 0)
			for _, wallet := range wallets[:pageSize] {
				accounts = append(accounts, sdk.Account{
					Address:  testEnv.Chart().GetMainBalanceAccount(wallet.ID),
					Metadata: wallet.LedgerMetadata(),
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

	req := newRequest(t, http.MethodGet, fmt.Sprintf("/wallets?pageSize=%d", pageSize), nil)
	rec := httptest.NewRecorder()
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Result().StatusCode)
	cursor := &sharedapi.Cursor[wallet.Wallet]{}
	readCursor(t, rec, cursor)
	require.Len(t, cursor.Data, pageSize)
	require.EqualValues(t, wallets[:pageSize], cursor.Data)

	req = newRequest(t, http.MethodGet, fmt.Sprintf("/wallets?cursor=%s", cursor.Next), nil)
	rec = httptest.NewRecorder()
	testEnv.Router().ServeHTTP(rec, req)
	cursor = &sharedapi.Cursor[wallet.Wallet]{}
	readCursor(t, rec, cursor)
	require.Len(t, cursor.Data, pageSize)
	require.EqualValues(t, cursor.Data, wallets[pageSize:pageSize*2])
}

func TestWalletsListByName(t *testing.T) {
	t.Parallel()

	var wallets []wallet.Wallet
	for i := 0; i < 10; i++ {
		wallets = append(wallets, wallet.NewWallet(uuid.NewString(), "default", metadata.Metadata{}))
	}

	var testEnv *testEnv
	testEnv = newTestEnv(
		WithListAccounts(func(ctx context.Context, ledger string, query wallet.ListAccountsQuery) (*sdk.AccountsCursorResponseCursor, error) {
			require.Equal(t, defaultLimit, query.Limit)
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.Equal(t, map[string]any{
				wallet.MetadataKeyWalletSpecType: wallet.PrimaryWallet,
				wallet.MetadataKeyWalletName:     wallets[1].Name,
			}, query.Metadata)

			hasMore := false
			next := ""
			return &sdk.AccountsCursorResponseCursor{
				PageSize: defaultLimit,
				HasMore:  hasMore,
				Next:     &next,
				Data: []sdk.Account{{
					Address:  testEnv.Chart().GetMainBalanceAccount(wallets[1].ID),
					Metadata: wallets[1].LedgerMetadata(),
				}},
			}, nil
		}),
	)

	req := newRequest(t, http.MethodGet, fmt.Sprintf("/wallets?name=%s", wallets[1].Name), nil)
	rec := httptest.NewRecorder()
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Result().StatusCode)
	cursor := &sharedapi.Cursor[wallet.Wallet]{}
	readCursor(t, rec, cursor)
	require.Len(t, cursor.Data, 1)
	require.EqualValues(t, wallets[1], cursor.Data[0])
}

func TestWalletsListFilterMetadata(t *testing.T) {
	t.Parallel()

	var wallets []wallet.Wallet
	for i := 0; i < 10; i++ {
		wallets = append(wallets, wallet.NewWallet(uuid.NewString(), "default", metadata.Metadata{
			"wallet": float64(i),
		}))
	}

	var testEnv *testEnv
	testEnv = newTestEnv(
		WithListAccounts(func(ctx context.Context, ledger string, query wallet.ListAccountsQuery) (*sdk.AccountsCursorResponseCursor, error) {
			require.Equal(t, defaultLimit, query.Limit)
			require.Equal(t, testEnv.LedgerName(), ledger)
			require.Equal(t, map[string]any{
				wallet.MetadataKeyWalletSpecType:               wallet.PrimaryWallet,
				wallet.MetadataKeyWalletCustomData + ".wallet": "2",
			}, query.Metadata)

			hasMore := false
			next := ""

			return &sdk.AccountsCursorResponseCursor{
				PageSize: defaultLimit,
				HasMore:  hasMore,
				Next:     &next,
				Data: []sdk.Account{{
					Address:  testEnv.Chart().GetMainBalanceAccount(wallets[2].ID),
					Metadata: wallets[2].LedgerMetadata(),
				}},
			}, nil
		}),
	)

	req := newRequest(t, http.MethodGet, "/wallets?metadata[wallet]=2", nil)
	rec := httptest.NewRecorder()
	testEnv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Result().StatusCode)
	cursor := &sharedapi.Cursor[wallet.Wallet]{}
	readCursor(t, rec, cursor)
	require.Len(t, cursor.Data, 1)
	require.EqualValues(t, wallets[2], cursor.Data[0])
}
