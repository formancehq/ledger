package ledger

import (
	"testing"

	"github.com/formancehq/go-libs/v2/logging"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestStats(t *testing.T) {

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)

	store.EXPECT().
		CountTransactions(gomock.Any(), NewListTransactionsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{}))).
		Return(10, nil)

	store.EXPECT().
		CountAccounts(gomock.Any(), NewListAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{}))).
		Return(10, nil)

	ledgerController := NewDefaultController(
		ledger.MustNewWithDefault("foo"),
		store,
		parser,
	)
	stats, err := ledgerController.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, stats.Transactions)
	require.Equal(t, 10, stats.Accounts)
}
