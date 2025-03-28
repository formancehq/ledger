package ledger

import (
	"github.com/formancehq/ledger/internal/storage/resources"
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
	transactions := NewMockPaginatedResource[ledger.Transaction, any, resources.ColumnPaginatedQuery[any]](ctrl)
	accounts := NewMockPaginatedResource[ledger.Account, any, resources.OffsetPaginatedQuery[any]](ctrl)

	store.EXPECT().Transactions().Return(transactions)
	transactions.EXPECT().Count(ctx, resources.ResourceQuery[any]{}).Return(10, nil)
	store.EXPECT().Accounts().Return(accounts)
	accounts.EXPECT().Count(ctx, resources.ResourceQuery[any]{}).Return(10, nil)

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
