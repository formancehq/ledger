package ledger

import (
	"context"
	"github.com/formancehq/go-libs/v2/query"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/migrations"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestCreateTransaction(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	numscriptRuntime := NewMockNumscriptRuntime(ctrl)
	parser := NewMockNumscriptParser(ctrl)

	l := NewDefaultController(ledger.Ledger{}, store, parser)

	runScript := RunScript{}

	parser.EXPECT().
		Parse(runScript.Plain).
		Return(numscriptRuntime, nil)

	store.EXPECT().
		BeginTX(gomock.Any(), nil).
		Return(store, nil)

	store.EXPECT().
		Commit().
		Return(nil)

	posting := ledger.NewPosting("world", "bank", "USD", big.NewInt(100))
	numscriptRuntime.EXPECT().
		Execute(gomock.Any(), store, runScript.Vars).
		Return(&NumscriptExecutionResult{
			Postings: ledger.Postings{posting},
		}, nil)

	store.EXPECT().
		CommitTransaction(gomock.Any(), gomock.Any()).
		Return(nil)

	store.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.NewLogType
		})).
		DoAndReturn(func(_ context.Context, x any) any {
			return x
		})

	_, _, err := l.CreateTransaction(context.Background(), Parameters[RunScript]{
		Input: runScript,
	})
	require.NoError(t, err)
}

func TestRevertTransaction(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()

	l := NewDefaultController(ledger.Ledger{}, store, parser)

	store.EXPECT().
		BeginTX(gomock.Any(), nil).
		Return(store, nil)

	store.EXPECT().
		Commit().
		Return(nil)

	txToRevert := ledger.Transaction{}
	store.EXPECT().
		RevertTransaction(gomock.Any(), 1, time.Time{}).
		DoAndReturn(func(_ context.Context, _ int, _ time.Time) (*ledger.Transaction, bool, error) {
			txToRevert.RevertedAt = pointer.For(time.Now())
			return &txToRevert, true, nil
		})

	store.EXPECT().
		GetBalances(gomock.Any(), gomock.Any()).
		Return(map[string]map[string]*big.Int{}, nil)

	store.EXPECT().
		CommitTransaction(gomock.Any(), gomock.Any()).
		Return(nil)

	store.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.RevertedTransactionLogType
		})).
		Return(nil)

	_, _, err := l.RevertTransaction(ctx, Parameters[RevertTransaction]{
		Input: RevertTransaction{
			TransactionID: 1,
		},
	})
	require.NoError(t, err)
}

func TestSaveTransactionMetadata(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()

	l := NewDefaultController(ledger.Ledger{}, store, parser)

	store.EXPECT().
		BeginTX(gomock.Any(), nil).
		Return(store, nil)

	store.EXPECT().
		Commit().
		Return(nil)

	m := metadata.Metadata{
		"foo": "bar",
	}
	store.EXPECT().
		UpdateTransactionMetadata(gomock.Any(), 1, m).
		Return(&ledger.Transaction{}, true, nil)

	store.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.SetMetadataLogType
		})).
		Return(nil)

	_, err := l.SaveTransactionMetadata(ctx, Parameters[SaveTransactionMetadata]{
		Input: SaveTransactionMetadata{
			Metadata:      m,
			TransactionID: 1,
		},
	})
	require.NoError(t, err)
}

func TestDeleteTransactionMetadata(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()

	l := NewDefaultController(ledger.Ledger{}, store, parser)

	store.EXPECT().
		BeginTX(gomock.Any(), nil).
		Return(store, nil)

	store.EXPECT().
		Commit().
		Return(nil)

	store.EXPECT().
		DeleteTransactionMetadata(gomock.Any(), 1, "foo").
		Return(&ledger.Transaction{}, true, nil)

	store.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.DeleteMetadataLogType
		})).
		Return(nil)

	_, err := l.DeleteTransactionMetadata(ctx, Parameters[DeleteTransactionMetadata]{
		Input: DeleteTransactionMetadata{
			TransactionID: 1,
			Key:           "foo",
		},
	})
	require.NoError(t, err)
}

func TestListTransactions(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	transactions := NewMockPaginatedResource[ledger.Transaction, any, ColumnPaginatedQuery[any]](ctrl)

	cursor := &bunpaginate.Cursor[ledger.Transaction]{}
	store.EXPECT().Transactions().Return(transactions)
	transactions.EXPECT().
		Paginate(gomock.Any(), ColumnPaginatedQuery[any]{
			PageSize: bunpaginate.QueryDefaultPageSize,
			Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
			Column:   "id",
		}).
		Return(cursor, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser)
	ret, err := l.ListTransactions(ctx, ColumnPaginatedQuery[any]{
		PageSize: bunpaginate.QueryDefaultPageSize,
		Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
		Column:   "id",
	})
	require.NoError(t, err)
	require.Equal(t, cursor, ret)
}

func TestCountAccounts(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	accounts := NewMockPaginatedResource[ledger.Account, any, OffsetPaginatedQuery[any]](ctrl)

	store.EXPECT().Accounts().Return(accounts)
	accounts.EXPECT().Count(gomock.Any(), ResourceQuery[any]{}).Return(1, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser)
	count, err := l.CountAccounts(ctx, ResourceQuery[any]{})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestGetTransaction(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	transactions := NewMockPaginatedResource[ledger.Transaction, any, ColumnPaginatedQuery[any]](ctrl)

	tx := ledger.Transaction{}
	store.EXPECT().Transactions().Return(transactions)
	transactions.EXPECT().GetOne(gomock.Any(), ResourceQuery[any]{
		Builder: query.Match("id", 1),
	}).Return(&tx, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser)
	ret, err := l.GetTransaction(ctx, ResourceQuery[any]{
		Builder: query.Match("id", 1),
	})
	require.NoError(t, err)
	require.Equal(t, tx, *ret)
}

func TestGetAccount(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	accounts := NewMockPaginatedResource[ledger.Account, any, OffsetPaginatedQuery[any]](ctrl)

	account := ledger.Account{}
	store.EXPECT().Accounts().Return(accounts)
	accounts.EXPECT().GetOne(gomock.Any(), ResourceQuery[any]{
		Builder: query.Match("address", "world"),
	}).Return(&account, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser)
	ret, err := l.GetAccount(ctx, ResourceQuery[any]{
		Builder: query.Match("address", "world"),
	})
	require.NoError(t, err)
	require.Equal(t, account, *ret)
}

func TestCountTransactions(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	transactions := NewMockPaginatedResource[ledger.Transaction, any, ColumnPaginatedQuery[any]](ctrl)

	store.EXPECT().Transactions().Return(transactions)
	transactions.EXPECT().Count(gomock.Any(), ResourceQuery[any]{}).Return(1, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser)
	count, err := l.CountTransactions(ctx, ResourceQuery[any]{})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestListAccounts(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	accounts := NewMockPaginatedResource[ledger.Account, any, OffsetPaginatedQuery[any]](ctrl)

	cursor := &bunpaginate.Cursor[ledger.Account]{}
	store.EXPECT().Accounts().Return(accounts)
	accounts.EXPECT().Paginate(gomock.Any(), OffsetPaginatedQuery[any]{
		PageSize: bunpaginate.QueryDefaultPageSize,
		Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
	}).Return(cursor, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser)
	ret, err := l.ListAccounts(ctx, OffsetPaginatedQuery[any]{
		PageSize: bunpaginate.QueryDefaultPageSize,
		Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
	})
	require.NoError(t, err)
	require.Equal(t, cursor, ret)
}

func TestGetAggregatedBalances(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	aggregatedBalances := NewMockResource[ledger.AggregatedVolumes, GetAggregatedVolumesOptions](ctrl)

	store.EXPECT().AggregatedBalances().Return(aggregatedBalances)
	aggregatedBalances.EXPECT().GetOne(gomock.Any(), ResourceQuery[GetAggregatedVolumesOptions]{}).
		Return(&ledger.AggregatedVolumes{}, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser)
	ret, err := l.GetAggregatedBalances(ctx, ResourceQuery[GetAggregatedVolumesOptions]{})
	require.NoError(t, err)
	require.Equal(t, ledger.BalancesByAssets{}, ret)
}

func TestListLogs(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	logs := NewMockPaginatedResource[ledger.Log, any, ColumnPaginatedQuery[any]](ctrl)

	cursor := &bunpaginate.Cursor[ledger.Log]{}
	store.EXPECT().Logs().Return(logs)
	logs.EXPECT().Paginate(gomock.Any(), ColumnPaginatedQuery[any]{
		PageSize: bunpaginate.QueryDefaultPageSize,
		Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
		Column:   "id",
	}).Return(cursor, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser)
	ret, err := l.ListLogs(ctx, ColumnPaginatedQuery[any]{
		PageSize: bunpaginate.QueryDefaultPageSize,
		Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
		Column:   "id",
	})
	require.NoError(t, err)
	require.Equal(t, cursor, ret)
}

func TestGetVolumesWithBalances(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	volumes := NewMockPaginatedResource[ledger.VolumesWithBalanceByAssetByAccount, GetVolumesOptions, OffsetPaginatedQuery[GetVolumesOptions]](ctrl)

	balancesByAssets := &bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{}
	store.EXPECT().Volumes().Return(volumes)
	volumes.EXPECT().Paginate(gomock.Any(), OffsetPaginatedQuery[GetVolumesOptions]{
		PageSize: bunpaginate.QueryDefaultPageSize,
		Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
	}).Return(balancesByAssets, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser)
	ret, err := l.GetVolumesWithBalances(ctx, OffsetPaginatedQuery[GetVolumesOptions]{
		PageSize: bunpaginate.QueryDefaultPageSize,
		Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
	})
	require.NoError(t, err)
	require.Equal(t, balancesByAssets, ret)
}

func TestGetMigrationsInfo(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()

	migrationsInfo := make([]migrations.Info, 0)
	store.EXPECT().
		GetMigrationsInfo(gomock.Any()).
		Return(migrationsInfo, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser)
	ret, err := l.GetMigrationsInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, migrationsInfo, ret)
}

func TestIsDatabaseUpToDate(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()

	store.EXPECT().
		IsUpToDate(gomock.Any()).
		Return(true, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser)
	ret, err := l.IsDatabaseUpToDate(ctx)
	require.NoError(t, err)
	require.True(t, ret)
}
