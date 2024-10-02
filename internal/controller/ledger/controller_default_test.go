package ledger

import (
	"context"
	"database/sql"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/migrations"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestCreateTransaction(t *testing.T) {
	t.Skip("This test is mocking the machine, which is not used anymore")

	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machine := NewMockMachine(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	sqlTX := NewMockTX(ctrl)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)

	runScript := ledger.RunScript{}

	machineFactory.EXPECT().
		Make(runScript.Plain).
		Return(machine, nil)

	store.EXPECT().
		WithTX(gomock.Any(), nil, gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *sql.TxOptions, fn func(tx TX) (bool, error)) error {
			_, err := fn(sqlTX)
			return err
		})

	posting := ledger.NewPosting("world", "bank", "USD", big.NewInt(100))
	machine.EXPECT().
		Execute(gomock.Any(), newVmStoreAdapter(sqlTX), runScript.Vars).
		Return(&MachineResult{
			Postings: ledger.Postings{posting},
		}, nil)

	sqlTX.EXPECT().
		CommitTransaction(gomock.Any(), gomock.Any()).
		Return(nil)

	sqlTX.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.NewTransactionLogType
		})).
		DoAndReturn(func(ctx context.Context, x any) any {
			return x
		})

	listener.EXPECT().
		CommittedTransactions(gomock.Any(), "", gomock.Any(), ledger.AccountMetadata{})

	_, err := l.CreateTransaction(context.Background(), Parameters{}, runScript)
	require.NoError(t, err)
}

func TestRevertTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	sqlTX := NewMockTX(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)

	store.EXPECT().
		WithTX(gomock.Any(), nil, gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *sql.TxOptions, fn func(tx TX) (bool, error)) error {
			_, err := fn(sqlTX)
			return err
		})

	txToRevert := &ledger.Transaction{}
	sqlTX.EXPECT().
		RevertTransaction(gomock.Any(), 1).
		DoAndReturn(func(_ context.Context, _ int) (*ledger.Transaction, bool, error) {
			txToRevert.RevertedAt = pointer.For(time.Now())
			return txToRevert, true, nil
		})

	sqlTX.EXPECT().
		GetBalances(gomock.Any(), gomock.Any()).
		Return(map[string]map[string]*big.Int{}, nil)

	sqlTX.EXPECT().
		CommitTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, newTx *ledger.Transaction) error {
			listener.EXPECT().
				RevertedTransaction(gomock.Any(), "", txToRevert, newTx)
			return nil
		})

	sqlTX.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.RevertedTransactionLogType
		})).
		Return(nil)

	_, err := l.RevertTransaction(ctx, Parameters{}, 1, false, false)
	require.NoError(t, err)
}

func TestSaveTransactionMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	sqlTX := NewMockTX(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)

	store.EXPECT().
		WithTX(gomock.Any(), nil, gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *sql.TxOptions, fn func(tx TX) (bool, error)) error {
			_, err := fn(sqlTX)
			return err
		})

	m := metadata.Metadata{
		"foo": "bar",
	}
	sqlTX.EXPECT().
		UpdateTransactionMetadata(gomock.Any(), 1, m).
		Return(&ledger.Transaction{}, true, nil)

	sqlTX.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.SetMetadataLogType
		})).
		Return(nil)

	listener.EXPECT().
		SavedMetadata(gomock.Any(), "", ledger.MetaTargetTypeTransaction, "1", m)

	err := l.SaveTransactionMetadata(ctx, Parameters{}, 1, m)
	require.NoError(t, err)
}

func TestDeleteTransactionMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	sqlTX := NewMockTX(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)

	store.EXPECT().
		WithTX(gomock.Any(), nil, gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *sql.TxOptions, fn func(tx TX) (bool, error)) error {
			_, err := fn(sqlTX)
			return err
		})

	sqlTX.EXPECT().
		DeleteTransactionMetadata(gomock.Any(), 1, "foo").
		Return(&ledger.Transaction{}, true, nil)

	sqlTX.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.DeleteMetadataLogType
		})).
		Return(nil)

	listener.EXPECT().
		DeletedMetadata(gomock.Any(), "", ledger.MetaTargetTypeTransaction, "1", "foo")

	err := l.DeleteTransactionMetadata(ctx, Parameters{}, 1, "foo")
	require.NoError(t, err)
}

func TestListTransactions(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	cursor := &bunpaginate.Cursor[ledger.Transaction]{}
	query := NewListTransactionsQuery(NewPaginatedQueryOptions[PITFilterWithVolumes](PITFilterWithVolumes{}))
	store.EXPECT().
		ListTransactions(gomock.Any(), query).
		Return(cursor, nil)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)
	ret, err := l.ListTransactions(ctx, query)
	require.NoError(t, err)
	require.Equal(t, cursor, ret)
}

func TestCountAccounts(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	query := NewListAccountsQuery(NewPaginatedQueryOptions[PITFilterWithVolumes](PITFilterWithVolumes{}))
	store.EXPECT().CountAccounts(gomock.Any(), query).Return(1, nil)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)
	count, err := l.CountAccounts(ctx, query)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestGetTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	tx := ledger.Transaction{}
	query := NewGetTransactionQuery(0)
	store.EXPECT().
		GetTransaction(gomock.Any(), query).
		Return(&tx, nil)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)
	ret, err := l.GetTransaction(ctx, query)
	require.NoError(t, err)
	require.Equal(t, tx, *ret)
}

func TestGetAccount(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	account := ledger.Account{}
	query := NewGetAccountQuery("world")
	store.EXPECT().
		GetAccount(gomock.Any(), query).
		Return(&account, nil)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)
	ret, err := l.GetAccount(ctx, query)
	require.NoError(t, err)
	require.Equal(t, account, *ret)
}

func TestCountTransactions(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	query := NewListTransactionsQuery(NewPaginatedQueryOptions[PITFilterWithVolumes](PITFilterWithVolumes{}))
	store.EXPECT().CountTransactions(gomock.Any(), query).Return(1, nil)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)
	count, err := l.CountTransactions(ctx, query)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestListAccounts(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	cursor := &bunpaginate.Cursor[ledger.Account]{}
	query := NewListAccountsQuery(NewPaginatedQueryOptions[PITFilterWithVolumes](PITFilterWithVolumes{}))
	store.EXPECT().
		ListAccounts(gomock.Any(), query).
		Return(cursor, nil)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)
	ret, err := l.ListAccounts(ctx, query)
	require.NoError(t, err)
	require.Equal(t, cursor, ret)
}

func TestGetAggregatedBalances(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	balancesByAssets := ledger.BalancesByAssets{}
	query := NewGetAggregatedBalancesQuery(PITFilter{}, nil, false)
	store.EXPECT().
		GetAggregatedBalances(gomock.Any(), query).
		Return(balancesByAssets, nil)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)
	ret, err := l.GetAggregatedBalances(ctx, query)
	require.NoError(t, err)
	require.Equal(t, balancesByAssets, ret)
}

func TestListLogs(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	cursor := &bunpaginate.Cursor[ledger.Log]{}
	query := NewListLogsQuery(NewPaginatedQueryOptions[any](nil))
	store.EXPECT().
		ListLogs(gomock.Any(), query).
		Return(cursor, nil)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)
	ret, err := l.ListLogs(ctx, query)
	require.NoError(t, err)
	require.Equal(t, cursor, ret)
}

func TestGetVolumesWithBalances(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	balancesByAssets := &bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{}
	query := NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions[FiltersForVolumes](FiltersForVolumes{}))
	store.EXPECT().
		GetVolumesWithBalances(gomock.Any(), query).
		Return(balancesByAssets, nil)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)
	ret, err := l.GetVolumesWithBalances(ctx, query)
	require.NoError(t, err)
	require.Equal(t, balancesByAssets, ret)
}

func TestGetMigrationsInfo(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	migrationsInfo := make([]migrations.Info, 0)
	store.EXPECT().
		GetMigrationsInfo(gomock.Any()).
		Return(migrationsInfo, nil)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)
	ret, err := l.GetMigrationsInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, migrationsInfo, ret)
}

func TestIsDatabaseUpToDate(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	machineFactory := NewMockMachineFactory(ctrl)
	listener := NewMockListener(ctrl)
	ctx := logging.TestingContext()

	store.EXPECT().
		IsUpToDate(gomock.Any()).
		Return(true, nil)

	l := NewDefaultController(ledger.Ledger{}, store, listener, machineFactory)
	ret, err := l.IsDatabaseUpToDate(ctx)
	require.NoError(t, err)
	require.True(t, ret)
}
