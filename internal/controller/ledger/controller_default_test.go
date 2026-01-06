package ledger

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/go-libs/v3/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine/vm"
	"github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

func TestCreateTransactionWithoutSchema(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	numscriptRuntime := NewMockNumscriptRuntime(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)

	runScript := RunScript{}

	parser.EXPECT().
		Parse(runScript.Plain).
		Return(numscriptRuntime, nil)

	store.EXPECT().
		BeginTX(gomock.Any(), nil).
		Return(store, &bun.Tx{}, nil)

	store.EXPECT().
		Commit(gomock.Any()).
		Return(nil)

	posting := ledger.NewPosting("world", "bank", "USD", big.NewInt(100))
	numscriptRuntime.EXPECT().
		Execute(gomock.Any(), store, runScript.Vars).
		Return(&NumscriptExecutionResult{
			Postings: ledger.Postings{posting},
		}, nil)

	store.EXPECT().
		FindLatestSchemaVersion(gomock.Any()).
		Return(nil, nil)

	store.EXPECT().
		CommitTransaction(gomock.Any(), gomock.Any()).
		Return(nil)
	store.EXPECT().UpsertAccounts(gomock.Any(), gomock.Any())

	store.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.NewTransactionLogType
		})).
		DoAndReturn(func(_ context.Context, log *ledger.Log) any {
			log.ID = pointer.For(uint64(0))
			return log
		})

	_, _, _, err := l.CreateTransaction(context.Background(), Parameters[CreateTransaction]{
		Input: CreateTransaction{
			RunScript: runScript,
		},
	})
	require.NoError(t, err)
}

func TestCreateTransactionWithSchema(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	numscriptRuntime := NewMockNumscriptRuntime(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)

	script := `
var {
	account $dest
}
send [EUR/2 100] (
	source = world
	destination = $dest
)`

	schemaVersion := "v1.0"
	schema := ledger.Schema{
		SchemaData: ledger.SchemaData{
			Chart: ledger.ChartOfAccounts{
				"world": {
					Account: &ledger.ChartAccount{
						Metadata: map[string]ledger.ChartAccountMetadata{
							"foo": {
								Default: pointer.For("bar"),
							},
						},
					},
				},
				"bank": {
					Account: &ledger.ChartAccount{},
				},
			},
			Transactions: ledger.TransactionTemplates{
				"TRANSFER": {
					Description: "Test tx template",
					Script:      script,
				},
			},
		},
		Version: schemaVersion,
	}

	parser.EXPECT().
		Parse(script).
		Return(numscriptRuntime, nil)

	store.EXPECT().
		BeginTX(gomock.Any(), nil).
		Return(store, &bun.Tx{}, nil)

	store.EXPECT().
		InsertSchema(gomock.Any(), &schema).
		Return(nil)

	store.EXPECT().
		Commit(gomock.Any()).
		Return(nil)

	store.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.InsertedSchemaLogType
		})).
		DoAndReturn(func(_ context.Context, log *ledger.Log) any {
			log.ID = pointer.For(uint64(0))
			return log
		})

	_, _, _, err := l.InsertSchema(context.Background(), Parameters[InsertSchema]{
		Input: InsertSchema{
			Version: schema.Version,
			Data:    schema.SchemaData,
		},
	})
	require.NoError(t, err)

	runScript := RunScript{
		Script: vm.Script{
			Plain:    "",
			Template: "TRANSFER",
			Vars: map[string]string{
				"dest": "bank",
			},
		},
	}

	parser.EXPECT().
		Parse(script).
		Return(numscriptRuntime, nil)

	store.EXPECT().
		BeginTX(gomock.Any(), nil).
		Return(store, &bun.Tx{}, nil)

	numscriptRuntime.EXPECT().
		Execute(gomock.Any(), store, runScript.Vars).
		Return(&NumscriptExecutionResult{
			Postings: ledger.Postings{
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			},
		}, nil)

	store.EXPECT().
		Commit(gomock.Any()).
		Return(nil)

	store.EXPECT().
		FindSchema(gomock.Any(), "v1.0").
		Return(&schema, nil)

	store.EXPECT().
		CommitTransaction(gomock.Any(), gomock.Any()).
		Return(nil)
	store.EXPECT().UpsertAccounts(gomock.Any(), gomock.Any())

	store.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.NewTransactionLogType
		})).
		DoAndReturn(func(_ context.Context, log *ledger.Log) any {
			log.ID = pointer.For(uint64(0))
			return log
		})

	_, _, _, err = l.CreateTransaction(context.Background(), Parameters[CreateTransaction]{
		SchemaVersion: schemaVersion,
		Input: CreateTransaction{
			RunScript: runScript,
		},
	})
	require.NoError(t, err)
}

func TestRevertTransaction(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)

	parser := NewMockNumscriptParser(ctrl)
	machineParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	interpreterParser := NewMockNumscriptParser(ctrl)
	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)

	store.EXPECT().
		BeginTX(gomock.Any(), nil).
		Return(store, &bun.Tx{}, nil)

	store.EXPECT().
		FindLatestSchemaVersion(gomock.Any()).
		Return(nil, nil)

	store.EXPECT().
		Commit(gomock.Any()).
		Return(nil)

	txToRevert := ledger.Transaction{
		ID: pointer.For(uint64(0)),
	}
	store.EXPECT().
		RevertTransaction(gomock.Any(), uint64(1), time.Time{}).
		DoAndReturn(func(_ context.Context, _ uint64, _ time.Time) (*ledger.Transaction, bool, error) {
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
		DoAndReturn(func(ctx context.Context, v *ledger.Log) error {
			v.ID = pointer.For(uint64(0))

			return nil
		})

	_, _, _, err := l.RevertTransaction(ctx, Parameters[RevertTransaction]{
		Input: RevertTransaction{
			TransactionID: uint64(1),
		},
	})
	require.NoError(t, err)
}

func TestSaveTransactionMetadata(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	ctx := logging.TestingContext()
	parser := NewMockNumscriptParser(ctrl)
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)

	store.EXPECT().
		BeginTX(gomock.Any(), nil).
		Return(store, &bun.Tx{}, nil)

	store.EXPECT().
		FindLatestSchemaVersion(gomock.Any()).
		Return(nil, nil)

	store.EXPECT().
		Commit(gomock.Any()).
		Return(nil)

	m := metadata.Metadata{
		"foo": "bar",
	}
	store.EXPECT().
		UpdateTransactionMetadata(gomock.Any(), uint64(1), m, time.Time{}).
		Return(&ledger.Transaction{}, true, nil)

	store.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.SetMetadataLogType
		})).
		DoAndReturn(func(ctx context.Context, log *ledger.Log) error {
			log.ID = pointer.For(uint64(0))

			return nil
		})

	_, _, err := l.SaveTransactionMetadata(ctx, Parameters[SaveTransactionMetadata]{
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
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)

	store.EXPECT().
		BeginTX(gomock.Any(), nil).
		Return(store, &bun.Tx{}, nil)

	store.EXPECT().
		FindLatestSchemaVersion(gomock.Any()).
		Return(nil, nil)

	store.EXPECT().
		Commit(gomock.Any()).
		Return(nil)

	store.EXPECT().
		DeleteTransactionMetadata(gomock.Any(), uint64(1), "foo", time.Time{}).
		Return(&ledger.Transaction{}, true, nil)

	store.EXPECT().
		InsertLog(gomock.Any(), gomock.Cond(func(x any) bool {
			return x.(*ledger.Log).Type == ledger.DeleteMetadataLogType
		})).
		DoAndReturn(func(ctx context.Context, log *ledger.Log) error {
			log.ID = pointer.For(uint64(0))

			return nil
		})

	_, _, err := l.DeleteTransactionMetadata(ctx, Parameters[DeleteTransactionMetadata]{
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
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()

	transactions := NewMockPaginatedResource[ledger.Transaction, any](ctrl)

	cursor := &bunpaginate.Cursor[ledger.Transaction]{}
	store.EXPECT().Transactions().Return(transactions)
	transactions.EXPECT().
		Paginate(gomock.Any(), common.InitialPaginatedQuery[any]{
			PageSize: bunpaginate.QueryDefaultPageSize,
			Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
			Column:   "id",
		}).
		Return(cursor, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)
	ret, err := l.ListTransactions(ctx, common.InitialPaginatedQuery[any]{
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
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	accounts := NewMockPaginatedResource[ledger.Account, any](ctrl)

	store.EXPECT().Accounts().Return(accounts)
	accounts.EXPECT().Count(gomock.Any(), common.ResourceQuery[any]{}).Return(1, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)
	count, err := l.CountAccounts(ctx, common.ResourceQuery[any]{})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestGetTransaction(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	transactions := NewMockPaginatedResource[ledger.Transaction, any](ctrl)

	tx := ledger.Transaction{}
	store.EXPECT().Transactions().Return(transactions)
	transactions.EXPECT().GetOne(gomock.Any(), common.ResourceQuery[any]{
		Builder: query.Match("id", 1),
	}).Return(&tx, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)
	ret, err := l.GetTransaction(ctx, common.ResourceQuery[any]{
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
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	accounts := NewMockPaginatedResource[ledger.Account, any](ctrl)

	account := ledger.Account{}
	store.EXPECT().Accounts().Return(accounts)
	accounts.EXPECT().GetOne(gomock.Any(), common.ResourceQuery[any]{
		Builder: query.Match("address", "world"),
	}).Return(&account, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)
	ret, err := l.GetAccount(ctx, common.ResourceQuery[any]{
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
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	transactions := NewMockPaginatedResource[ledger.Transaction, any](ctrl)

	store.EXPECT().Transactions().Return(transactions)
	transactions.EXPECT().Count(gomock.Any(), common.ResourceQuery[any]{}).Return(1, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)
	count, err := l.CountTransactions(ctx, common.ResourceQuery[any]{})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestListAccounts(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	accounts := NewMockPaginatedResource[ledger.Account, any](ctrl)

	cursor := &bunpaginate.Cursor[ledger.Account]{}
	store.EXPECT().Accounts().Return(accounts)
	accounts.EXPECT().Paginate(gomock.Any(), common.InitialPaginatedQuery[any]{
		PageSize: bunpaginate.QueryDefaultPageSize,
		Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
	}).Return(cursor, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)
	ret, err := l.ListAccounts(ctx, common.InitialPaginatedQuery[any]{
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
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	aggregatedBalances := NewMockResource[ledger.AggregatedVolumes, ledgerstore.GetAggregatedVolumesOptions](ctrl)

	store.EXPECT().AggregatedBalances().Return(aggregatedBalances)
	aggregatedBalances.EXPECT().GetOne(gomock.Any(), common.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]{}).
		Return(&ledger.AggregatedVolumes{}, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)
	ret, err := l.GetAggregatedBalances(ctx, common.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]{})
	require.NoError(t, err)
	require.Equal(t, ledger.BalancesByAssets{}, ret)
}

func TestListLogs(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	logs := NewMockPaginatedResource[ledger.Log, any](ctrl)

	cursor := &bunpaginate.Cursor[ledger.Log]{}
	store.EXPECT().Logs().Return(logs)
	logs.EXPECT().Paginate(gomock.Any(), common.InitialPaginatedQuery[any]{
		PageSize: bunpaginate.QueryDefaultPageSize,
		Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderDesc)),
		Column:   "id",
	}).Return(cursor, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)
	ret, err := l.ListLogs(ctx, common.InitialPaginatedQuery[any]{
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
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()
	volumes := NewMockPaginatedResource[ledger.VolumesWithBalanceByAssetByAccount, ledgerstore.GetVolumesOptions](ctrl)

	balancesByAssets := &bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{}
	store.EXPECT().Volumes().Return(volumes)
	volumes.EXPECT().Paginate(gomock.Any(), common.InitialPaginatedQuery[ledgerstore.GetVolumesOptions]{
		PageSize: bunpaginate.QueryDefaultPageSize,
		Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
	}).Return(balancesByAssets, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)
	ret, err := l.GetVolumesWithBalances(ctx, common.InitialPaginatedQuery[ledgerstore.GetVolumesOptions]{
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
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()

	migrationsInfo := make([]migrations.Info, 0)
	store.EXPECT().
		GetMigrationsInfo(gomock.Any()).
		Return(migrationsInfo, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)
	ret, err := l.GetMigrationsInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, migrationsInfo, ret)
}

func TestIsDatabaseUpToDate(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	store := NewMockStore(ctrl)
	parser := NewMockNumscriptParser(ctrl)
	machineParser := NewMockNumscriptParser(ctrl)
	interpreterParser := NewMockNumscriptParser(ctrl)
	ctx := logging.TestingContext()

	store.EXPECT().
		IsUpToDate(gomock.Any()).
		Return(true, nil)

	l := NewDefaultController(ledger.Ledger{}, store, parser, machineParser, interpreterParser)
	ret, err := l.IsDatabaseUpToDate(ctx)
	require.NoError(t, err)
	require.True(t, ret)
}
