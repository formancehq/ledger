package ledger

import (
	"context"
	"os"
	"testing"

	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/numscript"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestMain(t *testing.M) {
	if err := pgtesting.CreatePostgresServer(); err != nil {
		logging.Error(err)
		os.Exit(1)
	}
	code := t.Run()
	if err := pgtesting.DestroyPostgresServer(); err != nil {
		logging.Error(err)
	}
	os.Exit(code)
}

func runOnLedger(t interface {
	pgtesting.TestingT
	Parallel()
}, f func(l *Ledger)) {

	t.Parallel()

	storageDriver := ledgertesting.StorageDriver(t)
	require.NoError(t, storageDriver.Initialize(context.Background()))

	name := uuid.New()
	store, _, err := storageDriver.GetLedgerStore(context.Background(), name, true)
	require.NoError(t, err)

	_, err = store.Initialize(context.Background())
	require.NoError(t, err)

	cacheManager := cache.NewManager(storageDriver)
	ledgerCache, err := cacheManager.ForLedger(context.Background(), name)
	require.NoError(t, err)

	compiler := numscript.NewCompiler()

	runner, err := runner.New(store, lock.NewInMemory(), ledgerCache, compiler, false)
	require.NoError(t, err)

	queryWorker := query.NewWorker(query.DefaultWorkerConfig, storageDriver, query.NewNoOpMonitor())
	go func() {
		require.NoError(t, queryWorker.Run(context.Background()))
	}()

	l := New(store, ledgerCache, runner, lock.NewInMemory(), queryWorker)
	defer l.Close(context.Background())

	f(l)
}
