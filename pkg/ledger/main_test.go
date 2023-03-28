package ledger

import (
	"context"
	"os"
	"testing"

	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/query"
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

func newResolver(t interface{ pgtesting.TestingT }) *Resolver {
	storageDriver := ledgertesting.StorageDriver(t)
	require.NoError(t, storageDriver.Initialize(context.Background()))

	queryWorker := query.NewWorker(query.DefaultWorkerConfig, storageDriver, query.NewNoOpMonitor())
	go func() {
		require.NoError(t, queryWorker.Run(context.Background()))
	}()

	return NewResolver(storageDriver, lock.NewInMemory(), queryWorker, false)
}

func runOnLedger(t interface {
	pgtesting.TestingT
	Parallel()
}, f func(l *Ledger)) {
	t.Parallel()

	ledgerName := uuid.New()
	resolver := newResolver(t)
	l, err := resolver.GetLedger(context.Background(), ledgerName)
	require.NoError(t, err)
	defer l.Close(context.Background())

	f(l)
}
