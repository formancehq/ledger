//go:build it

package ledger_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestLockLedgerReleaseAfterContextCancellation(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)

	// The lock is typically taken with a request context, which is cancelled
	// as soon as the handler returns, before the release runs.
	ctx, cancel := context.WithCancel(logging.TestingContext())
	_, _, release, err := store.LockLedger(ctx)
	require.NoError(t, err)

	cancel()
	require.NoError(t, release())

	lockCtx, lockCancel := context.WithTimeout(logging.TestingContext(), 5*time.Second)
	defer lockCancel()
	_, _, release, err = store.LockLedger(lockCtx)
	require.NoError(t, err, "the ledger lock must be free after release")
	require.NoError(t, release())
}

func TestLockLedgerReleaseOnBrokenConnection(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	_, conn, release, err := store.LockLedger(ctx)
	require.NoError(t, err)

	var pid int
	require.NoError(t, conn.NewRaw("select pg_backend_pid()").Scan(ctx, &pid))
	_, err = defaultBunDB.GetValue().ExecContext(ctx, "select pg_terminate_backend(?)", pid)
	require.NoError(t, err)

	require.Error(t, release())

	lockCtx, lockCancel := context.WithTimeout(ctx, 5*time.Second)
	defer lockCancel()
	_, _, release, err = store.LockLedger(lockCtx)
	require.NoError(t, err, "the ledger lock must be free after a failed release")
	require.NoError(t, release())
}
